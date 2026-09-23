package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.ColumnReference;
import io.queryaudit.core.parser.EnhancedSqlParser;
import io.queryaudit.core.parser.JoinColumnPair;
import io.queryaudit.core.parser.SqlParser;
import io.queryaudit.core.parser.SqlTableReferences;
import io.queryaudit.core.parser.WhereColumnReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Detects missing indexes on WHERE, JOIN, ORDER BY, and GROUP BY columns by comparing query columns
 * against available index metadata.
 *
 * @author haroya
 * @since 0.2.0
 */
public class MissingIndexDetector implements DetectionRule {

  private static final Set<String> LOW_CARDINALITY_EXACT_NAMES =
      Set.of(
          "type",
          "status",
          "role",
          "gender",
          "category",
          "level",
          "kind",
          "state",
          "enabled",
          "active",
          "visible",
          "locked",
          "verified",
          "approved",
          "published",
          "archived");

  private static final Pattern LOW_CARDINALITY_PREFIX_PATTERN =
      Pattern.compile("^(is_|has_|flag).*$", Pattern.CASE_INSENSITIVE);

  private static final Pattern LOW_CARDINALITY_SUFFIX_PATTERN =
      Pattern.compile("^.+(_type|_status)$", Pattern.CASE_INSENSITIVE);

  private static final Set<String> SOFT_DELETE_COLUMN_NAMES =
      Set.of(
          "deleted_at",
          "deleted",
          "is_deleted",
          "removed_at",
          "deactivated_at",
          "discarded_at",
          "unsuspended_at");

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();
    if (indexMetadata == null || indexMetadata.isEmpty()) {
      MetadataSkipLog.warnEmptyMetadataOnce("MissingIndexDetector");
      return issues;
    }

    Set<String> seen = new LinkedHashSet<>();
    for (QueryRecord query : queries) {
      if (query.normalizedSql() == null || !seen.add(query.normalizedSql())) continue;
      if (!SqlParser.isSelectQuery(query.sql())) continue;

      QueryFacts facts = queryFacts(query, indexMetadata);
      WhereAnalysis where = checkWhereColumns(facts);
      issues.addAll(where.issues());
      checkJoinColumns(facts, issues);
      checkOrderByColumns(facts, where, issues);
      checkGroupByColumns(facts, issues);
    }
    return issues;
  }

  private QueryFacts queryFacts(QueryRecord query, IndexMetadata metadata) {
    Map<String, String> aliases = resolveAliases(query.sql());
    List<WhereColumnReference> where =
        EnhancedSqlParser.extractWhereColumnsWithOperators(query.sql());
    Map<String, TableFilterFacts> filters = tableFilters(where, aliases, metadata);
    boolean hasIndexedFilter =
        filters.values().stream().anyMatch(TableFilterFacts::hasIndexedColumn);
    Set<String> singleRowTables =
        hasIndexedFilter ? UniqueIndexEquality.singleRowTables(query.sql(), metadata) : Set.of();
    return new QueryFacts(
        query,
        metadata,
        Collections.unmodifiableMap(new LinkedHashMap<>(aliases)),
        List.copyOf(where),
        filters,
        singleRowTables);
  }

  private static Map<String, TableFilterFacts> tableFilters(
      List<WhereColumnReference> columns, Map<String, String> aliases, IndexMetadata metadata) {
    Map<String, Integer> counts = new HashMap<>();
    Map<String, List<String>> indexed = new HashMap<>();
    for (WhereColumnReference column : columns) {
      String table = resolveTable(column.tableOrAlias(), aliases);
      if (table == null) continue;
      counts.merge(table, 1, Integer::sum);
      if (metadata.hasTable(table) && metadata.hasIndexOn(table, column.columnName())) {
        indexed.computeIfAbsent(table, ignored -> new ArrayList<>()).add(column.columnName());
      }
    }
    Map<String, TableFilterFacts> result = new HashMap<>();
    counts.forEach(
        (table, count) ->
            result.put(table, new TableFilterFacts(count, indexed.getOrDefault(table, List.of()))));
    return Map.copyOf(result);
  }

  private WhereAnalysis checkWhereColumns(QueryFacts facts) {
    List<Issue> issues = new ArrayList<>();
    Map<String, List<String>> suggestedColumns = new HashMap<>();
    for (WhereColumnReference column : facts.whereColumns()) {
      String table = facts.table(column.tableOrAlias());
      if (!facts.missingIndex(table, column.columnName()) || skipWhereIndex(facts, table, column))
        continue;

      TableFilterFacts filters = facts.filters(table);
      if (isSoftDeleteColumn(column.columnName()) && isSoftDeleteOperator(column.operator())) {
        if (filters.columnCount() <= 1) {
          issues.add(
              facts.issue(
                  IssueType.MISSING_WHERE_INDEX,
                  Severity.INFO,
                  table,
                  column.columnName(),
                  "Soft-delete column '"
                      + column.columnName()
                      + "' with IS NULL/= false. When 99%+ rows match this condition, a"
                      + " standalone index provides no selectivity.",
                  "Consider a partial index or composite index with a more selective leading"
                      + " column."));
        }
        continue;
      }
      if (isLowCardinalityColumn(column.columnName(), facts.metadata(), table)) {
        if (!filters.hasIndexedColumn()) {
          issues.add(
              facts.issue(
                  IssueType.MISSING_WHERE_INDEX,
                  Severity.INFO,
                  table,
                  column.columnName(),
                  "Low cardinality column '"
                      + column.columnName()
                      + "'. "
                      + "A B-tree index on a low-cardinality column has poor selectivity; "
                      + "MySQL may prefer a full table scan over an index scan.",
                  "Consider a composite index with a more selective leading column."));
        }
        continue;
      }

      suggestedColumns
          .computeIfAbsent(table, ignored -> new ArrayList<>())
          .add(column.columnName());
      issues.add(
          facts.issue(
              IssueType.MISSING_WHERE_INDEX,
              filters.hasIndexedColumn() ? Severity.WARNING : Severity.ERROR,
              table,
              column.columnName(),
              "SHOW INDEX FROM "
                  + table
                  + " \u2192 "
                  + column.columnName()
                  + " column has no index. Without an index, MySQL performs a full table scan"
                  + " for every query filtering on this column.",
              buildWhereSuggestion(table, column.columnName(), filters.indexedColumns())));
    }
    return new WhereAnalysis(issues, suggestedColumns);
  }

  private boolean skipWhereIndex(QueryFacts facts, String table, WhereColumnReference column) {
    // Composite-index ordering and leading-wildcard LIKE have dedicated detectors.
    if (isInAnyCompositeIndex(facts.metadata(), table, column.columnName())
        || facts.singleRowTables().contains(table)) return true;
    if (isLikeOperator(column.operator())
        && isLeadingWildcardLike(facts.query().sql(), column.columnName())) return true;
    // Soft-delete NULL checks retain their separate, sole-filter advisory policy.
    return isNullCheckOperator(column.operator()) && !isSoftDeleteColumn(column.columnName());
  }

  private void checkJoinColumns(QueryFacts facts, List<Issue> issues) {
    for (JoinColumnPair pair : EnhancedSqlParser.extractJoinColumns(facts.query().sql())) {
      checkJoinColumn(pair.left(), facts, issues);
      checkJoinColumn(pair.right(), facts, issues);
    }
  }

  private void checkJoinColumn(ColumnReference column, QueryFacts facts, List<Issue> issues) {
    String table = facts.table(column.tableOrAlias());
    if (!facts.missingIndex(table, column.columnName())) return;
    issues.add(
        facts.issue(
            IssueType.MISSING_JOIN_INDEX,
            Severity.ERROR,
            table,
            column.columnName(),
            "SHOW INDEX FROM "
                + table
                + " \u2192 "
                + column.columnName()
                + " column has no index. Every JOIN without an index causes a full scan of the"
                + " joined table.",
            "Run: ALTER TABLE "
                + table
                + " ADD INDEX idx_"
                + column.columnName()
                + " ("
                + column.columnName()
                + ");\n"
                + "         This is typically a foreign key \u2014 consider adding a FK constraint"
                + " too."));
  }

  private void checkOrderByColumns(QueryFacts facts, WhereAnalysis where, List<Issue> issues) {
    for (ColumnReference column : EnhancedSqlParser.extractOrderByColumns(facts.query().sql())) {
      String table = facts.table(column.tableOrAlias());
      if (!facts.missingIndex(table, column.columnName())
          || facts.singleRowTables().contains(table)) continue;

      TableFilterFacts filters = facts.filters(table);
      issues.add(
          facts.issue(
              IssueType.MISSING_ORDER_BY_INDEX,
              filters.hasIndexedColumn() ? Severity.INFO : Severity.WARNING,
              table,
              column.columnName(),
              "SHOW INDEX FROM "
                  + table
                  + " \u2192 "
                  + column.columnName()
                  + " column has no index. MySQL uses filesort when ORDER BY column has no index.",
              buildOrderBySuggestion(
                  table,
                  column.columnName(),
                  filters.indexedColumns(),
                  where.suggestedColumns(table))));
    }
  }

  private void checkGroupByColumns(QueryFacts facts, List<Issue> issues) {
    List<ColumnReference> groupByColumns =
        EnhancedSqlParser.extractGroupByColumns(facts.query().sql());
    Map<String, Set<String>> groupedColumns = new HashMap<>();
    for (ColumnReference column : groupByColumns) {
      String table = facts.table(column.tableOrAlias());
      if (table != null) {
        groupedColumns
            .computeIfAbsent(table, ignored -> new HashSet<>())
            .add(column.columnName().toLowerCase());
      }
    }

    // Preserve GROUP BY's existing normalized-SQL filter policy. The shared collector makes this
    // source distinction explicit instead of implementing a second index-lookup algorithm.
    Map<String, TableFilterFacts> groupingFilters =
        tableFilters(
            EnhancedSqlParser.extractWhereColumnsWithOperators(facts.query().normalizedSql()),
            facts.aliases(),
            facts.metadata());
    for (ColumnReference column : groupByColumns) {
      String table = facts.table(column.tableOrAlias());
      if (!facts.missingIndex(table, column.columnName())) continue;
      if (allPrimaryKeyColumnsPresent(
              facts.metadata(), table, groupedColumns.getOrDefault(table, Set.of()))
          || groupingFilters.getOrDefault(table, TableFilterFacts.EMPTY).hasIndexedColumn())
        continue;

      issues.add(
          facts.issue(
              IssueType.MISSING_GROUP_BY_INDEX,
              Severity.WARNING,
              table,
              column.columnName(),
              "SHOW INDEX FROM "
                  + table
                  + " \u2192 "
                  + column.columnName()
                  + " column has no index. MySQL creates a temporary table for GROUP BY without"
                  + " index.",
              "Run: ALTER TABLE "
                  + table
                  + " ADD INDEX idx_"
                  + column.columnName()
                  + " ("
                  + column.columnName()
                  + ");"));
    }
  }

  /** Resolved filter facts are shared by clause policies; detector decisions do not mutate them. */
  private record TableFilterFacts(int columnCount, List<String> indexedColumns) {
    private static final TableFilterFacts EMPTY = new TableFilterFacts(0, List.of());

    TableFilterFacts {
      indexedColumns = List.copyOf(indexedColumns);
    }

    boolean hasIndexedColumn() {
      return !indexedColumns.isEmpty();
    }
  }

  /**
   * Only ordinary missing-WHERE decisions contribute columns to later composite ORDER BY advice.
   */
  private record WhereAnalysis(List<Issue> issues, Map<String, List<String>> suggestedColumns) {
    WhereAnalysis {
      issues = List.copyOf(issues);
      Map<String, List<String>> snapshot = new HashMap<>();
      suggestedColumns.forEach((table, columns) -> snapshot.put(table, List.copyOf(columns)));
      suggestedColumns = Map.copyOf(snapshot);
    }

    List<String> suggestedColumns(String table) {
      return suggestedColumns.getOrDefault(table, List.of());
    }
  }

  private record QueryFacts(
      QueryRecord query,
      IndexMetadata metadata,
      Map<String, String> aliases,
      List<WhereColumnReference> whereColumns,
      Map<String, TableFilterFacts> filters,
      Set<String> singleRowTables) {
    String table(String tableOrAlias) {
      return resolveTable(tableOrAlias, aliases);
    }

    TableFilterFacts filters(String table) {
      return filters.getOrDefault(table, TableFilterFacts.EMPTY);
    }

    boolean missingIndex(String table, String column) {
      return table != null && metadata.hasTable(table) && !metadata.hasIndexOn(table, column);
    }

    Issue issue(
        IssueType type,
        Severity severity,
        String table,
        String column,
        String detail,
        String suggestion) {
      return new Issue(
          type,
          severity,
          query.normalizedSql(),
          table,
          column,
          detail,
          suggestion,
          query.stackTrace());
    }
  }

  /**
   * Determine if a column is likely low cardinality based on naming patterns and/or index metadata
   * cardinality.
   */
  private boolean isLowCardinalityColumn(String columnName, IndexMetadata metadata, String table) {
    String lower = columnName.toLowerCase();

    if (LOW_CARDINALITY_EXACT_NAMES.contains(lower)) {
      return true;
    }

    if (LOW_CARDINALITY_PREFIX_PATTERN.matcher(lower).matches()) {
      return true;
    }

    if (LOW_CARDINALITY_SUFFIX_PATTERN.matcher(lower).matches()) {
      return true;
    }

    // Check index metadata cardinality: if the column exists in any index
    // with cardinality <= 10, treat as low cardinality
    List<IndexInfo> indexes = metadata.getIndexesForTable(table);
    for (IndexInfo idx : indexes) {
      if (idx.columnName() != null
          && idx.columnName().equalsIgnoreCase(columnName)
          && idx.cardinality() > 0
          && idx.cardinality() <= 10) {
        return true;
      }
    }

    return false;
  }

  /** Check if a column name matches common soft-delete patterns. */
  private boolean isSoftDeleteColumn(String columnName) {
    return SOFT_DELETE_COLUMN_NAMES.contains(columnName.toLowerCase());
  }

  /** Check if the operator is IS (covers IS NULL) or = (covers = false). */
  private boolean isSoftDeleteOperator(String operator) {
    if (operator == null) return false;
    String op = operator.trim().toUpperCase();
    return "IS".equals(op) || "=".equals(op);
  }

  /**
   * Check if the operator is LIKE or NOT LIKE. These columns are handled by LikeWildcardDetector
   * and should not get a generic B-tree index suggestion.
   */
  private boolean isLikeOperator(String operator) {
    if (operator == null) return false;
    String op = operator.trim().toUpperCase();
    return "LIKE".equals(op) || "NOT LIKE".equals(op) || "ILIKE".equals(op);
  }

  /** True if the raw SQL contains {@code <column> LIKE '%...'} for the given column. */
  private static boolean isLeadingWildcardLike(String sql, String column) {
    if (sql == null || column == null) return false;
    Pattern p =
        Pattern.compile(
            "(?i)(?:\\w+\\.)?\\b" + Pattern.quote(column) + "\\b\\s+(?:NOT\\s+)?I?LIKE\\s+'%");
    return p.matcher(sql).find();
  }

  /**
   * Check if the operator is IS (covers IS NULL and IS NOT NULL). NULL checks have poor selectivity
   * and rarely benefit from B-tree indexes, so we skip the missing index warning for these
   * conditions.
   */
  private boolean isNullCheckOperator(String operator) {
    if (operator == null) return false;
    String op = operator.trim().toUpperCase();
    return "IS".equals(op);
  }

  /**
   * Build a suggestion for a missing WHERE index. If there is already an indexed column in WHERE on
   * the same table, suggest extending it into a composite index.
   */
  private String buildWhereSuggestion(
      String table, String unindexedCol, List<String> indexedWhereCols) {
    if (!indexedWhereCols.isEmpty()) {
      String leadingCol = indexedWhereCols.get(0);
      String indexName = "idx_" + leadingCol + "_" + unindexedCol;
      return "Run: ALTER TABLE "
          + table
          + " ADD INDEX "
          + indexName
          + " ("
          + leadingCol
          + ", "
          + unindexedCol
          + ");\n"
          + "         Extending the existing index on '"
          + leadingCol
          + "' into a composite index avoids a separate index.";
    }
    return "Run: ALTER TABLE "
        + table
        + " ADD INDEX idx_"
        + unindexedCol
        + " ("
        + unindexedCol
        + ");\n"
        + "         If this column is often queried with other columns, consider a composite"
        + " index.";
  }

  /**
   * Build a suggestion for a missing ORDER BY index. Prefer composite (where_col, order_col) when
   * WHERE columns exist.
   */
  private String buildOrderBySuggestion(
      String table,
      String orderCol,
      List<String> indexedWhereCols,
      List<String> unindexedWhereCols) {
    // If there's an indexed WHERE column, suggest composite (where_col, order_col)
    if (!indexedWhereCols.isEmpty()) {
      String whereCol = indexedWhereCols.get(0);
      String indexName = "idx_" + whereCol + "_" + orderCol;
      return "Run: ALTER TABLE "
          + table
          + " ADD INDEX "
          + indexName
          + " ("
          + whereCol
          + ", "
          + orderCol
          + ");\n"
          + "         A composite index (where_col, order_col) eliminates both the scan and the"
          + " filesort.";
    }
    // If there's an unindexed WHERE column, still suggest composite
    if (!unindexedWhereCols.isEmpty()) {
      String whereCol = unindexedWhereCols.get(0);
      String indexName = "idx_" + whereCol + "_" + orderCol;
      return "Run: ALTER TABLE "
          + table
          + " ADD INDEX "
          + indexName
          + " ("
          + whereCol
          + ", "
          + orderCol
          + ");\n"
          + "         A composite index (where_col, order_col) eliminates both the scan and the"
          + " filesort.";
    }
    return "Run: ALTER TABLE "
        + table
        + " ADD INDEX idx_"
        + orderCol
        + " ("
        + orderCol
        + ");\n"
        + "         Tip: If used with WHERE, create a composite index (where_col, order_col) for"
        + " best performance.";
  }

  /** Check if a column appears in any composite index for the given table. */
  private boolean isInAnyCompositeIndex(IndexMetadata metadata, String table, String column) {
    Map<String, List<IndexInfo>> composites = metadata.getCompositeIndexes(table);
    for (List<IndexInfo> indexCols : composites.values()) {
      for (IndexInfo info : indexCols) {
        if (info.columnName() != null && info.columnName().equalsIgnoreCase(column)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Check if all primary key columns of a table are present in the given column set. Returns false
   * if the table has no primary key.
   */
  private boolean allPrimaryKeyColumnsPresent(
      IndexMetadata metadata, String table, Set<String> columns) {
    List<IndexInfo> indexes = metadata.getIndexesForTable(table);
    List<IndexInfo> pkColumns =
        indexes.stream().filter(idx -> "PRIMARY".equalsIgnoreCase(idx.indexName())).toList();

    if (pkColumns.isEmpty()) {
      return false;
    }

    return pkColumns.stream()
        .allMatch(pk -> pk.columnName() != null && columns.contains(pk.columnName().toLowerCase()));
  }

  static Map<String, String> resolveAliases(String sql) {
    return SqlTableReferences.resolveAliases(sql);
  }

  /**
   * Resolve an alias/table reference to the actual table name. If tableOrAlias is null, try to
   * infer from the first table in the alias map.
   */
  private static String resolveTable(String tableOrAlias, Map<String, String> aliasToTable) {
    if (tableOrAlias != null) {
      String resolved = aliasToTable.get(tableOrAlias.toLowerCase());
      if (resolved != null) return resolved;
      // Don't use unresolved Hibernate aliases (e.g., m1_0, r1_0, us1_0) as table names
      if (tableOrAlias.matches("(?i)[a-z]{1,3}\\d+_\\d+")) return null;
      return tableOrAlias.toLowerCase();
    }
    // If no table qualifier, use the first (main) table if there is exactly one
    if (aliasToTable.size() <= 2) {
      // Could be 1 table with its own name + alias = 2 entries, or 1 entry
      return aliasToTable.values().stream().findFirst().orElse(null);
    }
    // Ambiguous without qualifier - skip
    return null;
  }
}
