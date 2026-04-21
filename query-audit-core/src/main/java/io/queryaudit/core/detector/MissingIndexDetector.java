package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.ColumnReference;
import io.queryaudit.core.parser.JoinColumnPair;
import io.queryaudit.core.parser.SqlParser;
import io.queryaudit.core.parser.EnhancedSqlParser;
import io.queryaudit.core.parser.WhereColumnReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects missing indexes on WHERE, JOIN, ORDER BY, and GROUP BY columns by comparing query
 * columns against available index metadata.
 *
 * @author haroya
 * @since 0.2.0
 */
public class MissingIndexDetector implements DetectionRule {

  private static final Pattern FROM_ALIAS =
      Pattern.compile("\\bFROM\\s+(\\w+)(?:\\s+(?:AS\\s+)?(\\w+))?", Pattern.CASE_INSENSITIVE);

  private static final Pattern JOIN_ALIAS =
      Pattern.compile("\\bJOIN\\s+(\\w+)(?:\\s+(?:AS\\s+)?(\\w+))?", Pattern.CASE_INSENSITIVE);

  // ── Improvement 1: Low cardinality column name patterns ──────────
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

  // ── Improvement 2: Soft-delete column patterns ───────────────────
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
      return issues;
    }

    Set<String> seen = new LinkedHashSet<>();

    for (QueryRecord query : queries) {
      String normalized = query.normalizedSql();
      if (normalized == null || seen.contains(normalized)) {
        continue;
      }
      seen.add(normalized);

      String sql = query.sql();
      if (!SqlParser.isSelectQuery(sql)) {
        continue;
      }

      Map<String, String> aliasToTable = resolveAliases(sql);

      String stackTrace = query.stackTrace();

      // (a) WHERE columns (with operator info for soft-delete / low-cardinality detection)
      List<WhereColumnReference> whereColumnsWithOp =
          EnhancedSqlParser.extractWhereColumnsWithOperators(sql);
      List<ColumnReference> whereColumns =
          whereColumnsWithOp.stream().map(WhereColumnReference::toColumnReference).toList();

      // First pass: determine which WHERE columns already have indexes per table,
      // and whether any of them have a unique/primary key index with equality.
      Map<String, Boolean> tableHasUniqueIndexedWhereCol = new HashMap<>();
      Map<String, Boolean> tableHasAnyIndexedWhereCol = new HashMap<>();

      // Track indexed column names per table for composite suggestion (Improvement 4)
      Map<String, List<String>> indexedWhereColsByTable = new HashMap<>();
      // Track unindexed column names per table for composite suggestion
      Map<String, List<String>> unindexedWhereColsByTable = new HashMap<>();

      // Track total WHERE column count per table (for soft-delete sole-column check)
      Map<String, Integer> whereColumnCountByTable = new HashMap<>();

      for (int i = 0; i < whereColumns.size(); i++) {
        ColumnReference col = whereColumns.get(i);
        WhereColumnReference colWithOp = whereColumnsWithOp.get(i);
        String table = resolveTable(col.tableOrAlias(), aliasToTable);
        if (table != null) {
          whereColumnCountByTable.merge(table, 1, Integer::sum);
        }
        if (table != null
            && indexMetadata.hasTable(table)
            && indexMetadata.hasIndexOn(table, col.columnName())) {
          tableHasAnyIndexedWhereCol.put(table, true);
          indexedWhereColsByTable
              .computeIfAbsent(table, k -> new ArrayList<>())
              .add(col.columnName());
          if (hasUniqueOrPrimaryIndex(indexMetadata, table, col.columnName())
              && colWithOp.isEquality()) {
            tableHasUniqueIndexedWhereCol.put(table, true);
          }
        }
      }

      for (int i = 0; i < whereColumns.size(); i++) {
        ColumnReference col = whereColumns.get(i);
        WhereColumnReference colWithOp = whereColumnsWithOp.get(i);
        String table = resolveTable(col.tableOrAlias(), aliasToTable);
        if (table != null
            && indexMetadata.hasTable(table)
            && !indexMetadata.hasIndexOn(table, col.columnName())) {
          // Skip if the column is part of a composite index --
          // let CompositeIndexDetector handle it
          if (isInAnyCompositeIndex(indexMetadata, table, col.columnName())) {
            continue;
          }

          // If another WHERE column on the same table has a unique/PK index,
          // the result set is already tiny -- skip this column entirely.
          if (tableHasUniqueIndexedWhereCol.getOrDefault(table, false)) {
            continue;
          }

          // Improvement 3: Skip LIKE operator columns — LikeWildcardDetector handles
          // leading-wildcard LIKE, and for non-leading-wildcard LIKE we cannot determine
          // from normalized SQL whether a B-tree index would help. Avoid misleading advice.
          if (isLikeOperator(colWithOp.operator())) {
            continue;
          }

          // Skip IS NULL / IS NOT NULL checks for non-soft-delete columns:
          // NULL checks have poor selectivity and rarely benefit from B-tree indexes.
          // Soft-delete columns (e.g., deleted_at IS NULL) have their own handling below.
          if (isNullCheckOperator(colWithOp.operator()) && !isSoftDeleteColumn(col.columnName())) {
            continue;
          }

          boolean hasOtherIndexedCol = tableHasAnyIndexedWhereCol.getOrDefault(table, false);

          // ── Improvement 2: Soft-delete pattern ──────────────
          if (isSoftDeleteColumn(col.columnName()) && isSoftDeleteOperator(colWithOp.operator())) {
            int totalWhereColsOnTable = whereColumnCountByTable.getOrDefault(table, 0);
            if (totalWhereColsOnTable > 1) {
              // Other filter columns exist on the same table; the soft-delete
              // condition adds no selectivity value -- suppress entirely
              continue;
            }
            // Literally the ONLY filter column in WHERE -- downgrade to INFO
            issues.add(
                new Issue(
                    IssueType.MISSING_WHERE_INDEX,
                    Severity.INFO,
                    normalized,
                    table,
                    col.columnName(),
                    "Soft-delete column '"
                        + col.columnName()
                        + "' with IS NULL/= false. "
                        + "When 99%+ rows match this condition, a standalone index provides no selectivity.",
                    "Consider a partial index or composite index with a more selective leading column.",
                    stackTrace));
            continue;
          }

          // ── Improvement 1: Low cardinality column ───────────
          if (isLowCardinalityColumn(col.columnName(), indexMetadata, table)) {
            if (hasOtherIndexedCol) {
              // Another indexed column already narrows the scan; skip entirely
              continue;
            }
            // Sole column in WHERE -- downgrade to INFO
            issues.add(
                new Issue(
                    IssueType.MISSING_WHERE_INDEX,
                    Severity.INFO,
                    normalized,
                    table,
                    col.columnName(),
                    "Low cardinality column '"
                        + col.columnName()
                        + "'. "
                        + "A B-tree index on a low-cardinality column has poor selectivity; "
                        + "MySQL may prefer a full table scan over an index scan.",
                    "Consider a composite index with a more selective leading column.",
                    stackTrace));
            continue;
          }

          // Track unindexed columns for composite suggestion
          unindexedWhereColsByTable
              .computeIfAbsent(table, k -> new ArrayList<>())
              .add(col.columnName());

          // If another WHERE column on the same table has a regular index,
          // downgrade severity from ERROR to WARNING.
          Severity severity = hasOtherIndexedCol ? Severity.WARNING : Severity.ERROR;

          // ── Improvement 4: Improved suggestion text ──────────
          String suggestion =
              buildWhereSuggestion(
                  table, col.columnName(), indexedWhereColsByTable.getOrDefault(table, List.of()));

          issues.add(
              new Issue(
                  IssueType.MISSING_WHERE_INDEX,
                  severity,
                  normalized,
                  table,
                  col.columnName(),
                  "SHOW INDEX FROM "
                      + table
                      + " \u2192 "
                      + col.columnName()
                      + " column has no index. Without an index, MySQL performs a full table scan for every query filtering on this column.",
                  suggestion,
                  stackTrace));
        }
      }

      // (b) JOIN columns — use enhanced parser for better accuracy on complex SQL
      List<JoinColumnPair> joinPairs = EnhancedSqlParser.extractJoinColumns(sql);
      for (JoinColumnPair pair : joinPairs) {
        checkJoinColumn(pair.left(), aliasToTable, indexMetadata, normalized, stackTrace, issues);
        checkJoinColumn(pair.right(), aliasToTable, indexMetadata, normalized, stackTrace, issues);
      }

      // (c) ORDER BY columns
      List<ColumnReference> orderByColumns = EnhancedSqlParser.extractOrderByColumns(sql);
      for (ColumnReference col : orderByColumns) {
        String table = resolveTable(col.tableOrAlias(), aliasToTable);
        if (table != null
            && indexMetadata.hasTable(table)
            && !indexMetadata.hasIndexOn(table, col.columnName())) {

          // ── Improvement 3: Skip ORDER BY when result set is already tiny ──
          if (tableHasUniqueIndexedWhereCol.getOrDefault(table, false)) {
            // PK/unique equality in WHERE => at most 1 row, filesort is free
            continue;
          }

          // If any WHERE column has a regular index, downgrade ORDER BY to INFO
          Severity orderBySeverity =
              tableHasAnyIndexedWhereCol.getOrDefault(table, false)
                  ? Severity.INFO
                  : Severity.WARNING;

          // ── Improvement 4: Suggest composite (where_col, order_col) ──
          String orderBySuggestion =
              buildOrderBySuggestion(
                  table,
                  col.columnName(),
                  indexedWhereColsByTable.getOrDefault(table, List.of()),
                  unindexedWhereColsByTable.getOrDefault(table, List.of()));

          issues.add(
              new Issue(
                  IssueType.MISSING_ORDER_BY_INDEX,
                  orderBySeverity,
                  normalized,
                  table,
                  col.columnName(),
                  "SHOW INDEX FROM "
                      + table
                      + " \u2192 "
                      + col.columnName()
                      + " column has no index. MySQL uses filesort when ORDER BY column has no index.",
                  orderBySuggestion,
                  stackTrace));
        }
      }

      // (d) GROUP BY columns
      List<ColumnReference> groupByColumns = EnhancedSqlParser.extractGroupByColumns(sql);
      checkGroupByColumns(
          groupByColumns, aliasToTable, indexMetadata, normalized, stackTrace, issues);
    }

    return issues;
  }

  // ── Low cardinality detection ────────────────────────────────────

  /**
   * Determine if a column is likely low cardinality based on naming patterns and/or index metadata
   * cardinality.
   */
  private boolean isLowCardinalityColumn(String columnName, IndexMetadata metadata, String table) {
    String lower = columnName.toLowerCase();

    // Check exact name match
    if (LOW_CARDINALITY_EXACT_NAMES.contains(lower)) {
      return true;
    }

    // Check prefix patterns (is_*, has_*, flag*)
    if (LOW_CARDINALITY_PREFIX_PATTERN.matcher(lower).matches()) {
      return true;
    }

    // Check suffix patterns (*_type, *_status)
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

  // ── Soft-delete detection ────────────────────────────────────────

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

  // ── Improvement 4: Smart suggestion builders ─────────────────────

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
        + "         If this column is often queried with other columns, consider a composite index.";
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
          + "         A composite index (where_col, order_col) eliminates both the scan and the filesort.";
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
          + "         A composite index (where_col, order_col) eliminates both the scan and the filesort.";
    }
    return "Run: ALTER TABLE "
        + table
        + " ADD INDEX idx_"
        + orderCol
        + " ("
        + orderCol
        + ");\n"
        + "         Tip: If used with WHERE, create a composite index (where_col, order_col) for best performance.";
  }

  /**
   * Check GROUP BY columns for missing indexes. Skips when:
   *
   * <ul>
   *   <li>All primary key columns are present in GROUP BY (functional dependency)
   *   <li>A WHERE column on the same table has an index (narrow result set makes GROUP BY indexing
   *       pointless — based on pgMustard approach)
   * </ul>
   */
  private void checkGroupByColumns(
      List<ColumnReference> groupByColumns,
      Map<String, String> aliasToTable,
      IndexMetadata indexMetadata,
      String normalized,
      String stackTrace,
      List<Issue> issues) {
    // Collect GROUP BY column names per resolved table
    Map<String, Set<String>> groupByColumnsByTable = new HashMap<>();
    for (ColumnReference col : groupByColumns) {
      String table = resolveTable(col.tableOrAlias(), aliasToTable);
      if (table != null) {
        groupByColumnsByTable
            .computeIfAbsent(table, k -> new HashSet<>())
            .add(col.columnName().toLowerCase());
      }
    }

    // Determine which tables have an indexed WHERE column (narrow result set)
    Map<String, Boolean> tableHasIndexedWhereCol = new HashMap<>();
    List<WhereColumnReference> whereColsWithOp =
        EnhancedSqlParser.extractWhereColumnsWithOperators(normalized != null ? normalized : "");
    // Also try extracting from the original SQL (normalized may lose structure)
    // We use the aliasToTable map we already have
    for (WhereColumnReference wcol : whereColsWithOp) {
      String table = resolveTable(wcol.tableOrAlias(), aliasToTable);
      if (table != null
          && indexMetadata.hasTable(table)
          && indexMetadata.hasIndexOn(table, wcol.columnName())) {
        tableHasIndexedWhereCol.put(table, true);
      }
    }

    for (ColumnReference col : groupByColumns) {
      String table = resolveTable(col.tableOrAlias(), aliasToTable);
      if (table != null
          && indexMetadata.hasTable(table)
          && !indexMetadata.hasIndexOn(table, col.columnName())) {
        // If all PK columns are present in the GROUP BY for this table,
        // skip non-indexed columns (they are functionally dependent on PK)
        Set<String> groupByCols = groupByColumnsByTable.getOrDefault(table, Set.of());
        if (allPrimaryKeyColumnsPresent(indexMetadata, table, groupByCols)) {
          continue;
        }

        // Improvement 2: If a WHERE column on the same table already has an index,
        // the result set is already narrow — GROUP BY indexing is pointless.
        if (tableHasIndexedWhereCol.getOrDefault(table, false)) {
          continue;
        }

        issues.add(
            new Issue(
                IssueType.MISSING_GROUP_BY_INDEX,
                Severity.WARNING,
                normalized,
                table,
                col.columnName(),
                "SHOW INDEX FROM "
                    + table
                    + " \u2192 "
                    + col.columnName()
                    + " column has no index. MySQL creates a temporary table for GROUP BY without index.",
                "Run: ALTER TABLE "
                    + table
                    + " ADD INDEX idx_"
                    + col.columnName()
                    + " ("
                    + col.columnName()
                    + ");",
                stackTrace));
      }
    }
  }

  private void checkJoinColumn(
      ColumnReference col,
      Map<String, String> aliasToTable,
      IndexMetadata indexMetadata,
      String normalized,
      String stackTrace,
      List<Issue> issues) {
    String table = resolveTable(col.tableOrAlias(), aliasToTable);
    if (table != null
        && indexMetadata.hasTable(table)
        && !indexMetadata.hasIndexOn(table, col.columnName())) {
      issues.add(
          new Issue(
              IssueType.MISSING_JOIN_INDEX,
              Severity.ERROR,
              normalized,
              table,
              col.columnName(),
              "SHOW INDEX FROM "
                  + table
                  + " \u2192 "
                  + col.columnName()
                  + " column has no index. Every JOIN without an index causes a full scan of the joined table.",
              "Run: ALTER TABLE "
                  + table
                  + " ADD INDEX idx_"
                  + col.columnName()
                  + " ("
                  + col.columnName()
                  + ");\n"
                  + "         This is typically a foreign key \u2014 consider adding a FK constraint too.",
              stackTrace));
    }
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

  /** Check if a column has a unique or primary key index. */
  private boolean hasUniqueOrPrimaryIndex(IndexMetadata metadata, String table, String column) {
    List<IndexInfo> indexes = metadata.getIndexesForTable(table);
    for (IndexInfo idx : indexes) {
      if (idx.columnName() != null
          && idx.columnName().equalsIgnoreCase(column)
          && !idx.nonUnique()) {
        return true;
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

  /**
   * Build a mapping from alias (or table name) to actual table name. For example: "FROM orders o
   * JOIN users u ON ..." produces {o -> orders, u -> users, orders -> orders, users -> users}.
   */
  static Map<String, String> resolveAliases(String sql) {
    Map<String, String> aliasToTable = new HashMap<>();

    Matcher fromMatcher = FROM_ALIAS.matcher(sql);
    while (fromMatcher.find()) {
      String table = fromMatcher.group(1);
      String alias = fromMatcher.group(2);
      if (!isKeyword(table)) {
        aliasToTable.put(table.toLowerCase(), table.toLowerCase());
        if (alias != null && !isKeyword(alias)) {
          aliasToTable.put(alias.toLowerCase(), table.toLowerCase());
        }
      }
    }

    Matcher joinMatcher = JOIN_ALIAS.matcher(sql);
    while (joinMatcher.find()) {
      String table = joinMatcher.group(1);
      String alias = joinMatcher.group(2);
      if (!isKeyword(table)) {
        aliasToTable.put(table.toLowerCase(), table.toLowerCase());
        if (alias != null && !isKeyword(alias)) {
          aliasToTable.put(alias.toLowerCase(), table.toLowerCase());
        }
      }
    }

    return aliasToTable;
  }

  /**
   * Resolve an alias/table reference to the actual table name. If tableOrAlias is null, try to
   * infer from the first table in the alias map.
   */
  private String resolveTable(String tableOrAlias, Map<String, String> aliasToTable) {
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

  private static final Set<String> SQL_KEYWORDS =
      Set.of(
          "select",
          "from",
          "where",
          "and",
          "or",
          "not",
          "in",
          "is",
          "null",
          "between",
          "like",
          "join",
          "inner",
          "left",
          "right",
          "outer",
          "on",
          "order",
          "by",
          "group",
          "having",
          "limit",
          "offset",
          "as",
          "asc",
          "desc",
          "insert",
          "update",
          "delete",
          "set",
          "into",
          "values",
          "create",
          "drop",
          "alter",
          "table",
          "index",
          "exists",
          "case",
          "when",
          "then",
          "else",
          "end",
          "union",
          "all",
          "distinct",
          "cross",
          "natural",
          "full",
          "using");

  private static boolean isKeyword(String word) {
    return word != null && SQL_KEYWORDS.contains(word.toLowerCase());
  }
}
