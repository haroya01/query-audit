package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.ColumnReference;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Detects ORDER BY + LIMIT combinations where the ORDER BY column has no index, causing a full
 * filesort on the entire table before returning just a few rows.
 *
 * <p>This is especially wasteful for "top-N" queries (e.g., ORDER BY created_at DESC LIMIT 10)
 * where an index on the ORDER BY column would allow the database to stop after N rows.
 *
 * @author haroya
 * @since 0.2.0
 */
public class OrderByLimitWithoutIndexDetector implements DetectionRule {

  private static final Pattern LIMIT_PATTERN =
      Pattern.compile("\\bLIMIT\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern ORDER_BY_PATTERN =
      Pattern.compile("\\bORDER\\s+BY\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern FROM_ALIAS =
      Pattern.compile("\\bFROM\\s+(\\w+)(?:\\s+(?:AS\\s+)?(\\w+))?", Pattern.CASE_INSENSITIVE);

  private static final Pattern JOIN_ALIAS =
      Pattern.compile("\\bJOIN\\s+(\\w+)(?:\\s+(?:AS\\s+)?(\\w+))?", Pattern.CASE_INSENSITIVE);

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

      // Must have both ORDER BY and LIMIT
      if (!ORDER_BY_PATTERN.matcher(sql).find() || !LIMIT_PATTERN.matcher(sql).find()) {
        continue;
      }

      List<ColumnReference> orderByColumns = SqlParser.extractOrderByColumns(sql);
      if (orderByColumns.isEmpty()) {
        continue;
      }

      java.util.Map<String, String> aliasToTable = MissingIndexDetector.resolveAliases(sql);
      List<String> tables = SqlParser.extractTableNames(sql);
      if (tables.isEmpty()) {
        continue;
      }

      for (ColumnReference col : orderByColumns) {
        String table = resolveTable(col.tableOrAlias(), aliasToTable, tables);
        if (table == null || !indexMetadata.hasTable(table)) {
          continue;
        }

        // Skip if the column is a primary key
        if (isPrimaryKey(indexMetadata, table, col.columnName())) {
          continue;
        }

        // Skip if any non-primary index exists on the ORDER BY column
        if (hasNonPrimaryIndexOn(indexMetadata, table, col.columnName())) {
          continue;
        }

        // Check if WHERE columns exist for composite suggestion
        List<ColumnReference> whereColumns = SqlParser.extractWhereColumns(sql);
        boolean hasWhere = !whereColumns.isEmpty();

        String suggestion;
        if (hasWhere) {
          suggestion =
              "Add index on ORDER BY column: ALTER TABLE "
                  + table
                  + " ADD INDEX idx_"
                  + col.columnName()
                  + " ("
                  + col.columnName()
                  + "). Or use composite index (where_col, "
                  + col.columnName()
                  + ") if WHERE clause exists.";
        } else {
          suggestion =
              "Add index on ORDER BY column: ALTER TABLE "
                  + table
                  + " ADD INDEX idx_"
                  + col.columnName()
                  + " ("
                  + col.columnName()
                  + ").";
        }

        issues.add(
            new Issue(
                IssueType.ORDER_BY_LIMIT_WITHOUT_INDEX,
                Severity.WARNING,
                normalized,
                table,
                col.columnName(),
                "ORDER BY "
                    + col.columnName()
                    + " LIMIT N without index on '"
                    + col.columnName()
                    + "' causes full filesort before returning N rows",
                suggestion,
                query.stackTrace()));
        break; // Report once per query (first unindexed ORDER BY column)
      }
    }

    return issues;
  }

  /**
   * Checks if a non-primary index exists on the column. Separated from isPrimaryKey to avoid
   * redundancy: PRIMARY indexes are handled by isPrimaryKey, while this checks only secondary
   * indexes.
   */
  private boolean hasNonPrimaryIndexOn(IndexMetadata metadata, String table, String column) {
    List<IndexInfo> indexes = metadata.getIndexesForTable(table);
    return indexes.stream()
        .anyMatch(
            idx ->
                !"PRIMARY".equalsIgnoreCase(idx.indexName())
                    && idx.columnName() != null
                    && idx.columnName().equalsIgnoreCase(column));
  }

  private boolean isPrimaryKey(IndexMetadata metadata, String table, String column) {
    List<IndexInfo> indexes = metadata.getIndexesForTable(table);
    return indexes.stream()
        .anyMatch(
            idx ->
                "PRIMARY".equalsIgnoreCase(idx.indexName())
                    && idx.columnName() != null
                    && idx.columnName().equalsIgnoreCase(column));
  }

  private String resolveTable(
      String tableOrAlias, java.util.Map<String, String> aliasToTable, List<String> tables) {
    if (tableOrAlias != null) {
      String resolved = aliasToTable.get(tableOrAlias.toLowerCase());
      if (resolved != null) return resolved;
      // Don't use unresolved Hibernate aliases
      if (tableOrAlias.matches("(?i)[a-z]{1,3}\\d+_\\d+")) return null;
      return tableOrAlias.toLowerCase();
    }
    // If no qualifier, use the first (main) table if there's only one
    if (tables.size() == 1) {
      return tables.get(0);
    }
    return null;
  }
}
