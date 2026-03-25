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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Detects queries that use a non-leading column of a composite index without including the leading
 * column in the WHERE clause. This violates the leftmost prefix rule of B-tree indexes, causing
 * the database to skip the composite index entirely and potentially fall back to a full table scan.
 *
 * @author haroya
 * @since 0.2.0
 */
public class CompositeIndexDetector implements DetectionRule {

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

      String stackTrace = query.stackTrace();
      Map<String, String> aliasToTable = MissingIndexDetector.resolveAliases(sql);

      List<ColumnReference> whereColumns = EnhancedSqlParser.extractWhereColumns(sql);

      // Collect WHERE column names per resolved table
      Map<String, Set<String>> whereColumnsByTable = new java.util.HashMap<>();
      for (ColumnReference col : whereColumns) {
        String table = resolveTable(col.tableOrAlias(), aliasToTable);
        if (table != null) {
          whereColumnsByTable
              .computeIfAbsent(table, k -> new java.util.HashSet<>())
              .add(col.columnName().toLowerCase());
        }
      }

      // Improvement 1: Expand effectively constrained columns through JOIN conditions.
      // If table_a.col_x = table_b.col_y in JOIN, and col_x is in WHERE,
      // then col_y is also effectively constrained (and vice versa).
      expandConstrainedColumnsThroughJoins(sql, aliasToTable, whereColumnsByTable);

      // For each table referenced, check composite indexes
      List<String> tables = EnhancedSqlParser.extractTableNames(sql);
      for (String tableName : tables) {
        String table = tableName.toLowerCase();
        Set<String> usedColumns = whereColumnsByTable.getOrDefault(table, Set.of());

        Map<String, List<IndexInfo>> compositeIndexes = indexMetadata.getCompositeIndexes(table);
        for (Map.Entry<String, List<IndexInfo>> entry : compositeIndexes.entrySet()) {
          String indexName = entry.getKey();
          List<IndexInfo> indexColumns =
              entry.getValue().stream()
                  .sorted(Comparator.comparingInt(IndexInfo::seqInIndex))
                  .toList();

          if (indexColumns.isEmpty()) {
            continue;
          }

          // The leading column is the first one in the composite index
          String leadingColName = indexColumns.get(0).columnName();
          if (leadingColName == null) {
            continue; // functional index with no column name — skip
          }
          String leadingColumn = leadingColName.toLowerCase();

          // Check if any non-leading column is used in WHERE but leading column is NOT
          boolean leadingUsed = usedColumns.contains(leadingColumn);
          if (leadingUsed) {
            continue;
          }

          boolean anyNonLeadingUsed =
              indexColumns.stream()
                  .skip(1)
                  .filter(idx -> idx.columnName() != null)
                  .anyMatch(idx -> usedColumns.contains(idx.columnName().toLowerCase()));

          if (anyNonLeadingUsed) {
            String nonLeadingUsedColumn =
                indexColumns.stream()
                    .skip(1)
                    .filter(idx -> idx.columnName() != null)
                    .map(idx -> idx.columnName().toLowerCase())
                    .filter(usedColumns::contains)
                    .findFirst()
                    .orElse(null);

            // Skip if the non-leading column has its own standalone index
            // (including being a PK or having a single-column index)
            if (nonLeadingUsedColumn != null
                && hasStandaloneIndex(indexMetadata, table, nonLeadingUsedColumn)) {
              continue;
            }

            String columnList =
                indexColumns.stream().map(IndexInfo::columnName).collect(Collectors.joining(", "));

            issues.add(
                new Issue(
                    IssueType.COMPOSITE_INDEX_LEADING_COLUMN,
                    Severity.WARNING,
                    normalized,
                    table,
                    nonLeadingUsedColumn,
                    "Index "
                        + indexName
                        + "("
                        + columnList
                        + ") exists but leading column "
                        + leadingColumn
                        + " is not in WHERE clause",
                    "Include leading column in WHERE or create a separate index on "
                        + (nonLeadingUsedColumn != null ? nonLeadingUsedColumn : "the used column"),
                    stackTrace));
          }
        }
      }
    }

    return issues;
  }

  /**
   * Check if a column is covered by any other index: either a single-column (standalone) index, or
   * as the leading column of another composite index. This includes PRIMARY KEY.
   */
  private boolean hasStandaloneIndex(IndexMetadata indexMetadata, String table, String column) {
    List<IndexInfo> allIndexes = indexMetadata.getIndexesForTable(table);
    // Group by index name
    Map<String, List<IndexInfo>> byName =
        allIndexes.stream().collect(Collectors.groupingBy(IndexInfo::indexName));
    for (Map.Entry<String, List<IndexInfo>> entry : byName.entrySet()) {
      List<IndexInfo> cols = entry.getValue();
      // Single-column index that matches our column
      if (cols.size() == 1
          && cols.get(0).columnName() != null
          && cols.get(0).columnName().equalsIgnoreCase(column)) {
        return true;
      }
      // Leading column of another composite index that matches our column
      if (cols.size() > 1) {
        IndexInfo leading =
            cols.stream().min(Comparator.comparingInt(IndexInfo::seqInIndex)).orElse(null);
        if (leading != null
            && leading.columnName() != null
            && leading.columnName().equalsIgnoreCase(column)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Expand the set of effectively constrained columns through JOIN conditions. For each JOIN pair
   * (e.g., messages.id = message_reactions.message_id), if one side's column is already in the
   * WHERE constraint set (directly or via alias), then the other side's column is also effectively
   * constrained. This prevents false positives when a composite index leading column is constrained
   * transitively through a JOIN + WHERE combination.
   */
  private void expandConstrainedColumnsThroughJoins(
      String sql, Map<String, String> aliasToTable, Map<String, Set<String>> whereColumnsByTable) {
    List<JoinColumnPair> joinPairs = EnhancedSqlParser.extractJoinColumns(sql);
    if (joinPairs.isEmpty()) {
      return;
    }

    // Iterate until no more columns are added (handles transitive chains)
    boolean changed = true;
    while (changed) {
      changed = false;
      for (JoinColumnPair pair : joinPairs) {
        String leftTable = resolveTable(pair.left().tableOrAlias(), aliasToTable);
        String leftCol =
            pair.left().columnName() != null ? pair.left().columnName().toLowerCase() : null;
        String rightTable = resolveTable(pair.right().tableOrAlias(), aliasToTable);
        String rightCol =
            pair.right().columnName() != null ? pair.right().columnName().toLowerCase() : null;

        if (leftTable == null || leftCol == null || rightTable == null || rightCol == null) {
          continue;
        }

        Set<String> leftCols = whereColumnsByTable.getOrDefault(leftTable, Set.of());
        Set<String> rightCols = whereColumnsByTable.getOrDefault(rightTable, Set.of());

        // If left side is constrained, expand right side
        if (leftCols.contains(leftCol) && !rightCols.contains(rightCol)) {
          whereColumnsByTable
              .computeIfAbsent(rightTable, k -> new java.util.HashSet<>())
              .add(rightCol);
          changed = true;
        }
        // If right side is constrained, expand left side
        if (rightCols.contains(rightCol) && !leftCols.contains(leftCol)) {
          whereColumnsByTable
              .computeIfAbsent(leftTable, k -> new java.util.HashSet<>())
              .add(leftCol);
          changed = true;
        }
      }
    }
  }

  private String resolveTable(String tableOrAlias, Map<String, String> aliasToTable) {
    if (tableOrAlias != null) {
      String resolved = aliasToTable.get(tableOrAlias.toLowerCase());
      if (resolved != null) return resolved;
      // Don't use unresolved Hibernate aliases (e.g., m1_0, r1_0, us1_0) as table names
      if (tableOrAlias.matches("(?i)[a-z]{1,3}\\d+_\\d+")) return null;
      return tableOrAlias.toLowerCase();
    }
    if (aliasToTable.size() <= 2) {
      return aliasToTable.values().stream().findFirst().orElse(null);
    }
    return null;
  }
}
