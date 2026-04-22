package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.EnhancedSqlParser;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Detects redundant indexes where one index is a left prefix of another.
 *
 * <p>Based on jOOQ blog / pt-index-usage (Percona).
 *
 * <p>Example: Index A on (col_a) is redundant when Index B on (col_a, col_b) exists, because any
 * query that can use Index A can also use Index B.
 *
 * <p>Exception: If the shorter index is UNIQUE and the longer index is not, the UNIQUE constraint
 * has additional semantic meaning and is not redundant.
 *
 * @author haroya
 * @since 0.2.0
 */
public class IndexRedundancyDetector implements DetectionRule {

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();

    if (indexMetadata == null || indexMetadata.isEmpty()) {
      return issues;
    }

    // We need to examine all tables that appear in the queries
    Set<String> tables = new LinkedHashSet<>();
    for (QueryRecord query : queries) {
      String sql = query.sql();
      if (sql != null) {
        tables.addAll(EnhancedSqlParser.extractTableNames(sql));
      }
    }

    Set<String> reported = new LinkedHashSet<>();

    for (String table : tables) {
      List<IndexInfo> allIndexes = indexMetadata.getIndexesForTable(table);
      if (allIndexes.isEmpty()) {
        continue;
      }

      // Group indexes by name, preserving column order (seqInIndex)
      // Filter out entries with null indexName or null columnName
      Map<String, List<IndexInfo>> indexesByName =
          allIndexes.stream()
              .filter(idx -> idx.indexName() != null && idx.columnName() != null)
              .collect(
                  Collectors.groupingBy(
                      IndexInfo::indexName, LinkedHashMap::new, Collectors.toList()));

      // Build ordered column lists per index
      Map<String, List<String>> indexColumns = new LinkedHashMap<>();
      Map<String, Boolean> indexIsUnique = new LinkedHashMap<>();

      for (Map.Entry<String, List<IndexInfo>> entry : indexesByName.entrySet()) {
        String indexName = entry.getKey();
        List<IndexInfo> cols =
            entry.getValue().stream()
                .sorted((a, b) -> Integer.compare(a.seqInIndex(), b.seqInIndex()))
                .toList();
        List<String> colNames = cols.stream().map(idx -> idx.columnName().toLowerCase()).toList();
        indexColumns.put(indexName, colNames);

        // An index is unique if any of its entries has nonUnique=false
        boolean isUnique = cols.stream().anyMatch(idx -> !idx.nonUnique());
        indexIsUnique.put(indexName, isUnique);
      }

      // Compare each pair of indexes
      List<String> indexNames = new ArrayList<>(indexColumns.keySet());
      for (int i = 0; i < indexNames.size(); i++) {
        for (int j = 0; j < indexNames.size(); j++) {
          if (i == j) continue;

          String shortName = indexNames.get(i);
          String longName = indexNames.get(j);
          List<String> shortCols = indexColumns.get(shortName);
          List<String> longCols = indexColumns.get(longName);

          // Check if shortCols is a proper left prefix of longCols
          if (shortCols.size() >= longCols.size()) {
            continue;
          }

          if (!isLeftPrefix(shortCols, longCols)) {
            continue;
          }

          // Skip if the shorter index is PRIMARY — dropping a PRIMARY KEY
          // changes the InnoDB clustered index and has serious implications.
          if ("PRIMARY".equalsIgnoreCase(shortName)) {
            continue;
          }

          // Skip if the short index is UNIQUE and the long index is not
          boolean shortUnique = indexIsUnique.getOrDefault(shortName, false);
          boolean longUnique = indexIsUnique.getOrDefault(longName, false);
          if (shortUnique && !longUnique) {
            continue;
          }

          String reportKey = table + ":" + shortName + ":" + longName;
          if (reported.contains(reportKey)) {
            continue;
          }
          reported.add(reportKey);

          String shortColsStr = String.join(", ", shortCols);
          String longColsStr = String.join(", ", longCols);

          issues.add(
              new Issue(
                  IssueType.REDUNDANT_INDEX,
                  Severity.WARNING,
                  null,
                  table,
                  null,
                  "Index '"
                      + shortName
                      + "' on ("
                      + shortColsStr
                      + ") is redundant because '"
                      + longName
                      + "' on ("
                      + longColsStr
                      + ") already covers these columns",
                  "Drop the redundant index: ALTER TABLE "
                      + table
                      + " DROP INDEX "
                      + shortName
                      + ";",
                  null));
        }
      }
    }

    return issues;
  }

  /** Check if {@code prefix} is a left prefix of {@code full}. */
  private boolean isLeftPrefix(List<String> prefix, List<String> full) {
    if (prefix.size() > full.size()) {
      return false;
    }
    for (int i = 0; i < prefix.size(); i++) {
      if (!prefix.get(i).equals(full.get(i))) {
        return false;
      }
    }
    return true;
  }
}
