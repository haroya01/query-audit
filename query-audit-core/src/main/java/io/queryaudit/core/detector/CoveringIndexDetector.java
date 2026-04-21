package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.ColumnReference;
import io.queryaudit.core.parser.EnhancedSqlParser;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Detects when a query's SELECT columns could be fully served by an index (index-only scan) but
 * aren't. If an index exists on the WHERE columns but the SELECT includes additional columns not in
 * that index, the query requires a table lookup that could be avoided by extending the index.
 *
 * <p>Only flags when the number of additional columns needed is 3 or fewer, since adding too many
 * columns to an index has diminishing returns.
 *
 * @author haroya
 * @since 0.2.0
 */
public class CoveringIndexDetector implements DetectionRule {

  private static final int MAX_ADDITIONAL_COLUMNS = 3;

  /** Matches the start of SELECT (with optional DISTINCT) to find where the column list begins. */
  private static final Pattern SELECT_START_PATTERN =
      Pattern.compile("\\bSELECT\\s+(?:DISTINCT\\s+)?", Pattern.CASE_INSENSITIVE);

  /** Matches a word-boundary FROM keyword to find where the column list ends. */
  private static final Pattern FROM_KEYWORD =
      Pattern.compile("\\bFROM\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern COLUMN_NAME_PATTERN =
      Pattern.compile("(?:(\\w+)\\.)?(\\w+)(?:\\s+(?:AS\\s+)?\\w+)?", Pattern.CASE_INSENSITIVE);

  private static final Pattern FROM_ALIAS =
      Pattern.compile("\\bFROM\\s+(\\w+)(?:\\s+(?:AS\\s+)?(\\w+))?", Pattern.CASE_INSENSITIVE);

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
      if (!SqlParser.isSelectQuery(sql) || SqlParser.hasSelectAll(sql)) {
        continue;
      }

      List<String> selectColumns = extractSelectColumnNames(sql);
      if (selectColumns.isEmpty()) {
        continue;
      }

      List<ColumnReference> whereColumns = EnhancedSqlParser.extractWhereColumns(sql);
      if (whereColumns.isEmpty()) {
        continue;
      }

      List<String> tables = EnhancedSqlParser.extractTableNames(sql);
      if (tables.isEmpty()) {
        continue;
      }

      String table = tables.get(0);
      if (!indexMetadata.hasTable(table)) {
        continue;
      }

      Set<String> whereColNames =
          whereColumns.stream().map(c -> c.columnName().toLowerCase()).collect(Collectors.toSet());

      // Find indexes that cover the WHERE columns
      Map<String, List<IndexInfo>> compositeIndexes = indexMetadata.getCompositeIndexes(table);
      List<IndexInfo> allIndexes = indexMetadata.getIndexesForTable(table);

      // Group all indexes by name
      Map<String, List<IndexInfo>> indexesByName =
          allIndexes.stream()
              .filter(idx -> idx.indexName() != null)
              .collect(Collectors.groupingBy(IndexInfo::indexName));

      for (Map.Entry<String, List<IndexInfo>> entry : indexesByName.entrySet()) {
        String indexName = entry.getKey();
        List<IndexInfo> indexColumns = entry.getValue();

        Set<String> indexColNames =
            indexColumns.stream()
                .filter(idx -> idx.columnName() != null)
                .map(idx -> idx.columnName().toLowerCase())
                .collect(Collectors.toSet());

        // Check if this index covers the WHERE columns
        boolean coversWhere = whereColNames.stream().anyMatch(indexColNames::contains);

        if (!coversWhere) {
          continue;
        }

        // Find SELECT columns not in this index
        Set<String> selectColsLower =
            selectColumns.stream().map(String::toLowerCase).collect(Collectors.toSet());

        List<String> missingFromIndex =
            selectColsLower.stream()
                .filter(col -> !indexColNames.contains(col))
                .collect(Collectors.toList());

        if (missingFromIndex.isEmpty()) {
          continue; // Already a covering index
        }

        // In InnoDB, secondary indexes implicitly include the primary key columns,
        // so if all missing columns are PKs, the index already covers them.
        Set<String> pkColumns = getPrimaryKeyColumns(indexMetadata, table);
        if (!pkColumns.isEmpty()) {
          missingFromIndex.removeAll(pkColumns);
        }
        if (missingFromIndex.isEmpty()) {
          continue; // All missing columns are PKs — already covered by InnoDB
        }

        if (missingFromIndex.size() > MAX_ADDITIONAL_COLUMNS) {
          continue; // Too many columns to add
        }

        String missingCols = String.join(", ", missingFromIndex);
        String allCols = indexColNames.stream().collect(Collectors.joining(", "));
        String extendedCols = allCols + ", " + missingCols;

        issues.add(
            new Issue(
                IssueType.COVERING_INDEX_OPPORTUNITY,
                Severity.INFO,
                normalized,
                table,
                null,
                "Query could use index-only scan if columns ["
                    + missingCols
                    + "] were added to index '"
                    + indexName
                    + "'",
                "Extend the index to include SELECT columns: ALTER TABLE "
                    + table
                    + " ADD INDEX idx_covering ("
                    + extendedCols
                    + ")",
                query.stackTrace()));
        break; // Report once per query
      }
    }

    return issues;
  }

  /**
   * Returns the set of primary key column names (lowercased) for the given table.
   * In InnoDB, secondary indexes always include PK columns, so they are effectively covered.
   */
  private Set<String> getPrimaryKeyColumns(IndexMetadata indexMetadata, String table) {
    Set<String> pkCols = new HashSet<>();
    for (IndexInfo idx : indexMetadata.getIndexesForTable(table)) {
      if ("PRIMARY".equalsIgnoreCase(idx.indexName()) && idx.columnName() != null) {
        pkCols.add(idx.columnName().toLowerCase());
      }
    }
    return pkCols;
  }

  /**
   * Extract plain column names from the SELECT clause. Skips aggregate functions, expressions, and
   * wildcards. Uses manual boundary scanning to avoid (.+?) backtracking.
   */
  private List<String> extractSelectColumnNames(String sql) {
    List<String> columns = new ArrayList<>();
    Matcher startMatcher = SELECT_START_PATTERN.matcher(sql);
    if (!startMatcher.find()) {
      return columns;
    }
    int selectBodyStart = startMatcher.end();

    Matcher fromMatcher = FROM_KEYWORD.matcher(sql);
    if (!fromMatcher.find(selectBodyStart)) {
      return columns;
    }
    int selectBodyEnd = fromMatcher.start();

    if (selectBodyStart >= selectBodyEnd) {
      return columns;
    }
    String selectBody = sql.substring(selectBodyStart, selectBodyEnd).trim();
    // Split by top-level commas
    String[] parts = splitByTopLevelCommas(selectBody);
    for (String part : parts) {
      String trimmed = part.trim();
      // Skip aggregate functions and expressions
      if (trimmed.contains("(") || trimmed.equals("*") || trimmed.contains("*")) {
        continue;
      }
      Matcher colMatcher = COLUMN_NAME_PATTERN.matcher(trimmed);
      if (colMatcher.matches()) {
        String colName = colMatcher.group(2);
        if (colName != null && !isKeyword(colName)) {
          columns.add(colName);
        }
      }
    }
    return columns;
  }

  private String[] splitByTopLevelCommas(String s) {
    List<String> parts = new ArrayList<>();
    int depth = 0;
    int start = 0;
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '(') depth++;
      else if (c == ')') depth--;
      else if (c == ',' && depth == 0) {
        parts.add(s.substring(start, i));
        start = i + 1;
      }
    }
    parts.add(s.substring(start));
    return parts.toArray(new String[0]);
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
          "count",
          "sum",
          "avg",
          "min",
          "max");

  private static boolean isKeyword(String word) {
    return word != null && SQL_KEYWORDS.contains(word.toLowerCase());
  }
}
