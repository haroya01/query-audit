package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.EnhancedSqlParser;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects SELECT queries that fetch an excessive number of columns, suggesting the use of DTO
 * projections instead of loading full entities.
 *
 * <p>Loading full entities with many columns when only a few are needed wastes network I/O, memory,
 * and Hibernate first-level cache space.
 *
 * <p>Skips queries that use SELECT *, GROUP BY, or aggregate functions, as these are handled by
 * other detectors or are legitimate use cases.
 *
 * @author haroya
 * @since 0.2.0
 */
public class ExcessiveColumnFetchDetector implements DetectionRule {

  private static final int DEFAULT_THRESHOLD = 15;

  /**
   * Pattern to extract the column list between SELECT and FROM. Handles optional DISTINCT/ALL
   * keywords.
   */
  private static final Pattern SELECT_COLUMNS =
      Pattern.compile(
          "^\\s*SELECT\\s+(?:DISTINCT\\s+|ALL\\s+)?(.+?)\\s+FROM\\s+",
          Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  /** Pattern to detect aggregate functions in the select list. */
  private static final Pattern AGGREGATE_FUNCTION =
      Pattern.compile(
          "\\b(?:COUNT|SUM|AVG|MIN|MAX|GROUP_CONCAT|STRING_AGG)\\s*\\(", Pattern.CASE_INSENSITIVE);

  /** Pattern to detect GROUP BY clause. */
  private static final Pattern GROUP_BY =
      Pattern.compile("\\bGROUP\\s+BY\\b", Pattern.CASE_INSENSITIVE);

  /**
   * Pattern for a simple column reference (with optional table alias). Matches: column, t.column,
   * t0_.column, `column`, t.`column`
   */
  private static final Pattern COLUMN_REF = Pattern.compile("^\\s*(?:`?\\w+`?\\.)?`?\\w+`?\\s*$");

  private final int threshold;

  public ExcessiveColumnFetchDetector() {
    this(DEFAULT_THRESHOLD);
  }

  public ExcessiveColumnFetchDetector(int threshold) {
    this.threshold = threshold;
  }

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();
    Set<String> flaggedPatterns = new HashSet<>();

    for (QueryRecord query : queries) {
      String sql = query.sql();
      if (!SqlParser.isSelectQuery(sql)) {
        continue;
      }

      // Skip SELECT * (already caught by SelectAllDetector)
      if (SqlParser.hasSelectAll(sql)) {
        continue;
      }

      // Skip queries with GROUP BY (aggregation queries legitimately list many columns)
      if (GROUP_BY.matcher(sql).find()) {
        continue;
      }

      // Deduplicate by normalized SQL
      String normalized = query.normalizedSql();
      if (normalized != null && !flaggedPatterns.add(normalized)) {
        continue;
      }

      // Extract column list between SELECT and FROM
      String cleanedSql = EnhancedSqlParser.removeSubqueries(sql);
      Matcher m = SELECT_COLUMNS.matcher(cleanedSql);
      if (!m.find()) {
        continue;
      }

      String columnList = m.group(1);

      // Skip if aggregate functions are present
      if (AGGREGATE_FUNCTION.matcher(columnList).find()) {
        continue;
      }

      // Count simple column references (not expressions/functions)
      int columnCount = countSimpleColumns(columnList);

      if (columnCount > threshold) {
        String table = EnhancedSqlParser.extractTableNames(sql).stream().findFirst().orElse(null);
        issues.add(
            new Issue(
                IssueType.EXCESSIVE_COLUMN_FETCH,
                Severity.INFO,
                sql,
                table,
                null,
                "Query selects "
                    + columnCount
                    + " columns from "
                    + (table != null ? "table '" + table + "'" : "the query")
                    + " (threshold: "
                    + threshold
                    + ").",
                "Query selects "
                    + columnCount
                    + " columns. Consider using a DTO projection "
                    + "to fetch only needed columns, reducing network I/O and memory usage."));
      }
    }

    return issues;
  }

  /**
   * Counts simple column references in a comma-separated column list. Skips function calls,
   * expressions, and subqueries.
   */
  int countSimpleColumns(String columnList) {
    if (columnList == null || columnList.isBlank()) {
      return 0;
    }

    List<String> parts = splitByTopLevelCommas(columnList);
    int count = 0;

    for (String part : parts) {
      String trimmed = part.trim();
      // Remove alias: "col AS alias" or "col alias"
      trimmed = removeAlias(trimmed);

      // Skip if it contains parentheses (function call or expression)
      if (trimmed.contains("(")) {
        continue;
      }

      // Skip if it contains arithmetic operators
      if (trimmed.contains("+")
          || trimmed.contains("-")
          || trimmed.contains("/")
          || trimmed.contains("||")) {
        continue;
      }

      // Check if it looks like a simple column reference
      if (COLUMN_REF.matcher(trimmed).matches()) {
        count++;
      }
    }

    return count;
  }

  /** Removes column alias (AS alias or just alias) from a column expression. */
  private String removeAlias(String expr) {
    // Remove "AS alias" pattern
    String result = expr.replaceAll("(?i)\\s+AS\\s+\\S+\\s*$", "").trim();
    return result;
  }

  /** Splits a string by commas at the top level (not inside parentheses). */
  private List<String> splitByTopLevelCommas(String s) {
    List<String> parts = new ArrayList<>();
    int depth = 0;
    int start = 0;
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '(') {
        depth++;
      } else if (c == ')') {
        depth--;
      } else if (c == ',' && depth == 0) {
        parts.add(s.substring(start, i));
        start = i + 1;
      }
    }
    parts.add(s.substring(start));
    return parts;
  }
}
