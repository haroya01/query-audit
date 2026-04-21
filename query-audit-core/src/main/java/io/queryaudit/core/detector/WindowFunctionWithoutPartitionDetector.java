package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.EnhancedSqlParser;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects window functions (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, etc.) that use OVER() without
 * a PARTITION BY clause.
 *
 * <p>Without PARTITION BY, the window function processes the <em>entire</em> result set as a single
 * partition, causing a full-table sort. This is almost always unintentional and indicates a missing
 * PARTITION BY clause.
 *
 * <p>Exception: {@code ROW_NUMBER() OVER(ORDER BY ...)} at the top level for simple row numbering
 * is a common intentional pattern, but still flagged as INFO since it requires a full sort.
 *
 * @author haroya
 * @since 0.2.0
 */
public class WindowFunctionWithoutPartitionDetector implements DetectionRule {

  /**
   * Matches window function calls with OVER(...) that do NOT contain PARTITION BY. Uses a two-step
   * approach: first find OVER clauses, then check for PARTITION BY.
   */
  private static final Pattern WINDOW_FUNCTION_OVER =
      Pattern.compile(
          "\\b(ROW_NUMBER|RANK|DENSE_RANK|NTILE|LAG|LEAD|FIRST_VALUE|LAST_VALUE|NTH_VALUE"
              + "|SUM|COUNT|AVG|MIN|MAX)\\s*\\(.*?\\)\\s*OVER\\s*\\(",
          Pattern.CASE_INSENSITIVE);

  private static final Pattern PARTITION_BY =
      Pattern.compile("\\bPARTITION\\s+BY\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern ORDER_BY =
      Pattern.compile("\\bORDER\\s+BY\\b", Pattern.CASE_INSENSITIVE);

  /**
   * Window functions that are commonly used intentionally over the entire result set
   * for row numbering, ranking, or pagination purposes.
   */
  private static final Set<String> NUMBERING_FUNCTIONS =
      Set.of("ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE");

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();
    Set<String> seen = new LinkedHashSet<>();

    for (QueryRecord query : queries) {
      String normalized = query.normalizedSql();
      if (normalized == null || seen.contains(normalized)) {
        continue;
      }
      seen.add(normalized);

      String sql = query.sql();
      if (sql == null || !SqlParser.isSelectQuery(sql)) {
        continue;
      }

      Matcher windowMatcher = WINDOW_FUNCTION_OVER.matcher(sql);
      while (windowMatcher.find()) {
        String functionName = windowMatcher.group(1).toUpperCase();
        int overOpenParen = windowMatcher.end() - 1;

        // Extract the content inside OVER(...)
        String overContent = extractParenContent(sql, overOpenParen);
        if (overContent == null) {
          continue;
        }

        if (!PARTITION_BY.matcher(overContent).find()) {
          // Numbering/ranking functions (ROW_NUMBER, RANK, etc.) with ORDER BY
          // over the entire result set are intentional for pagination/ranking
          if (NUMBERING_FUNCTIONS.contains(functionName)
              && ORDER_BY.matcher(overContent).find()) {
            continue;
          }

          List<String> tables = EnhancedSqlParser.extractTableNames(sql);
          String table = tables.isEmpty() ? null : tables.get(0);

          issues.add(
              new Issue(
                  IssueType.WINDOW_FUNCTION_WITHOUT_PARTITION,
                  Severity.WARNING,
                  normalized,
                  table,
                  null,
                  functionName
                      + "() OVER() without PARTITION BY processes the entire table as one"
                      + " partition",
                  "Add PARTITION BY to limit the window scope. Without it, the database must "
                      + "sort and process all rows in a single partition, which is O(n) on the "
                      + "full result set.",
                  query.stackTrace()));
          break; // One issue per query is sufficient
        }
      }
    }

    return issues;
  }

  /** Extracts the content inside parentheses starting at the given position. */
  private String extractParenContent(String sql, int openParen) {
    if (openParen >= sql.length() || sql.charAt(openParen) != '(') {
      return null;
    }
    int depth = 1;
    int i = openParen + 1;
    while (i < sql.length() && depth > 0) {
      char c = sql.charAt(i);
      if (c == '(') depth++;
      else if (c == ')') depth--;
      i++;
    }
    if (depth != 0) {
      return null;
    }
    return sql.substring(openParen + 1, i - 1);
  }
}
