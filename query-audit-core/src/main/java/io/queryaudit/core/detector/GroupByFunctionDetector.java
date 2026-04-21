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
 * Detects function calls wrapping columns in GROUP BY clause, e.g. {@code GROUP BY
 * YEAR(created_at)} or {@code GROUP BY UPPER(name)}. Functions in GROUP BY prevent the database
 * from using indexes for grouping operations, forcing a temporary table and filesort instead.
 *
 * @author haroya
 * @since 0.2.0
 */
public class GroupByFunctionDetector implements DetectionRule {

  private static final Pattern GROUP_BY_START =
      Pattern.compile("\\bGROUP\\s+BY\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern[] GROUP_BY_TERMINATORS = {
    Pattern.compile("\\bHAVING\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bORDER\\s+BY\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bLIMIT\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bUNION\\b", Pattern.CASE_INSENSITIVE),
  };

  /**
   * Matches a function call like {@code FUNC_NAME(column ...)} in the GROUP BY body. Captures the
   * function name and the first argument (assumed to be a column name).
   */
  private static final Pattern FUNCTION_IN_GROUP_BY =
      Pattern.compile("\\b(\\w+)\\s*\\(\\s*(?:(\\w+)\\.)?(\\w+)", Pattern.CASE_INSENSITIVE);

  /**
   * Aggregate functions that are legitimate in GROUP BY with ROLLUP etc. These should not be
   * flagged.
   */
  private static final Set<String> AGGREGATE_FUNCTIONS =
      Set.of("COUNT", "SUM", "AVG", "MIN", "MAX", "GROUP_CONCAT", "ROLLUP", "CUBE", "GROUPING");

  private static final Set<String> KEYWORDS =
      Set.of(
          "select", "from", "where", "and", "or", "not", "in", "is", "null", "true", "false",
          "between", "like", "as", "on", "join", "left", "right", "inner", "outer", "cross",
          "order", "group", "by", "having", "limit", "offset", "insert", "update", "delete", "set",
          "into", "values");

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();
    Set<String> seen = new LinkedHashSet<>();

    for (QueryRecord query : queries) {
      String sql = query.sql();
      if (sql == null) {
        continue;
      }

      String normalized = query.normalizedSql();
      if (normalized != null && !seen.add(normalized)) {
        continue;
      }

      String groupByBody = extractGroupByBody(sql);
      if (groupByBody == null) {
        continue;
      }

      Matcher matcher = FUNCTION_IN_GROUP_BY.matcher(groupByBody);
      boolean found = false;
      while (matcher.find() && !found) {
        String funcName = matcher.group(1);
        String column = matcher.group(3);

        // Skip aggregate functions and keywords
        if (AGGREGATE_FUNCTIONS.contains(funcName.toUpperCase())) {
          continue;
        }
        if (KEYWORDS.contains(funcName.toLowerCase())) {
          continue;
        }
        if (KEYWORDS.contains(column.toLowerCase())) {
          continue;
        }

        List<String> tables = EnhancedSqlParser.extractTableNames(sql);
        String table = tables.isEmpty() ? null : tables.get(0);

        issues.add(
            new Issue(
                IssueType.GROUP_BY_FUNCTION,
                Severity.WARNING,
                normalized,
                table,
                column,
                funcName.toUpperCase()
                    + "() in GROUP BY prevents index usage on column '"
                    + column
                    + "'",
                "Function in GROUP BY prevents index usage. Consider adding a generated "
                    + "column with an index, or restructuring the query.",
                query.stackTrace()));
        found = true;
      }
    }

    return issues;
  }

  /** Extracts the GROUP BY clause body from a SQL query. */
  private static String extractGroupByBody(String sql) {
    if (sql == null) {
      return null;
    }
    String cleaned = EnhancedSqlParser.removeSubqueries(sql);
    Matcher m = GROUP_BY_START.matcher(cleaned);
    if (!m.find()) {
      return null;
    }

    int bodyStart = m.end();
    int bodyEnd = cleaned.length();
    for (Pattern terminator : GROUP_BY_TERMINATORS) {
      Matcher tm = terminator.matcher(cleaned);
      if (tm.find(bodyStart) && tm.start() < bodyEnd) {
        bodyEnd = tm.start();
      }
    }
    if (bodyStart >= bodyEnd) {
      return null;
    }
    return cleaned.substring(bodyStart, bodyEnd).trim();
  }
}
