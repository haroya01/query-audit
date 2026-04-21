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
import java.util.regex.Pattern;

/**
 * Detects SELECT queries with LIMIT but without ORDER BY, which returns non-deterministic results.
 *
 * <p>Without ORDER BY, the database engine is free to return rows in any order. Combined with LIMIT,
 * this means repeated executions may return different result sets, leading to inconsistent pagination
 * and hard-to-reproduce bugs.
 *
 * @author haroya
 * @since 0.2.0
 */
public class LimitWithoutOrderByDetector implements DetectionRule {

  private static final Pattern LIMIT_PATTERN =
      Pattern.compile("\\bLIMIT\\b|\\bFETCH\\s+FIRST\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern ORDER_BY_PATTERN =
      Pattern.compile("\\bORDER\\s+BY\\b", Pattern.CASE_INSENSITIVE);

  /** Matches FOR UPDATE / FOR SHARE clauses — LIMIT with FOR UPDATE is typically intentional. */
  private static final Pattern FOR_UPDATE_PATTERN =
      Pattern.compile("\\bFOR\\s+(?:UPDATE|SHARE)\\b", Pattern.CASE_INSENSITIVE);

  /** Matches aggregate functions that make ordering meaningless. */
  private static final Pattern AGGREGATE_PATTERN =
      Pattern.compile(
          "\\b(?:COUNT|SUM|AVG|MIN|MAX)\\s*\\(", Pattern.CASE_INSENSITIVE);

  /**
   * Matches LIMIT 1 or LIMIT ? — existence checks (e.g., {@code SELECT id FROM t WHERE cond LIMIT 1})
   * intentionally use LIMIT 1 without ORDER BY to quickly check if any row matches.
   * JPA existsBy* methods generate parameterized LIMIT (LIMIT ?) which should also be excluded.
   */
  private static final Pattern LIMIT_ONE_PATTERN =
      Pattern.compile(
          "\\bLIMIT\\s+(?:1\\b|\\?)|\\bFETCH\\s+FIRST\\s+(?:1\\b|\\?)\\s+ROWS?\\s+ONLY\\b",
          Pattern.CASE_INSENSITIVE);

  /**
   * Matches JPA existsBy* method names in the captured stack trace.
   * When a query originates from an existsBy* call, ordering is irrelevant.
   */
  private static final Pattern EXISTS_BY_METHOD =
      Pattern.compile("\\.existsBy\\w+:");

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

      String outerSql = EnhancedSqlParser.removeSubqueries(sql);

      if (!LIMIT_PATTERN.matcher(outerSql).find()) {
        continue;
      }

      if (ORDER_BY_PATTERN.matcher(outerSql).find()) {
        continue;
      }

      // Skip FOR UPDATE/SHARE — typically intentional single-row lock
      if (FOR_UPDATE_PATTERN.matcher(outerSql).find()) {
        continue;
      }

      // Skip aggregate queries — ordering is meaningless for COUNT(*) LIMIT 1 etc.
      if (AGGREGATE_PATTERN.matcher(outerSql).find()) {
        continue;
      }

      // Skip LIMIT 1 or LIMIT ? — commonly used for existence checks where ordering is irrelevant
      if (LIMIT_ONE_PATTERN.matcher(outerSql).find()) {
        continue;
      }

      // Skip queries originating from JPA existsBy* methods (intent-based detection)
      if (query.stackTrace() != null && EXISTS_BY_METHOD.matcher(query.stackTrace()).find()) {
        continue;
      }

      List<String> tables = EnhancedSqlParser.extractTableNames(sql);
      String table = tables.isEmpty() ? null : tables.get(0);

      issues.add(
          new Issue(
              IssueType.LIMIT_WITHOUT_ORDER_BY,
              Severity.WARNING,
              normalized,
              table,
              null,
              "SELECT with LIMIT but no ORDER BY returns non-deterministic results"
                  + (table != null ? " from table '" + table + "'" : ""),
              "Add an ORDER BY clause to ensure deterministic result ordering. "
                  + "Without ORDER BY, repeated queries may return different rows.",
              query.stackTrace()));
    }

    return issues;
  }
}
