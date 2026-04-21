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
 * Detects {@code = NULL} or {@code != NULL} / {@code <> NULL} comparisons which are always UNKNOWN
 * in SQL (three-valued logic). The correct forms are {@code IS NULL} and {@code IS NOT NULL}.
 *
 * <p>This is classified as ERROR severity because it is a logic bug, not just a performance concern
 * — the condition will never match any rows.
 *
 * @author haroya
 * @since 0.2.0
 */
public class NullComparisonDetector implements DetectionRule {

  /**
   * Matches column = NULL, column != NULL, column <> NULL patterns. Uses negative lookbehind to
   * exclude IS NULL and IS NOT NULL. Group 1: column name (possibly qualified with table/alias)
   * Group 2: comparison operator (=, !=, <>)
   */
  private static final Pattern NULL_COMPARISON =
      Pattern.compile(
          "(?<!IS\\s)(?<!IS\\sNOT\\s)(\\w+(?:\\.\\w+)?)\\s*([!=<>]+)\\s*NULL\\b",
          Pattern.CASE_INSENSITIVE);

  // WHERE clause extraction delegated to EnhancedSqlParser.extractWhereBody() to avoid
  // catastrophic backtracking from (.+?) with DOTALL patterns.

  /**
   * Matches: column = NULL, column != NULL, column <> NULL Does NOT match: column IS NULL, column
   * IS NOT NULL
   */
  private static final Pattern EQUALS_NULL =
      Pattern.compile("(\\w+(?:\\.\\w+)?)\\s*=\\s*NULL\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern NOT_EQUALS_NULL =
      Pattern.compile("(\\w+(?:\\.\\w+)?)\\s*(?:!=|<>)\\s*NULL\\b", Pattern.CASE_INSENSITIVE);

  /** Negative check: if preceded by IS or IS NOT, it's correct usage. */
  private static final Pattern IS_NULL_PATTERN =
      Pattern.compile("\\bIS\\s+(?:NOT\\s+)?NULL\\b", Pattern.CASE_INSENSITIVE);

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
      if (sql == null) {
        continue;
      }

      // Extract WHERE clause body using safe clause boundary scanning.
      // Strip subqueries first so that conditions inside subqueries do not
      // false-positive on the outer query's WHERE body.
      String whereBody = EnhancedSqlParser.extractWhereBody(EnhancedSqlParser.removeSubqueries(sql));
      if (whereBody == null) {
        continue;
      }

      // Check for = NULL
      Matcher eqMatcher = EQUALS_NULL.matcher(whereBody);
      while (eqMatcher.find()) {
        String columnExpr = eqMatcher.group(1);

        // Make sure this is not part of IS NULL
        int matchStart = eqMatcher.start();
        String before = whereBody.substring(Math.max(0, matchStart - 10), matchStart).trim();
        if (before.toUpperCase().endsWith("IS") || before.toUpperCase().endsWith("IS NOT")) {
          continue;
        }

        List<String> tables = EnhancedSqlParser.extractTableNames(sql);
        String table = tables.isEmpty() ? null : tables.get(0);

        issues.add(
            new Issue(
                IssueType.NULL_COMPARISON,
                Severity.ERROR,
                normalized,
                table,
                extractColumnName(columnExpr),
                "Comparison '"
                    + columnExpr
                    + " = NULL' always evaluates to UNKNOWN. "
                    + "Use 'IS NULL' instead.",
                "Change '" + columnExpr + " = NULL' to '" + columnExpr + " IS NULL'",
                query.stackTrace()));
        break; // Report once per query
      }

      // Check for != NULL or <> NULL (only if we haven't already reported)
      if (issues.isEmpty() || !issues.get(issues.size() - 1).query().equals(normalized)) {
        Matcher neqMatcher = NOT_EQUALS_NULL.matcher(whereBody);
        while (neqMatcher.find()) {
          String columnExpr = neqMatcher.group(1);

          List<String> tables = EnhancedSqlParser.extractTableNames(sql);
          String table = tables.isEmpty() ? null : tables.get(0);

          issues.add(
              new Issue(
                  IssueType.NULL_COMPARISON,
                  Severity.ERROR,
                  normalized,
                  table,
                  extractColumnName(columnExpr),
                  "Comparison '"
                      + columnExpr
                      + " != NULL' always evaluates to UNKNOWN. "
                      + "Use 'IS NOT NULL' instead.",
                  "Change '" + columnExpr + " != NULL' to '" + columnExpr + " IS NOT NULL'",
                  query.stackTrace()));
          break; // Report once per query
        }
      }
    }

    return issues;
  }

  /** Extract the column name from a possibly qualified reference (e.g., "t.col" -> "col"). */
  private String extractColumnName(String columnExpr) {
    if (columnExpr == null) return null;
    int dotIndex = columnExpr.lastIndexOf('.');
    return dotIndex >= 0 ? columnExpr.substring(dotIndex + 1) : columnExpr;
  }
}
