package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.SqlParser;
import io.queryaudit.core.parser.EnhancedSqlParser;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects correlated subqueries in the SELECT clause that execute once per outer row.
 *
 * <p>Based on: SQLCheck (SIGMOD 2020) and Yang et al. (ICSE 2018).
 *
 * <p>Pattern: {@code SELECT ..., (SELECT ... FROM inner_table WHERE inner_table.col =
 * outer_alias.col) ... FROM outer_table}
 *
 * <p>A correlated subquery in the SELECT list is re-evaluated for every row of the outer query,
 * leading to O(N) subquery executions. Rewriting as a LEFT JOIN + GROUP BY avoids this.
 *
 * @author haroya
 * @since 0.2.0
 */
public class CorrelatedSubqueryDetector implements DetectionRule {

  /**
   * Pattern to extract the FROM clause table names and aliases from the outer query. Matches: FROM
   * table [alias] and JOIN table [alias]
   */
  private static final Pattern OUTER_TABLE_ALIAS =
      Pattern.compile(
          "\\b(?:FROM|JOIN)\\s+(\\w+)(?:\\s+(?:AS\\s+)?(\\w+))?", Pattern.CASE_INSENSITIVE);

  // Pre-compiled pattern for qualified column references (alias.column).
  // Was previously compiled inside isCorrelated() on every invocation.
  private static final Pattern QUALIFIED_REF =
      Pattern.compile("(\\w+)\\.(\\w+)", Pattern.CASE_INSENSITIVE);

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

      if (hasCorrelatedSubqueryInSelect(sql)) {
        List<String> tables = EnhancedSqlParser.extractTableNames(sql);
        String table = tables.isEmpty() ? null : tables.get(0);

        issues.add(
            new Issue(
                IssueType.CORRELATED_SUBQUERY,
                Severity.WARNING,
                normalized,
                table,
                null,
                "Correlated subquery in SELECT clause executes once per outer row",
                "Rewrite as LEFT JOIN + GROUP BY to avoid per-row subquery execution.",
                query.stackTrace()));
      }
    }

    return issues;
  }

  /**
   * Check if the SQL contains a correlated subquery in the SELECT clause. A subquery is correlated
   * if its WHERE clause references an alias from the outer query.
   */
  boolean hasCorrelatedSubqueryInSelect(String sql) {
    // Find the outer FROM position to delimit the SELECT clause
    String upper = sql.toUpperCase();
    int outerFromIdx = findOuterFromIndex(sql, upper);
    if (outerFromIdx < 0) {
      return false;
    }

    String selectClause = sql.substring(0, outerFromIdx);

    // Collect outer query aliases (from the part after FROM)
    String afterFrom = sql.substring(outerFromIdx);
    Set<String> outerAliases = extractAliases(afterFrom);

    // Find subqueries in the SELECT clause
    List<String> subqueries = extractSubqueries(selectClause);
    for (String subquery : subqueries) {
      if (isCorrelated(subquery, outerAliases)) {
        return true;
      }
    }

    return false;
  }

  /** Find the position of the outer-level FROM keyword (not inside subqueries). */
  private int findOuterFromIndex(String sql, String upper) {
    int depth = 0;
    for (int i = 0; i < sql.length(); i++) {
      char c = sql.charAt(i);
      if (c == '(') {
        depth++;
      } else if (c == ')') {
        depth--;
      } else if (depth == 0 && i + 4 <= upper.length()) {
        if (upper.substring(i, i + 4).equals("FROM")
            && (i == 0 || !Character.isLetterOrDigit(sql.charAt(i - 1)))
            && (i + 4 >= sql.length() || !Character.isLetterOrDigit(sql.charAt(i + 4)))) {
          return i;
        }
      }
    }
    return -1;
  }

  /** Extract table names and aliases from a SQL fragment. */
  private Set<String> extractAliases(String sqlFragment) {
    Set<String> aliases = new LinkedHashSet<>();
    Matcher m = OUTER_TABLE_ALIAS.matcher(sqlFragment);
    while (m.find()) {
      String table = m.group(1);
      String alias = m.group(2);
      if (table != null) {
        aliases.add(table.toLowerCase());
      }
      if (alias != null) {
        aliases.add(alias.toLowerCase());
      }
    }
    return aliases;
  }

  /** Extract subquery strings from parenthesized SELECT expressions. */
  private List<String> extractSubqueries(String selectClause) {
    List<String> subqueries = new ArrayList<>();
    String upper = selectClause.toUpperCase();

    for (int i = 0; i < selectClause.length(); i++) {
      if (selectClause.charAt(i) == '(') {
        // Check if SELECT follows the opening paren
        int ahead = i + 1;
        while (ahead < selectClause.length()
            && Character.isWhitespace(selectClause.charAt(ahead))) {
          ahead++;
        }
        if (ahead + 6 <= upper.length() && upper.substring(ahead, ahead + 6).equals("SELECT")) {
          // Find matching closing paren
          int depth = 1;
          int start = i + 1;
          int j = start;
          while (j < selectClause.length() && depth > 0) {
            if (selectClause.charAt(j) == '(') depth++;
            else if (selectClause.charAt(j) == ')') depth--;
            if (depth > 0) j++;
          }
          if (depth == 0) {
            subqueries.add(selectClause.substring(start, j));
          }
        }
      }
    }
    return subqueries;
  }

  /** Check if a subquery references any of the outer aliases in its WHERE clause. */
  private boolean isCorrelated(String subquery, Set<String> outerAliases) {
    // Find WHERE clause body using safe clause boundary scanning
    String whereBody = SqlParser.extractWhereBody(subquery);
    if (whereBody == null) {
      return false;
    }

    // Collect the subquery's own table names and aliases to avoid false positives
    // when a subquery references its own table that happens to share a name with an outer alias.
    Set<String> innerAliases = extractAliases(subquery);

    // Check if any outer alias is referenced as a table qualifier: alias.column
    // Only flag if the qualifier matches an outer alias but NOT an inner alias
    Matcher refMatcher = QUALIFIED_REF.matcher(whereBody);
    while (refMatcher.find()) {
      String qualifier = refMatcher.group(1).toLowerCase();
      if (outerAliases.contains(qualifier) && !innerAliases.contains(qualifier)) {
        return true;
      }
    }

    return false;
  }
}
