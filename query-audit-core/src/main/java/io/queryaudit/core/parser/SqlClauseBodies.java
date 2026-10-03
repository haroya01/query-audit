package io.queryaudit.core.parser;

import static io.queryaudit.core.parser.SqlStatementScope.removeSubqueries;
import static io.queryaudit.core.parser.SqlStatementScope.stripCtePrefix;
import static io.queryaudit.core.parser.SqlText.stripComments;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Baseline clause-boundary grammar shared by column and expression extraction. Preserves each
 * clause's preprocessing contract.
 */
final class SqlClauseBodies {
  private SqlClauseBodies() {}

  // ── extractWhereBody ────────────────────────────────────────────────

  /**
   * Extracts the WHERE clause body by finding WHERE keyword and then scanning for the nearest
   * clause terminator. Uses literal-aware scanner with parenthesis depth tracking to avoid
   * false terminators inside subqueries, string literals, quoted identifiers, or comments.
   *
   * @return the WHERE clause body (without the WHERE keyword), or null if no WHERE found
   */
  static String extractWhereBody(String sql) {
    if (sql == null) return null;
    String effective = stripComments(sql);
    effective = stripCtePrefix(effective);
    return SqlSourceScanner.clauseBody(
        effective,
        "WHERE",
        "GROUP BY",
        "ORDER BY",
        "LIMIT",
        "HAVING",
        "UNION",
        "FETCH");
  }

  /**
   * Returns true if the SQL contains a WHERE clause.
   */
  static boolean hasWhereClause(String sql) {
    if (sql == null) return false;
    return SqlSourceScanner.scanForKeyword(sql, 0, "WHERE") >= 0;
  }

  /**
   * Returns true if the outer SQL (ignoring subqueries) contains a WHERE clause. This avoids false
   * negatives where a WHERE inside a subquery masks a missing outer WHERE.
   */
  static boolean hasOuterWhereClause(String sql) {
    if (sql == null) return false;
    String cleaned = removeSubqueries(sql);
    return SqlSourceScanner.scanForKeyword(cleaned, 0, "WHERE") >= 0;
  }

  /**
   * Returns true if the statement's own FROM scope has a JOIN clause. Shares the literal-aware
   * scanner with {@link #hasOuterWhereClause(String)}, so a JOIN spelled out inside a literal, a
   * quoted identifier or a comment is not a clause, and a JOIN belonging to a nested subquery does
   * not count for the outer statement.
   */
  static boolean hasOuterJoinClause(String sql) {
    return SqlSourceScanner.hasTopLevelClause(sql, "JOIN");
  }

  /**
   * Returns true if the statement's own FROM scope has a USING clause, as used by the PostgreSQL
   * {@code DELETE ... USING ...} form. Scanned the same way as {@link #hasOuterJoinClause(String)}:
   * only a top-level USING followed by a table reference is a clause.
   */
  static boolean hasOuterUsingClause(String sql) {
    return SqlSourceScanner.hasTopLevelClause(sql, "USING");
  }

  // ── extractOrderByBody ──────────────────────────────────────────────

  /**
   * Extracts the ORDER BY clause body.
   */
  static String orderByBody(String sql) {
    if (sql == null) return null;
    String effective = stripComments(sql);
    effective = stripCtePrefix(effective);
    return SqlSourceScanner.clauseBody(effective, "ORDER BY", "LIMIT", "OFFSET", "FETCH");
  }

  // ── extractGroupByBody ──────────────────────────────────────────────

  /**
   * Extracts the GROUP BY clause body.
   */
  static String groupByBody(String sql) {
    if (sql == null) return null;
    String effective = stripComments(sql);
    effective = stripCtePrefix(effective);
    return SqlSourceScanner.clauseBody(
        effective, "GROUP BY", "HAVING", "ORDER BY", "LIMIT", "FETCH");
  }

  // ── extractHavingClause ────────────────────────────────────────────

  /**
   * Extracts the HAVING clause body from a SQL query, or null if not present. Uses literal-aware
   * scanner with parenthesis depth tracking.
   *
   * @return the HAVING clause body (without the HAVING keyword), or null if no HAVING found
   */
  static String extractHavingClause(String sql) {
    if (sql == null) {
      return null;
    }
    String effective = stripComments(sql);
    String body = SqlSourceScanner.clauseBody(
        effective, "HAVING", "ORDER BY", "LIMIT", "UNION", "FETCH");
    return body != null ? body.trim() : null;
  }

  /**
   * Extracts the HAVING clause body, similar to {@link #extractWhereBody(String)}. Handles HAVING
   * after GROUP BY, HAVING before ORDER BY/LIMIT, and subqueries within HAVING.
   *
   * @return the HAVING clause body (without the HAVING keyword), or null if no HAVING found
   */
  static String extractHavingBody(String sql) {
    return extractHavingClause(sql);
  }

  // ── extractJoinOnBodies ─────────────────────────────────────────────

  /**
   * Extracts the JOIN ON clause bodies for all JOINs found in the SQL.
   *
   * @return list of ON clause body strings
   */
  static List<String> extractJoinOnBodies(String sql) {
    List<String> result = new ArrayList<>();
    if (sql == null) {
      return result;
    }
    String effective = stripCtePrefix(sql);
    for (JoinClause join : joinClauses(effective)) {
      result.add(join.body());
    }
    return result;
  }

  record JoinClause(String type, String table, String alias, String body) {}

  /** Parses headers and bodies together so JOIN consumers do not repeat boundary grammar. */
  static List<JoinClause> joinClauses(String sql) {
    List<JoinClause> result = new ArrayList<>();
    Matcher joinMatcher = JOIN_HEADER.matcher(sql);
    while (joinMatcher.find()) {
      int bodyStart = joinMatcher.end();
      String onBody = SqlSourceScanner.clauseBodyFrom(sql, bodyStart,
          "JOIN", "WHERE", "GROUP BY", "ORDER BY", "LIMIT", "HAVING");
      if (onBody != null) {
        result.add(
            new JoinClause(
                joinMatcher.group(1), joinMatcher.group(2), joinMatcher.group(3), onBody));
      }
    }
    return result;
  }

  // Pattern to match JOIN header: type, table name, optional alias, and ON keyword
  private static final Pattern JOIN_HEADER =
      Pattern.compile(
          "\\b(LEFT|RIGHT|INNER|CROSS|FULL)?\\s*(?:OUTER\\s+)?JOIN\\s+[`\"]?(\\w+)[`\"]?(?:\\s+(?:AS\\s+)?[`\"]?(\\w+)[`\"]?)?\\s+ON\\s+",
          Pattern.CASE_INSENSITIVE);
}
