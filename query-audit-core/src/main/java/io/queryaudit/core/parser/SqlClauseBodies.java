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

  private static final Pattern ORDER_BY_START =
      Pattern.compile("\\bORDER\\s+BY\\b", Pattern.CASE_INSENSITIVE);
  private static final Pattern[] ORDER_BY_TERMINATORS = {
    Pattern.compile("\\bLIMIT\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bOFFSET\\b", Pattern.CASE_INSENSITIVE),
  };
  private static final Pattern GROUP_BY_START =
      Pattern.compile("\\bGROUP\\s+BY\\b", Pattern.CASE_INSENSITIVE);
  private static final Pattern[] GROUP_BY_TERMINATORS = {
    Pattern.compile("\\bHAVING\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bORDER\\s+BY\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bLIMIT\\b", Pattern.CASE_INSENSITIVE),
  };

  static String orderByBody(String sql) {
    return findBody(sql, ORDER_BY_START, ORDER_BY_TERMINATORS);
  }

  static String groupByBody(String sql) {
    return findBody(sql, GROUP_BY_START, GROUP_BY_TERMINATORS);
  }

  private static String findBody(String sql, Pattern start, Pattern[] terminators) {
    if (sql == null) return null;
    Matcher matcher = start.matcher(sql);
    return matcher.find() ? extractClauseBody(sql, matcher.end(), terminators) : null;
  }

  // ── hasWhereClause ──────────────────────────────────────────────────

  /** Returns true if the SQL contains a WHERE clause. */
  static boolean hasWhereClause(String sql) {
    if (sql == null) return false;
    return WHERE_START.matcher(sql).find();
  }

  /**
   * Returns true if the outer SQL (ignoring subqueries) contains a WHERE clause. This avoids false
   * negatives where a WHERE inside a subquery masks a missing outer WHERE.
   */
  static boolean hasOuterWhereClause(String sql) {
    if (sql == null) return false;
    String cleaned = removeSubqueries(sql);
    return WHERE_START.matcher(cleaned).find();
  }

  // ── extractWhereColumns ────────────────────────────────────────────

  /** Keyword boundary pattern for finding WHERE keyword start position. */
  private static final Pattern WHERE_START =
      Pattern.compile("\\bWHERE\\b", Pattern.CASE_INSENSITIVE);

  /**
   * Clause terminators that end a WHERE body. Searched via manual scanning to avoid catastrophic
   * backtracking from (.+?) patterns with DOTALL.
   */
  private static final Pattern[] WHERE_TERMINATORS = {
    Pattern.compile("\\bGROUP\\s+BY\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bORDER\\s+BY\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bLIMIT\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bHAVING\\b", Pattern.CASE_INSENSITIVE),
  };

  /**
   * Extracts the WHERE clause body by finding WHERE keyword and then scanning for the nearest
   * clause terminator. This is O(n) per terminator pattern, avoiding the O(n^2) worst-case of (.+?)
   * with alternation terminators.
   *
   * @return the WHERE clause body (without the WHERE keyword), or null if no WHERE found
   */
  static String extractWhereBody(String sql) {
    if (sql == null) return null;
    String effective = stripComments(sql);
    effective = stripCtePrefix(effective);
    Matcher m = WHERE_START.matcher(effective);
    if (!m.find()) return null;
    int bodyStart = m.end();
    return extractClauseBody(effective, bodyStart, WHERE_TERMINATORS);
  }

  /**
   * Given a start position inside the SQL string, finds the nearest terminator and returns the
   * substring between start and the terminator (or end of string).
   */
  static String extractClauseBody(String sql, int bodyStart, Pattern[] terminators) {
    int bodyEnd = sql.length();
    for (Pattern terminator : terminators) {
      Matcher tm = terminator.matcher(sql);
      if (tm.find(bodyStart) && tm.start() < bodyEnd) {
        bodyEnd = tm.start();
      }
    }
    if (bodyStart >= bodyEnd) return null;
    return sql.substring(bodyStart, bodyEnd);
  }

  // ── extractHavingClause ──────────────────────────────────────────

  private static final Pattern HAVING_START =
      Pattern.compile("\\bHAVING\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern[] HAVING_TERMINATORS = {
    Pattern.compile("\\bORDER\\s+BY\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bLIMIT\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bUNION\\b", Pattern.CASE_INSENSITIVE),
  };

  /**
   * Extracts the HAVING clause body from a SQL query, or null if not present. Uses manual clause
   * boundary scanning to avoid regex backtracking.
   */
  static String extractHavingClause(String sql) {
    if (sql == null) {
      return null;
    }
    Matcher m = HAVING_START.matcher(sql);
    if (!m.find()) {
      return null;
    }
    String body = extractClauseBody(sql, m.end(), HAVING_TERMINATORS);
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
      String onBody = extractClauseBody(sql, joinMatcher.end(), JOIN_ON_TERMINATORS);
      if (onBody != null) {
        result.add(
            new JoinClause(
                joinMatcher.group(1), joinMatcher.group(2), joinMatcher.group(3), onBody));
      }
    }
    return result;
  }

  // ── detectJoinFunctions ─────────────────────────────────────────────

  /**
   * Pattern to match JOIN header: type, table name, optional alias, and ON keyword. The ON clause
   * body is extracted via manual scanning (extractJoinOnBody) to avoid catastrophic backtracking
   * from (.+?) with DOTALL.
   */
  private static final Pattern JOIN_HEADER =
      Pattern.compile(
          "\\b(LEFT|RIGHT|INNER|CROSS|FULL)?\\s*(?:OUTER\\s+)?JOIN\\s+[`\"]?(\\w+)[`\"]?(?:\\s+(?:AS\\s+)?[`\"]?(\\w+)[`\"]?)?\\s+ON\\s+",
          Pattern.CASE_INSENSITIVE);

  /**
   * Terminators that end a JOIN ON clause body. Used by extractJoinOnBody for manual boundary
   * scanning, replacing the previous (.+?) with DOTALL approach that caused catastrophic
   * backtracking.
   */
  private static final Pattern[] JOIN_ON_TERMINATORS = {
    Pattern.compile(
        "\\b(?:LEFT|RIGHT|INNER|CROSS|FULL)?\\s*(?:OUTER\\s+)?JOIN\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bWHERE\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bGROUP\\s+BY\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bORDER\\s+BY\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bLIMIT\\b", Pattern.CASE_INSENSITIVE),
    Pattern.compile("\\bHAVING\\b", Pattern.CASE_INSENSITIVE),
  };
}
