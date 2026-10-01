package io.queryaudit.core.parser;

import java.util.List;
import java.util.OptionalLong;

/**
 * Fast regex-based SQL parser. Always available, no external dependencies.
 *
 * <p>Use this class for simple pattern checks ({@code isSelectQuery}, {@code hasWhereClause},
 * {@code hasSelectAll}, etc.) where regex is perfectly adequate.
 *
 * <p>For complex structural extraction (WHERE columns, JOIN columns, table names), prefer {@link
 * EnhancedSqlParser} instead — it uses the required JSqlParser dependency for AST-level parsing and
 * falls back to the regex methods in this class for unsupported statements.
 *
 * @author haroya
 * @since 0.2.0
 */
public final class SqlParser {
  private SqlParser() {}

  /**
   * Strips CTE (WITH ... AS (...)) prefix from a SQL query, returning only the main query body.
   * Handles multiple CTEs, nested parentheses, and the RECURSIVE keyword.
   */
  public static String stripCtePrefix(String sql) {
    return SqlStatementScope.stripCtePrefix(sql);
  }

  // -- stripComments ----------------------------------------------------------

  /**
   * Strips SQL comments from the input while preserving content inside string literals. Handles
   * block comments (including nested), line comments, and preserves comment-like content inside
   * string literals.
   *
   * @param sql the SQL string to strip comments from
   * @return the SQL with comments replaced by a single space, or null if input is null
   */
  public static String stripComments(String sql) {
    return SqlText.stripComments(sql);
  }

  /**
   * Normalize a SQL query by replacing literal values with {@code ?}, collapsing whitespace, and
   * lowercasing. Useful for grouping identical query patterns (e.g. N+1 detection).
   */
  public static String normalize(String sql) {
    return SqlText.normalize(sql);
  }

  /**
   * Replaces single-quoted string literals with {@code ?}, handling SQL-standard escaped quotes
   * ({@code ''}) and MySQL backslash escaping ({@code \'}). Double-quoted identifiers are preserved
   * per SQL standard (PostgreSQL, Oracle, SQL Server). Uses a manual loop instead of regex to avoid
   * StackOverflowError on large inputs.
   *
   * <p>Public so other modules (notably suppression-pattern matching in {@code QueryAuditConfig})
   * can mask literals before doing structural comparisons against the SQL.
   *
   * @since 0.4.0
   */
  public static String replaceStringLiterals(String sql) {
    return SqlText.replaceStringLiterals(sql);
  }

  // ── isSelectQuery ──────────────────────────────────────────────────

  // Query type detection uses simple string prefix checks instead of regex.
  // These methods are called for every single query, so avoiding Matcher
  // allocation and regex engine overhead is a meaningful win.

  public static boolean isSelectQuery(String sql) {
    return SqlStatementPatterns.isSelectQuery(sql);
  }

  // ── DML query type detection ────────────────────────────────────────

  public static boolean isInsertQuery(String sql) {
    return SqlStatementPatterns.isInsertQuery(sql);
  }

  public static boolean isUpdateQuery(String sql) {
    return SqlStatementPatterns.isUpdateQuery(sql);
  }

  public static boolean isDeleteQuery(String sql) {
    return SqlStatementPatterns.isDeleteQuery(sql);
  }

  public static boolean isDmlQuery(String sql) {
    return SqlStatementPatterns.isDmlQuery(sql);
  }

  /** Extracts the target table name from an UPDATE statement. */
  public static String extractUpdateTable(String sql) {
    return SqlStatementPatterns.extractUpdateTable(sql);
  }

  /** Extracts the target table name from a DELETE statement. */
  public static String extractDeleteTable(String sql) {
    return SqlStatementPatterns.extractDeleteTable(sql);
  }

  /** Extracts the target table name from an INSERT statement. */
  public static String extractInsertTable(String sql) {
    return SqlStatementPatterns.extractInsertTable(sql);
  }

  // ── hasWhereClause ──────────────────────────────────────────────────

  /** Returns true if the SQL contains a WHERE clause. */
  public static boolean hasWhereClause(String sql) {
    return SqlClauseBodies.hasWhereClause(sql);
  }

  /**
   * Returns true if the outer SQL (ignoring subqueries) contains a WHERE clause. This avoids false
   * negatives where a WHERE inside a subquery masks a missing outer WHERE.
   */
  public static boolean hasOuterWhereClause(String sql) {
    return SqlClauseBodies.hasOuterWhereClause(sql);
  }

  public static boolean hasSelectAll(String sql) {
    return SqlStatementPatterns.hasSelectAll(sql);
  }

  /**
   * Extracts the WHERE clause body by finding WHERE keyword and then scanning for the nearest
   * clause terminator. This is O(n) per terminator pattern, avoiding the O(n^2) worst-case of (.+?)
   * with alternation terminators.
   *
   * @return the WHERE clause body (without the WHERE keyword), or null if no WHERE found
   */
  public static String extractWhereBody(String sql) {
    return SqlClauseBodies.extractWhereBody(sql);
  }

  public static List<ColumnReference> extractWhereColumns(String sql) {
    return SqlColumnReferences.extractWhereColumns(sql);
  }

  /**
   * Extract WHERE columns along with their operators (e.g., "=", "IS", "LIKE"). This enables
   * smarter analysis such as soft-delete detection (IS NULL) and equality vs range discrimination.
   */
  public static List<WhereColumnReference> extractWhereColumnsWithOperators(String sql) {
    return SqlColumnReferences.extractWhereColumnsWithOperators(sql);
  }

  public static List<JoinColumnPair> extractJoinColumns(String sql) {
    return SqlColumnReferences.extractJoinColumns(sql);
  }

  public static List<ColumnReference> extractOrderByColumns(String sql) {
    return SqlColumnReferences.extractOrderByColumns(sql);
  }

  public static List<ColumnReference> extractGroupByColumns(String sql) {
    return SqlColumnReferences.extractGroupByColumns(sql);
  }

  /**
   * Extracts the HAVING clause body from a SQL query, or null if not present. Uses manual clause
   * boundary scanning to avoid regex backtracking.
   */
  public static String extractHavingClause(String sql) {
    return SqlClauseBodies.extractHavingClause(sql);
  }

  /**
   * Extracts the HAVING clause body, similar to {@link #extractWhereBody(String)}. Handles HAVING
   * after GROUP BY, HAVING before ORDER BY/LIMIT, and subqueries within HAVING.
   *
   * @return the HAVING clause body (without the HAVING keyword), or null if no HAVING found
   */
  public static String extractHavingBody(String sql) {
    return SqlClauseBodies.extractHavingBody(sql);
  }

  /**
   * Detects function usage in HAVING clause that may disable index usage or indicate non-sargable
   * expressions. Works like {@link #detectWhereFunctions(String)} but for HAVING.
   */
  public static List<FunctionUsage> detectHavingFunctions(String sql) {
    return SqlFunctionExpressions.detectHavingFunctions(sql);
  }

  /**
   * Extracts the JOIN ON clause bodies for all JOINs found in the SQL.
   *
   * @return list of ON clause body strings
   */
  public static List<String> extractJoinOnBodies(String sql) {
    return SqlClauseBodies.extractJoinOnBodies(sql);
  }

  /**
   * Detects function usage in WHERE clause that disables index usage.
   *
   * <p>Improvement: If a function wraps a column on the comparison-value side (not the column being
   * searched/indexed), it is skipped. E.g., {@code WHERE m.id > COALESCE(rm.last_read, 0)} - the
   * index is on {@code m.id}, the function wraps {@code rm.last_read} which is the comparison
   * value, so no issue.
   */
  public static List<FunctionUsage> detectWhereFunctions(String sql) {
    return SqlFunctionExpressions.detectWhereFunctions(sql);
  }

  /**
   * Detects function usage in JOIN ON conditions that disable index usage.
   *
   * <p>Only flags functions that wrap columns on the LOOKUP side (the table that needs index
   * access), not the DRIVING side.
   *
   * <ul>
   *   <li>For LEFT JOIN: the right (joined) table is the lookup table
   *   <li>For RIGHT JOIN: the left (FROM) table is the lookup table
   *   <li>For INNER/CROSS JOIN: both sides need index access, flag both
   * </ul>
   */
  public static List<FunctionUsage> detectJoinFunctions(String sql) {
    return SqlFunctionExpressions.detectJoinFunctions(sql);
  }

  public static int countOrConditions(String sql) {
    return SqlOrPredicates.countOrConditions(sql);
  }

  /**
   * Counts effective OR conditions by excluding optional parameter patterns ({@code (? IS NULL OR
   * column = ?)}) which are short-circuited at bind time and are not real OR abuse.
   *
   * @return the number of OR conditions excluding optional parameter patterns
   */
  public static int countEffectiveOrConditions(String sql) {
    return SqlOrPredicates.countEffectiveOrConditions(sql);
  }

  /**
   * Extracts the column name referenced in each OR branch of the WHERE clause. Used to determine
   * whether every OR-branched column has its own index, enabling index_merge optimisation.
   *
   * @return list of lowercase column names (one per OR branch); empty list if unparseable
   */
  public static List<String> extractOrBranchColumns(String sql) {
    return SqlOrPredicates.extractOrBranchColumns(sql);
  }

  /**
   * Check whether all OR conditions in the WHERE clause reference the same column. This is
   * equivalent to an IN clause (e.g., "type = 'A' OR type = 'B'" is the same as "type IN ('A',
   * 'B')"), which MySQL optimizes identically.
   *
   * @return true if all OR-separated conditions reference the same column
   */
  public static boolean allOrConditionsOnSameColumn(String sql) {
    return SqlOrPredicates.allOrConditionsOnSameColumn(sql);
  }

  public static OptionalLong extractOffsetValue(String sql) {
    return SqlStatementPatterns.extractOffsetValue(sql);
  }

  /**
   * Returns true if the SQL contains an OFFSET clause (either with a literal value or a
   * parameterized placeholder). This is useful for detecting potential large OFFSET usage in JPA
   * queries where the actual value is always parameterized.
   */
  public static boolean hasOffsetClause(String sql) {
    return SqlStatementPatterns.hasOffsetClause(sql);
  }

  public static List<String> extractTableNames(String sql) {
    return SqlTableNames.extractTableNames(sql);
  }

  /**
   * Remove subqueries (nested SELECT statements) from SQL to avoid parsing their internals. Uses a
   * simple parenthesis-depth approach.
   */
  public static String removeSubqueries(String sql) {
    return SqlStatementScope.removeSubqueries(sql);
  }
}
