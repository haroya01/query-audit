package io.queryaudit.core.parser;

import java.util.List;

/**
 * Stable structural-extraction facade. The routing policy distinguishes unsupported/oversized SQL
 * from a broken required parser installation; extractors own only their particular SQL structure.
 *
 * <p>Existing extraction fallbacks retain the baseline {@link SqlParser} contract. Conservative
 * proof-evidence operations return no evidence when structural parsing is unavailable for the SQL;
 * this is not a second configurable parser.
 *
 * @author haroya
 * @since 0.2.0
 */
public final class EnhancedSqlParser {
  // Eager validation preserves the required-runtime contract, even for simple facade operations.
  private static final String PARSER_VERSION = SqlAstParser.version();

  private EnhancedSqlParser() {}

  /**
   * Returns true: JSqlParser is a required runtime dependency.
   *
   * @deprecated use {@link #parserName()} and {@link #parserVersion()}
   */
  @Deprecated(since = "0.6.0")
  public static boolean isJSqlParserAvailable() {
    return true;
  }

  /** Returns the structural parser used by this installation. */
  public static String parserName() {
    return "JSqlParser";
  }

  /** Returns the version packaged with the loaded required dependency. */
  public static String parserVersion() {
    return PARSER_VERSION;
  }

  /** Extract WHERE clause columns. */
  public static List<ColumnReference> extractWhereColumns(String sql) {
    return SqlParserRouting.list(
        sql, SqlAstColumns::extractWhereColumns, SqlParser::extractWhereColumns);
  }

  /** Extract JOIN column pairs. */
  public static List<JoinColumnPair> extractJoinColumns(String sql) {
    return SqlParserRouting.list(
        sql, SqlAstColumns::extractJoinColumns, SqlParser::extractJoinColumns);
  }

  /** Extract table names from FROM / JOIN / UPDATE / DELETE targets in scan order. */
  public static List<String> extractTableNames(String sql) {
    return SqlParserRouting.list(
        sql, SqlAstTableNames::extractTableNames, SqlParser::extractTableNames);
  }

  /** Extract plain column references from the outer ORDER BY clause; functions are skipped. */
  public static List<ColumnReference> extractOrderByColumns(String sql) {
    return SqlParserRouting.list(
        sql, SqlAstColumns::extractOrderByColumns, SqlParser::extractOrderByColumns);
  }

  /** Extract plain column references from the outer GROUP BY clause; functions are skipped. */
  public static List<ColumnReference> extractGroupByColumns(String sql) {
    return SqlParserRouting.list(
        sql, SqlAstColumns::extractGroupByColumns, SqlParser::extractGroupByColumns);
  }

  /** Extract WHERE columns with their comparison operators (=, IS, LIKE, IN, BETWEEN, etc.). */
  public static List<WhereColumnReference> extractWhereColumnsWithOperators(String sql) {
    return SqlParserRouting.list(
        sql,
        SqlAstColumns::extractWhereColumnsWithOperators,
        SqlParser::extractWhereColumnsWithOperators);
  }

  /**
   * Returns immutable, distinct column evidence for necessary scalar equalities in a single
   * base-table SELECT. Aliases are resolved to the unquoted table name. Only positive conjunctions
   * of column-to-literal/bind equality contribute evidence; OR, NOT, column-valued expressions and
   * subqueries do not. Joins, CTEs, derived tables and unsupported/oversized SQL yield no evidence.
   *
   * <p>This does not establish uniqueness: consumers must check complete constraints in metadata.
   */
  public static List<ColumnReference> extractRequiredScalarEqualities(String sql) {
    return List.copyOf(
        SqlParserRouting.list(
            sql, SqlAstEqualities::extractRequiredScalarEqualities, ignored -> List.of()));
  }

  /** Extract each JOIN's ON-clause body as a string, one entry per JOIN. */
  public static List<String> extractJoinOnBodies(String sql) {
    return SqlParserRouting.list(
        sql, SqlAstClauses::extractJoinOnBodies, SqlParser::extractJoinOnBodies);
  }

  /** Extract the HAVING clause body (without the HAVING keyword), or null if absent. */
  public static String extractHavingClause(String sql) {
    return SqlParserRouting.text(
        sql, SqlAstClauses::extractHavingClause, SqlParser::extractHavingClause);
  }

  /** Extract the WHERE clause body (without the WHERE keyword), or null if absent. */
  public static String extractWhereBody(String sql) {
    return SqlParserRouting.text(sql, SqlAstClauses::extractWhereBody, SqlParser::extractWhereBody);
  }

  /** Replace nested SELECTs with {@code (?)} without changing the no-subquery fast path. */
  public static String removeSubqueries(String sql) {
    return SqlParserRouting.rewriteSubqueries(sql);
  }

  /** Normalization does not require an AST. */
  public static String normalize(String sql) {
    return SqlParser.normalize(sql);
  }

  public static boolean hasSelectAll(String sql) {
    return SqlParser.hasSelectAll(sql);
  }

  public static boolean hasWhereClause(String sql) {
    return SqlParser.hasWhereClause(sql);
  }

  public static boolean isSelectQuery(String sql) {
    return SqlParser.isSelectQuery(sql);
  }
}
