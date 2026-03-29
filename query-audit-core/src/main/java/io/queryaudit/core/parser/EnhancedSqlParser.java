package io.queryaudit.core.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * JSqlParser-backed SQL parser for complex structural extraction (WHERE columns, JOIN columns,
 * table names). Falls back to the regex-based {@link SqlParser} when JSqlParser is not on the
 * classpath or fails to parse a query.
 *
 * <p>This is the recommended entry point for extraction methods that benefit from AST-level
 * accuracy (CTEs, window functions, nested subqueries). Simple pattern checks ({@code
 * isSelectQuery}, {@code hasWhereClause}, etc.) should use {@link SqlParser} directly — regex is
 * perfectly adequate for those.
 *
 * <p>JSqlParser is an optional dependency ({@code compileOnly}). All methods in this class are
 * safe to call regardless of whether JSqlParser is on the classpath.
 *
 * @author haroya
 * @since 0.2.0
 */
public final class EnhancedSqlParser {

  private static final System.Logger LOG = System.getLogger(EnhancedSqlParser.class.getName());

  private static final boolean JSQLPARSER_AVAILABLE;

  static {
    boolean available;
    try {
      Class.forName("net.sf.jsqlparser.parser.CCJSqlParserUtil");
      available = true;
    } catch (ClassNotFoundException e) {
      available = false;
    }
    JSQLPARSER_AVAILABLE = available;
  }

  private EnhancedSqlParser() {}

  /** Returns true if JSqlParser is on the classpath and available for use. */
  public static boolean isJSqlParserAvailable() {
    return JSQLPARSER_AVAILABLE;
  }

  // ── WHERE columns ──────────────────────────────────────────────────

  /**
   * Extract WHERE clause columns. Uses JSqlParser when available for accurate parsing of complex
   * SQL (CTEs, nested queries, etc.), falling back to regex-based {@link
   * SqlParser#extractWhereColumns(String)}.
   */
  public static List<ColumnReference> extractWhereColumns(String sql) {
    if (sql == null) {
      return List.of();
    }
    if (JSQLPARSER_AVAILABLE) {
      try {
        return JSqlParserDelegate.extractWhereColumns(sql);
      } catch (Exception e) {
        LOG.log(
            System.Logger.Level.DEBUG,
            "JSqlParser failed, falling back to regex: {0}",
            e.getMessage());
      }
    }
    return SqlParser.extractWhereColumns(sql);
  }

  // ── JOIN columns ───────────────────────────────────────────────────

  /**
   * Extract JOIN column pairs. Uses JSqlParser when available, falling back to regex-based {@link
   * SqlParser#extractJoinColumns(String)}.
   */
  public static List<JoinColumnPair> extractJoinColumns(String sql) {
    if (sql == null) {
      return List.of();
    }
    if (JSQLPARSER_AVAILABLE) {
      try {
        return JSqlParserDelegate.extractJoinColumns(sql);
      } catch (Exception e) {
        LOG.log(
            System.Logger.Level.DEBUG,
            "JSqlParser failed, falling back to regex: {0}",
            e.getMessage());
      }
    }
    return SqlParser.extractJoinColumns(sql);
  }

  // ── Table names ────────────────────────────────────────────────────

  /**
   * Extract table names from SQL. Uses JSqlParser when available, falling back to regex-based
   * {@link SqlParser#extractTableNames(String)}.
   */
  public static List<String> extractTableNames(String sql) {
    if (sql == null) {
      return List.of();
    }
    if (JSQLPARSER_AVAILABLE) {
      try {
        return JSqlParserDelegate.extractTableNames(sql);
      } catch (Exception e) {
        LOG.log(
            System.Logger.Level.DEBUG,
            "JSqlParser failed, falling back to regex: {0}",
            e.getMessage());
      }
    }
    return SqlParser.extractTableNames(sql);
  }

  // ── Simple delegations (regex is sufficient) ───────────────────────

  /** Normalize SQL — regex approach is optimal for this; no JSqlParser needed. */
  public static String normalize(String sql) {
    return SqlParser.normalize(sql);
  }

  /** Check for SELECT * — regex approach handles this well. */
  public static boolean hasSelectAll(String sql) {
    return SqlParser.hasSelectAll(sql);
  }

  /** Check for WHERE clause — regex approach handles this well. */
  public static boolean hasWhereClause(String sql) {
    return SqlParser.hasWhereClause(sql);
  }

  /** Query type detection — regex / prefix check is ideal here. */
  public static boolean isSelectQuery(String sql) {
    return SqlParser.isSelectQuery(sql);
  }

  // ── Inner delegate (loaded only when JSqlParser is on classpath) ───

  /**
   * Isolated delegate class that references JSqlParser types. This class is only loaded when
   * JSqlParser is on the classpath, avoiding {@link NoClassDefFoundError} at class-load time.
   */
  static final class JSqlParserDelegate {

    private JSqlParserDelegate() {}

    static List<ColumnReference> extractWhereColumns(String sql) throws Exception {
      net.sf.jsqlparser.statement.Statement statement =
          net.sf.jsqlparser.parser.CCJSqlParserUtil.parse(sql);

      if (!(statement instanceof net.sf.jsqlparser.statement.select.Select selectStmt)) {
        return SqlParser.extractWhereColumns(sql);
      }

      net.sf.jsqlparser.expression.Expression where = extractWhereExpression(selectStmt);
      if (where == null) {
        return List.of();
      }

      List<ColumnReference> result = new ArrayList<>();
      where.accept(
          new net.sf.jsqlparser.expression.ExpressionVisitorAdapter<Void>() {
            @Override
            public <S> Void visit(net.sf.jsqlparser.schema.Column column, S context) {
              String tableName = column.getTable() != null ? column.getTable().getName() : null;
              String colName = column.getColumnName();
              if (colName != null) {
                result.add(new ColumnReference(tableName, colName));
              }
              return null;
            }
          });
      return result;
    }

    static List<JoinColumnPair> extractJoinColumns(String sql) throws Exception {
      net.sf.jsqlparser.statement.Statement statement =
          net.sf.jsqlparser.parser.CCJSqlParserUtil.parse(sql);

      if (!(statement instanceof net.sf.jsqlparser.statement.select.Select selectStmt)) {
        return SqlParser.extractJoinColumns(sql);
      }

      List<JoinColumnPair> result = new ArrayList<>();
      net.sf.jsqlparser.statement.select.PlainSelect plainSelect = extractPlainSelect(selectStmt);

      if (plainSelect == null || plainSelect.getJoins() == null) {
        return result;
      }

      for (net.sf.jsqlparser.statement.select.Join join : plainSelect.getJoins()) {
        if (join.getOnExpressions() != null) {
          for (net.sf.jsqlparser.expression.Expression onExpr : join.getOnExpressions()) {
            extractJoinPairsFromExpression(onExpr, result);
          }
        }
      }
      return result;
    }

    static List<String> extractTableNames(String sql) throws Exception {
      List<String> result = new ArrayList<>();
      Set<String> tables = net.sf.jsqlparser.util.TablesNamesFinder.findTables(sql);
      for (String table : tables) {
        // Remove backticks, double quotes, and schema qualifiers
        String cleaned = table.replace("`", "").replace("\"", "");
        if (cleaned.contains(".")) {
          cleaned = cleaned.substring(cleaned.lastIndexOf('.') + 1);
        }
        if (!result.contains(cleaned)) {
          result.add(cleaned);
        }
      }
      return result;
    }

    // ── helpers ────────────────────────────────────────────────────

    private static net.sf.jsqlparser.expression.Expression extractWhereExpression(
        net.sf.jsqlparser.statement.select.Select select) {
      net.sf.jsqlparser.statement.select.PlainSelect ps = extractPlainSelect(select);
      return ps != null ? ps.getWhere() : null;
    }

    private static net.sf.jsqlparser.statement.select.PlainSelect extractPlainSelect(
        net.sf.jsqlparser.statement.select.Select select) {
      // getPlainSelect() handles CTEs, parenthesized selects, etc.
      return select.getPlainSelect();
    }

    private static void extractJoinPairsFromExpression(
        net.sf.jsqlparser.expression.Expression expr, List<JoinColumnPair> result) {
      if (expr instanceof net.sf.jsqlparser.expression.operators.relational.EqualsTo eq) {
        net.sf.jsqlparser.expression.Expression left = eq.getLeftExpression();
        net.sf.jsqlparser.expression.Expression right = eq.getRightExpression();
        if (left instanceof net.sf.jsqlparser.schema.Column leftCol
            && right instanceof net.sf.jsqlparser.schema.Column rightCol) {
          ColumnReference leftRef =
              new ColumnReference(
                  leftCol.getTable() != null ? leftCol.getTable().getName() : null,
                  leftCol.getColumnName());
          ColumnReference rightRef =
              new ColumnReference(
                  rightCol.getTable() != null ? rightCol.getTable().getName() : null,
                  rightCol.getColumnName());
          result.add(new JoinColumnPair(leftRef, rightRef));
        }
      } else if (expr
          instanceof net.sf.jsqlparser.expression.operators.conditional.AndExpression and) {
        extractJoinPairsFromExpression(and.getLeftExpression(), result);
        extractJoinPairsFromExpression(and.getRightExpression(), result);
      }
    }
  }
}
