package io.queryaudit.core.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.statement.update.Update;
import net.sf.jsqlparser.util.TablesNamesFinder;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import net.sf.jsqlparser.util.deparser.StatementDeParser;

/**
 * JSqlParser-backed SQL parser for extraction methods that benefit from AST-level accuracy. Falls
 * back to {@link SqlParser} when JSqlParser is not on the classpath or fails to parse.
 *
 * <p>JSqlParser is an optional dependency ({@code compileOnly}). All JSqlParser types are used only
 * inside {@link JSqlParserDelegate}, which the JVM loads lazily when {@link #JSQLPARSER_AVAILABLE}
 * is true.
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

  /** Skip JSqlParser for SQL above this length; the regex baseline handles pathological inputs. */
  private static final int LENGTH_BAIL_OUT = 10_000;

  /** Returns true if JSqlParser is on the classpath and available for use. */
  public static boolean isJSqlParserAvailable() {
    return JSQLPARSER_AVAILABLE;
  }

  // ── WHERE columns ──────────────────────────────────────────────────

  /** Extract WHERE clause columns. */
  public static List<ColumnReference> extractWhereColumns(String sql) {
    if (sql == null) {
      return new ArrayList<>();
    }
    if (JSQLPARSER_AVAILABLE && sql.length() <= LENGTH_BAIL_OUT) {
      try {
        return JSqlParserDelegate.extractWhereColumns(sql);
      } catch (Throwable e) {
        LOG.log(
            System.Logger.Level.DEBUG,
            "JSqlParser failed, falling back to regex: {0}",
            e.getMessage());
      }
    }
    return SqlParser.extractWhereColumns(sql);
  }

  // ── JOIN columns ───────────────────────────────────────────────────

  /** Extract JOIN column pairs. */
  public static List<JoinColumnPair> extractJoinColumns(String sql) {
    if (sql == null) {
      return new ArrayList<>();
    }
    if (JSQLPARSER_AVAILABLE && sql.length() <= LENGTH_BAIL_OUT) {
      try {
        return JSqlParserDelegate.extractJoinColumns(sql);
      } catch (Throwable e) {
        LOG.log(
            System.Logger.Level.DEBUG,
            "JSqlParser failed, falling back to regex: {0}",
            e.getMessage());
      }
    }
    return SqlParser.extractJoinColumns(sql);
  }

  // ── Table names ────────────────────────────────────────────────────

  /** Extract table names from FROM / JOIN / UPDATE / DELETE targets in scan order. */
  public static List<String> extractTableNames(String sql) {
    if (sql == null) {
      return new ArrayList<>();
    }
    if (JSQLPARSER_AVAILABLE && sql.length() <= LENGTH_BAIL_OUT) {
      try {
        return JSqlParserDelegate.extractTableNames(sql);
      } catch (Throwable e) {
        LOG.log(
            System.Logger.Level.DEBUG,
            "JSqlParser failed, falling back to regex: {0}",
            e.getMessage());
      }
    }
    return SqlParser.extractTableNames(sql);
  }

  // ── ORDER BY columns ───────────────────────────────────────────────

  /** Extract plain column references from the outer ORDER BY clause; functions are skipped. */
  public static List<ColumnReference> extractOrderByColumns(String sql) {
    if (sql == null) {
      return new ArrayList<>();
    }
    if (JSQLPARSER_AVAILABLE && sql.length() <= LENGTH_BAIL_OUT) {
      try {
        return JSqlParserDelegate.extractOrderByColumns(sql);
      } catch (Throwable e) {
        LOG.log(
            System.Logger.Level.DEBUG,
            "JSqlParser failed, falling back to regex: {0}",
            e.getMessage());
      }
    }
    return SqlParser.extractOrderByColumns(sql);
  }

  // ── GROUP BY columns ───────────────────────────────────────────────

  /** Extract plain column references from the outer GROUP BY clause; functions are skipped. */
  public static List<ColumnReference> extractGroupByColumns(String sql) {
    if (sql == null) {
      return new ArrayList<>();
    }
    if (JSQLPARSER_AVAILABLE && sql.length() <= LENGTH_BAIL_OUT) {
      try {
        return JSqlParserDelegate.extractGroupByColumns(sql);
      } catch (Throwable e) {
        LOG.log(
            System.Logger.Level.DEBUG,
            "JSqlParser failed, falling back to regex: {0}",
            e.getMessage());
      }
    }
    return SqlParser.extractGroupByColumns(sql);
  }

  // ── WHERE columns with operators ───────────────────────────────────

  /** Extract WHERE columns with their comparison operators (=, IS, LIKE, IN, BETWEEN, etc.). */
  public static List<WhereColumnReference> extractWhereColumnsWithOperators(String sql) {
    if (sql == null) {
      return new ArrayList<>();
    }
    if (JSQLPARSER_AVAILABLE && sql.length() <= LENGTH_BAIL_OUT) {
      try {
        return JSqlParserDelegate.extractWhereColumnsWithOperators(sql);
      } catch (Throwable e) {
        LOG.log(
            System.Logger.Level.DEBUG,
            "JSqlParser failed, falling back to regex: {0}",
            e.getMessage());
      }
    }
    return SqlParser.extractWhereColumnsWithOperators(sql);
  }

  // ── JOIN ON bodies ─────────────────────────────────────────────────

  /** Extract each JOIN's ON-clause body as a string, one entry per JOIN. */
  public static List<String> extractJoinOnBodies(String sql) {
    if (sql == null) {
      return new ArrayList<>();
    }
    if (JSQLPARSER_AVAILABLE && sql.length() <= LENGTH_BAIL_OUT) {
      try {
        return JSqlParserDelegate.extractJoinOnBodies(sql);
      } catch (Throwable e) {
        LOG.log(
            System.Logger.Level.DEBUG,
            "JSqlParser failed, falling back to regex: {0}",
            e.getMessage());
      }
    }
    return SqlParser.extractJoinOnBodies(sql);
  }

  // ── HAVING body ────────────────────────────────────────────────────

  /** Extract the HAVING clause body (without the HAVING keyword), or null if absent. */
  public static String extractHavingClause(String sql) {
    if (sql == null) {
      return null;
    }
    if (JSQLPARSER_AVAILABLE && sql.length() <= LENGTH_BAIL_OUT) {
      try {
        return JSqlParserDelegate.extractHavingClause(sql);
      } catch (Throwable e) {
        LOG.log(
            System.Logger.Level.DEBUG,
            "JSqlParser failed, falling back to regex: {0}",
            e.getMessage());
      }
    }
    return SqlParser.extractHavingClause(sql);
  }

  // ── WHERE body ─────────────────────────────────────────────────────

  /** Extract the WHERE clause body (without the WHERE keyword), or null if absent. */
  public static String extractWhereBody(String sql) {
    if (sql == null) {
      return null;
    }
    if (JSQLPARSER_AVAILABLE && sql.length() <= LENGTH_BAIL_OUT) {
      try {
        return JSqlParserDelegate.extractWhereBody(sql);
      } catch (Throwable e) {
        LOG.log(
            System.Logger.Level.DEBUG,
            "JSqlParser failed, falling back to regex: {0}",
            e.getMessage());
      }
    }
    return SqlParser.extractWhereBody(sql);
  }

  // ── Remove subqueries ──────────────────────────────────────────────

  /** Replace every nested SELECT with {@code (?)} so callers can run outer-query regex checks. */
  public static String removeSubqueries(String sql) {
    if (sql == null) {
      return null;
    }
    // Fast path: no "(SELECT" → nothing to strip.
    if (!containsNestedSelect(sql)) {
      return sql;
    }
    if (JSQLPARSER_AVAILABLE && sql.length() <= LENGTH_BAIL_OUT) {
      try {
        return JSqlParserDelegate.removeSubqueries(sql);
      } catch (Throwable t) {
        LOG.log(
            System.Logger.Level.DEBUG,
            "JSqlParser failed, falling back to regex: {0}",
            t.getMessage());
      }
    }
    return SqlParser.removeSubqueries(sql);
  }

  private static boolean containsNestedSelect(String sql) {
    int len = sql.length();
    for (int i = 0; i < len; i++) {
      if (sql.charAt(i) != '(') continue;
      int j = i + 1;
      while (j < len && Character.isWhitespace(sql.charAt(j))) j++;
      if (j + 6 > len) return false;
      char c0 = sql.charAt(j);
      char c1 = sql.charAt(j + 1);
      char c2 = sql.charAt(j + 2);
      char c3 = sql.charAt(j + 3);
      char c4 = sql.charAt(j + 4);
      char c5 = sql.charAt(j + 5);
      if ((c0 == 'S' || c0 == 's')
          && (c1 == 'E' || c1 == 'e')
          && (c2 == 'L' || c2 == 'l')
          && (c3 == 'E' || c3 == 'e')
          && (c4 == 'C' || c4 == 'c')
          && (c5 == 'T' || c5 == 't')) {
        return true;
      }
    }
    return false;
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

  /** Holds every JSqlParser reference; loaded lazily so the outer class runs without the dep. */
  static final class JSqlParserDelegate {

    private JSqlParserDelegate() {}

    /** Bounded parse cache; both successful and failed parses are memoised per SQL string. */
    private static final int PARSE_CACHE_MAX = 4096;

    private static final ConcurrentMap<String, Statement> PARSE_CACHE = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, Boolean> PARSE_FAILED = new ConcurrentHashMap<>();

    private static Statement parse(String sql) throws Exception {
      if (PARSE_FAILED.containsKey(sql)) {
        throw CachedParseFailure.INSTANCE;
      }
      Statement cached = PARSE_CACHE.get(sql);
      if (cached != null) {
        return cached;
      }
      try {
        Statement stmt = CCJSqlParserUtil.parse(sql);
        if (PARSE_CACHE.size() < PARSE_CACHE_MAX) {
          PARSE_CACHE.putIfAbsent(sql, stmt);
        }
        return stmt;
      } catch (Exception e) {
        if (PARSE_FAILED.size() < PARSE_CACHE_MAX) {
          PARSE_FAILED.putIfAbsent(sql, Boolean.TRUE);
        }
        throw e;
      }
    }

    /** Stackless marker re-thrown for an SQL we've already seen JSqlParser reject. */
    private static final class CachedParseFailure extends Exception {
      private static final long serialVersionUID = 1L;
      static final CachedParseFailure INSTANCE = new CachedParseFailure();

      private CachedParseFailure() {
        super("cached parse failure", null, false, false);
      }
    }

    static List<ColumnReference> extractWhereColumns(String sql) throws Exception {
      Statement statement = parse(sql);

      if (!(statement instanceof Select selectStmt)) {
        return SqlParser.extractWhereColumns(sql);
      }

      Expression where = extractWhereExpression(selectStmt);
      if (where == null) {
        return new ArrayList<>();
      }

      List<ColumnReference> result = new ArrayList<>();
      where.accept(
          new ExpressionVisitorAdapter<Void>() {
            @Override
            public <S> Void visit(Column column, S context) {
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
      Statement statement = parse(sql);

      if (!(statement instanceof Select selectStmt)) {
        return SqlParser.extractJoinColumns(sql);
      }

      List<JoinColumnPair> result = new ArrayList<>();
      PlainSelect plainSelect = extractPlainSelect(selectStmt);

      if (plainSelect == null || plainSelect.getJoins() == null) {
        return result;
      }

      for (Join join : plainSelect.getJoins()) {
        if (join.getOnExpressions() != null) {
          for (Expression onExpr : join.getOnExpressions()) {
            extractJoinPairsFromExpression(onExpr, result);
          }
        }
      }
      return result;
    }

    static List<WhereColumnReference> extractWhereColumnsWithOperators(String sql)
        throws Exception {
      Statement statement = parse(sql);
      Expression where = null;
      if (statement instanceof Select select) {
        PlainSelect ps = select.getPlainSelect();
        if (ps != null) where = ps.getWhere();
      } else if (statement instanceof Delete del) {
        where = del.getWhere();
      } else if (statement instanceof Update upd) {
        where = upd.getWhere();
      }
      if (where == null) {
        return new ArrayList<>();
      }

      List<WhereColumnReference> result = new ArrayList<>();
      where.accept(
          new ExpressionVisitorAdapter<Void>() {
            @Override
            public <S> Void visit(EqualsTo eq, S ctx) {
              addColumnOp(eq.getLeftExpression(), eq.getRightExpression(), "=", result);
              return super.visit(eq, ctx);
            }

            @Override
            public <S> Void visit(NotEqualsTo ne, S ctx) {
              addColumnOp(ne.getLeftExpression(), ne.getRightExpression(), "!=", result);
              return super.visit(ne, ctx);
            }

            @Override
            public <S> Void visit(GreaterThan gt, S ctx) {
              addColumnOp(gt.getLeftExpression(), gt.getRightExpression(), ">", result);
              return super.visit(gt, ctx);
            }

            @Override
            public <S> Void visit(GreaterThanEquals ge, S ctx) {
              addColumnOp(ge.getLeftExpression(), ge.getRightExpression(), ">=", result);
              return super.visit(ge, ctx);
            }

            @Override
            public <S> Void visit(MinorThan lt, S ctx) {
              addColumnOp(lt.getLeftExpression(), lt.getRightExpression(), "<", result);
              return super.visit(lt, ctx);
            }

            @Override
            public <S> Void visit(MinorThanEquals le, S ctx) {
              addColumnOp(le.getLeftExpression(), le.getRightExpression(), "<=", result);
              return super.visit(le, ctx);
            }

            @Override
            public <S> Void visit(IsNullExpression isn, S ctx) {
              if (isn.getLeftExpression() instanceof Column col) {
                result.add(toWhereRef(col, isn.isNot() ? "IS NOT" : "IS"));
              }
              return super.visit(isn, ctx);
            }

            @Override
            public <S> Void visit(InExpression in, S ctx) {
              if (in.getLeftExpression() instanceof Column col) {
                result.add(toWhereRef(col, in.isNot() ? "NOT IN" : "IN"));
              }
              return super.visit(in, ctx);
            }

            @Override
            public <S> Void visit(LikeExpression like, S ctx) {
              if (like.getLeftExpression() instanceof Column col) {
                String op = like.isNot() ? "NOT LIKE" : "LIKE";
                result.add(toWhereRef(col, op));
              }
              return super.visit(like, ctx);
            }

            @Override
            public <S> Void visit(Between btw, S ctx) {
              if (btw.getLeftExpression() instanceof Column col) {
                result.add(toWhereRef(col, "BETWEEN"));
              }
              return super.visit(btw, ctx);
            }
          });
      return result;
    }

    private static void addColumnOp(
        Expression left, Expression right, String op, List<WhereColumnReference> out) {
      if (left instanceof Column col) {
        out.add(toWhereRef(col, op));
      } else if (right instanceof Column col) {
        out.add(toWhereRef(col, op));
      }
    }

    private static WhereColumnReference toWhereRef(Column col, String op) {
      String table = col.getTable() != null ? col.getTable().getName() : null;
      return new WhereColumnReference(table, col.getColumnName(), op);
    }

    static List<String> extractJoinOnBodies(String sql) throws Exception {
      Statement statement = parse(sql);
      if (!(statement instanceof Select select)) {
        return new ArrayList<>();
      }
      PlainSelect ps = select.getPlainSelect();
      if (ps == null || ps.getJoins() == null) {
        return new ArrayList<>();
      }
      List<String> result = new ArrayList<>();
      for (Join join : ps.getJoins()) {
        if (join.getOnExpressions() == null) continue;
        for (Expression onExpr : join.getOnExpressions()) {
          try {
            result.add(onExpr.toString());
          } catch (StackOverflowError soe) {
            // ON expression deeply nested — skip this entry; regex fallback at the public
            // level will handle the query as a whole if every ON fails this way.
          }
        }
      }
      return result;
    }

    static String extractHavingClause(String sql) throws Exception {
      sql = SqlParser.stripComments(sql);
      Statement statement = parse(sql);
      if (!(statement instanceof Select)) {
        return null;
      }
      int havingStart = scanForKeyword(sql, 0, "HAVING");
      if (havingStart < 0) {
        return null;
      }
      int bodyStart = havingStart + "HAVING".length();
      int bodyEnd = sql.length();
      String[] terminators = {"ORDER BY", "LIMIT", "UNION", "FETCH"};
      for (String term : terminators) {
        int pos = scanForKeyword(sql, bodyStart, term);
        if (pos >= 0 && pos < bodyEnd) {
          bodyEnd = pos;
        }
      }
      return sql.substring(bodyStart, bodyEnd).trim();
    }

    static String extractWhereBody(String sql) throws Exception {
      // Strip comments so "-- GROUP BY" inside a trailing line comment doesn't terminate the scan.
      sql = SqlParser.stripComments(sql);

      Statement statement = parse(sql);
      boolean hasWhereCapable =
          statement instanceof Select || statement instanceof Delete || statement instanceof Update;
      if (!hasWhereCapable) {
        return null;
      }

      // Scan the source SQL (literal-aware) instead of Expression.toString() — toString
      // recurses per operand and blows the stack on WHERE clauses with thousands of operands.
      int whereStart = scanForKeyword(sql, 0, "WHERE");
      if (whereStart < 0) {
        return null;
      }
      int bodyStart = whereStart + "WHERE".length();
      int bodyEnd = sql.length();
      String[] terminators = {"GROUP BY", "ORDER BY", "LIMIT", "HAVING", "UNION", "FETCH"};
      for (String term : terminators) {
        int pos = scanForKeyword(sql, bodyStart, term);
        if (pos >= 0 && pos < bodyEnd) {
          bodyEnd = pos;
        }
      }
      return sql.substring(bodyStart, bodyEnd).trim();
    }

    /**
     * Find the next case-insensitive whole-word occurrence of {@code keyword} starting at {@code
     * from}, skipping content inside single-quoted string literals and double-quoted identifiers.
     * Returns -1 if not found.
     */
    private static int scanForKeyword(String sql, int from, String keyword) {
      int len = sql.length();
      int klen = keyword.length();
      int i = from;
      while (i < len) {
        char c = sql.charAt(i);
        if (c == '\'') {
          i++;
          while (i < len) {
            char q = sql.charAt(i);
            if (q == '\\' && i + 1 < len) {
              i += 2;
            } else if (q == '\'' && i + 1 < len && sql.charAt(i + 1) == '\'') {
              i += 2;
            } else if (q == '\'') {
              i++;
              break;
            } else {
              i++;
            }
          }
          continue;
        }
        if (c == '"') {
          i++;
          while (i < len && sql.charAt(i) != '"') i++;
          if (i < len) i++;
          continue;
        }
        // Word boundary check + keyword match
        if (i + klen <= len && sql.regionMatches(true, i, keyword, 0, klen)) {
          boolean leftOk = i == 0 || !Character.isLetterOrDigit(sql.charAt(i - 1));
          boolean rightOk = i + klen == len || !Character.isLetterOrDigit(sql.charAt(i + klen));
          if (leftOk && rightOk) {
            return i;
          }
        }
        i++;
      }
      return -1;
    }

    static List<ColumnReference> extractOrderByColumns(String sql) throws Exception {
      Statement statement = parse(sql);
      if (!(statement instanceof Select select)) {
        return SqlParser.extractOrderByColumns(sql);
      }
      PlainSelect ps = select.getPlainSelect();
      if (ps == null || ps.getOrderByElements() == null) {
        return new ArrayList<>();
      }
      List<ColumnReference> result = new ArrayList<>();
      for (OrderByElement elem : ps.getOrderByElements()) {
        Expression expr = elem.getExpression();
        if (expr instanceof Column col) {
          String tableAlias = col.getTable() != null ? col.getTable().getName() : null;
          String colName = col.getColumnName();
          if (colName != null) {
            result.add(new ColumnReference(tableAlias, colName));
          }
        }
        // Function calls / arithmetic / literals intentionally skipped — matches
        // SqlParser.extractOrderByColumns which also skipped "(" / function shapes.
      }
      return result;
    }

    static List<ColumnReference> extractGroupByColumns(String sql) throws Exception {
      Statement statement = parse(sql);
      if (!(statement instanceof Select select)) {
        return SqlParser.extractGroupByColumns(sql);
      }
      PlainSelect ps = select.getPlainSelect();
      if (ps == null) {
        return new ArrayList<>();
      }
      GroupByElement gb = ps.getGroupBy();
      if (gb == null) {
        return new ArrayList<>();
      }
      ExpressionList<?> exprs = gb.getGroupByExpressionList();
      if (exprs == null) {
        return new ArrayList<>();
      }
      List<ColumnReference> result = new ArrayList<>();
      for (Object o : exprs) {
        if (o instanceof Column col) {
          String tableAlias = col.getTable() != null ? col.getTable().getName() : null;
          String colName = col.getColumnName();
          if (colName != null) {
            result.add(new ColumnReference(tableAlias, colName));
          }
        }
      }
      return result;
    }

    static String removeSubqueries(String sql) throws Exception {
      Statement statement = parse(sql);

      StringBuilder buf = new StringBuilder();

      ExpressionDeParser exprDeParser =
          new ExpressionDeParser() {
            @Override
            public <S> StringBuilder visit(ParenthesedSelect ps, S context) {
              getBuilder().append("(?)");
              return getBuilder();
            }

            @Override
            public <S> StringBuilder visit(ExistsExpression exists, S context) {
              if (exists.isNot()) {
                getBuilder().append("NOT ");
              }
              getBuilder().append("EXISTS (?)");
              return getBuilder();
            }
          };

      // Top-level select is emitted as-is; anything nested becomes "(?)".
      int[] depth = {0};
      SelectDeParser selectDeParser =
          new SelectDeParser(exprDeParser, buf) {
            @Override
            public <S> StringBuilder visit(ParenthesedSelect ps, S context) {
              if (depth[0] > 0) {
                getBuilder().append("(?)");
                return getBuilder();
              }
              depth[0]++;
              try {
                return super.visit(ps, context);
              } finally {
                depth[0]--;
              }
            }

            @Override
            public <S> StringBuilder visit(PlainSelect ps, S context) {
              if (depth[0] > 0) {
                getBuilder().append("(?)");
                return getBuilder();
              }
              depth[0]++;
              try {
                return super.visit(ps, context);
              } finally {
                depth[0]--;
              }
            }

            @Override
            public <S> StringBuilder visit(SetOperationList sol, S context) {
              if (depth[0] > 0) {
                getBuilder().append("(?)");
                return getBuilder();
              }
              depth[0]++;
              try {
                return super.visit(sol, context);
              } finally {
                depth[0]--;
              }
            }
          };

      exprDeParser.setSelectVisitor(selectDeParser);
      exprDeParser.setBuilder(buf);

      StatementDeParser stmtDeParser = new StatementDeParser(exprDeParser, selectDeParser, buf);
      statement.accept(stmtDeParser);

      return buf.toString();
    }

    static List<String> extractTableNames(String sql) throws Exception {
      // Mirrors the regex baseline: only FROM / JOIN / UPDATE / DELETE targets, not INSERT.
      Statement statement = parse(sql);

      List<String> result = new ArrayList<>();
      String outer = outermostTable(statement);
      if (outer != null) {
        addTable(outer, result);
      }

      if (statement instanceof Insert ins && ins.getSelect() == null) {
        return result;
      }

      TablesNamesFinder<?> finder = new TablesNamesFinder<>();
      for (String raw : finder.getTableList(statement)) {
        if (statement instanceof Insert insSel
            && insSel.getTable() != null
            && stripQualifier(raw).equals(stripQualifier(insSel.getTable().getName()))) {
          continue;
        }
        addTable(raw, result);
      }
      return result;
    }

    private static String stripQualifier(String raw) {
      if (raw == null) return "";
      String s = raw.replace("`", "").replace("\"", "");
      int dot = s.lastIndexOf('.');
      return dot >= 0 ? s.substring(dot + 1) : s;
    }

    private static String outermostTable(Statement stmt) {
      if (stmt instanceof Select sel) {
        PlainSelect ps = sel.getPlainSelect();
        if (ps != null && ps.getFromItem() instanceof Table t) {
          return t.getName();
        }
      } else if (stmt instanceof Delete del && del.getTable() != null) {
        return del.getTable().getName();
      } else if (stmt instanceof Update upd && upd.getTable() != null) {
        return upd.getTable().getName();
      }
      return null;
    }

    private static void addTable(String raw, List<String> out) {
      if (raw == null) return;
      String cleaned = raw.replace("`", "").replace("\"", "");
      if (cleaned.contains(".")) {
        cleaned = cleaned.substring(cleaned.lastIndexOf('.') + 1);
      }
      if (!cleaned.isEmpty() && !out.contains(cleaned)) {
        out.add(cleaned);
      }
    }

    // ── helpers ────────────────────────────────────────────────────

    private static Expression extractWhereExpression(Select select) {
      PlainSelect ps = extractPlainSelect(select);
      return ps != null ? ps.getWhere() : null;
    }

    private static PlainSelect extractPlainSelect(Select select) {
      // getPlainSelect() handles CTEs, parenthesized selects, etc.
      return select.getPlainSelect();
    }

    private static void extractJoinPairsFromExpression(
        Expression expr, List<JoinColumnPair> result) {
      if (expr instanceof EqualsTo eq) {
        Expression left = eq.getLeftExpression();
        Expression right = eq.getRightExpression();
        if (left instanceof Column leftCol && right instanceof Column rightCol) {
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
      } else if (expr instanceof AndExpression and) {
        extractJoinPairsFromExpression(and.getLeftExpression(), result);
        extractJoinPairsFromExpression(and.getRightExpression(), result);
      }
    }
  }
}
