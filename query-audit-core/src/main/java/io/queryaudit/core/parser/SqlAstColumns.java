package io.queryaudit.core.parser;

import static io.queryaudit.core.parser.SqlAstParser.parse;

import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

/** Read-only AST traversal for WHERE, JOIN and ordering/grouping column references. */
final class SqlAstColumns {
  private SqlAstColumns() {}

  static List<ColumnReference> extractWhereColumns(String sql) throws Exception {
    Statement statement = parse(sql);

    if (!(statement instanceof Select selectStmt)) {
      throw SqlParserRouting.unsupportedStatement();
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
      throw SqlParserRouting.unsupportedStatement();
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

  static List<WhereColumnReference> extractWhereColumnsWithOperators(String sql) throws Exception {
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

  static void addColumnOp(
      Expression left, Expression right, String op, List<WhereColumnReference> out) {
    if (left instanceof Column col) {
      out.add(toWhereRef(col, op));
    } else if (right instanceof Column col) {
      out.add(toWhereRef(col, op));
    }
  }

  static WhereColumnReference toWhereRef(Column col, String op) {
    String table = col.getTable() != null ? col.getTable().getName() : null;
    return new WhereColumnReference(table, col.getColumnName(), op);
  }

  static List<ColumnReference> extractOrderByColumns(String sql) throws Exception {
    Statement statement = parse(sql);
    if (!(statement instanceof Select select)) {
      throw SqlParserRouting.unsupportedStatement();
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
      throw SqlParserRouting.unsupportedStatement();
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

  // ── helpers ────────────────────────────────────────────────────

  static Expression extractWhereExpression(Select select) {
    PlainSelect ps = extractPlainSelect(select);
    return ps != null ? ps.getWhere() : null;
  }

  static PlainSelect extractPlainSelect(Select select) {
    // getPlainSelect() handles CTEs, parenthesized selects, etc.
    return select.getPlainSelect();
  }

  static void extractJoinPairsFromExpression(Expression expr, List<JoinColumnPair> result) {
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
