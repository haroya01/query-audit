package io.queryaudit.core.parser;

import static io.queryaudit.core.parser.SqlAstParser.parse;

import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.ParenthesedSelect;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SetOperationList;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import net.sf.jsqlparser.util.deparser.StatementDeParser;

/** AST rendering that replaces nested SELECTs while preserving the outer statement. */
final class SqlAstSubqueries {
  private SqlAstSubqueries() {}

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
}
