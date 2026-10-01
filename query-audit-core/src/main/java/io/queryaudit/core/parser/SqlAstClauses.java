package io.queryaudit.core.parser;

import static io.queryaudit.core.parser.SqlAstParser.parse;

import java.util.ArrayList;
import java.util.List;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;

/** Validates supported AST statement shapes before source-preserving clause extraction. */
final class SqlAstClauses {
  private SqlAstClauses() {}

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
          // Preserve the legacy per-ON behavior: omit this unrenderable expression but retain
          // other JOIN bodies. This is distinct from failure of the entire AST extraction.
        }
      }
    }
    return result;
  }

  static String extractHavingClause(String sql) throws Exception {
    sql = SqlText.stripComments(sql);
    Statement statement = parse(sql);
    if (!(statement instanceof Select)) {
      return null;
    }
    return SqlSourceScanner.clauseBody(sql, "HAVING", "ORDER BY", "LIMIT", "UNION", "FETCH");
  }

  static String extractWhereBody(String sql) throws Exception {
    // Strip comments so "-- GROUP BY" inside a trailing line comment doesn't terminate the scan.
    sql = SqlText.stripComments(sql);

    Statement statement = parse(sql);
    boolean hasWhereCapable =
        statement instanceof Select || statement instanceof Delete || statement instanceof Update;
    if (!hasWhereCapable) {
      return null;
    }

    // Scan the source SQL (literal-aware) instead of Expression.toString() — toString
    // recurses per operand and blows the stack on WHERE clauses with thousands of operands.
    return SqlSourceScanner.clauseBody(
        sql, "WHERE", "GROUP BY", "ORDER BY", "LIMIT", "HAVING", "UNION", "FETCH");
  }
}
