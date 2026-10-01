package io.queryaudit.core.parser;

import static io.queryaudit.core.parser.SqlAstParser.parse;

import java.util.ArrayDeque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ParenthesedExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;

/** Necessary scalar equalities in one base-table scope; index policy belongs to detectors. */
final class SqlAstEqualities {
  private SqlAstEqualities() {}

  static List<ColumnReference> extractRequiredScalarEqualities(String sql) throws Exception {
    if (!(parse(sql) instanceof PlainSelect select)
        || !(select.getFromItem() instanceof Table table)
        || select.getWhere() == null
        || select.getJoins() != null && !select.getJoins().isEmpty()
        || select.getWithItemsList() != null && !select.getWithItemsList().isEmpty()) {
      // Joins and derived/CTE relations need scope-specific evidence. Two aliases of one table
      // must not contribute columns to one shared constraint proof.
      return List.of();
    }
    Set<String> columns = scalarEqualityColumns(select.getWhere(), table);
    String tableName = table.getUnquotedName().toLowerCase(Locale.ROOT);
    return columns.stream().map(column -> new ColumnReference(tableName, column)).toList();
  }

  private static Set<String> scalarEqualityColumns(Expression where, Table table) {
    Set<String> columns = new LinkedHashSet<>();
    ArrayDeque<Expression> pending = new ArrayDeque<>();
    pending.add(where);
    while (!pending.isEmpty()) {
      Expression expression = unwrap(pending.removeFirst());
      if (expression instanceof AndExpression and) {
        pending.addFirst(and.getRightExpression());
        pending.addFirst(and.getLeftExpression());
      } else if (expression instanceof EqualsTo equality) {
        addScalarEquality(
            equality.getLeftExpression(), equality.getRightExpression(), table, columns);
        addScalarEquality(
            equality.getRightExpression(), equality.getLeftExpression(), table, columns);
      }
      // Only positive conjunctions establish necessary predicates. Do not descend into OR,
      // NOT, CASE, functions or subqueries, even when they contain apparently useful equalities.
    }
    return columns;
  }

  private static void addScalarEquality(
      Expression candidate, Expression value, Table table, Set<String> columns) {
    if (!(unwrap(candidate) instanceof Column column) || !isScalar(value)) return;
    String qualifier = column.getUnquotedTableName();
    if (qualifier == null
        || qualifier.isEmpty()
        || qualifier.equalsIgnoreCase(table.getUnquotedName())
        || table.getAlias() != null
            && qualifier.equalsIgnoreCase(table.getAlias().getUnquotedName())) {
      columns.add(column.getUnquotedColumnName());
    }
  }

  private static boolean isScalar(Expression expression) {
    Expression value = unwrap(expression);
    if (value instanceof SignedExpression signed) value = unwrap(signed.getExpression());
    return value instanceof JdbcParameter
        || value instanceof JdbcNamedParameter
        || value instanceof LongValue
        || value instanceof DoubleValue
        || value instanceof StringValue
        || value instanceof BooleanValue
        || value instanceof DateValue
        || value instanceof TimeValue
        || value instanceof TimestampValue
        || value instanceof HexValue;
  }

  private static Expression unwrap(Expression expression) {
    Expression result = expression;
    while (result instanceof ParenthesedExpressionList<?> list && list.size() == 1) {
      result = list.get(0);
    }
    return result;
  }
}
