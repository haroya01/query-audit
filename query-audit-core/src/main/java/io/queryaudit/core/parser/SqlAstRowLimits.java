package io.queryaudit.core.parser;

import static io.queryaudit.core.parser.SqlAstParser.parse;

import java.util.List;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SetOperationList;

/**
 * Row-limiting clauses proven on the outermost statement only.
 *
 * <p>A row limit bounds the rows a statement hands back. A limit on a derived table or a scalar
 * subquery bounds only that inner scope, so the outer statement stays unbounded and is deliberately
 * not counted here. A set operation is the one shape that needs extra care, because its trailing
 * limit is genuine and statement-wide yet lands in different places in the tree; see {@link
 * #hasOuterRowLimit(String)}.
 *
 * <p>Detection reads parsed clause objects rather than scanning SQL text, so {@code LIMIT} and
 * {@code FETCH} occurring inside a string literal, a quoted identifier or a comment are not
 * mistaken for a clause.
 */
final class SqlAstRowLimits {
  private SqlAstRowLimits() {}

  /**
   * Returns true when the outermost SELECT of {@code sql} carries LIMIT, LIMIT BY or FETCH FIRST.
   *
   * <p>Set operations need care because JSqlParser distributes the trailing limit over two different
   * places depending on the shape of the statement:
   *
   * <ul>
   *   <li>{@code SELECT ... UNION SELECT ... ORDER BY x LIMIT 1} and {@code ... FETCH FIRST n ROWS}
   *       keep the clause on the {@link SetOperationList} itself;
   *   <li>{@code SELECT ... UNION SELECT ... LIMIT 1} attaches it to the <em>last</em> operand.
   * </ul>
   *
   * <p>Only a {@link PlainSelect} last operand is accepted, which is what distinguishes a
   * statement-wide limit from a branch-local one: a parenthesized branch such as
   * {@code SELECT ... UNION (SELECT ... LIMIT 1)} parses as a {@code ParenthesedSelect} and is
   * therefore not mistaken for a bound on the union.
   */
  static boolean hasOuterRowLimit(String sql) throws Exception {
    if (!(parse(sql) instanceof Select select)) {
      return false;
    }
    if (select.getLimit() != null || select.getFetch() != null) {
      return true;
    }
    if (select instanceof PlainSelect plain && plain.getLimitBy() != null) {
      return true;
    }
    return select instanceof SetOperationList setOperation
        && lastOperand(setOperation) instanceof PlainSelect operand
        && carriesRowLimit(operand);
  }

  private static Select lastOperand(SetOperationList setOperation) {
    List<Select> operands = setOperation.getSelects();
    return operands == null || operands.isEmpty() ? null : operands.get(operands.size() - 1);
  }

  /** LIMIT n [OFFSET m], TiDB/ClickHouse {@code LIMIT n BY group}, or {@code FETCH FIRST n ROWS}. */
  private static boolean carriesRowLimit(PlainSelect select) {
    return select.getLimit() != null || select.getLimitBy() != null || select.getFetch() != null;
  }
}
