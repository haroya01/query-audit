package io.queryaudit.core.parser;

import static io.queryaudit.core.parser.SqlAstParser.parse;

import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;

/**
 * Row-limiting clauses proven on the outermost statement only.
 *
 * <p>MySQL {@code EXPLAIN}-style plans aside, a row limit bounds the rows a statement hands back. A
 * limit on a derived table, a scalar subquery or a set operand bounds only that inner scope, so the
 * outer statement stays unbounded and is deliberately not counted here.
 *
 * <p>Detection reads parsed clause objects rather than scanning SQL text, so {@code LIMIT} and
 * {@code FETCH} occurring inside a string literal, a quoted identifier or a comment are not
 * mistaken for a clause.
 */
final class SqlAstRowLimits {
  private SqlAstRowLimits() {}

  /**
   * Returns true when the outermost SELECT of {@code sql} carries LIMIT, LIMIT BY or FETCH FIRST.
   */
  static boolean hasOuterRowLimit(String sql) throws Exception {
    if (!(parse(sql) instanceof Select select)) {
      return false;
    }
    if (select.getLimit() != null || select.getLimitBy() != null || select.getFetch() != null) {
      return true;
    }
    // A set operation keeps its own limit on the SetOperationList; a plain select keeps it on the
    // PlainSelect. Checking only the outermost scope keeps inner limits from proving an outer
    // bound.
    PlainSelect plain = select.getPlainSelect();
    return plain != null
        && (plain.getLimit() != null || plain.getLimitBy() != null || plain.getFetch() != null);
  }
}
