package io.queryaudit.core.parser;

/**
 * A column reference in a WHERE clause, including the operator used. Extends {@link
 * ColumnReference} semantics with operator context for smarter index analysis (e.g., soft-delete
 * detection, equality checks).
 *
 * @author haroya
 * @since 0.2.0
 */
public record WhereColumnReference(String tableOrAlias, String columnName, String operator) {

  public WhereColumnReference(String columnName, String operator) {
    this(null, columnName, operator);
  }

  /** Convert to a plain {@link ColumnReference} (for backward compatibility). */
  public ColumnReference toColumnReference() {
    return new ColumnReference(tableOrAlias, columnName);
  }

  /** Returns true if the operator is an equality operator (=, IS, IS NULL). */
  public boolean isEquality() {
    if (operator == null) return false;
    String op = operator.trim().toUpperCase();
    return "=".equals(op) || "IS".equals(op);
  }
}
