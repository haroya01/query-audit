package io.queryaudit.core.parser;

/**
 * Represents a reference to a database column, optionally qualified with a table name or alias.
 * Used by SQL parsers to convey column references extracted from WHERE, JOIN, and other clauses.
 *
 * @author haroya
 * @since 0.2.0
 */
public record ColumnReference(String tableOrAlias, String columnName) {

  public ColumnReference(String columnName) {
    this(null, columnName);
  }
}
