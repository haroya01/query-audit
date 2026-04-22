package io.queryaudit.core.parser;

/**
 * Represents a SQL function invocation on a column, such as {@code UPPER(name)} or {@code
 * DATE(created_at)}. Captures the function name, the column it wraps, and an optional table or
 * alias qualifier. Used to detect non-sargable expressions in WHERE clauses.
 *
 * @author haroya
 * @since 0.2.0
 */
public record FunctionUsage(String functionName, String columnName, String tableOrAlias) {

  /** Backward-compatible constructor without table/alias. */
  public FunctionUsage(String functionName, String columnName) {
    this(functionName, columnName, null);
  }
}
