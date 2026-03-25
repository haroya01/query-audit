package io.queryaudit.core.model;

/**
 * Represents a single issue detected during SQL query analysis. Contains the issue type,
 * severity, the offending query, relevant table and column, a human-readable detail message,
 * a suggestion for resolution, and an optional source location.
 *
 * @author haroya
 * @since 0.2.0
 */
public record Issue(
    IssueType type,
    Severity severity,
    String query,
    String table,
    String column,
    String detail,
    String suggestion,
    String sourceLocation) {

  /** Convenience constructor for backward compatibility (sourceLocation defaults to null). */
  public Issue(
      IssueType type,
      Severity severity,
      String query,
      String table,
      String column,
      String detail,
      String suggestion) {
    this(type, severity, query, table, column, detail, suggestion, null);
  }
}
