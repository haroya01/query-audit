package io.queryaudit.core.model;

import io.queryaudit.core.parser.SqlParser;

/**
 * Records a single SQL query captured during test execution, including the raw and normalized
 * SQL text, execution time, timestamp, stack trace, and a hash of the full call stack used
 * for N+1 detection grouping.
 *
 * @author haroya
 * @since 0.2.0
 */
public record QueryRecord(
    String sql,
    String normalizedSql,
    long executionTimeNanos,
    long timestamp,
    String stackTrace,
    int fullStackHash) {

  public QueryRecord(String sql, long executionTimeNanos, long timestamp, String stackTrace) {
    this(
        sql,
        SqlParser.normalize(sql),
        executionTimeNanos,
        timestamp,
        stackTrace,
        stackTrace == null ? 0 : stackTrace.hashCode());
  }

  public QueryRecord(
      String sql, long executionTimeNanos, long timestamp, String stackTrace, int fullStackHash) {
    this(sql, SqlParser.normalize(sql), executionTimeNanos, timestamp, stackTrace, fullStackHash);
  }

  /**
   * Returns the hash of the full call stack. This is the "location key" used by the Prosopite
   * algorithm to group queries by call site.
   */
  public int stackHash() {
    return fullStackHash;
  }
}
