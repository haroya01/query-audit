package io.queryaudit.core.analyzer;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryRecord;
import java.sql.Connection;
import java.util.List;

/**
 * Analyzes captured queries by running EXPLAIN against the actual database. Implementations are
 * database-specific and are discovered via {@link java.util.ServiceLoader}.
 *
 * @author haroya
 * @since 0.2.0
 */
public interface ExplainAnalyzer {
  /** Returns the database identifier this analyzer supports (e.g. "mysql", "postgresql"). */
  String supportedDatabase();

  /**
   * Runs EXPLAIN on the given queries and returns any performance issues found.
   *
   * @param connection live JDBC connection to the test database
   * @param queries captured queries from the test execution
   * @return list of issues detected via EXPLAIN analysis
   */
  List<Issue> analyze(Connection connection, List<QueryRecord> queries);
}
