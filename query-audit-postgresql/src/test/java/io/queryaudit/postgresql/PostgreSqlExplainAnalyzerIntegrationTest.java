package io.queryaudit.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.analyzer.ExplainAnalysisException;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Tag("integration")
@Testcontainers
@DisplayName("PostgreSqlExplainAnalyzer integration test")
class PostgreSqlExplainAnalyzerIntegrationTest {

  @Container
  static PostgreSQLContainer<?> postgres =
      new PostgreSQLContainer<>("postgres:16-alpine")
          .withDatabaseName("testdb")
          .withInitScript("init.sql");

  private static PostgreSqlExplainAnalyzer analyzer;

  @BeforeAll
  static void setUp() {
    analyzer = new PostgreSqlExplainAnalyzer();
  }

  private Connection getConnection() throws SQLException {
    return DriverManager.getConnection(
        postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
  }

  @Test
  @DisplayName("detects Seq Scan on unindexed column")
  void detectsSeqScan() throws SQLException {
    try (Connection conn = getConnection()) {
      List<QueryRecord> queries =
          List.of(new QueryRecord("SELECT * FROM orders WHERE total > 100", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(conn, queries);

      assertThat(issues)
          .anyMatch(i -> i.type() == IssueType.FULL_TABLE_SCAN && i.severity() == Severity.INFO);
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT * FROM users WHERE id = ?",
        "SELECT * FROM orders WHERE user_id = ? AND status = ?",
        "SELECT '?' FROM users"
      })
  void unsupportedQuestionMarksDoNotBecomeInventedPlans(String sql) throws SQLException {
    try (Connection connection = getConnection()) {
      assertThatThrownBy(
              () -> analyzer.analyze(connection, List.of(new QueryRecord(sql, 0L, 0L, null))))
          .isInstanceOfSatisfying(
              ExplainAnalysisException.class,
              failure -> {
                assertThat(failure.getReason())
                    .isEqualTo(ExplainAnalysisException.Reason.UNSUPPORTED_PARAMETERS);
                assertThat(failure.getCompletedIssues()).isEmpty();
                assertThat(failure)
                    .hasCauseInstanceOf(java.sql.SQLFeatureNotSupportedException.class);
              });
    }
  }

  @Test
  void literalValuesCanBeExplainedWithoutRewriting() throws SQLException {
    try (Connection connection = getConnection()) {
      List<QueryRecord> queries =
          List.of(
              new QueryRecord(
                  "SELECT * FROM orders WHERE user_id = 1 AND status = 'active'", 0L, 0L, null));

      assertThat(analyzer.analyze(connection, queries)).isNotNull();
    }
  }

  @Test
  void failedTypeResolutionRetainsThePostgresCause() throws SQLException {
    try (Connection connection = getConnection()) {
      List<QueryRecord> queries =
          List.of(new QueryRecord("SELECT * FROM orders WHERE status = 1", 0L, 0L, null));

      assertThatThrownBy(() -> analyzer.analyze(connection, queries))
          .isInstanceOfSatisfying(
              ExplainAnalysisException.class,
              failure -> {
                assertThat(failure.getReason())
                    .isEqualTo(ExplainAnalysisException.Reason.EXECUTION_FAILED);
                assertThat(failure.getCause()).isInstanceOf(SQLException.class);
                assertThat(((SQLException) failure.getCause()).getSQLState()).isEqualTo("42883");
                assertThat(failure.getCause()).hasMessageContaining("character varying = integer");
                assertThat(failure).hasMessage("EXPLAIN analysis did not complete");
              });
    }
  }

  @Test
  @DisplayName("skips non-SELECT queries")
  void skipsNonSelect() throws SQLException {
    try (Connection conn = getConnection()) {
      List<QueryRecord> queries =
          List.of(
              new QueryRecord(
                  "INSERT INTO users (email, username, status) VALUES ('a@b.com', 'a', 'active')",
                  0L,
                  0L,
                  null));

      List<Issue> issues = analyzer.analyze(conn, queries);

      assertThat(issues).isEmpty();
    }
  }

  @Test
  @DisplayName("all issues have INFO severity")
  void allIssuesAreInfoSeverity() throws SQLException {
    try (Connection conn = getConnection()) {
      List<QueryRecord> queries =
          List.of(
              new QueryRecord("SELECT * FROM orders WHERE total > 100", 0L, 0L, null),
              new QueryRecord("SELECT * FROM orders ORDER BY total", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(conn, queries);

      assertThat(issues).allMatch(i -> i.severity() == Severity.INFO);
    }
  }
}
