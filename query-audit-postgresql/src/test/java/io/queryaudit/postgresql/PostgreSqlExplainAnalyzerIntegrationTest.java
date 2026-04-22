package io.queryaudit.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

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

  @Test
  @DisplayName("EXPLAIN works with single ? placeholder in parameterized SQL")
  void explainWorksWithSinglePlaceholder() throws SQLException {
    try (Connection conn = getConnection()) {
      List<QueryRecord> queries =
          List.of(new QueryRecord("SELECT * FROM users WHERE id = ?", 0L, 0L, null));

      // Should NOT throw — the ? is replaced with 1 before EXPLAIN
      List<Issue> issues = analyzer.analyze(conn, queries);

      assertThat(issues).isNotNull();
    }
  }

  @Test
  @DisplayName("EXPLAIN works with multiple ? placeholders")
  void explainWorksWithMultiplePlaceholders() throws SQLException {
    try (Connection conn = getConnection()) {
      List<QueryRecord> queries =
          List.of(
              new QueryRecord(
                  "SELECT * FROM orders WHERE user_id = ? AND status = ?", 0L, 0L, null));

      // Should NOT throw
      List<Issue> issues = analyzer.analyze(conn, queries);

      assertThat(issues).isNotNull();
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
