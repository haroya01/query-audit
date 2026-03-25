package io.queryaudit.mysql;

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
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Tag("integration")
@Testcontainers
@DisplayName("MySqlExplainAnalyzer integration test")
class MySqlExplainAnalyzerIntegrationTest {

  @Container
  static MySQLContainer<?> mysql =
      new MySQLContainer<>("mysql:8.0").withDatabaseName("testdb").withInitScript("init.sql");

  private static MySqlExplainAnalyzer analyzer;

  @BeforeAll
  static void setUp() {
    analyzer = new MySqlExplainAnalyzer();
  }

  private Connection getConnection() throws SQLException {
    return DriverManager.getConnection(mysql.getJdbcUrl(), mysql.getUsername(), mysql.getPassword());
  }

  @Test
  @DisplayName("detects full table scan on unindexed column")
  void detectsFullTableScan() throws SQLException {
    try (Connection conn = getConnection()) {
      List<QueryRecord> queries =
          List.of(
              new QueryRecord(
                  "SELECT * FROM orders WHERE total > 100", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(conn, queries);

      assertThat(issues)
          .anyMatch(
              i ->
                  i.type() == IssueType.FULL_TABLE_SCAN
                      && i.severity() == Severity.INFO
                      && "orders".equals(i.table()));
    }
  }

  @Test
  @DisplayName("does not detect full table scan on indexed column")
  void noFullTableScanOnIndexedColumn() throws SQLException {
    try (Connection conn = getConnection()) {
      List<QueryRecord> queries =
          List.of(
              new QueryRecord(
                  "SELECT * FROM users WHERE email = 'test@example.com'", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(conn, queries);

      assertThat(issues)
          .noneMatch(i -> i.type() == IssueType.FULL_TABLE_SCAN);
    }
  }

  @Test
  @DisplayName("detects filesort on ORDER BY unindexed column")
  void detectsFilesort() throws SQLException {
    try (Connection conn = getConnection()) {
      List<QueryRecord> queries =
          List.of(
              new QueryRecord(
                  "SELECT * FROM orders ORDER BY total", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(conn, queries);

      assertThat(issues)
          .anyMatch(i -> i.type() == IssueType.FILESORT);
    }
  }

  @Test
  @DisplayName("skips non-SELECT queries")
  void skipsNonSelect() throws SQLException {
    try (Connection conn = getConnection()) {
      List<QueryRecord> queries =
          List.of(
              new QueryRecord(
                  "INSERT INTO users (username, email, status, created_at) VALUES ('a', 'a@b.com', 'active', NOW())",
                  0L, 0L, null));

      List<Issue> issues = analyzer.analyze(conn, queries);

      assertThat(issues).isEmpty();
    }
  }

  @Test
  @DisplayName("EXPLAIN works with single ? placeholder in parameterized SQL")
  void explainWorksWithSinglePlaceholder() throws SQLException {
    try (Connection conn = getConnection()) {
      // This is the SQL that datasource-proxy captures — with ? placeholders
      List<QueryRecord> queries =
          List.of(
              new QueryRecord(
                  "SELECT * FROM users WHERE id = ?", 0L, 0L, null));

      // Should NOT throw — the ? is replaced with 1 before EXPLAIN
      List<Issue> issues = analyzer.analyze(conn, queries);

      // The query uses the primary key, so no full table scan expected
      assertThat(issues)
          .noneMatch(i -> i.type() == IssueType.FULL_TABLE_SCAN);
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

      // Result depends on index usage — we just verify no exception
      assertThat(issues).isNotNull();
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
