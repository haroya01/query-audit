package io.queryaudit.mysql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.*;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class MySqlExplainAnalyzerTest {

  private MySqlExplainAnalyzer analyzer;

  @Mock private Connection connection;
  @Mock private Statement statement;
  @Mock private ResultSet resultSet;

  @BeforeEach
  void setUp() throws SQLException {
    MockitoAnnotations.openMocks(this);
    analyzer = new MySqlExplainAnalyzer();
    when(connection.createStatement()).thenReturn(statement);
  }

  @Test
  @DisplayName("supportedDatabase() returns 'mysql'")
  void supportedDatabaseReturnsMysql() {
    assertThat(analyzer.supportedDatabase()).isEqualTo("mysql");
  }

  @Nested
  @DisplayName("Full table scan detection")
  class FullTableScanTests {

    @Test
    @DisplayName("detects type=ALL as full table scan")
    void detectsFullTableScan() throws SQLException {
      mockExplainResult("users", "ALL", null, 10000L, null);

      List<QueryRecord> queries = List.of(new QueryRecord("SELECT * FROM users", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(connection, queries);

      assertThat(issues).hasSize(1);
      Issue issue = issues.get(0);
      assertThat(issue.type()).isEqualTo(IssueType.FULL_TABLE_SCAN);
      assertThat(issue.severity()).isEqualTo(Severity.INFO);
      assertThat(issue.table()).isEqualTo("users");
      assertThat(issue.detail()).contains("type=ALL").contains("10000");
    }

    @Test
    @DisplayName("does not flag non-ALL types")
    void doesNotFlagNonAll() throws SQLException {
      mockExplainResult("users", "ref", "idx_email", 5L, null);

      List<QueryRecord> queries =
          List.of(
              new QueryRecord("SELECT * FROM users WHERE email = 'test@test.com'", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(connection, queries);

      assertThat(issues).isEmpty();
    }
  }

  @Nested
  @DisplayName("Filesort detection")
  class FilesortTests {

    @Test
    @DisplayName("detects 'Using filesort' in Extra")
    void detectsFilesort() throws SQLException {
      mockExplainResult("orders", "ref", "idx_user_id", 100L, "Using filesort");

      List<QueryRecord> queries =
          List.of(
              new QueryRecord(
                  "SELECT * FROM orders WHERE user_id = 1 ORDER BY created_at", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(connection, queries);

      assertThat(issues).hasSize(1);
      Issue issue = issues.get(0);
      assertThat(issue.type()).isEqualTo(IssueType.FILESORT);
      assertThat(issue.severity()).isEqualTo(Severity.INFO);
      assertThat(issue.detail()).contains("Using filesort");
    }
  }

  @Nested
  @DisplayName("Temporary table detection")
  class TemporaryTableTests {

    @Test
    @DisplayName("detects 'Using temporary' in Extra")
    void detectsTemporaryTable() throws SQLException {
      mockExplainResult("orders", "ALL", null, 5000L, "Using temporary; Using filesort");

      List<QueryRecord> queries =
          List.of(
              new QueryRecord("SELECT status, COUNT(*) FROM orders GROUP BY status", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(connection, queries);

      // Both FULL_TABLE_SCAN (ALL) + FILESORT + TEMPORARY_TABLE
      assertThat(issues).hasSize(3);
      assertThat(issues)
          .extracting(Issue::type)
          .containsExactlyInAnyOrder(
              IssueType.FULL_TABLE_SCAN, IssueType.FILESORT, IssueType.TEMPORARY_TABLE);
    }
  }

  @Nested
  @DisplayName("Query filtering and deduplication")
  class FilteringTests {

    @Test
    @DisplayName("skips non-SELECT queries")
    void skipsNonSelectQueries() throws SQLException {
      List<QueryRecord> queries =
          List.of(
              new QueryRecord("INSERT INTO users (name) VALUES ('test')", 0L, 0L, null),
              new QueryRecord("UPDATE users SET name = 'test' WHERE id = 1", 0L, 0L, null),
              new QueryRecord("DELETE FROM users WHERE id = 1", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(connection, queries);

      assertThat(issues).isEmpty();
      verify(statement, never()).executeQuery(startsWith("EXPLAIN"));
    }

    @Test
    @DisplayName("deduplicates by normalized SQL")
    void deduplicatesByNormalizedSql() throws SQLException {
      mockExplainResult("users", "ALL", null, 100L, null);

      // These two queries normalize to the same pattern
      List<QueryRecord> queries =
          List.of(
              new QueryRecord("SELECT * FROM users WHERE id = 1", 0L, 0L, null),
              new QueryRecord("SELECT * FROM users WHERE id = 2", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(connection, queries);

      // Should only produce one FULL_TABLE_SCAN, not two
      assertThat(issues).hasSize(1);
      verify(statement, times(1)).executeQuery(startsWith("EXPLAIN"));
    }

    @Test
    @DisplayName("gracefully handles EXPLAIN failures")
    void handlesExplainFailures() throws SQLException {
      when(statement.executeQuery(startsWith("EXPLAIN")))
          .thenThrow(new SQLException("Syntax error"));

      List<QueryRecord> queries =
          List.of(new QueryRecord("SELECT * FROM nonexistent_table", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(connection, queries);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("returns empty list for empty query list")
    void emptyQueriesReturnEmpty() {
      List<Issue> issues = analyzer.analyze(connection, List.of());

      assertThat(issues).isEmpty();
    }
  }

  @Nested
  @DisplayName("Placeholder replacement for EXPLAIN")
  class PlaceholderReplacementTests {

    @Test
    @DisplayName("replaces single ? placeholder with dummy value")
    void replacesSinglePlaceholder() {
      String result = MySqlExplainAnalyzer.prepareForExplain("SELECT * FROM users WHERE id = ?");
      assertThat(result).isEqualTo("SELECT * FROM users WHERE id = 1");
    }

    @Test
    @DisplayName("replaces multiple ? placeholders")
    void replacesMultiplePlaceholders() {
      String result =
          MySqlExplainAnalyzer.prepareForExplain("SELECT * FROM users WHERE a = ? AND b = ?");
      assertThat(result).isEqualTo("SELECT * FROM users WHERE a = 1 AND b = 1");
    }

    @Test
    @DisplayName("returns SQL unchanged when no placeholders")
    void noPlaceholdersUnchanged() {
      String sql = "SELECT * FROM users WHERE id = 42";
      String result = MySqlExplainAnalyzer.prepareForExplain(sql);
      assertThat(result).isEqualTo(sql);
    }

    @Test
    @DisplayName("EXPLAIN succeeds with parameterized SQL after replacement")
    void explainSucceedsWithParameterizedSql() throws SQLException {
      mockExplainResult("users", "ref", "PRIMARY", 1L, null);

      List<QueryRecord> queries =
          List.of(new QueryRecord("SELECT * FROM users WHERE id = ?", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(connection, queries);

      // Should not throw — the ? was replaced before EXPLAIN
      verify(statement).executeQuery("EXPLAIN SELECT * FROM users WHERE id = 1");
    }

    @Test
    @DisplayName("EXPLAIN succeeds with multi-placeholder SQL after replacement")
    void explainSucceedsWithMultiPlaceholder() throws SQLException {
      mockExplainResult("users", "ref", "PRIMARY", 1L, null);

      List<QueryRecord> queries =
          List.of(new QueryRecord("SELECT * FROM users WHERE a = ? AND b = ?", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(connection, queries);

      verify(statement).executeQuery("EXPLAIN SELECT * FROM users WHERE a = 1 AND b = 1");
    }
  }

  @Nested
  @DisplayName("Mixed scenarios")
  class MixedTests {

    @Test
    @DisplayName("handles multiple queries with different EXPLAIN results")
    void handlesMultipleQueries() throws SQLException {
      Statement stmt1 = mock(Statement.class);
      Statement stmt2 = mock(Statement.class);
      ResultSet rs1 = mock(ResultSet.class);
      ResultSet rs2 = mock(ResultSet.class);

      when(connection.createStatement()).thenReturn(stmt1, stmt2);

      // First query: full table scan
      when(stmt1.executeQuery(startsWith("EXPLAIN"))).thenReturn(rs1);
      when(rs1.next()).thenReturn(true);
      when(rs1.getString("table")).thenReturn("users");
      when(rs1.getString("type")).thenReturn("ALL");
      when(rs1.getString("key")).thenReturn(null);
      when(rs1.getLong("rows")).thenReturn(10000L);
      when(rs1.getString("Extra")).thenReturn(null);

      // Second query: uses index
      when(stmt2.executeQuery(startsWith("EXPLAIN"))).thenReturn(rs2);
      when(rs2.next()).thenReturn(true);
      when(rs2.getString("table")).thenReturn("orders");
      when(rs2.getString("type")).thenReturn("ref");
      when(rs2.getString("key")).thenReturn("idx_user_id");
      when(rs2.getLong("rows")).thenReturn(5L);
      when(rs2.getString("Extra")).thenReturn(null);

      List<QueryRecord> queries =
          List.of(
              new QueryRecord("SELECT * FROM users", 0L, 0L, null),
              new QueryRecord("SELECT * FROM orders WHERE user_id = 1", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(connection, queries);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.FULL_TABLE_SCAN);
      assertThat(issues.get(0).table()).isEqualTo("users");
    }
  }

  private void mockExplainResult(String table, String type, String key, long rows, String extra)
      throws SQLException {
    when(statement.executeQuery(startsWith("EXPLAIN"))).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getString("table")).thenReturn(table);
    when(resultSet.getString("type")).thenReturn(type);
    when(resultSet.getString("key")).thenReturn(key);
    when(resultSet.getLong("rows")).thenReturn(rows);
    when(resultSet.getString("Extra")).thenReturn(extra);
  }
}
