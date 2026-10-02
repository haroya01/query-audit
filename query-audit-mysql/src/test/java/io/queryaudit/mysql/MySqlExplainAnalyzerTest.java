package io.queryaudit.mysql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.*;

import io.queryaudit.core.analyzer.ExplainAnalysisException;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
    @DisplayName("deduplicates identical captured SQL")
    void deduplicatesIdenticalSql() throws SQLException {
      mockExplainResult("users", "ALL", null, 100L, null);

      // The same statement can reuse its plan.
      List<QueryRecord> queries =
          List.of(
              new QueryRecord("SELECT * FROM users WHERE id = 1", 0L, 0L, null),
              new QueryRecord("SELECT * FROM users WHERE id = 1", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(connection, queries);

      // Should only produce one FULL_TABLE_SCAN, not two
      assertThat(issues).hasSize(1);
      verify(statement, times(1)).executeQuery(startsWith("EXPLAIN"));
    }

    @Test
    @DisplayName("EXPLAIN failures do not look like a clean plan")
    void handlesExplainFailures() throws SQLException {
      when(statement.executeQuery(startsWith("EXPLAIN")))
          .thenThrow(new SQLException("Syntax error"));

      List<QueryRecord> queries =
          List.of(new QueryRecord("SELECT * FROM nonexistent_table", 0L, 0L, null));

      assertThatThrownBy(() -> analyzer.analyze(connection, queries))
          .isInstanceOf(ExplainAnalysisException.class)
          .hasCauseInstanceOf(SQLException.class)
          .hasMessage("EXPLAIN analysis did not complete");
    }

    @Test
    @DisplayName("returns empty list for empty query list")
    void emptyQueriesReturnEmpty() {
      List<Issue> issues = analyzer.analyze(connection, List.of());

      assertThat(issues).isEmpty();
    }
  }

  @Test
  void differentLiteralValuesAreExplainedSeparately() throws SQLException {
    mockExplainResult("users", "ALL", null, 100L, null);
    List<QueryRecord> queries =
        List.of(
            new QueryRecord("SELECT * FROM users WHERE id = 1", 0L, 0L, null),
            new QueryRecord("SELECT * FROM users WHERE id = 2", 0L, 0L, null));

    analyzer.analyze(connection, queries);

    verify(statement).executeQuery("EXPLAIN SELECT * FROM users WHERE id = 1");
    verify(statement).executeQuery("EXPLAIN SELECT * FROM users WHERE id = 2");
  }

  @Nested
  @DisplayName("EXPLAIN input safety")
  class InputSafetyTests {

    @ParameterizedTest
    @ValueSource(
        strings = {
          "SELECT * FROM users WHERE id = ?",
          "SELECT * FROM users WHERE a = ? AND b = ?",
          "SELECT '?' FROM users",
          "SELECT * FROM users WHERE payload ? 'active'",
          "SELECT * FROM users /* ? */"
        })
    void rejectsQuestionMarksWithoutExecutingChangedSql(String sql) {
      assertThatThrownBy(
              () -> analyzer.analyze(connection, List.of(new QueryRecord(sql, 0L, 0L, null))))
          .isInstanceOfSatisfying(
              ExplainAnalysisException.class,
              failure -> {
                assertThat(failure.getReason())
                    .isEqualTo(ExplainAnalysisException.Reason.UNSUPPORTED_PARAMETERS);
                assertThat(failure.getCompletedIssues()).isEmpty();
                assertThat(failure).hasMessageContaining("bind values and types are unavailable");
              });
      verifyNoInteractions(connection);
    }

    @Test
    void anEarlierLiteralPlanDoesNotHideUnsupportedParameters() throws SQLException {
      mockExplainResult("users", "ALL", null, 100L, null);
      List<QueryRecord> queries =
          List.of(
              new QueryRecord("SELECT * FROM users WHERE id = 1", 0L, 0L, null),
              new QueryRecord("SELECT * FROM users WHERE id = ?", 0L, 0L, null));

      assertThatThrownBy(() -> analyzer.analyze(connection, queries))
          .isInstanceOfSatisfying(
              ExplainAnalysisException.class,
              failure -> {
                assertThat(failure.getReason())
                    .isEqualTo(ExplainAnalysisException.Reason.UNSUPPORTED_PARAMETERS);
                assertThat(failure.getCompletedIssues()).hasSize(1);
              });
      verify(statement, times(1)).executeQuery(startsWith("EXPLAIN"));
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

  @Test
  void aLaterFailureRetainsCompletedFindingsAndTheOriginalCause() throws SQLException {
    mockExplainResult("users", "ALL", null, 100L, null);
    SQLException failure = new SQLException("private SQL and connection details");
    when(statement.executeQuery(startsWith("EXPLAIN"))).thenReturn(resultSet).thenThrow(failure);
    List<QueryRecord> queries =
        List.of(
            new QueryRecord("SELECT * FROM users", 0L, 0L, null),
            new QueryRecord("SELECT * FROM orders", 0L, 0L, null));

    assertThatThrownBy(() -> analyzer.analyze(connection, queries))
        .isInstanceOfSatisfying(
            ExplainAnalysisException.class,
            incomplete -> {
              assertThat(incomplete.getCause()).isSameAs(failure);
              assertThat(incomplete.getMessage()).doesNotContain("private SQL");
              assertThat(incomplete.getCompletedIssues())
                  .extracting(Issue::type)
                  .containsExactly(IssueType.FULL_TABLE_SCAN);
            });
  }

    @Test
    void anEmptyExplainResponseIsIncomplete() throws SQLException {
        when(statement.executeQuery(startsWith("EXPLAIN"))).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(false);
        assertThatThrownBy(
                () ->
                    analyzer.analyze(
                        connection, List.of(new QueryRecord("SELECT * FROM users", 0L, 0L, null))))
            .isInstanceOf(ExplainAnalysisException.class);
    }

    @Nested
    @DisplayName("Multi-row EXPLAIN results")
    class MultiRowTests {

        @Test
        @DisplayName("detects issues in second row when first row is clean")
        void detectsIssueInSecondRow() throws SQLException {
            // First row: const/PRIMARY (clean)
            // Second row: ALL + Using filesort (problematic)
            when(statement.executeQuery(startsWith("EXPLAIN"))).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(true, true, false); // Two rows, then end
            
            // First row
            when(resultSet.getString("table")).thenReturn("a", "o");
            when(resultSet.getString("type")).thenReturn("const", "ALL");
            when(resultSet.getString("key")).thenReturn("PRIMARY", null);
            when(resultSet.getLong("rows")).thenReturn(1L, 100000L);
            when(resultSet.getString("Extra")).thenReturn("", "Using filesort");
            
            List<QueryRecord> queries = List.of(
                new QueryRecord("SELECT a.id, o.amount FROM accounts a JOIN orders o ON o.account_id = a.id WHERE a.id = 1", 0L, 0L, null));

            List<Issue> issues = analyzer.analyze(connection, queries);

            // Should find exactly one issue: filesort on table 'o'
            assertThat(issues).hasSize(1);
            Issue issue = issues.get(0);
            assertThat(issue.type()).isEqualTo(IssueType.FILESORT);
            assertThat(issue.table()).isEqualTo("o");
            assertThat(issue.detail()).contains("Using filesort").contains("o").contains("100000");
        }

        @Test
        @DisplayName("detects issues only in the last row of multiple rows")
        void detectsIssueOnlyInLastRow() throws SQLException {
            // Three rows: first two clean, last problematic
            when(statement.executeQuery(startsWith("EXPLAIN"))).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(true, true, true, false); // Three rows, then end
            
            // Row 1: const/PRIMARY
            when(resultSet.getString("table")).thenReturn("a", "b", "o");
            when(resultSet.getString("type")).thenReturn("const", "const", "ALL");
            when(resultSet.getString("key")).thenReturn("PRIMARY", "PRIMARY", null);
            when(resultSet.getLong("rows")).thenReturn(1L, 1L, 50000L);
            when(resultSet.getString("Extra")).thenReturn("", "", "Using temporary");
            
            List<QueryRecord> queries = List.of(
                new QueryRecord("SELECT * FROM a JOIN b ON a.id = b.a_id JOIN o ON b.id = o.b_id WHERE a.id = 1", 0L, 0L, null));

            List<Issue> issues = analyzer.analyze(connection, queries);

            // Should find exactly one issue: temporary table on table 'o'
            assertThat(issues).hasSize(1);
            Issue issue = issues.get(0);
            assertThat(issue.type()).isEqualTo(IssueType.TEMPORARY_TABLE);
            assertThat(issue.table()).isEqualTo("o");
            assertThat(issue.detail()).contains("Using temporary").contains("o").contains("50000");
        }

        @Test
        @DisplayName("reports all distinct issues from multiple problematic rows")
        void reportsAllDistinctIssues() throws SQLException {
            // Two problematic rows: one with filesort, one with temporary table
            when(statement.executeQuery(startsWith("EXPLAIN"))).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(true, true, false); // Two rows, then end
            
            // Row 1: Using filesort
            // Row 2: Using temporary
            when(resultSet.getString("table")).thenReturn("orders", "customers");
            when(resultSet.getString("type")).thenReturn("ref", "ALL");
            when(resultSet.getString("key")).thenReturn("idx_user_id", null);
            when(resultSet.getLong("rows")).thenReturn(100L, 1000L);
            when(resultSet.getString("Extra")).thenReturn("Using filesort", "Using temporary");
            
            List<QueryRecord> queries = List.of(
                new QueryRecord("SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id WHERE orders.amount > 100", 0L, 0L, null));

            List<Issue> issues = analyzer.analyze(connection, queries);

            // Check that we have one filesort issue on orders and one temporary table issue on customers
            boolean foundFilesortOnOrders = false;
            boolean foundTemporaryTableOnCustomers = false;
            for (Issue issue : issues) {
                if (issue.type() == IssueType.FILESORT && issue.table().equals("orders")) {
                    foundFilesortOnOrders = true;
                }
                if (issue.type() == IssueType.TEMPORARY_TABLE && issue.table().equals("customers")) {
                    foundTemporaryTableOnCustomers = true;
                }
            }
            assertThat(foundFilesortOnOrders).isTrue();
            assertThat(foundTemporaryTableOnCustomers).isTrue();
        }

        @Test
        @DisplayName("deduplicates same table/problem appearing in multiple rows")
        void deduplicatesSameTableAndIssue() throws SQLException {
            // Two rows for same table, both showing same issue type
            when(statement.executeQuery(startsWith("EXPLAIN"))).thenReturn(resultSet);
            when(resultSet.next()).thenReturn(true, true, false); // Two rows, then end
            
            // Both rows for table 'big_table', both showing Using filesort
            when(resultSet.getString("table")).thenReturn("big_table", "big_table");
            when(resultSet.getString("type")).thenReturn("ALL", "ALL");
            when(resultSet.getString("key")).thenReturn(null, null);
            when(resultSet.getLong("rows")).thenReturn(1000L, 5000L);
            when(resultSet.getString("Extra")).thenReturn("Using filesort", "Using filesort");
            
            List<QueryRecord> queries = List.of(
                new QueryRecord("SELECT * FROM big_table WHERE category IN (SELECT cat FROM categories)", 0L, 0L, null));

            List<Issue> issues = analyzer.analyze(connection, queries);

            // Should find exactly one issue for filesort on big_table (deduplicated)
            assertThat(issues).hasSize(1);
            Issue issue = issues.get(0);
            assertThat(issue.type()).isEqualTo(IssueType.FILESORT);
            assertThat(issue.table()).isEqualTo("big_table");
            // Note: the detail will show the values from whichever row was processed first
            // The important thing is that we don't get two issues for the same table+issue type
        }

        @Test
        @DisplayName("single-row plan behaves identically to before")
        void singleRowPlanUnchanged() throws SQLException {
            mockExplainResult("users", "ALL", null, 100L, null);
            
            List<QueryRecord> queries = List.of(
                new QueryRecord("SELECT * FROM users", 0L, 0L, null));

            List<Issue> issues = analyzer.analyze(connection, queries);

            assertThat(issues).hasSize(1);
            Issue issue = issues.get(0);
            assertThat(issue.type()).isEqualTo(IssueType.FULL_TABLE_SCAN);
            assertThat(issue.table()).isEqualTo("users");
            assertThat(issue.detail()).contains("type=ALL").contains("100");
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
