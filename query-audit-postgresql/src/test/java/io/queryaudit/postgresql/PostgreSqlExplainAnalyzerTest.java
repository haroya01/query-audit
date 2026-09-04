package io.queryaudit.postgresql;

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

class PostgreSqlExplainAnalyzerTest {

  private PostgreSqlExplainAnalyzer analyzer;

  @Mock private Connection connection;
  @Mock private Statement statement;
  @Mock private ResultSet resultSet;

  @BeforeEach
  void setUp() throws SQLException {
    MockitoAnnotations.openMocks(this);
    analyzer = new PostgreSqlExplainAnalyzer();
    when(connection.createStatement()).thenReturn(statement);
  }

  @Test
  @DisplayName("supportedDatabase() returns 'postgresql'")
  void supportedDatabaseReturnsPostgresql() {
    assertThat(analyzer.supportedDatabase()).isEqualTo("postgresql");
  }

  @Nested
  @DisplayName("Full table scan detection")
  class FullTableScanTests {

    @Test
    @DisplayName("detects Seq Scan as full table scan")
    void detectsSeqScan() throws SQLException {
      String json =
          """
          [{"Plan": {"Node Type": "Seq Scan", "Relation Name": "users", "Plan Rows": 1000}}]
          """;
      mockExplainResult(json);

      List<QueryRecord> queries = List.of(new QueryRecord("SELECT * FROM users", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(connection, queries);

      assertThat(issues)
          .anyMatch(
              i ->
                  i.type() == IssueType.FULL_TABLE_SCAN
                      && i.severity() == Severity.INFO
                      && "users".equals(i.table()));
    }
  }

  @Nested
  @DisplayName("Sort detection")
  class SortTests {

    @Test
    @DisplayName("detects Sort node as filesort")
    void detectsSort() throws SQLException {
      String json =
          """
          [{"Plan": {"Node Type": "Sort", "Relation Name": "orders", "Plan Rows": 500}}]
          """;
      mockExplainResult(json);

      List<QueryRecord> queries =
          List.of(new QueryRecord("SELECT * FROM orders ORDER BY total", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(connection, queries);

      assertThat(issues).anyMatch(i -> i.type() == IssueType.FILESORT);
    }
  }

  @Nested
  @DisplayName("Temporary table detection")
  class TemporaryTableTests {

    @Test
    @DisplayName("detects Hash node as temporary table")
    void detectsHash() throws SQLException {
      String json =
          """
          [{"Plan": {"Node Type": "Hash", "Relation Name": "orders", "Plan Rows": 100}}]
          """;
      mockExplainResult(json);

      List<QueryRecord> queries = List.of(new QueryRecord("SELECT * FROM orders", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(connection, queries);

      assertThat(issues).anyMatch(i -> i.type() == IssueType.TEMPORARY_TABLE);
    }

    @Test
    @DisplayName("detects Materialize node as temporary table")
    void detectsMaterialize() throws SQLException {
      String json =
          """
          [{"Plan": {"Node Type": "Materialize", "Relation Name": "orders"}}]
          """;
      mockExplainResult(json);

      List<QueryRecord> queries = List.of(new QueryRecord("SELECT * FROM orders", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(connection, queries);

      assertThat(issues).anyMatch(i -> i.type() == IssueType.TEMPORARY_TABLE);
    }
  }

  @Nested
  @DisplayName("Query filtering")
  class FilteringTests {

    @Test
    @DisplayName("skips non-SELECT queries")
    void skipsNonSelect() throws SQLException {
      List<QueryRecord> queries =
          List.of(new QueryRecord("INSERT INTO users (name) VALUES ('test')", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(connection, queries);

      assertThat(issues).isEmpty();
      verify(statement, never()).executeQuery(startsWith("EXPLAIN"));
    }

    @Test
    @DisplayName("deduplicates identical captured SQL")
    void deduplicates() throws SQLException {
      String json =
          """
          [{"Plan": {"Node Type": "Seq Scan", "Relation Name": "users", "Plan Rows": 100}}]
          """;
      mockExplainResult(json);

      List<QueryRecord> queries =
          List.of(
              new QueryRecord("SELECT * FROM users WHERE id = 1", 0L, 0L, null),
              new QueryRecord("SELECT * FROM users WHERE id = 1", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(connection, queries);

      assertThat(issues).hasSize(1);
      verify(statement, times(1)).executeQuery(startsWith("EXPLAIN"));
    }
  }

  @Test
  void differentLiteralValuesAreExplainedSeparately() throws SQLException {
    mockExplainResult(
        "[{\"Plan\": {\"Node Type\": \"Seq Scan\", \"Relation Name\": \"users\", \"Plan Rows\": 100}}]");
    List<QueryRecord> queries =
        List.of(
            new QueryRecord("SELECT * FROM users WHERE id = 1", 0L, 0L, null),
            new QueryRecord("SELECT * FROM users WHERE id = 2", 0L, 0L, null));

    analyzer.analyze(connection, queries);

    verify(statement).executeQuery("EXPLAIN (FORMAT JSON) SELECT * FROM users WHERE id = 1");
    verify(statement).executeQuery("EXPLAIN (FORMAT JSON) SELECT * FROM users WHERE id = 2");
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
      mockExplainResult(
          "[{\"Plan\": {\"Node Type\": \"Seq Scan\", \"Relation Name\": \"users\", \"Plan Rows\": 100}}]");
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
  @DisplayName("JSON field extraction")
  class JsonExtractionTests {

    @Test
    @DisplayName("extractJsonField extracts string value")
    void extractsStringField() {
      String json = """
          {"Relation Name": "users", "Node Type": "Seq Scan"}
          """;
      assertThat(PostgreSqlExplainAnalyzer.extractJsonField(json, "Relation Name"))
          .isEqualTo("users");
    }

    @Test
    @DisplayName("extractJsonField returns null when field not found")
    void returnsNullWhenNotFound() {
      assertThat(PostgreSqlExplainAnalyzer.extractJsonField("{}", "missing")).isNull();
    }

    @Test
    @DisplayName("extractJsonLong extracts numeric value")
    void extractsLongField() {
      String json = """
          {"Plan Rows": 1234}
          """;
      assertThat(PostgreSqlExplainAnalyzer.extractJsonLong(json, "Plan Rows")).isEqualTo(1234L);
    }

    @Test
    @DisplayName("extractJsonLong returns 0 when field not found")
    void returnsZeroWhenNotFound() {
      assertThat(PostgreSqlExplainAnalyzer.extractJsonLong("{}", "missing")).isEqualTo(0L);
    }
  }

  @Test
  void aLaterFailureRetainsCompletedFindingsAndTheOriginalCause() throws SQLException {
    mockExplainResult("[{\"Plan\": {\"Node Type\": \"Seq Scan\", \"Relation Name\": \"users\"}}]");
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

  private void mockExplainResult(String jsonOutput) throws SQLException {
    when(statement.executeQuery(startsWith("EXPLAIN"))).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getString(1)).thenReturn(jsonOutput);
  }
}
