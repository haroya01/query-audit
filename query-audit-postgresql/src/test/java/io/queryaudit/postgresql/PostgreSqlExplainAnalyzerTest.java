package io.queryaudit.postgresql;

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

      List<QueryRecord> queries =
          List.of(new QueryRecord("SELECT * FROM users", 0L, 0L, null));

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
          List.of(
              new QueryRecord(
                  "SELECT * FROM orders ORDER BY total", 0L, 0L, null));

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

      List<QueryRecord> queries =
          List.of(new QueryRecord("SELECT * FROM orders", 0L, 0L, null));

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

      List<QueryRecord> queries =
          List.of(new QueryRecord("SELECT * FROM orders", 0L, 0L, null));

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
    @DisplayName("deduplicates by normalized SQL")
    void deduplicates() throws SQLException {
      String json =
          """
          [{"Plan": {"Node Type": "Seq Scan", "Relation Name": "users", "Plan Rows": 100}}]
          """;
      mockExplainResult(json);

      List<QueryRecord> queries =
          List.of(
              new QueryRecord("SELECT * FROM users WHERE id = 1", 0L, 0L, null),
              new QueryRecord("SELECT * FROM users WHERE id = 2", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(connection, queries);

      assertThat(issues).hasSize(1);
      verify(statement, times(1)).executeQuery(startsWith("EXPLAIN"));
    }
  }

  @Nested
  @DisplayName("Placeholder replacement for EXPLAIN")
  class PlaceholderReplacementTests {

    @Test
    @DisplayName("replaces single ? placeholder with dummy value")
    void replacesSinglePlaceholder() {
      String result = PostgreSqlExplainAnalyzer.prepareForExplain(
          "SELECT * FROM users WHERE id = ?");
      assertThat(result).isEqualTo("SELECT * FROM users WHERE id = 1");
    }

    @Test
    @DisplayName("replaces multiple ? placeholders")
    void replacesMultiplePlaceholders() {
      String result = PostgreSqlExplainAnalyzer.prepareForExplain(
          "SELECT * FROM users WHERE a = ? AND b = ?");
      assertThat(result).isEqualTo("SELECT * FROM users WHERE a = 1 AND b = 1");
    }

    @Test
    @DisplayName("returns SQL unchanged when no placeholders")
    void noPlaceholdersUnchanged() {
      String sql = "SELECT * FROM users WHERE id = 42";
      String result = PostgreSqlExplainAnalyzer.prepareForExplain(sql);
      assertThat(result).isEqualTo(sql);
    }

    @Test
    @DisplayName("EXPLAIN succeeds with parameterized SQL after replacement")
    void explainSucceedsWithParameterizedSql() throws SQLException {
      String json =
          """
          [{"Plan": {"Node Type": "Index Scan", "Relation Name": "users", "Plan Rows": 1}}]
          """;
      mockExplainResult(json);

      List<QueryRecord> queries =
          List.of(new QueryRecord("SELECT * FROM users WHERE id = ?", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(connection, queries);

      verify(statement).executeQuery(
          "EXPLAIN (FORMAT JSON) SELECT * FROM users WHERE id = 1");
    }

    @Test
    @DisplayName("EXPLAIN succeeds with multi-placeholder SQL after replacement")
    void explainSucceedsWithMultiPlaceholder() throws SQLException {
      String json =
          """
          [{"Plan": {"Node Type": "Index Scan", "Relation Name": "users", "Plan Rows": 1}}]
          """;
      mockExplainResult(json);

      List<QueryRecord> queries =
          List.of(new QueryRecord(
              "SELECT * FROM users WHERE a = ? AND b = ?", 0L, 0L, null));

      List<Issue> issues = analyzer.analyze(connection, queries);

      verify(statement).executeQuery(
          "EXPLAIN (FORMAT JSON) SELECT * FROM users WHERE a = 1 AND b = 1");
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

  private void mockExplainResult(String jsonOutput) throws SQLException {
    when(statement.executeQuery(startsWith("EXPLAIN"))).thenReturn(resultSet);
    when(resultSet.next()).thenReturn(true);
    when(resultSet.getString(1)).thenReturn(jsonOutput);
  }
}
