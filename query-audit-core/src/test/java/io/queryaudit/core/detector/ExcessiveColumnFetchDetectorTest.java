package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ExcessiveColumnFetchDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private final ExcessiveColumnFetchDetector detector = new ExcessiveColumnFetchDetector();

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private static String buildSelectWithColumns(String table, int columnCount) {
    StringBuilder sb = new StringBuilder("SELECT ");
    for (int i = 1; i <= columnCount; i++) {
      if (i > 1) sb.append(", ");
      sb.append("t.col").append(i);
    }
    sb.append(" FROM ").append(table).append(" t WHERE t.id = 1");
    return sb.toString();
  }

  // ── Positive: Issues detected ───────────────────────────────────────

  @Nested
  @DisplayName("Detects excessive column fetch")
  class PositiveCases {

    @Test
    @DisplayName("Detects query with 16 columns (above default threshold of 15)")
    void detectsAboveThreshold() {
      String sql = buildSelectWithColumns("users", 16);
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.EXCESSIVE_COLUMN_FETCH);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
      assertThat(issues.get(0).detail()).contains("16 columns");
    }

    @Test
    @DisplayName("Detects query with 20 columns")
    void detectsWith20Columns() {
      String sql = buildSelectWithColumns("orders", 20);
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).table()).isEqualTo("orders");
    }

    @Test
    @DisplayName("Suggestion mentions DTO projection")
    void suggestionMentionsDtoProjection() {
      String sql = buildSelectWithColumns("users", 20);
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues.get(0).suggestion()).contains("DTO projection");
      assertThat(issues.get(0).suggestion()).contains("network I/O");
    }

    @Test
    @DisplayName("Handles columns with aliases")
    void handlesColumnsWithAliases() {
      String sql =
          "SELECT t.col1 AS c1, t.col2, t.col3, t.col4, t.col5, "
              + "t.col6, t.col7, t.col8, t.col9, t.col10, "
              + "t.col11, t.col12, t.col13, t.col14, t.col15, t.col16 "
              + "FROM users t WHERE t.id = 1";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Handles columns without table prefix")
    void handlesColumnsWithoutTablePrefix() {
      String sql =
          "SELECT col1, col2, col3, col4, col5, "
              + "col6, col7, col8, col9, col10, "
              + "col11, col12, col13, col14, col15, col16 "
              + "FROM users WHERE id = 1";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }
  }

  // ── Negative: No issues ─────────────────────────────────────────────

  @Nested
  @DisplayName("No issue cases")
  class NegativeCases {

    @Test
    @DisplayName("No issue when column count is at threshold (15)")
    void noIssueAtThreshold() {
      String sql = buildSelectWithColumns("users", 15);
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue when few columns selected")
    void noIssueWithFewColumns() {
      String sql = "SELECT id, name, email FROM users WHERE id = 1";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for SELECT * (handled by SelectAllDetector)")
    void noIssueForSelectStar() {
      String sql = "SELECT * FROM users WHERE id = 1";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for queries with GROUP BY")
    void noIssueWithGroupBy() {
      String sql = buildSelectWithColumns("orders", 20) + " GROUP BY t.col1";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for queries with aggregate functions")
    void noIssueWithAggregateFunctions() {
      String sql =
          "SELECT t.col1, t.col2, t.col3, t.col4, t.col5, "
              + "t.col6, t.col7, t.col8, t.col9, t.col10, "
              + "t.col11, t.col12, t.col13, t.col14, t.col15, "
              + "COUNT(t.col16) FROM orders t WHERE t.id = 1";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for INSERT queries")
    void noIssueForInsert() {
      String sql =
          "INSERT INTO users (col1, col2, col3, col4, col5, "
              + "col6, col7, col8, col9, col10, col11, col12, col13, col14, col15, col16) "
              + "VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for UPDATE queries")
    void noIssueForUpdate() {
      String sql = "UPDATE users SET name = 'test' WHERE id = 1";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for DELETE queries")
    void noIssueForDelete() {
      String sql = "DELETE FROM users WHERE id = 1";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Empty query list")
    void emptyQueryList() {
      List<Issue> issues = detector.evaluate(List.of(), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // ── Edge Cases ──────────────────────────────────────────────────────

  @Nested
  @DisplayName("Edge Cases")
  class EdgeCases {

    @Test
    @DisplayName("Custom threshold works")
    void customThreshold() {
      ExcessiveColumnFetchDetector strict = new ExcessiveColumnFetchDetector(5);
      String sql = buildSelectWithColumns("users", 6);

      List<Issue> issues = strict.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Duplicate queries flagged only once")
    void duplicateQueriesFlaggedOnce() {
      String sql = buildSelectWithColumns("users", 20);
      List<QueryRecord> queries = List.of(record(sql), record(sql), record(sql));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Function columns not counted")
    void functionColumnsNotCounted() {
      // Only 14 simple columns + 2 function calls = should not trigger
      String sql =
          "SELECT t.col1, t.col2, t.col3, t.col4, t.col5, "
              + "t.col6, t.col7, t.col8, t.col9, t.col10, "
              + "t.col11, t.col12, t.col13, t.col14, "
              + "COALESCE(t.col15, 0), UPPER(t.col16) "
              + "FROM users t WHERE t.id = 1";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("countSimpleColumns correctly counts plain references")
    void countSimpleColumnsTest() {
      String columnList = "t.col1, t.col2, col3, UPPER(col4), col5 AS c5";
      int count = detector.countSimpleColumns(columnList);
      // t.col1, t.col2, col3, col5 (alias removed) = 4; UPPER(col4) is skipped
      assertThat(count).isEqualTo(4);
    }

    @Test
    @DisplayName("countSimpleColumns returns 0 for null/blank")
    void countSimpleColumnsNullBlank() {
      assertThat(detector.countSimpleColumns(null)).isEqualTo(0);
      assertThat(detector.countSimpleColumns("")).isEqualTo(0);
      assertThat(detector.countSimpleColumns("  ")).isEqualTo(0);
    }

    @Test
    @DisplayName("Handles DISTINCT keyword before columns")
    void handlesDistinct() {
      String sql =
          "SELECT DISTINCT "
              + "t.col1, t.col2, t.col3, t.col4, t.col5, "
              + "t.col6, t.col7, t.col8, t.col9, t.col10, "
              + "t.col11, t.col12, t.col13, t.col14, t.col15, t.col16 "
              + "FROM users t WHERE t.id = 1";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }
  }
}
