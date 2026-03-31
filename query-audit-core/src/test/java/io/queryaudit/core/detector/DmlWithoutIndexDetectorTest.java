package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexInfo;
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

class DmlWithoutIndexDetectorTest {

  private final DmlWithoutIndexDetector detector = new DmlWithoutIndexDetector();

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private static IndexMetadata indexOn(String table, String indexName, String column) {
    return new IndexMetadata(
        Map.of(table, List.of(new IndexInfo(table, indexName, column, 1, true, 1000))));
  }

  private static IndexMetadata indexWithPrimaryKey(String table) {
    return new IndexMetadata(
        Map.of(table, List.of(new IndexInfo(table, "PRIMARY", "id", 1, false, 10000))));
  }

  // ── Positive: Issues detected ───────────────────────────────────────

  @Nested
  @DisplayName("Detects DML without index")
  class PositiveCases {

    @Test
    @DisplayName("Detects UPDATE WHERE on unindexed column")
    void detectsUpdateWithoutIndex() {
      String sql = "UPDATE orders SET processed = true WHERE status = 'pending'";
      IndexMetadata metadata = indexWithPrimaryKey("orders");

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.DML_WITHOUT_INDEX);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
      assertThat(issues.get(0).table()).isEqualTo("orders");
      assertThat(issues.get(0).detail()).contains("full table scan").contains("row locks");
    }

    @Test
    @DisplayName("Detects DELETE WHERE on unindexed column")
    void detectsDeleteWithoutIndex() {
      String sql = "DELETE FROM sessions WHERE user_id = 123";
      IndexMetadata metadata = indexWithPrimaryKey("sessions");

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.DML_WITHOUT_INDEX);
      assertThat(issues.get(0).table()).isEqualTo("sessions");
    }

    @Test
    @DisplayName("Reports column names in detail")
    void reportsColumnNames() {
      String sql = "UPDATE users SET active = false WHERE email = 'test@test.com'";
      IndexMetadata metadata = indexWithPrimaryKey("users");

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).column()).contains("email");
    }

    @Test
    @DisplayName("Suggestion mentions index and locking")
    void suggestionMentionsIndexAndLocking() {
      String sql = "DELETE FROM logs WHERE created_at < '2024-01-01'";
      IndexMetadata metadata = indexWithPrimaryKey("logs");

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      assertThat(issues.get(0).suggestion()).contains("index");
      assertThat(issues.get(0).suggestion()).contains("lock");
    }
  }

  // ── Negative: No issues ─────────────────────────────────────────────

  @Nested
  @DisplayName("No issue when index exists")
  class NegativeCases {

    @Test
    @DisplayName("No issue when WHERE column has index")
    void noIssueWhenIndexed() {
      String sql = "UPDATE orders SET processed = true WHERE status = 'pending'";
      IndexMetadata metadata = indexOn("orders", "idx_status", "status");

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue when WHERE uses primary key")
    void noIssueOnPrimaryKey() {
      String sql = "DELETE FROM orders WHERE id = 123";
      IndexMetadata metadata = indexWithPrimaryKey("orders");

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for SELECT queries")
    void ignoresSelectQueries() {
      String sql = "SELECT * FROM orders WHERE status = 'pending'";
      IndexMetadata metadata = indexWithPrimaryKey("orders");

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue when no index metadata available")
    void noIssueWithoutMetadata() {
      String sql = "UPDATE orders SET processed = true WHERE status = 'pending'";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), null);
      assertThat(issues).isEmpty();

      List<Issue> issues2 = detector.evaluate(List.of(record(sql)), new IndexMetadata(Map.of()));
      assertThat(issues2).isEmpty();
    }

    @Test
    @DisplayName("No issue when UPDATE has no WHERE (handled by UpdateWithoutWhereDetector)")
    void noIssueWithoutWhere() {
      String sql = "UPDATE users SET active = false";
      IndexMetadata metadata = indexWithPrimaryKey("users");

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue when table not in metadata")
    void noIssueWhenTableNotInMetadata() {
      String sql = "UPDATE unknown_table SET col = 1 WHERE id = 1";
      IndexMetadata metadata = indexWithPrimaryKey("other_table");

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      assertThat(issues).isEmpty();
    }
  }

  // ── Edge Cases ──────────────────────────────────────────────────────

  @Nested
  @DisplayName("Edge Cases")
  class EdgeCases {

    @Test
    @DisplayName("Deduplicates same normalized query")
    void deduplicates() {
      String sql1 = "UPDATE orders SET processed = true WHERE status = 'pending'";
      String sql2 = "UPDATE orders SET processed = true WHERE status = 'active'";
      IndexMetadata metadata = indexWithPrimaryKey("orders");

      List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), metadata);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Checks leading column of composite index (seqInIndex=1)")
    void checksLeadingColumn() {
      String sql = "UPDATE orders SET processed = true WHERE user_id = 1";
      // composite index (status, user_id) — user_id is at position 2, not leading
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "orders",
                  List.of(
                      new IndexInfo("orders", "idx_status_user", "status", 1, true, 1000),
                      new IndexInfo("orders", "idx_status_user", "user_id", 2, true, 1000))));

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      // user_id is not the leading column, so issue should be reported
      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("No issue when WHERE column matches leading column of composite index")
    void noIssueWithLeadingColumn() {
      String sql = "UPDATE orders SET processed = true WHERE status = 'pending'";
      // status is the leading column
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "orders",
                  List.of(
                      new IndexInfo("orders", "idx_status_user", "status", 1, true, 1000),
                      new IndexInfo("orders", "idx_status_user", "user_id", 2, true, 1000))));

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Empty query list returns no issues")
    void emptyQueryList() {
      List<Issue> issues = detector.evaluate(List.of(), indexWithPrimaryKey("orders"));
      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No NPE when expression-based index has null columnName (GitHub #31)")
    void noNpeOnExpressionBasedIndexWithNullColumnName() {
      String sql = "UPDATE orders SET processed = true WHERE status = 'pending'";
      // Expression-based index returns null for columnName
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "orders",
                  List.of(
                      new IndexInfo("orders", "idx_expr", null, 1, true, 500),
                      new IndexInfo("orders", "PRIMARY", "id", 1, false, 10000))));

      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      // Should not throw NPE; status is not indexed so issue is reported
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.DML_WITHOUT_INDEX);
    }
  }

  // ── Mutation-killing tests ──────────────────────────────────────────

  /**
   * Kills NegateConditionalsMutator on line 85: !hasIndexedColumn negated. If negated, issues would
   * be reported when the column IS indexed (false positive), and NOT reported when the column is
   * NOT indexed (false negative).
   */
  @Test
  @DisplayName("Correctly distinguishes indexed vs unindexed WHERE columns (kills line 85)")
  void indexedColumnDoesNotTriggerIssue_unindexedDoes_killsLine85() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "PRIMARY", "id", 1, false, 10000),
                    new IndexInfo("orders", "idx_status", "status", 1, true, 1000))));

    // Indexed column -> no issue
    String indexed = "UPDATE orders SET processed = true WHERE status = 'pending'";
    List<Issue> indexedIssues = detector.evaluate(List.of(record(indexed)), metadata);
    assertThat(indexedIssues).isEmpty();

    // Unindexed column -> issue
    String unindexed = "UPDATE orders SET processed = true WHERE region = 'US'";
    List<Issue> unindexedIssues = detector.evaluate(List.of(record(unindexed)), metadata);
    assertThat(unindexedIssues).hasSize(1);
    assertThat(unindexedIssues.get(0).type()).isEqualTo(IssueType.DML_WITHOUT_INDEX);
  }

  @Test
  @DisplayName("No false positive for UPDATE with parameterized WHERE on primary key")
  void noFalsePositiveForParameterizedPrimaryKey() {
    String sql = "UPDATE users SET name = ? WHERE id = ?";
    IndexMetadata metadata = indexWithPrimaryKey("users");

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  @DisplayName("No false positive for DELETE with parameterized WHERE on primary key")
  void noFalsePositiveForDeleteParameterizedPrimaryKey() {
    String sql = "DELETE FROM users WHERE id = ?";
    IndexMetadata metadata = indexWithPrimaryKey("users");

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  /**
   * Kills EmptyObjectReturnValsMutator on line 30: replaced return with Collections.emptyList. When
   * indexMetadata IS present and an issue IS detected, the returned list must NOT be empty.
   */
  @Test
  @DisplayName("Returns non-empty list when issue detected (kills line 30)")
  void evaluateReturnsNonEmptyWhenIssueDetected_killsLine30() {
    String sql = "DELETE FROM sessions WHERE user_id = 123";
    IndexMetadata metadata = indexWithPrimaryKey("sessions");

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isNotEmpty();
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.DML_WITHOUT_INDEX);
    assertThat(issues.get(0).detail()).contains("DELETE");
  }
}
