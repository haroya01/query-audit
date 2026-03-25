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

class NotInSubqueryDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private final NotInSubqueryDetector detector = new NotInSubqueryDetector();

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  // ── Positive: Issues detected ───────────────────────────────────────

  @Nested
  @DisplayName("Detects NOT IN (SELECT ...) patterns")
  class PositiveCases {

    @Test
    @DisplayName("Detects basic NOT IN (SELECT ...)")
    void detectsBasicPattern() {
      String sql = "SELECT * FROM orders WHERE customer_id NOT IN (SELECT id FROM blacklist)";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.NOT_IN_SUBQUERY);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
      assertThat(issues.get(0).table()).isEqualTo("orders");
    }

    @Test
    @DisplayName("Detects NOT IN with whitespace variations")
    void detectsWithWhitespace() {
      String sql = "SELECT * FROM users WHERE id NOT  IN  (  SELECT user_id FROM banned )";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Case insensitive detection")
    void caseInsensitive() {
      String sql = "select * from orders where id not in (select order_id from cancelled)";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Suggestion mentions NOT EXISTS")
    void suggestionMentionsNotExists() {
      String sql = "SELECT * FROM orders WHERE id NOT IN (SELECT order_id FROM cancelled)";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues.get(0).suggestion()).contains("NOT EXISTS");
    }

    @Test
    @DisplayName("Detail mentions NULL correctness bug")
    void detailMentionsNull() {
      String sql = "SELECT * FROM orders WHERE id NOT IN (SELECT order_id FROM cancelled)";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues.get(0).detail()).contains("NULL");
    }
  }

  // ── Negative: No issues ─────────────────────────────────────────────

  @Nested
  @DisplayName("No issue cases")
  class NegativeCases {

    @Test
    @DisplayName("No issue for NOT IN with literal list")
    void noIssueForLiteralList() {
      String sql = "SELECT * FROM users WHERE status NOT IN ('active', 'pending')";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for IN (SELECT ...)")
    void noIssueForInSubquery() {
      String sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for NOT EXISTS")
    void noIssueForNotExists() {
      String sql =
          "SELECT * FROM orders WHERE NOT EXISTS (SELECT 1 FROM cancelled WHERE cancelled.id = orders.id)";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for simple SELECT")
    void noIssueForSimpleSelect() {
      String sql = "SELECT * FROM users WHERE id = 1";
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
    @DisplayName("Deduplicates same normalized query")
    void deduplicates() {
      String sql1 =
          "SELECT * FROM orders WHERE id NOT IN (SELECT order_id FROM cancelled WHERE reason = 'fraud')";
      String sql2 =
          "SELECT * FROM orders WHERE id NOT IN (SELECT order_id FROM cancelled WHERE reason = 'refund')";
      List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Detects multiple NOT IN subqueries in different queries")
    void multipleQueries() {
      String sql1 = "SELECT * FROM orders WHERE id NOT IN (SELECT order_id FROM cancelled)";
      String sql2 = "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned)";
      List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), EMPTY_INDEX);

      assertThat(issues).hasSize(2);
    }
  }

  // ── False Positive Prevention ────────────────────────────────────────

  @Nested
  @DisplayName("False positive prevention")
  class FalsePositivePrevention {

    @Test
    @DisplayName("No issue for NOT EXISTS pattern (efficient alternative)")
    void noIssueForNotExistsPattern() {
      // NOT EXISTS is the recommended alternative — should not be flagged
      String sql =
          "SELECT * FROM orders o WHERE NOT EXISTS "
              + "(SELECT 1 FROM cancelled c WHERE c.order_id = o.id)";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for NOT IN with literal values")
    void noIssueForNotInLiteral() {
      // NOT IN with literal values has no NULL subquery risk
      String sql = "SELECT * FROM users WHERE status NOT IN ('deleted', 'banned')";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Detects NOT IN subquery even with complex conditions")
    void detectsNotInWithComplexSubquery() {
      String sql =
          "SELECT * FROM orders WHERE customer_id NOT IN "
              + "(SELECT id FROM blacklist WHERE region = 'EU' AND created_at > '2024-01-01')";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.NOT_IN_SUBQUERY);
    }
  }
}
