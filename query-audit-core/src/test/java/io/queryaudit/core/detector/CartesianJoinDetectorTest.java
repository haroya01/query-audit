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

class CartesianJoinDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private final CartesianJoinDetector detector = new CartesianJoinDetector();

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  // -- Positive: Issues detected --

  @Nested
  @DisplayName("Detects Cartesian JOIN patterns")
  class PositiveCases {

    @Test
    @DisplayName("Detects JOIN without ON clause")
    void detectsJoinWithoutOn() {
      String sql = "SELECT * FROM users JOIN orders WHERE users.id = 1";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.CARTESIAN_JOIN);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
    }

    @Test
    @DisplayName("Detects implicit Cartesian join (FROM a, b) without WHERE")
    void detectsImplicitCartesianWithoutWhere() {
      String sql = "SELECT * FROM users, orders";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.CARTESIAN_JOIN);
    }
  }

  // -- Negative: No issues (false positive prevention) --

  @Nested
  @DisplayName("No issue cases — false positive prevention")
  class NegativeCases {

    @Test
    @DisplayName("No issue for CROSS JOIN (intentional Cartesian product)")
    void noIssueForCrossJoin() {
      String sql = "SELECT * FROM calendar_dates CROSS JOIN time_slots";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for NATURAL JOIN")
    void noIssueForNaturalJoin() {
      String sql = "SELECT * FROM departments NATURAL JOIN employees";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for JOIN with ON clause")
    void noIssueForJoinWithOn() {
      String sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for JOIN with USING clause")
    void noIssueForJoinWithUsing() {
      String sql = "SELECT * FROM users JOIN orders USING (user_id)";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for implicit join with WHERE clause")
    void noIssueForImplicitJoinWithWhere() {
      String sql = "SELECT * FROM users, orders WHERE users.id = orders.user_id";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for CROSS JOIN LATERAL (legitimate usage)")
    void noIssueForCrossJoinLateral() {
      String sql =
          "SELECT u.id, l.recent_order FROM users u "
              + "CROSS JOIN LATERAL (SELECT order_id FROM orders WHERE user_id = u.id ORDER BY created_at DESC LIMIT 1) l";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for JOIN LATERAL (PostgreSQL lateral join)")
    void noIssueForJoinLateral() {
      String sql =
          "SELECT u.id, l.cnt FROM users u "
              + "JOIN LATERAL (SELECT COUNT(*) AS cnt FROM orders WHERE user_id = u.id) l ON true";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for LEFT JOIN LATERAL")
    void noIssueForLeftJoinLateral() {
      String sql =
          "SELECT u.id, l.last_login FROM users u "
              + "LEFT JOIN LATERAL (SELECT login_time AS last_login FROM logins WHERE user_id = u.id ORDER BY login_time DESC LIMIT 1) l ON true";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }

  // -- Edge cases --

  @Nested
  @DisplayName("Edge Cases")
  class EdgeCases {

    @Test
    @DisplayName("Deduplicates same normalized query")
    void deduplicates() {
      String sql = "SELECT * FROM users JOIN orders WHERE users.id = 1";
      List<Issue> issues = detector.evaluate(List.of(record(sql), record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("No issue for null SQL")
    void noIssueForNullSql() {
      QueryRecord nullRecord = new QueryRecord(null, null, 0L, 0L, null, 0);
      List<Issue> issues = detector.evaluate(List.of(nullRecord), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Empty query list")
    void emptyQueryList() {
      List<Issue> issues = detector.evaluate(List.of(), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }
}
