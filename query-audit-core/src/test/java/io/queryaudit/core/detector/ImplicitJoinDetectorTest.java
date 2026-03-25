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

class ImplicitJoinDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private final ImplicitJoinDetector detector = new ImplicitJoinDetector();

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  // ── Positive: Issues detected ───────────────────────────────────────

  @Nested
  @DisplayName("Detects implicit comma-separated joins")
  class PositiveCases {

    @Test
    @DisplayName("Detects basic FROM a, b WHERE pattern")
    void detectsBasicPattern() {
      String sql = "SELECT * FROM users, orders WHERE users.id = orders.user_id";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.IMPLICIT_JOIN);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
      assertThat(issues.get(0).table()).isEqualTo("users");
    }

    @Test
    @DisplayName("Detects three tables with commas")
    void detectsThreeTables() {
      String sql = "SELECT * FROM a, b, c WHERE a.id = b.a_id AND b.id = c.b_id";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Case insensitive detection")
    void caseInsensitive() {
      String sql = "select * from users, orders where users.id = orders.user_id";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Suggestion mentions explicit JOIN syntax")
    void suggestionContent() {
      String sql = "SELECT * FROM users, orders WHERE users.id = orders.user_id";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues.get(0).suggestion()).contains("explicit JOIN syntax");
    }
  }

  // ── Negative: No issues ─────────────────────────────────────────────

  @Nested
  @DisplayName("No issue cases")
  class NegativeCases {

    @Test
    @DisplayName("No issue for explicit JOIN")
    void noIssueForExplicitJoin() {
      String sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for single table FROM")
    void noIssueForSingleTable() {
      String sql = "SELECT * FROM users WHERE id = 1";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for comma in VALUES")
    void noIssueForCommaInValues() {
      String sql = "INSERT INTO users (name, email) VALUES ('Alice', 'alice@test.com')";
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
      String sql1 = "SELECT * FROM users, orders WHERE users.id = orders.user_id AND users.id = 1";
      String sql2 = "SELECT * FROM users, orders WHERE users.id = orders.user_id AND users.id = 2";
      List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Implicit join in subquery is excluded")
    void subqueryExcluded() {
      String sql = "SELECT * FROM main_table WHERE id IN (SELECT id FROM a, b WHERE a.x = b.x)";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // Subquery is removed, so the implicit join in it is not detected
      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Different tables produce separate issues")
    void differentTables() {
      String sql1 = "SELECT * FROM users, orders WHERE users.id = orders.user_id";
      String sql2 = "SELECT * FROM products, categories WHERE products.cat_id = categories.id";
      List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), EMPTY_INDEX);

      assertThat(issues).hasSize(2);
    }
  }

  // ── False Positive Prevention ──────────────────────────────────────────

  @Nested
  @DisplayName("False positive prevention")
  class FalsePositivePrevention {

    @Test
    @DisplayName("No issue for function call in FROM clause (e.g., generate_series)")
    void noIssueForFunctionCallInFrom() {
      String sql = "SELECT * FROM generate_series(1, 10) AS s";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for table-valued function with comma args")
    void noIssueForTableValuedFunction() {
      String sql = "SELECT * FROM unnest(ARRAY[1, 2, 3]) AS t(val)";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Still detects real implicit join")
    void stillDetectsRealImplicitJoin() {
      String sql = "SELECT * FROM users u, orders o WHERE u.id = o.user_id";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.IMPLICIT_JOIN);
    }
  }
}
