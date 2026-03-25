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

class FindInSetDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private final FindInSetDetector detector = new FindInSetDetector();

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  // ── Positive: Issues detected ───────────────────────────────────────

  @Nested
  @DisplayName("Detects FIND_IN_SET usage")
  class PositiveCases {

    @Test
    @DisplayName("Detects FIND_IN_SET in WHERE clause")
    void detectsFindInSet() {
      String sql = "SELECT * FROM users WHERE FIND_IN_SET('admin', roles) > 0";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.FIND_IN_SET_USAGE);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
      assertThat(issues.get(0).table()).isEqualTo("users");
    }

    @Test
    @DisplayName("Detects FIND_IN_SET with no space before parenthesis")
    void detectsNoSpace() {
      String sql = "SELECT * FROM products WHERE FIND_IN_SET(?, tags)";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Case insensitive detection")
    void caseInsensitive() {
      String sql = "select * from users where find_in_set('admin', roles) > 0";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Suggestion mentions junction table")
    void suggestionMentionsJunctionTable() {
      String sql = "SELECT * FROM users WHERE FIND_IN_SET('admin', roles) > 0";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues.get(0).suggestion()).contains("junction table");
    }

    @Test
    @DisplayName("Detail mentions first normal form")
    void detailMentionsNormalForm() {
      String sql = "SELECT * FROM users WHERE FIND_IN_SET('admin', roles) > 0";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues.get(0).detail()).contains("first normal form");
    }
  }

  // ── Negative: No issues ─────────────────────────────────────────────

  @Nested
  @DisplayName("No issue cases")
  class NegativeCases {

    @Test
    @DisplayName("No issue for IN clause")
    void noIssueForIn() {
      String sql = "SELECT * FROM users WHERE role IN ('admin', 'user')";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for plain SELECT")
    void noIssueForPlainSelect() {
      String sql = "SELECT * FROM users WHERE id = 1";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for INSERT")
    void noIssueForInsert() {
      String sql = "INSERT INTO users (name) VALUES ('test')";
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
      String sql1 = "SELECT * FROM users WHERE FIND_IN_SET('admin', roles) > 0";
      String sql2 = "SELECT * FROM users WHERE FIND_IN_SET('editor', roles) > 0";
      List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("FIND_IN_SET in complex query")
    void findInSetInComplexQuery() {
      String sql =
          "SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE FIND_IN_SET(u.status, 'active,pending')";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }
  }

  // ── False Positive Fixes ──────────────────────────────────────────

  @Nested
  @DisplayName("False positive fixes")
  class FalsePositiveFixes {

    @Test
    @DisplayName("Should not flag FIND_IN_SET in SELECT clause")
    void shouldNotFlagWhenExpressionInSelectClause() {
      String sql = "SELECT FIND_IN_SET('admin', roles) AS position FROM users WHERE id = 1";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Should not flag FIND_IN_SET in ORDER BY clause")
    void shouldNotFlagWhenExpressionInOrderByClause() {
      String sql =
          "SELECT * FROM users WHERE status = 'active' ORDER BY FIND_IN_SET(role, 'admin,user,guest')";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Should not flag FIND_IN_SET in HAVING clause")
    void shouldNotFlagWhenExpressionInHavingClause() {
      String sql =
          "SELECT department, COUNT(*) FROM users WHERE active = 1 GROUP BY department HAVING FIND_IN_SET(department, 'eng,sales') > 0";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Should still flag FIND_IN_SET in WHERE even when also in SELECT")
    void shouldFlagInWhereEvenWhenAlsoInSelect() {
      String sql =
          "SELECT FIND_IN_SET('admin', roles) AS pos FROM users WHERE FIND_IN_SET('admin', roles) > 0";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Should flag FIND_IN_SET in JOIN ON condition")
    void shouldFlagInJoinOnCondition() {
      String sql =
          "SELECT * FROM users u JOIN roles r ON FIND_IN_SET(r.name, u.role_list) > 0";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }
  }
}
