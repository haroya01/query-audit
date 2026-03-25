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

class RegexpInsteadOfLikeDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private final RegexpInsteadOfLikeDetector detector = new RegexpInsteadOfLikeDetector();

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  // ── Positive: Issues detected ───────────────────────────────────────

  @Nested
  @DisplayName("Detects REGEXP/RLIKE usage")
  class PositiveCases {

    @Test
    @DisplayName("Detects REGEXP in WHERE clause")
    void detectsRegexp() {
      String sql = "SELECT * FROM users WHERE name REGEXP '^A.*'";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.REGEXP_INSTEAD_OF_LIKE);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
      assertThat(issues.get(0).table()).isEqualTo("users");
    }

    @Test
    @DisplayName("Detects RLIKE in WHERE clause")
    void detectsRlike() {
      String sql = "SELECT * FROM products WHERE description RLIKE 'pattern'";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.REGEXP_INSTEAD_OF_LIKE);
    }

    @Test
    @DisplayName("Case insensitive detection")
    void caseInsensitive() {
      String sql = "select * from users where name regexp '^test'";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Suggestion mentions LIKE alternative")
    void suggestionMentionsLike() {
      String sql = "SELECT * FROM users WHERE name REGEXP '^A'";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues.get(0).suggestion()).contains("LIKE");
    }
  }

  // ── Negative: No issues ─────────────────────────────────────────────

  @Nested
  @DisplayName("No issue cases")
  class NegativeCases {

    @Test
    @DisplayName("No issue for LIKE usage")
    void noIssueForLike() {
      String sql = "SELECT * FROM users WHERE name LIKE 'A%'";
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
      String sql1 = "SELECT * FROM users WHERE name REGEXP '^A'";
      String sql2 = "SELECT * FROM users WHERE name REGEXP '^B'";
      List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("REGEXP in complex query")
    void regexpInComplexQuery() {
      String sql =
          "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.email REGEXP '@gmail\\.com$'";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }
  }

  // ── False Positive Fixes ──────────────────────────────────────────

  @Nested
  @DisplayName("False positive fixes")
  class FalsePositiveFixes {

    @Test
    @DisplayName("Should not flag REGEXP in SELECT clause")
    void shouldNotFlagWhenExpressionInSelectClause() {
      String sql =
          "SELECT name REGEXP '^A' AS starts_with_a FROM users WHERE id = 1";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Should not flag REGEXP in ORDER BY clause")
    void shouldNotFlagWhenExpressionInOrderByClause() {
      String sql =
          "SELECT * FROM users WHERE status = 'active' ORDER BY name REGEXP '^[A-Z]' DESC";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Should not flag RLIKE in SELECT clause")
    void shouldNotFlagRlikeInSelectClause() {
      String sql = "SELECT description RLIKE 'pattern' AS matched FROM products WHERE id > 0";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Should still flag REGEXP in WHERE even when also in SELECT")
    void shouldFlagInWhereEvenWhenAlsoInSelect() {
      String sql =
          "SELECT name REGEXP '^A' AS flag FROM users WHERE name REGEXP '^A'";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Should not flag REGEXP in HAVING clause")
    void shouldNotFlagWhenExpressionInHavingClause() {
      String sql =
          "SELECT department FROM users WHERE active = 1 GROUP BY department HAVING department REGEXP '^eng'";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }
}
