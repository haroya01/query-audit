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

class OrderByRandDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private final OrderByRandDetector detector = new OrderByRandDetector();

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  // ── Positive: Issues detected ───────────────────────────────────────

  @Nested
  @DisplayName("Detects ORDER BY RAND() variants")
  class PositiveCases {

    @Test
    @DisplayName("Detects ORDER BY RAND()")
    void detectsRand() {
      String sql = "SELECT * FROM users ORDER BY RAND() LIMIT 10";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.ORDER_BY_RAND);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
      assertThat(issues.get(0).table()).isEqualTo("users");
    }

    @Test
    @DisplayName("Detects ORDER BY RANDOM()")
    void detectsRandom() {
      String sql = "SELECT id FROM products ORDER BY RANDOM() LIMIT 5";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Detects ORDER BY NEWID()")
    void detectsNewId() {
      String sql = "SELECT TOP 10 * FROM items ORDER BY NEWID()";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Detects ORDER BY DBMS_RANDOM()")
    void detectsDbmsRandom() {
      String sql = "SELECT * FROM employees ORDER BY DBMS_RANDOM() FETCH FIRST 5 ROWS ONLY";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Case insensitive detection")
    void caseInsensitive() {
      String sql = "select * from users order by rand() limit 1";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Suggestion mentions application-side random")
    void suggestionContent() {
      String sql = "SELECT * FROM users ORDER BY RAND()";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues.get(0).suggestion()).contains("application-side random offset");
    }
  }

  // ── Negative: No issues ─────────────────────────────────────────────

  @Nested
  @DisplayName("No issue cases")
  class NegativeCases {

    @Test
    @DisplayName("No issue for ORDER BY column")
    void noIssueForOrderByColumn() {
      String sql = "SELECT * FROM users ORDER BY name";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for query without ORDER BY")
    void noIssueWithoutOrderBy() {
      String sql = "SELECT * FROM users WHERE id = 1";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for RAND() in SELECT clause")
    void noIssueForRandInSelect() {
      String sql = "SELECT RAND() AS random_val FROM users ORDER BY id";
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
      String sql1 = "SELECT * FROM users ORDER BY RAND() LIMIT 1";
      String sql2 = "SELECT * FROM users ORDER BY RAND() LIMIT 2";
      List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Handles extra whitespace")
    void handlesExtraWhitespace() {
      String sql = "SELECT * FROM users ORDER  BY   RAND  ()";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }
  }
}
