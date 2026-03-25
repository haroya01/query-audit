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

class InsertSelectAllDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private final InsertSelectAllDetector detector = new InsertSelectAllDetector();

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  // ── Positive: Issues detected ───────────────────────────────────────

  @Nested
  @DisplayName("Detects INSERT ... SELECT *")
  class PositiveCases {

    @Test
    @DisplayName("Detects basic INSERT ... SELECT *")
    void detectsBasicPattern() {
      String sql =
          "INSERT INTO orders_archive SELECT * FROM orders WHERE created_at < '2023-01-01'";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.INSERT_SELECT_ALL);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
      assertThat(issues.get(0).table()).isEqualTo("orders_archive");
    }

    @Test
    @DisplayName("Detects INSERT INTO ... SELECT * without WHERE")
    void detectsWithoutWhere() {
      String sql = "INSERT INTO backup_users SELECT * FROM users";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Detects INSERT ... SELECT DISTINCT *")
    void detectsSelectDistinctAll() {
      String sql = "INSERT INTO archive SELECT DISTINCT * FROM logs";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Detects INSERT ... SELECT table.*")
    void detectsTableQualifiedStar() {
      String sql = "INSERT INTO archive SELECT o.* FROM orders o WHERE o.status = 'closed'";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Detail mentions schema fragility")
    void detailMentionsSchemaFragility() {
      String sql = "INSERT INTO archive SELECT * FROM orders";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues.get(0).detail()).contains("Schema changes");
    }

    @Test
    @DisplayName("Suggestion mentions explicit columns")
    void suggestionMentionsExplicitColumns() {
      String sql = "INSERT INTO archive SELECT * FROM orders";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues.get(0).suggestion()).contains("Explicitly list the columns");
    }
  }

  // ── Negative: No issues ─────────────────────────────────────────────

  @Nested
  @DisplayName("No issue cases")
  class NegativeCases {

    @Test
    @DisplayName("No issue for INSERT with explicit SELECT columns")
    void noIssueForExplicitColumns() {
      String sql = "INSERT INTO archive (id, name) SELECT id, name FROM orders";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for INSERT VALUES")
    void noIssueForInsertValues() {
      String sql = "INSERT INTO users (name, email) VALUES ('Alice', 'alice@test.com')";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for plain SELECT *")
    void noIssueForPlainSelect() {
      String sql = "SELECT * FROM orders";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for INSERT ... SELECT COUNT(*)")
    void noIssueForSelectCountStar() {
      String sql = "INSERT INTO stats (total) SELECT COUNT(*) FROM orders";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for UPDATE query")
    void ignoresUpdate() {
      String sql = "UPDATE users SET name = 'test'";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for DELETE query")
    void ignoresDelete() {
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
    @DisplayName("Deduplicates same normalized query")
    void deduplicates() {
      String sql1 = "INSERT INTO archive SELECT * FROM orders WHERE id = 1";
      String sql2 = "INSERT INTO archive SELECT * FROM orders WHERE id = 2";
      // Both normalize to the same pattern
      List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Different target tables create separate issues")
    void differentTargets() {
      String sql1 = "INSERT INTO archive1 SELECT * FROM orders";
      String sql2 = "INSERT INTO archive2 SELECT * FROM users";
      List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), EMPTY_INDEX);

      assertThat(issues).hasSize(2);
    }

    @Test
    @DisplayName("Case insensitive detection")
    void caseInsensitive() {
      String sql = "insert into archive select * from orders";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("No false positive for INSERT ... SELECT with * in subquery only")
    void noFalsePositiveForSelectStarInSubquery() {
      String sql =
          "INSERT INTO stats (id, total) SELECT id, (SELECT COUNT(*) FROM items WHERE items.order_id = orders.id) FROM orders";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No false positive when SELECT * is only inside a subquery expression")
    void noFalsePositiveForSelectStarInSubqueryExpression() {
      String sql =
          "INSERT INTO t1 (col) SELECT (SELECT * FROM t2 WHERE t2.id = t3.id LIMIT 1) FROM t3";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }
}
