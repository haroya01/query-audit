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
import org.junit.jupiter.api.Test;

class NonDeterministicPaginationDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final NonDeterministicPaginationDetector detector =
      new NonDeterministicPaginationDetector();

  @Test
  void detectsNonUniqueOrderByWithLimit() {
    // created_at has a non-unique index
    IndexMetadata indexMetadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(new IndexInfo("orders", "idx_created_at", "created_at", 1, true, 1000))));

    String sql = "SELECT * FROM orders ORDER BY created_at LIMIT 10";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), indexMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.NON_DETERMINISTIC_PAGINATION);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    assertThat(issues.get(0).suggestion()).contains("unique tiebreaker");
  }

  @Test
  void noIssueWhenOrderByHasUniqueColumn() {
    // id has a unique (primary) index
    IndexMetadata indexMetadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 10000))));

    String sql = "SELECT * FROM orders ORDER BY id LIMIT 10";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), indexMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWhenOrderByIncludesUniqueTiebreaker() {
    IndexMetadata indexMetadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "idx_created_at", "created_at", 1, true, 1000),
                    new IndexInfo("orders", "PRIMARY", "id", 1, false, 10000))));

    String sql = "SELECT * FROM orders ORDER BY created_at, id LIMIT 10";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), indexMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithoutLimit() {
    IndexMetadata indexMetadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(new IndexInfo("orders", "idx_created_at", "created_at", 1, true, 1000))));

    String sql = "SELECT * FROM orders ORDER BY created_at";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), indexMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithoutOrderBy() {
    String sql = "SELECT * FROM orders LIMIT 10";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithEmptyIndexMetadata() {
    // Cannot determine uniqueness without metadata — skip to avoid false positives
    String sql = "SELECT * FROM orders ORDER BY created_at LIMIT 10";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForNonSelectQuery() {
    IndexMetadata indexMetadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(new IndexInfo("orders", "idx_created_at", "created_at", 1, true, 1000))));

    String sql = "UPDATE orders SET status = 'DONE' ORDER BY created_at LIMIT 10";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), indexMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void deduplicatesSameNormalizedQuery() {
    IndexMetadata indexMetadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(new IndexInfo("orders", "idx_created_at", "created_at", 1, true, 1000))));

    String sql = "SELECT * FROM orders ORDER BY created_at LIMIT 10";

    List<Issue> issues = detector.evaluate(List.of(record(sql), record(sql)), indexMetadata);

    assertThat(issues).hasSize(1);
  }

  @Test
  void detectsWithAliasedTable() {
    IndexMetadata indexMetadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(new IndexInfo("orders", "idx_created_at", "created_at", 1, true, 1000))));

    String sql = "SELECT o.* FROM orders o ORDER BY o.created_at LIMIT 10";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), indexMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.NON_DETERMINISTIC_PAGINATION);
  }

  // ── False-positive reduction tests ─────────────────────────────────

  @Test
  void noIssueWhenOrderByIncludesPrimaryKey() {
    // ORDER BY created_at, id LIMIT 10 -> id is PK (unique) -> deterministic
    IndexMetadata indexMetadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "idx_created_at", "created_at", 1, true, 1000),
                    new IndexInfo("orders", "PRIMARY", "id", 1, false, 10000))));

    String sql = "SELECT * FROM orders ORDER BY created_at DESC, id ASC LIMIT 10";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), indexMetadata);

    // id is a unique tiebreaker -> no issue
    assertThat(issues).isEmpty();
  }

  @Test
  void skipsWhenTableNotInMetadata() {
    // Index metadata exists but not for the table in the query
    IndexMetadata indexMetadata =
        new IndexMetadata(
            Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 10000))));

    String sql = "SELECT * FROM orders ORDER BY created_at LIMIT 10";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), indexMetadata);

    // Table 'orders' not in metadata -> can't determine, skip
    assertThat(issues).isEmpty();
  }
}
