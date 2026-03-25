package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class InsertSelectLocksSourceDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private final InsertSelectLocksSourceDetector detector = new InsertSelectLocksSourceDetector();

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  // ── Positive cases ──────────────────────────────────────────────────

  @Test
  void detectsInsertSelectWithExplicitColumns() {
    String sql =
        "INSERT INTO archive (id, name, status) SELECT id, name, status FROM orders WHERE status = 'OLD'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.INSERT_SELECT_LOCKS_SOURCE);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    assertThat(issues.get(0).table()).isEqualTo("orders");
    assertThat(issues.get(0).suggestion()).contains("shared next-key locks");
  }

  @Test
  void detectsInsertSelectStar() {
    String sql = "INSERT INTO archive SELECT * FROM orders WHERE created_at < '2024-01-01'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.INSERT_SELECT_LOCKS_SOURCE);
    assertThat(issues.get(0).table()).isEqualTo("orders");
  }

  @Test
  void detectsInsertSelectWithBacktickedTable() {
    String sql = "INSERT INTO `archive` SELECT id, name FROM `orders` WHERE status = 'OLD'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.INSERT_SELECT_LOCKS_SOURCE);
  }

  @Test
  void detectsInsertSelectWithJoin() {
    String sql =
        "INSERT INTO report_data SELECT o.id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.INSERT_SELECT_LOCKS_SOURCE);
  }

  // ── Negative cases ──────────────────────────────────────────────────

  @Test
  void noIssueForInsertValues() {
    String sql = "INSERT INTO orders (id, status) VALUES (1, 'NEW')";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForPlainSelect() {
    String sql = "SELECT * FROM orders WHERE status = 'OLD'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForUpdate() {
    String sql = "UPDATE orders SET status = 'CANCELLED' WHERE id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForDelete() {
    String sql = "DELETE FROM orders WHERE id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  // ── Edge cases ──────────────────────────────────────────────────────

  @Test
  void skipsDuplicateNormalizedQueries() {
    String sql1 = "INSERT INTO archive SELECT * FROM orders WHERE id = 1";
    String sql2 = "INSERT INTO archive SELECT * FROM orders WHERE id = 2";

    List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), EMPTY_INDEX);

    // Both normalize to the same pattern
    assertThat(issues).hasSize(1);
  }

  @Test
  void handlesInsertSelectWithSubquery() {
    String sql = "INSERT INTO summary SELECT COUNT(*) FROM orders WHERE created_at > '2024-01-01'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.INSERT_SELECT_LOCKS_SOURCE);
  }

  @Test
  void detectsInsertSelectWithParameterizedValues() {
    String sql = "INSERT INTO archive (id, name) SELECT id, name FROM orders WHERE status = ?";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.INSERT_SELECT_LOCKS_SOURCE);
    assertThat(issues.get(0).table()).isEqualTo("orders");
  }

  @Test
  void caseInsensitiveDetection() {
    String sql = "insert into archive select * from orders where status = 'OLD'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.INSERT_SELECT_LOCKS_SOURCE);
  }

  @Test
  void handlesNullSql() {
    QueryRecord record = new QueryRecord(null, 0L, System.currentTimeMillis(), "");

    List<Issue> issues = detector.evaluate(List.of(record), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }
}
