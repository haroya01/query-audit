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

class RangeLockDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final RangeLockDetector detector = new RangeLockDetector();

  @Test
  void detectsRangeForUpdateOnUnindexedColumn() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000))));

    String sql = "SELECT * FROM orders WHERE created_at > '2024-01-01' FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.RANGE_LOCK_RISK);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    assertThat(issues.get(0).column()).isEqualTo("created_at");
    assertThat(issues.get(0).detail()).contains("gap locks");
    assertThat(issues.get(0).suggestion()).contains("Add index");
  }

  @Test
  void detectsBetweenForUpdate() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000))));

    String sql = "SELECT * FROM orders WHERE amount BETWEEN 100 AND 500 FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("amount");
  }

  @Test
  void detectsLessThanForShare() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "accounts", List.of(new IndexInfo("accounts", "PRIMARY", "id", 1, false, 1000))));

    String sql = "SELECT * FROM accounts WHERE balance < 0 FOR SHARE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("balance");
  }

  @Test
  void noIssueWhenRangeColumnHasIndex() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000),
                    new IndexInfo("orders", "idx_created_at", "created_at", 1, true, 500))));

    String sql = "SELECT * FROM orders WHERE created_at > '2024-01-01' FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForEqualityCondition() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000))));

    // Equality condition -- handled by ForUpdateWithoutIndexDetector, not this detector
    String sql = "SELECT * FROM orders WHERE status = 'PENDING' FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithoutForUpdate() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000))));

    String sql = "SELECT * FROM orders WHERE created_at > '2024-01-01'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithEmptyIndexMetadata() {
    String sql = "SELECT * FROM orders WHERE created_at > '2024-01-01' FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithNullIndexMetadata() {
    String sql = "SELECT * FROM orders WHERE created_at > '2024-01-01' FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), null);

    assertThat(issues).isEmpty();
  }

  @Test
  void detectsGreaterThanOrEquals() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000))));

    String sql = "SELECT * FROM orders WHERE priority >= 5 FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("priority");
  }

  @Test
  void detectsLessThanOrEquals() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000))));

    String sql = "SELECT * FROM orders WHERE priority <= 3 FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("priority");
  }

  @Test
  void detectsMultipleRangeConditions() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000))));

    String sql =
        "SELECT * FROM orders WHERE created_at > '2024-01-01' AND amount < 1000 FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(2);
    assertThat(issues).extracting(Issue::column).containsExactlyInAnyOrder("created_at", "amount");
  }

  @Test
  void caseInsensitiveDetection() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000))));

    String sql = "select * from orders where created_at > '2024-01-01' for update";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.RANGE_LOCK_RISK);
  }

  @Test
  void deduplicatesByNormalizedSql() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000))));

    String sql1 = "SELECT * FROM orders WHERE created_at > '2024-01-01' FOR UPDATE";
    String sql2 = "SELECT * FROM orders WHERE created_at > '2024-06-01' FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), metadata);

    assertThat(issues).hasSize(1);
  }

  // ── False-positive reduction tests ─────────────────────────────────

  @Test
  void noIssueForRangeOnUniqueIndex() {
    // Range on unique/primary key column should not be flagged because
    // the column has an index (hasIndexOn returns true).
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 10000))));

    String sql = "SELECT * FROM orders WHERE id > 100 FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    // id is the PK (has an index) -> no issue
    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithoutWhereClause() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000))));

    // No WHERE clause -- handled by ForUpdateWithoutIndexDetector
    String sql = "SELECT * FROM orders FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }
}
