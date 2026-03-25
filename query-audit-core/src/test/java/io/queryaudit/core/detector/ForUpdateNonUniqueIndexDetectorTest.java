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

class ForUpdateNonUniqueIndexDetectorTest {

  private final ForUpdateNonUniqueIndexDetector detector = new ForUpdateNonUniqueIndexDetector();

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  // ── Positive cases ──────────────────────────────────────────────────

  @Test
  void detectsForUpdateOnNonUniqueIndex() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000),
                    new IndexInfo("orders", "idx_status", "status", 1, true, 5))));

    String sql = "SELECT * FROM orders WHERE status = 'PENDING' FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.FOR_UPDATE_NON_UNIQUE);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    assertThat(issues.get(0).column()).isEqualTo("status");
    assertThat(issues.get(0).suggestion()).contains("gap locks");
  }

  @Test
  void detectsForShareOnNonUniqueIndex() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000),
                    new IndexInfo("orders", "idx_status", "status", 1, true, 5))));

    String sql = "SELECT * FROM orders WHERE status = 'ACTIVE' FOR SHARE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.FOR_UPDATE_NON_UNIQUE);
  }

  @Test
  void detectsWithAlias() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000),
                    new IndexInfo("orders", "idx_status", "status", 1, true, 5))));

    String sql = "SELECT * FROM orders o WHERE o.status = 'PENDING' FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("status");
  }

  // ── Negative cases ──────────────────────────────────────────────────

  @Test
  void noIssueWhenWhereColumnHasUniqueIndex() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000))));

    String sql = "SELECT * FROM orders WHERE id = 1 FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWhenWhereColumnHasPrimaryKey() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 1000))));

    String sql = "SELECT * FROM users WHERE id = 42 FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWhenNoForUpdate() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "idx_status", "status", 1, true, 5))));

    String sql = "SELECT * FROM orders WHERE status = 'PENDING'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWhenColumnHasNoIndex() {
    // This case is handled by ForUpdateWithoutIndexDetector, not this one
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000))));

    String sql = "SELECT * FROM orders WHERE status = 'PENDING' FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWhenNoWhereClause() {
    // FOR UPDATE without WHERE is handled by ForUpdateWithoutIndexDetector
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "idx_status", "status", 1, true, 5))));

    String sql = "SELECT * FROM orders FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWhenIndexMetadataIsEmpty() {
    IndexMetadata metadata = new IndexMetadata(Map.of());

    String sql = "SELECT * FROM orders WHERE status = 'PENDING' FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWhenIndexMetadataIsNull() {
    String sql = "SELECT * FROM orders WHERE status = 'PENDING' FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), null);

    assertThat(issues).isEmpty();
  }

  // ── Edge cases ──────────────────────────────────────────────────────

  @Test
  void noFalsePositiveForUniqueIndexLookup() {
    // FOR UPDATE on unique index is safe - no gap lock
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "users",
                List.of(
                    new IndexInfo("users", "PRIMARY", "id", 1, false, 1000),
                    new IndexInfo("users", "uq_email", "email", 1, false, 5000))));

    String sql = "SELECT * FROM users WHERE email = ? FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noFalsePositiveForParameterizedPrimaryKeyLookup() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 1000))));

    String sql = "SELECT * FROM orders WHERE id = ? FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void skipsDuplicateNormalizedQueries() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("orders", List.of(new IndexInfo("orders", "idx_status", "status", 1, true, 5))));

    String sql1 = "SELECT * FROM orders WHERE status = 'PENDING' FOR UPDATE";
    String sql2 = "SELECT * FROM orders WHERE status = 'ACTIVE' FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), metadata);

    // Both normalize to same pattern, so only one issue
    assertThat(issues).hasSize(1);
  }

  @Test
  void noIssueWhenColumnHasBothUniqueAndNonUniqueIndex() {
    // If a unique index exists on the column, gap locking is not needed
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "idx_status", "status", 1, true, 5),
                    new IndexInfo("orders", "uq_status", "status", 1, false, 5))));

    String sql = "SELECT * FROM orders WHERE status = 'PENDING' FOR UPDATE";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }
}
