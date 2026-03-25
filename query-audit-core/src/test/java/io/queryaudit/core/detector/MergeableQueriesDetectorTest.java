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

class MergeableQueriesDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final MergeableQueriesDetector detector = new MergeableQueriesDetector();

  @Test
  void detectsMergeableQueriesWithDifferentWhereValues() {
    List<QueryRecord> queries =
        List.of(
            record("SELECT name, email FROM users WHERE id = 1"),
            record("SELECT name, email FROM users WHERE status = 'ACTIVE'"),
            record("SELECT name, email FROM users WHERE role = 'ADMIN'"));

    List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.MERGEABLE_QUERIES);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    assertThat(issues.get(0).detail()).contains("users");
    assertThat(issues.get(0).suggestion()).contains("IN clause");
  }

  @Test
  void noIssueForFewerThanThresholdQueries() {
    List<QueryRecord> queries =
        List.of(
            record("SELECT name, email FROM users WHERE id = 1"),
            record("SELECT name, email FROM users WHERE id = 2"));

    List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForIdenticalNormalizedSqlNPlusOne() {
    // Same normalized SQL = N+1 pattern, not mergeable queries
    List<QueryRecord> queries =
        List.of(
            record("SELECT name, email FROM users WHERE id = 1"),
            record("SELECT name, email FROM users WHERE id = 2"),
            record("SELECT name, email FROM users WHERE id = 3"));

    List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

    // These normalize to the same SQL (id = ?), so it's N+1, not mergeable
    assertThat(issues).isEmpty();
  }

  @Test
  void detectsQueriesWithDifferentWhereColumns() {
    List<QueryRecord> queries =
        List.of(
            record("SELECT name FROM orders WHERE status = 'PENDING'"),
            record("SELECT name FROM orders WHERE user_id = 42"),
            record("SELECT name FROM orders WHERE created_at > '2024-01-01'"));

    List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).table()).isEqualTo("orders");
  }

  @Test
  void noIssueForDifferentTables() {
    List<QueryRecord> queries =
        List.of(
            record("SELECT name FROM users WHERE id = 1"),
            record("SELECT name FROM orders WHERE id = 1"),
            record("SELECT name FROM products WHERE id = 1"));

    List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForDifferentSelectColumns() {
    List<QueryRecord> queries =
        List.of(
            record("SELECT name FROM users WHERE status = 'A'"),
            record("SELECT email FROM users WHERE status = 'B'"),
            record("SELECT phone FROM users WHERE status = 'C'"));

    List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForNonSelectQueries() {
    List<QueryRecord> queries =
        List.of(
            record("UPDATE users SET name = 'a' WHERE id = 1"),
            record("UPDATE users SET name = 'b' WHERE id = 2"),
            record("UPDATE users SET name = 'c' WHERE id = 3"));

    List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void customThreshold() {
    MergeableQueriesDetector customDetector = new MergeableQueriesDetector(2);

    List<QueryRecord> queries =
        List.of(
            record("SELECT name FROM users WHERE status = 'ACTIVE'"),
            record("SELECT name FROM users WHERE role = 'ADMIN'"));

    List<Issue> issues = customDetector.evaluate(queries, EMPTY_INDEX);

    assertThat(issues).hasSize(1);
  }

  // ── False positive prevention ─────────────────────────────────────────

  @Test
  void noIssueForQueriesWithDifferentJoins() {
    // Queries to the same table but with different JOINs are structurally different
    // and cannot simply be merged with an IN clause
    List<QueryRecord> queries =
        List.of(
            record("SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = true"),
            record("SELECT u.name FROM users u JOIN payments p ON u.id = p.user_id WHERE u.active = true"),
            record("SELECT u.name FROM users u JOIN logins l ON u.id = l.user_id WHERE u.active = true"));

    List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForQueriesWithDifferentJoinTypes() {
    // Same table and columns but LEFT JOIN vs INNER JOIN — different semantics
    MergeableQueriesDetector customDetector = new MergeableQueriesDetector(2);
    List<QueryRecord> queries =
        List.of(
            record("SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE u.status = 'A'"),
            record("SELECT u.name FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.status = 'B'"));

    List<Issue> issues = customDetector.evaluate(queries, EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void stillDetectsMergeableQueriesWithSameJoins() {
    // Same table, same columns, same JOIN structure — truly mergeable
    List<QueryRecord> queries =
        List.of(
            record("SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE u.status = 'ACTIVE'"),
            record("SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE u.role = 'ADMIN'"),
            record("SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE u.region = 'US'"));

    List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.MERGEABLE_QUERIES);
  }
}
