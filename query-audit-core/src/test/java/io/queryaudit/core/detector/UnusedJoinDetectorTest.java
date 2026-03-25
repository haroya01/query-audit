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

class UnusedJoinDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final UnusedJoinDetector detector = new UnusedJoinDetector();

  @Test
  void detectsUnusedLeftJoin() {
    String sql =
        "SELECT u.name FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.active = true";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.UNUSED_JOIN);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    assertThat(issues.get(0).detail()).contains("o");
    assertThat(issues.get(0).suggestion()).contains("never referenced");
  }

  @Test
  void noIssueWhenLeftJoinIsReferencedInSelect() {
    String sql = "SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWhenLeftJoinIsReferencedInWhere() {
    String sql =
        "SELECT u.name FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE o.status = 'ACTIVE'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWhenLeftJoinIsReferencedInOrderBy() {
    String sql =
        "SELECT u.name FROM users u LEFT JOIN orders o ON u.id = o.user_id ORDER BY o.created_at";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWhenLeftJoinIsReferencedInGroupBy() {
    String sql =
        "SELECT u.name, COUNT(*) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY o.status";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForInnerJoin() {
    // INNER JOIN affects row filtering even without column reference
    String sql =
        "SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = true";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void detectsUnusedLeftJoinWithTableNameAsAlias() {
    String sql =
        "SELECT users.name FROM users LEFT JOIN orders ON users.id = orders.user_id WHERE users.active = true";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.UNUSED_JOIN);
  }

  @Test
  void noIssueWhenLeftJoinTableNameReferencedDirectly() {
    String sql =
        "SELECT users.name, orders.total FROM users LEFT JOIN orders ON users.id = orders.user_id";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void detectsMultipleUnusedLeftJoins() {
    String sql =
        "SELECT u.name FROM users u LEFT JOIN orders o ON u.id = o.user_id LEFT JOIN payments p ON o.id = p.order_id WHERE u.active = true";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(2);
    assertThat(issues).allMatch(i -> i.type() == IssueType.UNUSED_JOIN);
  }

  @Test
  void deduplicatesSameNormalizedQuery() {
    String sql =
        "SELECT u.name FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.active = true";

    List<Issue> issues = detector.evaluate(List.of(record(sql), record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
  }

  @Test
  void noIssueForNonSelectQuery() {
    String sql = "UPDATE users SET active = false";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWhenLeftJoinReferencedInHaving() {
    String sql =
        "SELECT u.name, COUNT(*) as cnt FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.name HAVING o.status IS NOT NULL";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  // ── False positive prevention tests ──────────────────────────────────

  @Test
  void noIssueForSelectStarWithLeftJoin() {
    // SELECT * implicitly references all joined table columns — not unused
    String sql =
        "SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.active = true";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForSelectDistinctStarWithLeftJoin() {
    // SELECT DISTINCT * also references all columns
    String sql =
        "SELECT DISTINCT * FROM users u LEFT JOIN orders o ON u.id = o.user_id";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void stillDetectsUnusedLeftJoinWithSpecificColumns() {
    // SELECT with specific columns that don't reference the joined table
    String sql =
        "SELECT u.name, u.email FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.active = true";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.UNUSED_JOIN);
  }
}
