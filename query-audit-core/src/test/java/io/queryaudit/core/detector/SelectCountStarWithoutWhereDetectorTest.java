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

class SelectCountStarWithoutWhereDetectorTest {

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final SelectCountStarWithoutWhereDetector detector =
      new SelectCountStarWithoutWhereDetector();
  private final IndexMetadata emptyMetadata = new IndexMetadata(Map.of());

  @Test
  void detectsCountStarWithoutWhere() {
    String sql = "SELECT COUNT(*) FROM users";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.COUNT_STAR_WITHOUT_WHERE);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    assertThat(issues.get(0).detail()).contains("COUNT(*)");
    assertThat(issues.get(0).suggestion()).contains("approximate counts");
  }

  @Test
  void detectsCount1WithoutWhere() {
    String sql = "SELECT COUNT(1) FROM orders";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.COUNT_STAR_WITHOUT_WHERE);
  }

  @Test
  void noIssueWithWhereClause() {
    String sql = "SELECT COUNT(*) FROM users WHERE active = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithGroupBy() {
    String sql = "SELECT status, COUNT(*) FROM orders GROUP BY status";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForNonSelectQuery() {
    String sql = "DELETE FROM users";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForSelectWithoutCount() {
    String sql = "SELECT * FROM users";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithNullSql() {
    QueryRecord nullRecord = new QueryRecord(null, null, 0L, 0L, null, 0);

    List<Issue> issues = detector.evaluate(List.of(nullRecord), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void caseInsensitiveDetection() {
    String sql = "select count(*) from users";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
  }

  @Test
  void detectsCountStarWithSpaces() {
    String sql = "SELECT COUNT( * ) FROM users";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
  }

  @Test
  void deduplicatesSameNormalizedQuery() {
    String sql1 = "SELECT COUNT(*) FROM users";
    String sql2 = "SELECT COUNT(*) FROM users";

    List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), emptyMetadata);

    assertThat(issues).hasSize(1);
  }

  // ── false positive fix: COUNT(*) in subquery ──────────────────────

  @Test
  void noIssueForCountStarInSubquery() {
    // COUNT(*) in a subquery should not trigger — the outer query controls the result
    String sql =
        "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders GROUP BY user_id HAVING COUNT(*) > 5)";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForCountStarInDerivedTable() {
    String sql =
        "SELECT t.user_id, t.cnt FROM (SELECT user_id, COUNT(*) as cnt FROM orders GROUP BY user_id) t WHERE t.cnt > 3";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void stillDetectsCountStarInOuterQuery() {
    // COUNT(*) in the outer query without WHERE should still be detected
    String sql = "SELECT COUNT(*) FROM users";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
  }
}
