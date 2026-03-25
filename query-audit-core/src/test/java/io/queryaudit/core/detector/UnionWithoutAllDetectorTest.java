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

class UnionWithoutAllDetectorTest {

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final UnionWithoutAllDetector detector = new UnionWithoutAllDetector();
  private final IndexMetadata emptyMetadata = new IndexMetadata(Map.of());

  @Test
  void detectsUnionWithoutAll() {
    String sql = "SELECT id FROM users UNION SELECT id FROM admins";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.UNION_WITHOUT_ALL);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    assertThat(issues.get(0).detail()).contains("deduplication");
    assertThat(issues.get(0).suggestion()).contains("UNION ALL");
  }

  @Test
  void noIssueForUnionAll() {
    String sql = "SELECT id FROM users UNION ALL SELECT id FROM admins";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForQueryWithoutUnion() {
    String sql = "SELECT * FROM users WHERE id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void caseInsensitiveDetection() {
    String sql = "select id from users union select id from admins";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.UNION_WITHOUT_ALL);
  }

  @Test
  void unionAllCaseInsensitive() {
    String sql = "select id from users Union All select id from admins";

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
  void deduplicatesSameNormalizedQuery() {
    // Two queries with same normalized SQL should produce only one issue
    String sql1 =
        "SELECT id FROM users WHERE status = 1 UNION SELECT id FROM admins WHERE status = 1";
    String sql2 =
        "SELECT id FROM users WHERE status = 2 UNION SELECT id FROM admins WHERE status = 2";

    List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), emptyMetadata);

    // Both normalize to the same pattern (numbers replaced with ?), so only 1 issue
    assertThat(issues).hasSize(1);
  }

  @Test
  void detectsUnionInSubquery() {
    String sql = "SELECT * FROM (SELECT id FROM users UNION SELECT id FROM admins) t";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
  }

  @Test
  void multipleUnionsInOneQuery() {
    // Even with multiple UNIONs, we report once per normalized SQL
    String sql = "SELECT id FROM a UNION SELECT id FROM b UNION SELECT id FROM c";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
  }

  // ── false positive fix: UNION with DISTINCT ────────────────────────

  @Test
  void noIssueWhenUnionUsedWithDistinct() {
    // When DISTINCT is used, UNION dedup is intentional
    String sql = "SELECT DISTINCT name FROM users UNION SELECT DISTINCT name FROM admins";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWhenOneSelectUsesDistinct() {
    String sql = "SELECT DISTINCT email FROM users UNION SELECT email FROM admins";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void stillDetectsUnionWithoutDistinct() {
    // UNION without DISTINCT should still be flagged
    String sql = "SELECT name FROM users UNION SELECT name FROM admins";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
  }
}
