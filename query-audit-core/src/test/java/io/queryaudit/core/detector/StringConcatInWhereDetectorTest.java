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

class StringConcatInWhereDetectorTest {

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final StringConcatInWhereDetector detector = new StringConcatInWhereDetector();
  private final IndexMetadata emptyMetadata = new IndexMetadata(Map.of());

  @Test
  void detectsConcatOperatorOnColumn() {
    String sql = "SELECT * FROM users WHERE first_name || ' ' || last_name = 'John Doe'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.STRING_CONCAT_IN_WHERE);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    assertThat(issues.get(0).column()).isEqualTo("first_name");
    assertThat(issues.get(0).detail()).contains("||");
  }

  @Test
  void detectsConcatOperatorWithTableAlias() {
    String sql = "SELECT * FROM users u WHERE u.name || '@domain.com' = 'test@domain.com'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
  }

  @Test
  void noIssueWithoutConcatOperator() {
    String sql = "SELECT * FROM users WHERE name = 'John'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithoutWhereClause() {
    String sql = "SELECT first_name || ' ' || last_name AS full_name FROM users";

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
    String sql1 = "SELECT * FROM users WHERE name || 'a' = 'testa'";
    String sql2 = "SELECT * FROM users WHERE name || 'b' = 'testb'";

    List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), emptyMetadata);

    // Both normalize to similar pattern, so only 1 issue
    assertThat(issues).hasSize(1);
  }

  @Test
  void caseInsensitiveDetection() {
    String sql = "select * from users where name || 'suffix' = 'value'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.STRING_CONCAT_IN_WHERE);
  }

  // ── False Positive Fixes ──────────────────────────────────────────

  @Test
  void shouldNotFlagConcatInSelectOnly() {
    // Concat in SELECT clause with no WHERE concat is not a performance concern
    String sql = "SELECT first_name || ' ' || last_name AS full_name FROM users WHERE id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void shouldNotFlagConcatInOrderByClause() {
    // Concat in ORDER BY should not be flagged
    String sql = "SELECT * FROM users WHERE status = 'active' ORDER BY first_name || last_name";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void shouldNotFlagKeywordsAsColumns() {
    // SQL keywords before || should be skipped
    String sql = "SELECT * FROM users WHERE name = 'test'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }
}
