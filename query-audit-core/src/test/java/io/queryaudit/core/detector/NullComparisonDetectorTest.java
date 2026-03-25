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

class NullComparisonDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final NullComparisonDetector detector = new NullComparisonDetector();

  @Test
  void detectsEqualsNull() {
    String sql = "SELECT * FROM users WHERE email = NULL";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.NULL_COMPARISON);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
    assertThat(issues.get(0).detail()).contains("= NULL");
    assertThat(issues.get(0).detail()).contains("UNKNOWN");
    assertThat(issues.get(0).suggestion()).contains("IS NULL");
  }

  @Test
  void detectsNotEqualsNull() {
    String sql = "SELECT * FROM users WHERE status != NULL";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.NULL_COMPARISON);
    assertThat(issues.get(0).detail()).contains("!= NULL");
    assertThat(issues.get(0).suggestion()).contains("IS NOT NULL");
  }

  @Test
  void detectsAngleBracketNotEqualsNull() {
    String sql = "SELECT * FROM orders WHERE deleted_at <> NULL";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.NULL_COMPARISON);
  }

  @Test
  void noIssueForIsNull() {
    String sql = "SELECT * FROM users WHERE email IS NULL";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForIsNotNull() {
    String sql = "SELECT * FROM users WHERE email IS NOT NULL";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithoutWhereClause() {
    String sql = "SELECT * FROM users";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithNullSql() {
    QueryRecord nullRecord = new QueryRecord(null, null, 0L, 0L, null, 0);

    List<Issue> issues = detector.evaluate(List.of(nullRecord), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void detectsQualifiedColumnEqualsNull() {
    String sql = "SELECT * FROM users u WHERE u.email = NULL";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.NULL_COMPARISON);
  }

  @Test
  void deduplicatesSameNormalizedQuery() {
    String sql = "SELECT * FROM users WHERE email = NULL";

    List<Issue> issues = detector.evaluate(List.of(record(sql), record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
  }

  @Test
  void extractsTableName() {
    String sql = "SELECT * FROM orders WHERE status = NULL";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).table()).isEqualTo("orders");
  }

  @Test
  void caseInsensitiveDetection() {
    String sql = "select * from users where email = null";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.NULL_COMPARISON);
  }

  @Test
  void extractsColumnNameFromQualifiedReference() {
    String sql = "SELECT * FROM users u WHERE u.email = NULL";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    // extractColumnName("u.email") should return "email", not "u.email" or ""
    assertThat(issues.get(0).column()).isEqualTo("email");
  }

  @Test
  void extractsUnqualifiedColumnName() {
    String sql = "SELECT * FROM users WHERE email = NULL";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    // extractColumnName("email") should return "email" as-is (no dot)
    assertThat(issues.get(0).column()).isEqualTo("email");
  }

  @Test
  void notEqualsNullExtractsTableName() {
    String sql = "SELECT * FROM orders WHERE status != NULL";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).table()).isEqualTo("orders");
  }

  @Test
  void notEqualsNullExtractsColumnFromQualifiedReference() {
    String sql = "SELECT * FROM orders o WHERE o.status != NULL";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("status");
  }

  @Test
  void equalsNullSuppressesNotEqualsNullForSameQuery() {
    // When a query has both = NULL and != NULL, only = NULL is reported
    String sql = "SELECT * FROM users WHERE email = NULL AND status != NULL";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("= NULL");
    assertThat(issues.get(0).detail()).doesNotContain("!= NULL");
  }

  @Test
  void notEqualsNullReportedWhenNoEqualsNullInSameQuery() {
    // When a query has ONLY != NULL (no = NULL), it should still be detected
    String sql1 = "SELECT * FROM users WHERE email = NULL";
    String sql2 = "SELECT * FROM orders WHERE status != NULL";

    List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), EMPTY_INDEX);

    assertThat(issues).hasSize(2);
    assertThat(issues.get(0).detail()).contains("= NULL");
    assertThat(issues.get(1).detail()).contains("!= NULL");
  }

  @Test
  void notEqualsNullWithAngleBracketsExtractsColumn() {
    String sql = "SELECT * FROM orders WHERE o.deleted_at <> NULL";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("deleted_at");
  }

  @Test
  void noFalsePositiveFromSubqueryNullComparison() {
    // The = NULL is inside the subquery, not in the outer WHERE.
    // After stripping subqueries, the outer WHERE body should not contain = NULL.
    String sql = "SELECT * FROM orders WHERE id IN (SELECT id FROM items WHERE price = NULL)";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    // The outer WHERE body after subquery removal is "id IN (?)" which has no = NULL,
    // so no issue should be reported for the outer query.
    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForNullNormalizedSql() {
    // normalizedSql is null but sql is not - tests the null check on normalizedSql
    QueryRecord record =
        new QueryRecord("SELECT * FROM users WHERE email = NULL", null, 0L, 0L, "", 0);

    List<Issue> issues = detector.evaluate(List.of(record), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }
}
