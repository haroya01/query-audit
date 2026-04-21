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

class ImplicitTypeConversionDetectorTest {

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final ImplicitTypeConversionDetector detector = new ImplicitTypeConversionDetector();
  private final IndexMetadata emptyMetadata = new IndexMetadata(Map.of());

  @Test
  void detectsStringColumnComparedToNumber() {
    String sql = "SELECT * FROM users WHERE user_name = 123";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.IMPLICIT_TYPE_CONVERSION);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    assertThat(issues.get(0).column()).isEqualTo("user_name");
    assertThat(issues.get(0).detail()).contains("user_name");
    assertThat(issues.get(0).detail()).contains("123");
    assertThat(issues.get(0).suggestion()).contains("'123'");
  }

  @Test
  void detectsEmailColumnComparedToNumber() {
    String sql = "SELECT * FROM contacts WHERE contact_email = 456";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("contact_email");
  }

  @Test
  void detectsCodeColumnComparedToNumber() {
    String sql = "SELECT * FROM products WHERE product_code = 789";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("product_code");
  }

  @Test
  void detectsTokenColumnComparedToNumber() {
    String sql = "SELECT * FROM sessions WHERE session_token = 999";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("session_token");
  }

  @Test
  void detectsTypeColumnComparedToNumber() {
    String sql = "SELECT * FROM rooms WHERE room_type = 2";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("room_type");
  }

  @Test
  void noIssueForNonStringColumn() {
    // "user_id" does not contain string indicators
    String sql = "SELECT * FROM orders WHERE user_id = 123";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForStringComparison() {
    // Properly quoted string comparison -- no issue
    String sql = "SELECT * FROM users WHERE user_name = '123'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithoutWhereClause() {
    String sql = "SELECT user_name FROM users";

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
  void deduplicatesSameColumnAndValue() {
    // Same column = number pattern in two queries should produce only one issue
    String sql1 = "SELECT * FROM users WHERE user_name = 123";
    String sql2 = "SELECT id FROM users WHERE user_name = 123";

    List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), emptyMetadata);

    assertThat(issues).hasSize(1);
  }

  @Test
  void detectsMultipleColumnsInSameQuery() {
    String sql = "SELECT * FROM users WHERE user_name = 123 AND user_email = 456";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(2);
    assertThat(issues)
        .extracting(Issue::column)
        .containsExactlyInAnyOrder("user_name", "user_email");
  }

  @Test
  void detectsUrlColumnComparedToNumber() {
    String sql = "SELECT * FROM pages WHERE page_url = 42";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("page_url");
  }

  @Test
  void detectsPathColumnComparedToNumber() {
    String sql = "SELECT * FROM files WHERE file_path = 7";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("file_path");
  }

  @Test
  void detectsTitleColumnComparedToNumber() {
    String sql = "SELECT * FROM posts WHERE post_title = 100";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("post_title");
  }

  @Test
  void detectsAddressColumnComparedToNumber() {
    String sql = "SELECT * FROM customers WHERE shipping_address = 55";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("shipping_address");
  }

  // ── False Positive Fixes ──────────────────────────────────────────

  @Test
  void shouldNotFlagStringColumnInOrderByClause() {
    // user_name = 123 appears in ORDER BY, not WHERE -- should not be flagged
    String sql = "SELECT * FROM users WHERE id = 1 ORDER BY user_name = 123";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void shouldNotFlagStringColumnInGroupByClause() {
    // The comparison in HAVING/GROUP BY should not cause a false positive
    String sql =
        "SELECT user_type, COUNT(*) FROM users WHERE active = 1 GROUP BY user_type HAVING COUNT(*) > 5";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void shouldNotFlagStringColumnInLimitClause() {
    // Ensure trailing clauses after WHERE are not analyzed
    String sql = "SELECT * FROM users WHERE active = 1 LIMIT 10";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void shouldStillFlagInWhereWithTrailingClauses() {
    // user_name = 123 is in WHERE, should still be detected even with ORDER BY
    String sql = "SELECT * FROM users WHERE user_name = 123 ORDER BY id";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("user_name");
  }

  // Regression for #95: the pre-fix detector only matched the `_indicator` form, so columns
  // whose name fused the indicator to another token (ucode, firstname, codebook) and standalone
  // string-typed column names (name, email) slipped through.

  @Test
  void detectsFusedSuffixColumn_ucode() {
    String sql = "SELECT * FROM users WHERE ucode = 42";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("ucode");
  }

  @Test
  void detectsFusedSuffixColumn_firstname() {
    String sql = "SELECT * FROM users WHERE firstname = 5";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("firstname");
  }

  @Test
  void detectsStandaloneIndicatorColumn_name() {
    String sql = "SELECT * FROM users WHERE name = 7";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("name");
  }

  @Test
  void noIssueForFkStyleIntColumn_descriptionId() {
    // A FK-style numeric column ending in _id should NOT be flagged even though the
    // leading 'description' token would otherwise match the string indicator set.
    String sql = "SELECT * FROM items WHERE description_id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForCountColumn_commentCount() {
    String sql = "SELECT * FROM posts WHERE comment_count = 5";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }
}
