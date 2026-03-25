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

class CountInsteadOfExistsDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final CountInsteadOfExistsDetector detector = new CountInsteadOfExistsDetector();

  @Test
  void detectsCountStarWithWhere() {
    String sql = "SELECT COUNT(*) FROM users WHERE email = 'test@example.com'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.COUNT_INSTEAD_OF_EXISTS);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    assertThat(issues.get(0).detail()).contains("COUNT").contains("EXISTS");
  }

  @Test
  void detectsCountColumnWithWhere() {
    String sql = "SELECT COUNT(id) FROM orders WHERE user_id = 42";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.COUNT_INSTEAD_OF_EXISTS);
  }

  @Test
  void detectsCountWithQualifiedColumn() {
    String sql = "SELECT COUNT(u.id) FROM users u WHERE u.active = true";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
  }

  @Test
  void noIssueForCountWithGroupBy() {
    // GROUP BY means real aggregation, not existence check
    String sql = "SELECT COUNT(*) FROM orders WHERE status = 'ACTIVE' GROUP BY user_id";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForCountWithHaving() {
    String sql = "SELECT COUNT(*) FROM orders WHERE status = 'ACTIVE' HAVING COUNT(*) > 5";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForCountWithoutWhere() {
    // Full table count might be intentional
    String sql = "SELECT COUNT(*) FROM users";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForNonCountQuery() {
    String sql = "SELECT * FROM users WHERE id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void deduplicatesSameNormalizedQuery() {
    String sql = "SELECT COUNT(*) FROM users WHERE id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql), record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
  }

  @Test
  void extractsTableName() {
    String sql = "SELECT COUNT(*) FROM orders WHERE user_id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).table()).isEqualTo("orders");
  }

  @Test
  void includesSuggestion() {
    String sql = "SELECT COUNT(*) FROM users WHERE email = 'test@example.com'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).suggestion()).contains("EXISTS").contains("short-circuits");
  }

  @Test
  void noIssueForCountDistinct() {
    // COUNT(DISTINCT ...) computes a distinct count value, not an existence check
    String sql = "SELECT COUNT(DISTINCT user_id) FROM orders WHERE status = 'ACTIVE'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForCountInSubquery() {
    // COUNT in subquery SELECT is used as a column value
    String sql =
        "SELECT p.id, (SELECT COUNT(*) FROM votes WHERE poll_id = p.id) as vote_count FROM polls p WHERE p.room_id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void severityIsInfo() {
    // Severity should be INFO (suggestion) not WARNING, since we can't know
    // if the count value is needed or just used for boolean existence
    String sql = "SELECT COUNT(*) FROM notifications WHERE user_id = 42 AND is_read = false";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
  }
}
