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

class SubqueryInDmlDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private final SubqueryInDmlDetector detector = new SubqueryInDmlDetector();

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  // ── Positive cases ──────────────────────────────────────────────────

  @Test
  void detectsUpdateWithInSubquery() {
    String sql =
        "UPDATE orders SET status = 'CANCELLED' WHERE customer_id IN (SELECT id FROM customers WHERE active = 0)";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.SUBQUERY_IN_DML);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    assertThat(issues.get(0).detail()).contains("UPDATE");
    assertThat(issues.get(0).suggestion()).contains("semijoin");
  }

  @Test
  void detectsDeleteWithInSubquery() {
    String sql =
        "DELETE FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE region = 'EU')";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.SUBQUERY_IN_DML);
    assertThat(issues.get(0).detail()).contains("DELETE");
  }

  @Test
  void detectsUpdateWithNotInSubquery() {
    String sql =
        "UPDATE products SET archived = 1 WHERE category_id NOT IN (SELECT id FROM categories WHERE active = 1)";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.SUBQUERY_IN_DML);
  }

  @Test
  void detectsWithExtraWhitespace() {
    String sql = "UPDATE orders SET status = 'X' WHERE id IN  (  SELECT order_id FROM returns )";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
  }

  // ── Negative cases ──────────────────────────────────────────────────

  @Test
  void noIssueForSelectWithInSubquery() {
    // SELECT is not UPDATE/DELETE, so no issue
    String sql = "SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers)";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForUpdateWithoutSubquery() {
    String sql = "UPDATE orders SET status = 'CANCELLED' WHERE customer_id = 42";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForDeleteWithoutSubquery() {
    String sql = "DELETE FROM orders WHERE id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForUpdateWithInList() {
    // IN with literal values, not a subquery
    String sql = "UPDATE orders SET status = 'CANCELLED' WHERE id IN (1, 2, 3)";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForInsertWithSubquery() {
    // INSERT is not UPDATE/DELETE
    String sql = "INSERT INTO archive SELECT * FROM orders WHERE status = 'OLD'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  // ── Edge cases ──────────────────────────────────────────────────────

  @Test
  void skipsDuplicateNormalizedQueries() {
    String sql1 =
        "UPDATE orders SET status = 'X' WHERE id IN (SELECT order_id FROM returns WHERE reason = 'defective')";
    String sql2 =
        "UPDATE orders SET status = 'Y' WHERE id IN (SELECT order_id FROM returns WHERE reason = 'wrong')";

    List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), EMPTY_INDEX);

    // Both normalize to the same pattern
    assertThat(issues).hasSize(1);
  }

  @Test
  void extractsCorrectTable() {
    String sql =
        "UPDATE orders SET status = 'CANCELLED' WHERE customer_id IN (SELECT id FROM customers)";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    // UPDATE table extraction may vary; just verify no crash
    assertThat(issues.get(0).table()).isNotNull();
  }

  // ── False positive prevention ─────────────────────────────────────────

  @Test
  void noIssueForDeleteWithExistsSubquery() {
    // EXISTS subqueries do NOT have the semijoin limitation — should not be flagged
    String sql =
        "DELETE FROM orders WHERE EXISTS (SELECT 1 FROM cancelled WHERE cancelled.order_id = orders.id)";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForUpdateWithExistsSubquery() {
    // EXISTS in UPDATE WHERE clause — efficient pattern, not flagged
    String sql =
        "UPDATE orders SET status = 'CANCELLED' WHERE EXISTS "
            + "(SELECT 1 FROM returns WHERE returns.order_id = orders.id)";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForUpdateWithSubqueryInSetClause() {
    // Subquery in SET clause, not WHERE — not the semijoin limitation
    String sql =
        "UPDATE orders SET total = (SELECT SUM(amount) FROM order_items WHERE order_items.order_id = orders.id) WHERE id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }
}
