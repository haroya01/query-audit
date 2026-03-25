package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class WindowFunctionWithoutPartitionDetectorTest {

  private final WindowFunctionWithoutPartitionDetector detector =
      new WindowFunctionWithoutPartitionDetector();
  private final IndexMetadata emptyIndex = new IndexMetadata(Map.of());

  private static QueryRecord q(String sql) {
    return new QueryRecord(sql, 1000L, System.currentTimeMillis(), null);
  }

  @Test
  void noIssueForRowNumberWithOrderByWithoutPartitionBy() {
    // ROW_NUMBER() OVER(ORDER BY ...) without PARTITION BY is intentional for pagination
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id, ROW_NUMBER() OVER(ORDER BY created_at) AS rn FROM users")),
            emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithPartitionBy() {
    List<Issue> issues =
        detector.evaluate(
            List.of(
                q("SELECT id, ROW_NUMBER() OVER(PARTITION BY department_id ORDER BY created_at) AS rn FROM users")),
            emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForRankWithOrderByWithoutPartition() {
    // RANK() OVER(ORDER BY ...) without PARTITION BY is intentional for ranking
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id, RANK() OVER(ORDER BY score DESC) AS rnk FROM students")),
            emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void detectsLagWithoutPartition() {
    List<Issue> issues =
        detector.evaluate(
            List.of(
                q("SELECT id, LAG(price, 1) OVER(ORDER BY date) AS prev_price FROM stock_prices")),
            emptyIndex);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("LAG");
  }

  @Test
  void detectsAggregateWindowWithoutPartition() {
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id, SUM(amount) OVER(ORDER BY id) AS running_total FROM orders")),
            emptyIndex);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("SUM");
  }

  @Test
  void noIssueWithoutWindowFunction() {
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id, name FROM users WHERE id = 1")), emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void skipsNonSelectQueries() {
    List<Issue> issues =
        detector.evaluate(
            List.of(q("INSERT INTO users (name) VALUES ('test')")), emptyIndex);
    assertThat(issues).isEmpty();
  }

  // ── false positive fix: intentional numbering over entire result set ─

  @Test
  void noIssueForRowNumberOverOrderByWithoutPartition() {
    // ROW_NUMBER() OVER(ORDER BY ...) is intentional for pagination/numbering
    List<Issue> issues =
        detector.evaluate(
            List.of(
                q("SELECT id, name, ROW_NUMBER() OVER(ORDER BY id) AS rn FROM users")),
            emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForRankOverOrderByWithoutPartition() {
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id, RANK() OVER(ORDER BY score DESC) AS rnk FROM students")),
            emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForDenseRankOverOrderByWithoutPartition() {
    List<Issue> issues =
        detector.evaluate(
            List.of(
                q("SELECT id, DENSE_RANK() OVER(ORDER BY salary DESC) AS dr FROM employees")),
            emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void stillDetectsAggregateWindowWithoutPartition() {
    // SUM() OVER(ORDER BY ...) without PARTITION BY is still flagged
    // because running totals over the entire table are usually unintentional
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id, SUM(amount) OVER(ORDER BY id) AS running_total FROM orders")),
            emptyIndex);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("SUM");
  }

  @Test
  void stillDetectsRowNumberOverEmptyOver() {
    // ROW_NUMBER() OVER() with no ORDER BY and no PARTITION BY — truly meaningless
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id, ROW_NUMBER() OVER() AS rn FROM users")),
            emptyIndex);
    assertThat(issues).hasSize(1);
  }
}
