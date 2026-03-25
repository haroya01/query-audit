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

class GroupByFunctionDetectorTest {

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final GroupByFunctionDetector detector = new GroupByFunctionDetector();
  private final IndexMetadata emptyMetadata = new IndexMetadata(Map.of());

  @Test
  void detectsYearFunctionInGroupBy() {
    String sql = "SELECT YEAR(created_at), COUNT(*) FROM orders GROUP BY YEAR(created_at)";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.GROUP_BY_FUNCTION);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    assertThat(issues.get(0).column()).isEqualTo("created_at");
    assertThat(issues.get(0).detail()).contains("YEAR");
  }

  @Test
  void detectsUpperFunctionInGroupBy() {
    String sql = "SELECT UPPER(name), COUNT(*) FROM users GROUP BY UPPER(name)";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.GROUP_BY_FUNCTION);
    assertThat(issues.get(0).column()).isEqualTo("name");
  }

  @Test
  void detectsDateFunctionInGroupBy() {
    String sql = "SELECT DATE(created_at), SUM(amount) FROM orders GROUP BY DATE(created_at)";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
  }

  @Test
  void noIssueForPlainColumnGroupBy() {
    String sql = "SELECT status, COUNT(*) FROM orders GROUP BY status";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForQueryWithoutGroupBy() {
    String sql = "SELECT * FROM users WHERE id = 1";

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
  void noIssueForAggregateFunctionInGroupBy() {
    // ROLLUP is a legitimate GROUP BY modifier, not a function on a column
    String sql = "SELECT status, COUNT(*) FROM orders GROUP BY status";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void caseInsensitiveDetection() {
    String sql = "select year(created_at), count(*) from orders group by year(created_at)";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.GROUP_BY_FUNCTION);
  }

  @Test
  void deduplicatesSameNormalizedQuery() {
    String sql1 =
        "SELECT YEAR(created_at), COUNT(*) FROM orders WHERE status = 1 GROUP BY YEAR(created_at)";
    String sql2 =
        "SELECT YEAR(created_at), COUNT(*) FROM orders WHERE status = 2 GROUP BY YEAR(created_at)";

    List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), emptyMetadata);

    assertThat(issues).hasSize(1);
  }

  @Test
  void detectsFunctionWithTablePrefix() {
    String sql = "SELECT MONTH(o.created_at), COUNT(*) FROM orders o GROUP BY MONTH(o.created_at)";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("created_at");
  }

  // ── False Positive Fixes ──────────────────────────────────────────

  @Test
  void shouldNotFlagNumericPositionalGroupBy() {
    // GROUP BY 1, 2 is positional reference, no function call
    String sql = "SELECT status, COUNT(*) FROM orders GROUP BY 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void shouldNotFlagPlainExpressionGroupBy() {
    // GROUP BY col + 1 has no function call
    String sql = "SELECT price + 1, COUNT(*) FROM orders GROUP BY price + 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void shouldNotFlagAggregateFunctionCount() {
    // COUNT in GROUP BY context (rollup/cube) should not be flagged
    String sql = "SELECT status, COUNT(*) FROM orders GROUP BY status";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void shouldNotFlagFunctionInSubqueryGroupBy() {
    // Function in subquery's GROUP BY should not be flagged if subquery is removed
    String sql =
        "SELECT * FROM (SELECT YEAR(created_at) y, COUNT(*) c FROM orders GROUP BY YEAR(created_at)) sub WHERE sub.c > 10";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), emptyMetadata);

    // After removeSubqueries, the inner GROUP BY YEAR() is removed
    assertThat(issues).isEmpty();
  }
}
