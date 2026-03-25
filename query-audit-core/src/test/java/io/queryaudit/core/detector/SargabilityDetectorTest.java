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

class SargabilityDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final SargabilityDetector detector = new SargabilityDetector();

  @Test
  void detectsColumnPlusLiteral() {
    String sql = "SELECT * FROM orders WHERE price + 1 = 100";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.NON_SARGABLE_EXPRESSION);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
    assertThat(issues.get(0).column()).isEqualTo("price");
    assertThat(issues.get(0).detail()).contains("Arithmetic on column 'price'");
  }

  @Test
  void detectsColumnMultiplyLiteral() {
    String sql = "SELECT * FROM orders WHERE amount * 2 > 500";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("amount");
    assertThat(issues.get(0).suggestion()).contains("/");
  }

  @Test
  void detectsColumnDivideLiteral() {
    String sql = "SELECT * FROM products WHERE price / 10 = 5";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("price");
  }

  @Test
  void detectsColumnMinusLiteral() {
    String sql = "SELECT * FROM events WHERE year - 1 = 2024";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("year");
  }

  @Test
  void detectsQualifiedColumnArithmetic() {
    String sql = "SELECT * FROM orders o WHERE o.price + 1 = 100";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("price");
  }

  @Test
  void noIssueForSargableQuery() {
    String sql = "SELECT * FROM orders WHERE price = 100";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForArithmeticOnLiteralSide() {
    // Arithmetic on the literal side is fine: WHERE price = 100 - 1
    // This is sargable because the column is not wrapped
    String sql = "SELECT * FROM orders WHERE price = 100 - 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForQueryWithoutWhere() {
    String sql = "SELECT * FROM orders";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void detectsParameterizedArithmetic() {
    String sql = "SELECT * FROM orders WHERE price + ? = ?";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("price");
  }

  @Test
  void noFalsePositiveFromSubqueryArithmetic() {
    // The arithmetic "price + 1 > 100" is inside the subquery, not the outer WHERE.
    // After stripping subqueries, the outer WHERE body should not contain column arithmetic.
    String sql =
        "SELECT * FROM orders WHERE id IN (SELECT order_id FROM items WHERE price + 1 > 100)";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void deduplicatesSameColumn() {
    // Same column arithmetic appearing -- only report once
    String sql = "SELECT * FROM orders WHERE price + 1 = 100 AND price + 2 > 50";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("price");
  }

  // ── False Positive Fixes ──────────────────────────────────────────

  @Test
  void shouldNotFlagArithmeticInSelectClause() {
    // Arithmetic in SELECT is not a sargability concern
    String sql = "SELECT price + 1 AS adjusted FROM orders WHERE id = ?";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void shouldNotFlagArithmeticInOrderByClause() {
    // Arithmetic in ORDER BY should not be flagged as non-sargable
    String sql = "SELECT * FROM orders WHERE status = 1 ORDER BY price + 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void shouldNotFlagArithmeticOnLiteralOnlyRhs() {
    // Arithmetic on pure literal RHS: WHERE col = 100 + 50 is sargable
    String sql = "SELECT * FROM orders WHERE price = 100 + 50";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    // "100 + 50" doesn't match COL_ARITHMETIC_THEN_CMP because 100 starts with digit
    assertThat(issues).isEmpty();
  }

  @Test
  void shouldNotFlagSqlKeywordsAsColumns() {
    // SQL keywords appearing in arithmetic-like patterns should be skipped
    String sql = "SELECT * FROM orders WHERE id = 1 AND status = 2";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }
}
