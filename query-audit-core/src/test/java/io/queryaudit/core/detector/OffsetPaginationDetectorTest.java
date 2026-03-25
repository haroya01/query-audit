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

class OffsetPaginationDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final OffsetPaginationDetector detector = new OffsetPaginationDetector(1000);

  // ── Literal OFFSET tests (existing behavior) ──────────────────────

  @Test
  void detectsLargeOffsetLiteral() {
    String sql = "SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 5000";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.OFFSET_PAGINATION);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    assertThat(issues.get(0).detail()).contains("5000");
  }

  @Test
  void noIssueForSmallOffsetLiteral() {
    String sql = "SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 50";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void detectsMySqlStyleLargeOffset() {
    String sql = "SELECT * FROM users ORDER BY id LIMIT 5000, 10";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
  }

  // ── Parameterized OFFSET tests (new behavior) ─────────────────────

  @Test
  void detectsParameterizedOffset() {
    // JPA generates OFFSET with ? placeholder
    String sql = "SELECT * FROM users ORDER BY id LIMIT ? OFFSET ?";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.OFFSET_PAGINATION);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    assertThat(issues.get(0).detail()).contains("Parameterized OFFSET");
    assertThat(issues.get(0).suggestion()).contains("cursor-based pagination");
  }

  @Test
  void detectsParameterizedMySqlStyleOffset() {
    // MySQL-style LIMIT ?, ? with parameterized values
    String sql = "SELECT * FROM users ORDER BY id LIMIT ?, ?";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
  }

  @Test
  void detectsStandaloneParameterizedOffset() {
    String sql = "SELECT * FROM users ORDER BY id OFFSET ?";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
  }

  @Test
  void noIssueForQueryWithoutOffset() {
    String sql = "SELECT * FROM users ORDER BY id LIMIT 10";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForSimpleQuery() {
    String sql = "SELECT * FROM users WHERE id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void literalOffsetTakesPriorityOverParameterized() {
    // When literal OFFSET is large, should be WARNING not INFO
    String sql = "SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 2000";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
  }

  @Test
  void deduplicatesByNormalizedSql() {
    String sql1 = "SELECT * FROM users ORDER BY id LIMIT 10 OFFSET ?";
    String sql2 = "SELECT * FROM users ORDER BY id LIMIT 10 OFFSET ?";

    List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
  }

  // ── Mutation-killing tests ──────────────────────────────────────────

  @Test
  void literalOffsetReportsTableName() {
    // Kills: L56 NegateConditionalsMutator (tables.isEmpty() negated)
    // If negated, table would be null when tables is NOT empty
    String sql = "SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 5000";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).table()).isEqualTo("users");
  }

  @Test
  void parameterizedOffsetReportsTableName() {
    // Kills: L75 NegateConditionalsMutator (tables.isEmpty() negated)
    // If negated, table would be null when tables is NOT empty
    String sql = "SELECT * FROM orders ORDER BY id LIMIT ? OFFSET ?";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).table()).isEqualTo("orders");
  }

  @Test
  void offsetExactlyAtThresholdIsDetected() {
    // Test boundary: offset == threshold (1000) should be detected (>= threshold)
    String sql = "SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 1000";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
  }

  @Test
  void offsetJustBelowThresholdIsNotDetected() {
    // offset = 999 < threshold (1000) should NOT be detected as WARNING
    String sql = "SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 999";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }
}
