package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class LargeInListDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final LargeInListDetector detector = new LargeInListDetector();

  @Test
  void detectsLargeInListWarning() {
    // 101 literal values -> WARNING
    String values =
        IntStream.range(0, 101).mapToObj(String::valueOf).collect(Collectors.joining(", "));
    String sql = "SELECT * FROM users WHERE id IN (" + values + ")";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.LARGE_IN_LIST);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    assertThat(issues.get(0).detail()).contains("101");
  }

  @Test
  void detectsLargeInListError() {
    // 1001 literal values -> ERROR
    String values =
        IntStream.range(0, 1001).mapToObj(String::valueOf).collect(Collectors.joining(", "));
    String sql = "SELECT * FROM users WHERE id IN (" + values + ")";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.LARGE_IN_LIST);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
  }

  @Test
  void noIssueForSmallInList() {
    String sql = "SELECT * FROM users WHERE id IN (1, 2, 3, 4, 5)";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForExactlyThreshold() {
    // Exactly 100 values -> no issue (threshold is >100)
    String placeholders =
        IntStream.range(0, 100).mapToObj(i -> "?").collect(Collectors.joining(", "));
    String sql = "SELECT * FROM users WHERE id IN (" + placeholders + ")";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithoutInClause() {
    String sql = "SELECT * FROM users WHERE id = 1";

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
  void deduplicatesSameNormalizedQuery() {
    String values =
        IntStream.range(0, 101).mapToObj(String::valueOf).collect(Collectors.joining(", "));
    String sql = "SELECT * FROM users WHERE id IN (" + values + ")";

    List<Issue> issues = detector.evaluate(List.of(record(sql), record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
  }

  @Test
  void suggestionMentionsTempTable() {
    String values =
        IntStream.range(0, 101).mapToObj(String::valueOf).collect(Collectors.joining(", "));
    String sql = "SELECT * FROM users WHERE id IN (" + values + ")";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).suggestion()).contains("temporary table");
  }

  @Test
  void detectsLargeInListWithLiteralValues() {
    // Use literal integers instead of placeholders
    String values =
        IntStream.range(0, 150).mapToObj(String::valueOf).collect(Collectors.joining(", "));
    String sql = "SELECT * FROM orders WHERE order_id IN (" + values + ")";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.LARGE_IN_LIST);
  }

  // ── false positive fix: parameterized IN lists ─────────────────────

  @Test
  void noIssueForParameterizedInList() {
    // IN lists with only ? placeholders — actual count is unknown at analysis time
    String placeholders =
        IntStream.range(0, 200).mapToObj(i -> "?").collect(Collectors.joining(", "));
    String sql = "SELECT * FROM users WHERE id IN (" + placeholders + ")";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void stillDetectsMixedPlaceholdersAndLiterals() {
    // Mixed IN list with literals and placeholders should still be detected
    StringBuilder values = new StringBuilder();
    for (int i = 0; i < 101; i++) {
      if (i > 0) values.append(", ");
      values.append(i % 2 == 0 ? "?" : String.valueOf(i));
    }
    String sql = "SELECT * FROM users WHERE id IN (" + values + ")";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
  }
}
