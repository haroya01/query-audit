package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class SlowQueryDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql, long executionTimeNanos) {
    return new QueryRecord(sql, executionTimeNanos, System.currentTimeMillis(), "");
  }

  private final SlowQueryDetector detector = new SlowQueryDetector();

  @Test
  void detectsWarningLevelSlowQuery() {
    // 600ms > 500ms default warning threshold
    long nanos = TimeUnit.MILLISECONDS.toNanos(600);
    List<Issue> issues =
        detector.evaluate(
            List.of(record("SELECT * FROM users WHERE name = 'test'", nanos)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.SLOW_QUERY);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    assertThat(issues.get(0).detail()).contains("600ms").contains("threshold: 500ms");
  }

  @Test
  void detectsErrorLevelSlowQuery() {
    // 4000ms > 3000ms default error threshold
    long nanos = TimeUnit.MILLISECONDS.toNanos(4000);
    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM orders WHERE id = 1", nanos)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
    assertThat(issues.get(0).detail()).contains("4000ms").contains("threshold: 3000ms");
  }

  @Test
  void noIssueForFastQuery() {
    // 50ms < 500ms warning threshold
    long nanos = TimeUnit.MILLISECONDS.toNanos(50);
    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM users WHERE id = 1", nanos)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForModerateQuery() {
    // 200ms is below the 500ms warning threshold (H2/CI noise range)
    long nanos = TimeUnit.MILLISECONDS.toNanos(200);
    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM users WHERE id = 1", nanos)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void skipsZeroExecutionTime() {
    List<Issue> issues = detector.evaluate(List.of(record("SELECT * FROM users", 0L)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void usesMedianForSameQueryPattern() {
    // Same normalized query with multiple executions: 100ms, 150ms, 600ms
    // Median = 150ms, which is below 500ms threshold -> no issue
    List<Issue> issues =
        detector.evaluate(
            List.of(
                record("SELECT * FROM users WHERE id = 1", TimeUnit.MILLISECONDS.toNanos(100)),
                record("SELECT * FROM users WHERE id = 2", TimeUnit.MILLISECONDS.toNanos(150)),
                record("SELECT * FROM users WHERE id = 3", TimeUnit.MILLISECONDS.toNanos(600))),
            EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void medianExceedingThresholdTriggersIssue() {
    // Same normalized query: 600ms, 700ms, 800ms -> median = 700ms > 500ms
    List<Issue> issues =
        detector.evaluate(
            List.of(
                record("SELECT * FROM users WHERE id = 1", TimeUnit.MILLISECONDS.toNanos(600)),
                record("SELECT * FROM users WHERE id = 2", TimeUnit.MILLISECONDS.toNanos(700)),
                record("SELECT * FROM users WHERE id = 3", TimeUnit.MILLISECONDS.toNanos(800))),
            EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("700ms").contains("median of 3 executions");
  }

  @Test
  void singleExecutionStillFlagsIfAboveThreshold() {
    // Single execution at 600ms > 500ms -> still flags
    long nanos = TimeUnit.MILLISECONDS.toNanos(600);
    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM users WHERE id = 1", nanos)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("600ms");
    // Single execution should not mention "median of N executions"
    assertThat(issues.get(0).detail()).doesNotContain("median of");
  }

  @Test
  void customThresholds() {
    SlowQueryDetector custom = new SlowQueryDetector(50, 500);

    // 80ms > 50ms custom warning threshold
    long nanos = TimeUnit.MILLISECONDS.toNanos(80);
    List<Issue> issues =
        custom.evaluate(List.of(record("SELECT * FROM users WHERE id = 1", nanos)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    assertThat(issues.get(0).detail()).contains("threshold: 50ms");
  }

  @Test
  void customErrorThreshold() {
    SlowQueryDetector custom = new SlowQueryDetector(50, 500);

    // 600ms > 500ms custom error threshold
    long nanos = TimeUnit.MILLISECONDS.toNanos(600);
    List<Issue> issues =
        custom.evaluate(
            List.of(record("SELECT * FROM orders WHERE status = 'PENDING'", nanos)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
    assertThat(issues.get(0).detail()).contains("threshold: 500ms");
  }

  @Test
  void extractsTableName() {
    long nanos = TimeUnit.MILLISECONDS.toNanos(600);
    List<Issue> issues =
        detector.evaluate(
            List.of(record("SELECT * FROM orders WHERE status = 'ACTIVE'", nanos)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).table()).isEqualTo("orders");
  }

  @Test
  void includesSuggestion() {
    long nanos = TimeUnit.MILLISECONDS.toNanos(600);
    List<Issue> issues =
        detector.evaluate(
            List.of(record("SELECT * FROM users WHERE name = 'test'", nanos)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).suggestion()).contains("indexes");
  }

  @Test
  void medianWithEvenNumberOfExecutions() {
    // 4 executions: 400ms, 600ms, 700ms, 800ms -> median = (600+700)/2 = 650ms
    List<Issue> issues =
        detector.evaluate(
            List.of(
                record("SELECT * FROM users WHERE id = 1", TimeUnit.MILLISECONDS.toNanos(400)),
                record("SELECT * FROM users WHERE id = 2", TimeUnit.MILLISECONDS.toNanos(600)),
                record("SELECT * FROM users WHERE id = 3", TimeUnit.MILLISECONDS.toNanos(700)),
                record("SELECT * FROM users WHERE id = 4", TimeUnit.MILLISECONDS.toNanos(800))),
            EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("650ms").contains("median of 4 executions");
  }

  @Test
  void oneOffSpikeDoesNotTriggerWithMedian() {
    // 5 executions: 50ms, 50ms, 50ms, 50ms, 2000ms
    // Median = 50ms -> no issue despite one spike
    List<Issue> issues =
        detector.evaluate(
            List.of(
                record("SELECT * FROM users WHERE id = 1", TimeUnit.MILLISECONDS.toNanos(50)),
                record("SELECT * FROM users WHERE id = 2", TimeUnit.MILLISECONDS.toNanos(50)),
                record("SELECT * FROM users WHERE id = 3", TimeUnit.MILLISECONDS.toNanos(50)),
                record("SELECT * FROM users WHERE id = 4", TimeUnit.MILLISECONDS.toNanos(50)),
                record("SELECT * FROM users WHERE id = 5", TimeUnit.MILLISECONDS.toNanos(2000))),
            EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  // ── Mutation-killing tests ──────────────────────────────────────────

  @Test
  void exactlyAtWarningThresholdIsNotFlagged() {
    // Kills: L84 ConditionalsBoundaryMutator (> vs >=)
    // medianMs == warningThresholdMs (500) should NOT be flagged (> not >=)
    long nanos = TimeUnit.MILLISECONDS.toNanos(500);
    List<Issue> issues =
        detector.evaluate(
            List.of(record("SELECT * FROM users WHERE name = 'boundary'", nanos)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void oneAboveWarningThresholdIsFlagged() {
    // 501ms > 500ms -> should be flagged as WARNING
    long nanos = TimeUnit.MILLISECONDS.toNanos(501);
    List<Issue> issues =
        detector.evaluate(
            List.of(record("SELECT * FROM users WHERE name = 'boundary501'", nanos)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
  }

  @Test
  void exactlyAtErrorThresholdIsWarningNotError() {
    // Kills: L81 ConditionalsBoundaryMutator (> vs >=)
    // medianMs == errorThresholdMs (3000) should be WARNING, not ERROR
    long nanos = TimeUnit.MILLISECONDS.toNanos(3000);
    List<Issue> issues =
        detector.evaluate(
            List.of(record("SELECT * FROM orders WHERE status = 'boundary3000'", nanos)),
            EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
  }

  @Test
  void oneAboveErrorThresholdIsError() {
    // 3001ms > 3000ms -> should be ERROR
    long nanos = TimeUnit.MILLISECONDS.toNanos(3001);
    List<Issue> issues =
        detector.evaluate(
            List.of(record("SELECT * FROM orders WHERE status = 'boundary3001'", nanos)),
            EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
  }

  @Test
  void zeroMsExecutionTimeIsSkipped() {
    // Kills: L68 ConditionalsBoundaryMutator (> 0 vs >= 0)
    // Execution time converting to exactly 0ms should be skipped
    // Use a value that converts to 0ms (< 1ms in nanos)
    long nanos = TimeUnit.MICROSECONDS.toNanos(500); // 0.5ms -> truncates to 0ms
    List<Issue> issues =
        detector.evaluate(
            List.of(record("SELECT * FROM users WHERE name = 'zeroboundary'", nanos)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  // ── False-positive reduction tests ─────────────────────────────────

  @Test
  void oneOffSpikeFilteredByMedian() {
    // A single slow execution among many fast ones is filtered by median.
    // This is the primary false-positive protection: cold cache / GC spikes
    // don't trigger when query is run multiple times.
    // 5 executions: 50ms, 50ms, 50ms, 50ms, 5000ms -> median = 50ms -> no issue
    List<Issue> issues =
        detector.evaluate(
            List.of(
                record("SELECT * FROM products WHERE cat = 'cold1'", TimeUnit.MILLISECONDS.toNanos(50)),
                record("SELECT * FROM products WHERE cat = 'cold2'", TimeUnit.MILLISECONDS.toNanos(50)),
                record("SELECT * FROM products WHERE cat = 'cold3'", TimeUnit.MILLISECONDS.toNanos(50)),
                record("SELECT * FROM products WHERE cat = 'cold4'", TimeUnit.MILLISECONDS.toNanos(50)),
                record("SELECT * FROM products WHERE cat = 'cold5'", TimeUnit.MILLISECONDS.toNanos(5000))),
            EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void multipleExecutionsAboveWarningKeepWarning() {
    // Multiple executions consistently slow -> WARNING (confirmed slow, not cold cache)
    List<Issue> issues =
        detector.evaluate(
            List.of(
                record("SELECT * FROM users WHERE id = 1", TimeUnit.MILLISECONDS.toNanos(600)),
                record("SELECT * FROM users WHERE id = 2", TimeUnit.MILLISECONDS.toNanos(700))),
            EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
  }

  @Test
  void multipleExecutionsAboveErrorKeepError() {
    // Multiple executions consistently very slow -> ERROR
    List<Issue> issues =
        detector.evaluate(
            List.of(
                record("SELECT * FROM users WHERE id = 1", TimeUnit.MILLISECONDS.toNanos(4000)),
                record("SELECT * FROM users WHERE id = 2", TimeUnit.MILLISECONDS.toNanos(5000))),
            EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
  }

  @Test
  void medianDependsOnSortOrder() {
    // Kills: L118 VoidMethodCallMutator (removed call to Collections::sort)
    // If sort is removed, median of unsorted [800, 100, 600] would be 100 (middle index)
    // instead of 600 (correct sorted median). 600 > 500 -> WARNING
    List<Issue> issues =
        detector.evaluate(
            List.of(
                record(
                    "SELECT * FROM accounts WHERE type = 'sort1'",
                    TimeUnit.MILLISECONDS.toNanos(800)),
                record(
                    "SELECT * FROM accounts WHERE type = 'sort2'",
                    TimeUnit.MILLISECONDS.toNanos(100)),
                record(
                    "SELECT * FROM accounts WHERE type = 'sort3'",
                    TimeUnit.MILLISECONDS.toNanos(600))),
            EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("600ms");
  }
}
