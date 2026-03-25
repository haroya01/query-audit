package io.queryaudit.core.regression;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.Severity;
import java.util.List;
import org.junit.jupiter.api.Test;

class QueryCountRegressionDetectorTest {

  private final QueryCountRegressionDetector detector = new QueryCountRegressionDetector();

  // ── Regression detected ─────────────────────────────────────────────

  @Test
  void detectsRegressionWhenQueryCountDoubles() {
    // baseline 10, current 20 -> 100% increase, +10 queries
    QueryCounts baseline = new QueryCounts(10, 0, 0, 0, 10);
    QueryCounts current = new QueryCounts(20, 0, 0, 0, 20);

    List<Issue> issues = detector.detect("RoomApiTest", "testCreateRoom", current, baseline);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.QUERY_COUNT_REGRESSION);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    assertThat(issues.get(0).detail()).contains("10 -> 20").contains("+10");
  }

  // ── No regression: small increase below thresholds ──────────────────

  @Test
  void noRegressionWhenIncreaseBelowThreshold() {
    // baseline 10, current 12 -> 20% increase, only +2 queries
    // Below both the 50% ratio and 5-query absolute threshold
    QueryCounts baseline = new QueryCounts(10, 0, 0, 0, 10);
    QueryCounts current = new QueryCounts(12, 0, 0, 0, 12);

    List<Issue> issues = detector.detect("RoomApiTest", "testCreateRoom", current, baseline);

    assertThat(issues).isEmpty();
  }

  // ── No baseline: first run ──────────────────────────────────────────

  @Test
  void noIssueWhenBaselineIsNull() {
    QueryCounts current = new QueryCounts(15, 0, 0, 0, 15);

    List<Issue> issues = detector.detect("RoomApiTest", "testCreateRoom", current, null);

    assertThat(issues).isEmpty();
  }

  // ── Large regression: 10x increase -> ERROR ─────────────────────────

  @Test
  void detectsSevereRegressionAsError() {
    // baseline 5, current 50 -> 10x increase
    QueryCounts baseline = new QueryCounts(5, 0, 0, 0, 5);
    QueryCounts current = new QueryCounts(50, 0, 0, 0, 50);

    List<Issue> issues = detector.detect("MessageApiTest", "testSendMessage", current, baseline);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.QUERY_COUNT_REGRESSION);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
    assertThat(issues.get(0).detail()).contains("5 -> 50");
  }

  // ── Decrease: improvement, no issue ─────────────────────────────────

  @Test
  void noIssueWhenQueryCountDecreases() {
    // baseline 20, current 10 -> improvement
    QueryCounts baseline = new QueryCounts(20, 0, 0, 0, 20);
    QueryCounts current = new QueryCounts(10, 0, 0, 0, 10);

    List<Issue> issues = detector.detect("RoomApiTest", "testCreateRoom", current, baseline);

    assertThat(issues).isEmpty();
  }

  // ── Exact same count: no issue ──────────────────────────────────────

  @Test
  void noIssueWhenCountsAreIdentical() {
    QueryCounts baseline = new QueryCounts(10, 0, 0, 0, 10);
    QueryCounts current = new QueryCounts(10, 0, 0, 0, 10);

    List<Issue> issues = detector.detect("RoomApiTest", "testCreateRoom", current, baseline);

    assertThat(issues).isEmpty();
  }

  // ── SELECT-specific regression when total is fine ────────────────────

  @Test
  void detectsSelectSpecificRegression() {
    // Total count: baseline 20, current 22 -> 10% increase, only +2 (below total threshold)
    // SELECT count: baseline 5, current 15 -> 3x increase, +10 (above SELECT threshold)
    QueryCounts baseline = new QueryCounts(5, 10, 3, 2, 20);
    QueryCounts current = new QueryCounts(15, 5, 1, 1, 22);

    List<Issue> issues = detector.detect("UserApiTest", "testGetUser", current, baseline);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("SELECT count regression");
    assertThat(issues.get(0).detail()).contains("5 -> 15");
  }

  // ── High ratio but low absolute increase: no regression ─────────────

  @Test
  void noRegressionWhenAbsoluteIncreaseTooSmall() {
    // baseline 2, current 6 -> 3x ratio but only +4 (below absolute threshold of 5)
    QueryCounts baseline = new QueryCounts(2, 0, 0, 0, 2);
    QueryCounts current = new QueryCounts(6, 0, 0, 0, 6);

    List<Issue> issues = detector.detect("RoomApiTest", "testCreateRoom", current, baseline);

    assertThat(issues).isEmpty();
  }

  // ── Large absolute increase but low ratio: no regression ────────────

  @Test
  void noRegressionWhenRatioTooSmall() {
    // baseline 100, current 110 -> 10% increase, +10 queries
    // Above absolute threshold but below ratio threshold (1.5)
    QueryCounts baseline = new QueryCounts(100, 0, 0, 0, 100);
    QueryCounts current = new QueryCounts(110, 0, 0, 0, 110);

    List<Issue> issues = detector.detect("RoomApiTest", "testCreateRoom", current, baseline);

    assertThat(issues).isEmpty();
  }

  // ── Baseline with zero total count ───────────────────────────────────

  @Test
  void handlesZeroBaselineGracefully() {
    // baseline 0, current 10 -> avoid division by zero
    QueryCounts baseline = new QueryCounts(0, 0, 0, 0, 0);
    QueryCounts current = new QueryCounts(10, 0, 0, 0, 10);

    List<Issue> issues = detector.detect("RoomApiTest", "testCreateRoom", current, baseline);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.QUERY_COUNT_REGRESSION);
  }

  // ── 2x increase -> WARNING severity ─────────────────────────────────

  @Test
  void twoTimesIncreaseIsWarning() {
    QueryCounts baseline = new QueryCounts(10, 0, 0, 0, 10);
    QueryCounts current = new QueryCounts(25, 0, 0, 0, 25);

    List<Issue> issues = detector.detect("RoomApiTest", "testCreateRoom", current, baseline);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
  }

  // ── Suggestion includes update baseline command ─────────────────────

  @Test
  void suggestionIncludesUpdateBaselineCommand() {
    QueryCounts baseline = new QueryCounts(10, 0, 0, 0, 10);
    QueryCounts current = new QueryCounts(20, 0, 0, 0, 20);

    List<Issue> issues = detector.detect("RoomApiTest", "testCreateRoom", current, baseline);

    assertThat(issues.get(0).suggestion()).contains("queryGuard.updateBaseline=true");
  }
}
