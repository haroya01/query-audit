package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ForUpdateWithoutTimeoutDetectorTest {

  private final ForUpdateWithoutTimeoutDetector detector = new ForUpdateWithoutTimeoutDetector();
  private final IndexMetadata emptyIndex = new IndexMetadata(Map.of());

  private static QueryRecord q(String sql) {
    return new QueryRecord(sql, 1000L, System.currentTimeMillis(), null);
  }

  @Test
  void detectsForUpdateWithoutNowait() {
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id FROM accounts WHERE id = 1 FOR UPDATE")), emptyIndex);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.FOR_UPDATE_WITHOUT_TIMEOUT);
  }

  @Test
  void noIssueWithNowait() {
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id FROM accounts WHERE id = 1 FOR UPDATE NOWAIT")), emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithSkipLocked() {
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id FROM accounts WHERE id = 1 FOR UPDATE SKIP LOCKED")), emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithWaitTimeout() {
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id FROM accounts WHERE id = 1 FOR UPDATE WAIT 5")), emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void detectsForShareWithoutTimeout() {
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id FROM accounts WHERE id = 1 FOR SHARE")), emptyIndex);
    assertThat(issues).hasSize(1);
  }

  @Test
  void noIssueWithoutForUpdate() {
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id FROM accounts WHERE id = 1")), emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithForUpdateOfColumnNowait() {
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id FROM accounts WHERE id = 1 FOR UPDATE OF id NOWAIT")), emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithForShareSkipLocked() {
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id FROM accounts WHERE id = 1 FOR SHARE SKIP LOCKED")), emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void noFalsePositiveForParameterizedForUpdate() {
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id FROM accounts WHERE id = ? FOR UPDATE NOWAIT")), emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void detectsForUpdateWithParameterizedWhereWithoutTimeout() {
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id FROM accounts WHERE id = ? FOR UPDATE")), emptyIndex);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.FOR_UPDATE_WITHOUT_TIMEOUT);
  }
}
