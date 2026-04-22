package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CaseInWhereDetectorTest {

  private final CaseInWhereDetector detector = new CaseInWhereDetector();
  private final IndexMetadata emptyIndex = new IndexMetadata(Map.of());

  private static QueryRecord q(String sql) {
    return new QueryRecord(sql, 1000L, System.currentTimeMillis(), null);
  }

  @Test
  void detectsCaseInWhere() {
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id FROM users WHERE CASE WHEN status = 'A' THEN 1 ELSE 0 END = 1")),
            emptyIndex);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.CASE_IN_WHERE);
  }

  @Test
  void noIssueWithoutCaseInWhere() {
    List<Issue> issues =
        detector.evaluate(List.of(q("SELECT id FROM users WHERE status = 'active'")), emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWhenCaseOnlyInSelect() {
    List<Issue> issues =
        detector.evaluate(
            List.of(
                q(
                    "SELECT CASE WHEN status = 'A' THEN 'Active' ELSE 'Inactive' END AS label FROM users WHERE id = 1")),
            emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void noCaseOnRightHandSide() {
    // CASE on the RHS of a comparison does not prevent index usage on the LHS column
    List<Issue> issues =
        detector.evaluate(
            List.of(
                q(
                    "SELECT id FROM orders WHERE amount > CASE WHEN type = 'premium' THEN 100 ELSE 50 END")),
            emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithoutWhereClause() {
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT CASE WHEN 1=1 THEN 'yes' ELSE 'no' END FROM dual")), emptyIndex);
    assertThat(issues).isEmpty();
  }

  // ── False Positive Fixes ──────────────────────────────────────────

  @Test
  void shouldNotFlagCaseOnRhsInParentheses() {
    // CASE on RHS inside parentheses: index on 'status' is still usable
    List<Issue> issues =
        detector.evaluate(
            List.of(
                q(
                    "SELECT id FROM users WHERE status = (CASE WHEN role = 'admin' THEN 'active' ELSE 'inactive' END)")),
            emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void shouldNotFlagCaseInInList() {
    // CASE used to compute a value for IN list: index on 'status' is still usable
    List<Issue> issues =
        detector.evaluate(
            List.of(
                q(
                    "SELECT id FROM users WHERE status IN (CASE WHEN flag = 1 THEN 'active' ELSE 'inactive' END)")),
            emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void shouldStillFlagCaseOnLhs() {
    // CASE wrapping a column on the LHS prevents index usage
    List<Issue> issues =
        detector.evaluate(
            List.of(
                q(
                    "SELECT id FROM users WHERE CASE WHEN status = 'A' THEN 1 WHEN status = 'B' THEN 2 END = 1")),
            emptyIndex);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.CASE_IN_WHERE);
  }
}
