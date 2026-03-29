package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Verifies false-positive scenarios reported in GitHub issue #40.
 *
 * <h2>Background</h2>
 *
 * <p>Hibernate translates Spring Data {@code existsBy*} repository methods into
 * {@code SELECT count(col) > ?} SQL. The key insight is <b>actionability</b>:
 * a detection rule should only flag queries the developer can actually improve.
 *
 * <ul>
 *   <li>{@code COUNT(col) > ?} — developer already wrote {@code existsBy*}; nothing to fix → must NOT flag
 *   <li>{@code COUNT(col) > 0} — same boolean expression in SQL; nothing to fix → must NOT flag
 *   <li>Plain {@code SELECT COUNT(*) … WHERE …} — developer could switch to EXISTS → INFO is appropriate
 * </ul>
 */
class CountInsteadOfExistsDetectorHibernateFalsePositiveTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final CountInsteadOfExistsDetector detector = new CountInsteadOfExistsDetector();

  // ── SQL already contains a boolean comparison (count > ?) ────────────────
  // These queries express existence intent AT the SQL level.
  // The developer either wrote existsBy* (Hibernate) or an explicit comparison.
  // Suggesting "use EXISTS" is non-actionable noise.

  @Nested
  @DisplayName("COUNT with comparison operator — already boolean, must NOT flag")
  class CountWithComparisonOperator {

    @Test
    @DisplayName("Hibernate existsBy*: select count(col) > ? from ... where ...")
    void hibernateExistsByWithBindParam() {
      String sql =
          "select count(ul1_0.user_id) > ? from user_location ul1_0 where ul1_0.user_id = ?";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues)
          .as("count(col) > ? is Hibernate's existsBy* translation — developer cannot improve this")
          .isEmpty();
    }

    @Test
    @DisplayName("Hibernate existsBy* variant: select count(*) > ? from ... where ...")
    void hibernateExistsByCountStarWithBindParam() {
      String sql = "select count(*) > ? from users u1_0 where u1_0.email = ?";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues)
          .as("count(*) > ? is also a Hibernate existsBy* pattern")
          .isEmpty();
    }

    @Test
    @DisplayName("Explicit comparison: SELECT count(e.id) > 0 FROM ... WHERE ...")
    void explicitCountGreaterThanZero() {
      String sql = "SELECT count(e.id) > 0 FROM employee e WHERE e.department_id = 5";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues)
          .as("count > 0 already expresses boolean intent in SQL — nothing to improve")
          .isEmpty();
    }

    @Test
    @DisplayName("Comparison with >= 1: select count(*) >= 1 from ... where ...")
    void countGreaterThanOrEqualOne() {
      String sql = "select count(*) >= 1 from orders o where o.customer_id = ?";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues)
          .as("count(*) >= 1 is equivalent to existence check")
          .isEmpty();
    }
  }

  // ── Plain COUNT queries (no comparison in SQL) ───────────────────────────
  // The detector CANNOT know if the count value is used for existence or display.
  // Flagging with INFO + "ignore if count needed" is the right trade-off.

  @Nested
  @DisplayName("Plain COUNT + WHERE — ambiguous intent, INFO flag is appropriate")
  class PlainCountWithWhere {

    @Test
    @DisplayName("SELECT COUNT(*) FROM ... WHERE ... → should flag as INFO")
    void plainCountStarWithWhereShouldFlag() {
      String sql = "SELECT COUNT(*) FROM users WHERE active = true";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.COUNT_INSTEAD_OF_EXISTS);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
      assertThat(issues.get(0).suggestion()).contains("Ignore if the actual count value is needed");
    }

    @Test
    @DisplayName("SELECT COUNT(id) FROM ... WHERE ... → should flag as INFO")
    void plainCountColumnWithWhereShouldFlag() {
      String sql = "SELECT COUNT(id) FROM orders WHERE user_id = 42";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }
  }
}
