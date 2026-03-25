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

class TooManyJoinsDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private final TooManyJoinsDetector detector = new TooManyJoinsDetector();

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  // ── Positive: Issues detected ───────────────────────────────────────

  @Nested
  @DisplayName("Detects too many JOINs")
  class PositiveCases {

    @Test
    @DisplayName("Detects query with 6 JOINs (default threshold 5)")
    void detectsSixJoins() {
      String sql =
          "SELECT * FROM a "
              + "JOIN b ON a.id = b.a_id "
              + "JOIN c ON b.id = c.b_id "
              + "JOIN d ON c.id = d.c_id "
              + "JOIN e ON d.id = e.d_id "
              + "JOIN f ON e.id = f.e_id "
              + "JOIN g ON f.id = g.f_id";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.TOO_MANY_JOINS);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    }

    @Test
    @DisplayName("Detail includes join count")
    void detailIncludesCount() {
      String sql =
          "SELECT * FROM a "
              + "JOIN b ON a.id = b.a_id "
              + "JOIN c ON b.id = c.b_id "
              + "JOIN d ON c.id = d.c_id "
              + "JOIN e ON d.id = e.d_id "
              + "JOIN f ON e.id = f.e_id "
              + "JOIN g ON f.id = g.f_id";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues.get(0).detail()).contains("6 JOINs");
    }

    @Test
    @DisplayName("Suggestion mentions splitting queries")
    void suggestionContent() {
      String sql =
          "SELECT * FROM a "
              + "JOIN b ON a.id = b.a_id "
              + "JOIN c ON b.id = c.b_id "
              + "JOIN d ON c.id = d.c_id "
              + "JOIN e ON d.id = e.d_id "
              + "JOIN f ON e.id = f.e_id "
              + "JOIN g ON f.id = g.f_id";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues.get(0).suggestion()).contains("splitting");
    }

    @Test
    @DisplayName("Counts LEFT JOIN, RIGHT JOIN, INNER JOIN etc.")
    void countsJoinVariants() {
      String sql =
          "SELECT * FROM a "
              + "LEFT JOIN b ON a.id = b.a_id "
              + "RIGHT JOIN c ON b.id = c.b_id "
              + "INNER JOIN d ON c.id = d.c_id "
              + "FULL OUTER JOIN e ON d.id = e.d_id "
              + "CROSS JOIN f "
              + "JOIN g ON f.id = g.f_id";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }
  }

  // ── Negative: No issues ─────────────────────────────────────────────

  @Nested
  @DisplayName("No issue cases")
  class NegativeCases {

    @Test
    @DisplayName("No issue for query with 5 JOINs (at threshold)")
    void noIssueAtThreshold() {
      String sql =
          "SELECT * FROM a "
              + "JOIN b ON a.id = b.a_id "
              + "JOIN c ON b.id = c.b_id "
              + "JOIN d ON c.id = d.c_id "
              + "JOIN e ON d.id = e.d_id "
              + "JOIN f ON e.id = f.e_id";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for simple query with 1 JOIN")
    void noIssueForOneJoin() {
      String sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for query without JOINs")
    void noIssueWithoutJoins() {
      String sql = "SELECT * FROM users WHERE id = 1";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Empty query list")
    void emptyQueryList() {
      List<Issue> issues = detector.evaluate(List.of(), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // ── Threshold Configuration ─────────────────────────────────────────

  @Nested
  @DisplayName("Custom threshold")
  class ThresholdCases {

    @Test
    @DisplayName("Custom threshold of 2 detects 3 JOINs")
    void customThreshold() {
      TooManyJoinsDetector customDetector = new TooManyJoinsDetector(2);
      String sql =
          "SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id JOIN d ON c.id = d.c_id";
      List<Issue> issues = customDetector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Custom threshold of 2 does not detect 2 JOINs")
    void customThresholdAtLimit() {
      TooManyJoinsDetector customDetector = new TooManyJoinsDetector(2);
      String sql = "SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id";
      List<Issue> issues = customDetector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }

  // ── Edge Cases ──────────────────────────────────────────────────────

  @Nested
  @DisplayName("Edge Cases")
  class EdgeCases {

    @Test
    @DisplayName("Deduplicates same normalized query")
    void deduplicates() {
      String sql1 =
          "SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id "
              + "JOIN d ON c.id = d.c_id JOIN e ON d.id = e.d_id JOIN f ON e.id = f.e_id "
              + "JOIN g ON f.id = g.f_id WHERE a.status = 'active'";
      String sql2 =
          "SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c ON b.id = c.b_id "
              + "JOIN d ON c.id = d.c_id JOIN e ON d.id = e.d_id JOIN f ON e.id = f.e_id "
              + "JOIN g ON f.id = g.f_id WHERE a.status = 'inactive'";
      List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("JOINs in subqueries are excluded")
    void subqueryJoinsExcluded() {
      String sql =
          "SELECT * FROM a JOIN b ON a.id = b.a_id "
              + "WHERE a.id IN (SELECT x.id FROM x JOIN y ON x.id = y.x_id "
              + "JOIN z ON y.id = z.y_id JOIN w ON z.id = w.z_id "
              + "JOIN v ON w.id = v.w_id JOIN u ON v.id = u.v_id)";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // Only 1 JOIN in outer query, so no issue
      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for 5 LEFT JOINs at threshold (Hibernate entity graph pattern)")
    void noIssueForHibernateEntityGraphAtThreshold() {
      // Hibernate entity graphs commonly produce many LEFT JOINs for eager fetching
      String sql =
          "SELECT * FROM user_entity u "
              + "LEFT JOIN user_roles ur ON u.id = ur.user_id "
              + "LEFT JOIN roles r ON ur.role_id = r.id "
              + "LEFT JOIN user_addresses ua ON u.id = ua.user_id "
              + "LEFT JOIN addresses a ON ua.address_id = a.id "
              + "LEFT JOIN user_preferences up ON u.id = up.user_id";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // 5 JOINs at the threshold (not above), so no issue
      assertThat(issues).isEmpty();
    }
  }
}
