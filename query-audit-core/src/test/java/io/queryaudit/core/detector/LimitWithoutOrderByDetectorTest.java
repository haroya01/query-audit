package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LimitWithoutOrderByDetectorTest {

  private final LimitWithoutOrderByDetector detector = new LimitWithoutOrderByDetector();
  private final IndexMetadata emptyIndex = new IndexMetadata(Map.of());

  private static QueryRecord q(String sql) {
    return new QueryRecord(sql, 1000L, System.currentTimeMillis(), null);
  }

  @Test
  void detectsLimitWithoutOrderBy() {
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id, name FROM users WHERE status = 'active' LIMIT 10")), emptyIndex);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.LIMIT_WITHOUT_ORDER_BY);
  }

  @Test
  void noIssueWhenOrderByPresent() {
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id, name FROM users ORDER BY created_at LIMIT 10")), emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithoutLimit() {
    List<Issue> issues =
        detector.evaluate(List.of(q("SELECT id, name FROM users WHERE id = 1")), emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void skipsForUpdateQueries() {
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id FROM users WHERE id = 1 LIMIT 1 FOR UPDATE")), emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void skipsAggregateQueries() {
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT COUNT(*) FROM users WHERE status = 'active' LIMIT 1")), emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void ignoresLimitInSubquery() {
    List<Issue> issues =
        detector.evaluate(
            List.of(
                q(
                    "SELECT id FROM users WHERE id IN (SELECT user_id FROM orders LIMIT 5) ORDER BY id LIMIT 10")),
            emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void skipsNonSelectQueries() {
    List<Issue> issues =
        detector.evaluate(List.of(q("DELETE FROM users WHERE id = 1 LIMIT 1")), emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void deduplicatesByNormalizedSql() {
    List<Issue> issues =
        detector.evaluate(
            List.of(
                q("SELECT id FROM users WHERE status = 'active' LIMIT 10"),
                q("SELECT id FROM users WHERE status = 'inactive' LIMIT 10")),
            emptyIndex);
    // Both normalize to the same pattern
    assertThat(issues).hasSize(1);
  }

  // ── false positive fix: LIMIT 1 for existence checks ──────────────

  @Test
  void noIssueForLimit1ExistenceCheck() {
    // LIMIT 1 without ORDER BY is a common existence check pattern
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id FROM users WHERE email = 'test@test.com' LIMIT 1")), emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForLimit1WithWhereClause() {
    List<Issue> issues =
        detector.evaluate(List.of(q("SELECT 1 FROM users WHERE username = ? LIMIT 1")), emptyIndex);
    assertThat(issues).isEmpty();
  }

  // ── #34: JPA existsBy* false positive fix ──────────────────────

  @Test
  void noIssueForParameterizedLimitInExistenceCheck() {
    // JPA existsBy* generates: SELECT id FROM users WHERE nickname=? AND discriminator=? LIMIT ?
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id FROM users WHERE nickname=? AND discriminator=? LIMIT ?")),
            emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForSelect1WithParameterizedLimit() {
    List<Issue> issues =
        detector.evaluate(List.of(q("SELECT 1 FROM users WHERE username = ? LIMIT ?")), emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForFetchFirstParameterized() {
    // Hibernate generates "fetch first ? rows only" instead of "LIMIT ?"
    List<Issue> issues =
        detector.evaluate(
            List.of(
                q("select m1_0.id from members m1_0 where m1_0.email=? fetch first ? rows only")),
            emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForExistsByMethodInStackTrace() {
    // Even with a non-trivial LIMIT, existsBy* in the stack trace should skip
    String stackTrace =
        "jdk.proxy3.$Proxy122.existsByEmail:-1\n" + "com.example.UserService.checkExists:42";
    QueryRecord record =
        new QueryRecord(
            "select m1_0.id from members m1_0 where m1_0.email=? fetch first ? rows only",
            1000L,
            System.currentTimeMillis(),
            stackTrace);
    List<Issue> issues = detector.evaluate(List.of(record), emptyIndex);
    assertThat(issues).isEmpty();
  }

  @Test
  void stillDetectsWhenStackTraceHasNoExistsBy() {
    // Non-existsBy call with LIMIT but no ORDER BY should still be flagged
    String stackTrace =
        "jdk.proxy3.$Proxy122.findByStatus:-1\n" + "com.example.UserService.getUsers:50";
    QueryRecord record =
        new QueryRecord(
            "SELECT id, name FROM users WHERE status = ? LIMIT 10",
            1000L,
            System.currentTimeMillis(),
            stackTrace);
    List<Issue> issues = detector.evaluate(List.of(record), emptyIndex);
    assertThat(issues).hasSize(1);
  }

  @Test
  void stillDetectsLimitGreaterThan1WithoutOrderBy() {
    // LIMIT > 1 without ORDER BY is still non-deterministic
    List<Issue> issues =
        detector.evaluate(
            List.of(q("SELECT id, name FROM users WHERE status = 'active' LIMIT 10")), emptyIndex);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.LIMIT_WITHOUT_ORDER_BY);
  }
}
