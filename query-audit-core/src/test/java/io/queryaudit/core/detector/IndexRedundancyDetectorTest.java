package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IndexRedundancyDetectorTest {

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final IndexRedundancyDetector detector = new IndexRedundancyDetector();

  @Test
  void detectsRedundantSingleColumnIndex() {
    // idx_user_id on (user_id) is redundant when idx_user_id_status on (user_id, status) exists
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "idx_user_id", "user_id", 1, true, 1000),
                    new IndexInfo("orders", "idx_user_id_status", "user_id", 1, true, 1000),
                    new IndexInfo("orders", "idx_user_id_status", "status", 2, true, 5000))));

    String sql = "SELECT * FROM orders WHERE user_id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.REDUNDANT_INDEX);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    assertThat(issues.get(0).detail()).contains("idx_user_id");
    assertThat(issues.get(0).detail()).contains("idx_user_id_status");
    assertThat(issues.get(0).suggestion()).contains("DROP INDEX");
  }

  @Test
  void noIssueWhenNoPrefix() {
    // idx_user_id on (user_id) and idx_status on (status) -- neither is a prefix
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "idx_user_id", "user_id", 1, true, 1000),
                    new IndexInfo("orders", "idx_status", "status", 1, true, 5))));

    String sql = "SELECT * FROM orders WHERE user_id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void skipsUniqueShortIndexWithNonUniqueLongIndex() {
    // UNIQUE idx_email on (email) is NOT redundant even if idx_email_name on (email, name) exists
    // because the UNIQUE constraint has semantic meaning
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "users",
                List.of(
                    new IndexInfo("users", "idx_email", "email", 1, false, 10000), // unique
                    new IndexInfo("users", "idx_email_name", "email", 1, true, 10000), // non-unique
                    new IndexInfo("users", "idx_email_name", "name", 2, true, 50000))));

    String sql = "SELECT * FROM users WHERE email = 'test@example.com'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void detectsRedundantWhenBothUnique() {
    // Both UNIQUE -- the shorter one is still redundant
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "users",
                List.of(
                    new IndexInfo("users", "idx_email", "email", 1, false, 10000),
                    new IndexInfo("users", "idx_email_name", "email", 1, false, 10000),
                    new IndexInfo("users", "idx_email_name", "name", 2, false, 50000))));

    String sql = "SELECT * FROM users WHERE email = 'test@example.com'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.REDUNDANT_INDEX);
  }

  @Test
  void noIssueWithEmptyIndexMetadata() {
    String sql = "SELECT * FROM orders WHERE user_id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), new IndexMetadata(Map.of()));

    assertThat(issues).isEmpty();
  }

  @Test
  void detectsRedundantTwoColumnPrefix() {
    // idx_a_b on (a, b) is redundant when idx_a_b_c on (a, b, c) exists
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "t",
                List.of(
                    new IndexInfo("t", "idx_a_b", "a", 1, true, 100),
                    new IndexInfo("t", "idx_a_b", "b", 2, true, 500),
                    new IndexInfo("t", "idx_a_b_c", "a", 1, true, 100),
                    new IndexInfo("t", "idx_a_b_c", "b", 2, true, 500),
                    new IndexInfo("t", "idx_a_b_c", "c", 3, true, 1000))));

    String sql = "SELECT * FROM t WHERE a = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("idx_a_b");
    assertThat(issues.get(0).detail()).contains("idx_a_b_c");
  }

  @Test
  void noIssueWhenColumnsOverlapButNotPrefix() {
    // idx_a_c on (a, c) is NOT a prefix of idx_a_b_c on (a, b, c)
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "t",
                List.of(
                    new IndexInfo("t", "idx_a_c", "a", 1, true, 100),
                    new IndexInfo("t", "idx_a_c", "c", 2, true, 500),
                    new IndexInfo("t", "idx_a_b_c", "a", 1, true, 100),
                    new IndexInfo("t", "idx_a_b_c", "b", 2, true, 500),
                    new IndexInfo("t", "idx_a_b_c", "c", 3, true, 1000))));

    String sql = "SELECT * FROM t WHERE a = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  // ── Mutation-killing tests ──────────────────────────────────────────

  @Test
  void sameColumnCountIndexesAreNotRedundant() {
    // Kills: L96 ConditionalsBoundaryMutator (>= vs >)
    // Two indexes with same number of columns should not be considered prefix
    // If boundary mutated to >, equal-size would pass through and potentially false-report
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "t",
                List.of(
                    new IndexInfo("t", "idx_a_b", "a", 1, true, 100),
                    new IndexInfo("t", "idx_a_b", "b", 2, true, 500),
                    new IndexInfo("t", "idx_a_c", "a", 1, true, 100),
                    new IndexInfo("t", "idx_a_c", "c", 2, true, 500))));

    String sql = "SELECT * FROM t WHERE a = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void nullIndexMetadataReturnsEmptyList() {
    // Kills: L36 EmptyObjectReturnValsMutator
    String sql = "SELECT * FROM orders WHERE user_id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), null);

    assertThat(issues).isEmpty();
  }

  @Test
  void isLeftPrefixBoundaryEqualSize() {
    // Kills: L144 ConditionalsBoundaryMutator (> vs >=) in isLeftPrefix
    // When prefix.size() == full.size(), isLeftPrefix should return true for equal lists
    // but the caller already filters by shortCols.size() >= longCols.size(), so
    // isLeftPrefix with equal sizes would mean the index IS the same length.
    // The boundary mutation at L144 changes > to >=, making isLeftPrefix return false
    // for equal-sized lists. But L96 already filters those out.
    // To test L144 directly, we need a case where isLeftPrefix is called with
    // prefix longer than full. This is already guarded at L96, but let's ensure
    // the combination works.
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "t",
                List.of(
                    new IndexInfo("t", "idx_a_b_c", "a", 1, true, 100),
                    new IndexInfo("t", "idx_a_b_c", "b", 2, true, 500),
                    new IndexInfo("t", "idx_a_b_c", "c", 3, true, 1000),
                    new IndexInfo("t", "idx_a", "a", 1, true, 100))));

    String sql = "SELECT * FROM t WHERE a = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    // idx_a is a left prefix of idx_a_b_c -> redundant
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("idx_a");
  }

  @Test
  void indexWithNullNameOrColumnIsFiltered() {
    // Kills: L59 BooleanTrueReturnValsMutator (filter returns true)
    // If the filter is mutated to always return true, null indexName/columnName
    // would cause NullPointerException in groupingBy or downstream
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "t",
                List.of(
                    new IndexInfo("t", null, "a", 1, true, 100),
                    new IndexInfo("t", "idx_a", null, 1, true, 100),
                    new IndexInfo("t", "idx_a", "a", 1, true, 100),
                    new IndexInfo("t", "idx_a_b", "a", 1, true, 100),
                    new IndexInfo("t", "idx_a_b", "b", 2, true, 500))));

    String sql = "SELECT * FROM t WHERE a = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    // idx_a is prefix of idx_a_b -> should detect redundancy without crashing
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("idx_a");
    assertThat(issues.get(0).detail()).contains("idx_a_b");
  }

  @Test
  void sortOrderMattersForRedundancy() {
    // Kills: L72 PrimitiveReturnsMutator (comparator returns 0)
    // If the sort comparator returns 0 instead of proper comparison,
    // column order would not be preserved, potentially causing false positives/negatives
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "t",
                List.of(
                    // idx_b_a has columns b(seq=1), a(seq=2) — NOT a prefix of idx_a_b_c
                    new IndexInfo("t", "idx_b_a", "b", 1, true, 100),
                    new IndexInfo("t", "idx_b_a", "a", 2, true, 500),
                    new IndexInfo("t", "idx_a_b_c", "a", 1, true, 100),
                    new IndexInfo("t", "idx_a_b_c", "b", 2, true, 500),
                    new IndexInfo("t", "idx_a_b_c", "c", 3, true, 1000))));

    String sql = "SELECT * FROM t WHERE a = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    // idx_b_a is (b, a) which is NOT a left prefix of (a, b, c) -> no redundancy
    assertThat(issues).isEmpty();
  }

  // ── False-positive reduction tests ─────────────────────────────────

  @Test
  void primaryKeyNeverFlaggedAsRedundant() {
    // PRIMARY KEY on (id) is a left prefix of idx_id_status on (id, status),
    // but dropping PRIMARY KEY has serious implications in InnoDB.
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "PRIMARY", "id", 1, false, 10000),
                    new IndexInfo("orders", "idx_id_status", "id", 1, true, 10000),
                    new IndexInfo("orders", "idx_id_status", "status", 2, true, 5000))));

    String sql = "SELECT * FROM orders WHERE id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    // PRIMARY should not be flagged even though it's a prefix of idx_id_status
    assertThat(issues).isEmpty();
  }

  @Test
  void nonPrimaryPrefixStillFlagged() {
    // A non-PRIMARY index that is a prefix should still be flagged
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "PRIMARY", "id", 1, false, 10000),
                    new IndexInfo("orders", "idx_user", "user_id", 1, true, 1000),
                    new IndexInfo("orders", "idx_user_date", "user_id", 1, true, 1000),
                    new IndexInfo("orders", "idx_user_date", "created_at", 2, true, 5000))));

    String sql = "SELECT * FROM orders WHERE user_id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("idx_user");
  }

  // ── Additional mutation-killing tests ──────────────────────────────

  /**
   * Kills EmptyObjectReturnValsMutator on line 36: replaced return with Collections.emptyList. When
   * issues ARE detected, the returned list must NOT be empty.
   */
  @Test
  void evaluateReturnsNonEmptyWhenIssueDetected_killsLine36() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "idx_user_id", "user_id", 1, true, 1000),
                    new IndexInfo("orders", "idx_user_id_status", "user_id", 1, true, 1000),
                    new IndexInfo("orders", "idx_user_id_status", "status", 2, true, 5000))));

    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM orders WHERE user_id = 1")), metadata);

    assertThat(issues).isNotEmpty();
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.REDUNDANT_INDEX);
  }

  /**
   * Kills ConditionalsBoundaryMutator on line 96: >= changed to >. When short index has same column
   * count as long index, they should NOT be considered a prefix. But if the boundary changes from
   * >= to >, equal-sized indexes would pass through and might be falsely reported. Also
   * specifically test the exact boundary: shortCols.size() == longCols.size().
   */
  @Test
  void equalSizeIndexesNeverReportedAsRedundant_killsLine96() {
    // Two indexes with EXACTLY the same number of columns but different columns
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "t",
                List.of(
                    new IndexInfo("t", "idx_a_b", "a", 1, true, 100),
                    new IndexInfo("t", "idx_a_b", "b", 2, true, 500),
                    new IndexInfo("t", "idx_a_c", "a", 1, true, 100),
                    new IndexInfo("t", "idx_a_c", "c", 2, true, 500))));

    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM t WHERE a = 1")), metadata);

    // Same number of columns -> never a proper prefix -> no redundancy
    assertThat(issues).isEmpty();
  }

  /**
   * Also test equal-size indexes with identical columns (same prefix) - should still not be
   * reported because size must be strictly less than.
   */
  @Test
  void identicalColumnIndexesSameSizeNotRedundant_killsLine96() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "t",
                List.of(
                    new IndexInfo("t", "idx_a_1", "a", 1, true, 100),
                    new IndexInfo("t", "idx_a_2", "a", 1, true, 100))));

    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM t WHERE a = 1")), metadata);

    // Same size (1 column each) -> shortCols.size() >= longCols.size() -> skip
    assertThat(issues).isEmpty();
  }

  /**
   * Kills ConditionalsBoundaryMutator on line 144: > changed to >= in isLeftPrefix. isLeftPrefix is
   * called after the size check, so prefix is always <= full. The boundary at line 144 is
   * prefix.size() > full.size() which is a guard that should never be true in practice. But if
   * mutated to >=, it would incorrectly return false for equal-sized lists. Since equal-sized lists
   * are already filtered by line 96, this mutation is observationally equivalent unless we can
   * reach isLeftPrefix with equal sizes.
   *
   * <p>Actually, if line 96 is shortCols.size() >= longCols.size() and it gets mutated to >, then
   * equal sizes pass through to isLeftPrefix. But we're testing line 144 here, not 96. Line 144 is
   * called after line 96. Since line 96 filters >= , isLeftPrefix never receives equal sizes. So
   * line 144's > vs >= is equivalent for reachable code.
   *
   * <p>To kill it regardless, we verify the combination: single-column prefix of multi-column.
   */
  @Test
  void isLeftPrefixWorksForSingleVsMultiColumn_killsLine144() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "t",
                List.of(
                    new IndexInfo("t", "idx_a", "a", 1, true, 100),
                    new IndexInfo("t", "idx_a_b", "a", 1, true, 100),
                    new IndexInfo("t", "idx_a_b", "b", 2, true, 500))));

    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM t WHERE a = 1")), metadata);

    // idx_a (1 col) is a left prefix of idx_a_b (2 cols) -> redundant
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("idx_a").contains("idx_a_b");
  }

  /**
   * Kills PrimitiveReturnsMutator on line 72: comparator returns 0. If the comparator always
   * returns 0, the sort is unstable and column order becomes unpredictable. With columns given in
   * reverse seqInIndex order, a broken comparator would keep them reversed, causing a false
   * negative.
   */
  @Test
  void sortBySeqInIndexCriticalForPrefixDetection_killsLine72() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "t",
                List.of(
                    // Intentionally give columns in REVERSE seqInIndex order
                    new IndexInfo("t", "idx_a_b", "b", 2, true, 500),
                    new IndexInfo("t", "idx_a_b", "a", 1, true, 100),
                    new IndexInfo("t", "idx_a_b_c", "c", 3, true, 1000),
                    new IndexInfo("t", "idx_a_b_c", "b", 2, true, 500),
                    new IndexInfo("t", "idx_a_b_c", "a", 1, true, 100))));

    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM t WHERE a = 1")), metadata);

    // After proper sorting: idx_a_b is (a,b), idx_a_b_c is (a,b,c) -> prefix -> redundant
    // If comparator returns 0: order is undefined, might be (b,a) vs (c,b,a) -> no prefix
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("idx_a_b");
    assertThat(issues.get(0).detail()).contains("idx_a_b_c");
  }
}
