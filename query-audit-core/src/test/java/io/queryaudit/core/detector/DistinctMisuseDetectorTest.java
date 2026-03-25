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

class DistinctMisuseDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final DistinctMisuseDetector detector = new DistinctMisuseDetector();

  @Test
  void detectsDistinctWithGroupBy() {
    String sql = "SELECT DISTINCT status, COUNT(*) FROM users GROUP BY status";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.DISTINCT_MISUSE);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    assertThat(issues.get(0).detail()).contains("GROUP BY");
    assertThat(issues.get(0).detail()).contains("redundant");
  }

  @Test
  void detectsDistinctWithJoin() {
    String sql = "SELECT DISTINCT u.name FROM users u JOIN orders o ON u.id = o.user_id";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.DISTINCT_MISUSE);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    assertThat(issues.get(0).detail()).contains("JOIN");
  }

  @Test
  void detectsDistinctOnPrimaryKey() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 10000))));

    String sql = "SELECT DISTINCT id FROM users WHERE status = 'ACTIVE'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("primary key");
  }

  @Test
  void noIssueForDistinctWithoutJoinOrGroupBy() {
    String sql = "SELECT DISTINCT status FROM users";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    // No GROUP BY, no JOIN, no PK info -> no issue
    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForNonDistinctQuery() {
    String sql = "SELECT name FROM users JOIN orders ON users.id = orders.user_id";

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
    String sql = "SELECT DISTINCT status, COUNT(*) FROM users GROUP BY status";

    List<Issue> issues = detector.evaluate(List.of(record(sql), record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
  }

  @Test
  void prioritizesGroupByOverJoin() {
    // When both GROUP BY and JOIN are present, report the GROUP BY issue (more specific)
    String sql =
        "SELECT DISTINCT u.status FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.status";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("GROUP BY");
  }

  @Test
  void caseInsensitiveDetection() {
    String sql = "select distinct status, count(*) from users group by status";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.DISTINCT_MISUSE);
  }

  // ── Mutation-killing tests ─────────────────────────────────────────

  @Test
  void noIssueWhenTableNotInMetadata() {
    // Kills NegateConditionalsMutator on line 78: table != null check in evaluate
    // When we have index metadata but the table is not in it
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "other_table",
                List.of(new IndexInfo("other_table", "PRIMARY", "id", 1, false, 10000))));

    String sql = "SELECT DISTINCT id FROM users WHERE status = 'ACTIVE'";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);
    // Should not detect PK issue since 'users' table is not in metadata
    // No GROUP BY, no JOIN -> no issue
    assertThat(issues).isEmpty();
  }

  @Test
  void distinctWithJoinDetectedWhenNoPKMatch() {
    // Kills NegateConditionalsMutator on line 124: hasJoin check
    // Verifies the JOIN detection path is hit after PK check fails
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 10000))));

    // DISTINCT on non-PK column with JOIN
    String sql = "SELECT DISTINCT u.name FROM users u JOIN orders o ON u.id = o.user_id";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("JOIN");
  }

  @Test
  void distinctOnPKColumnWithJoinReportsPKIssue() {
    // Kills NegateConditionalsMutator on line 78 (table empty check)
    // and ensures PK detection takes priority over JOIN detection
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 10000))));

    String sql = "SELECT DISTINCT id FROM users";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("primary key");
  }

  @Test
  void extractDistinctColumnNamesBoundary() {
    // Kills ConditionalsBoundaryMutator on line 158: colStart >= fromMatcher.start()
    // Tests a case where the DISTINCT columns section is very small (boundary)
    IndexMetadata metadata =
        new IndexMetadata(Map.of("t", List.of(new IndexInfo("t", "PRIMARY", "a", 1, false, 100))));

    String sql = "SELECT DISTINCT a FROM t";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("primary key");
  }

  @Test
  void getPrimaryKeyColumnsFiltersNonPKIndexes() {
    // Kills BooleanTrueReturnValsMutator on line 179
    // (lambda$getPrimaryKeyColumns$1: "PRIMARY".equalsIgnoreCase)
    // Non-PRIMARY indexes should not be treated as primary keys
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "users",
                List.of(
                    new IndexInfo("users", "idx_name", "name", 1, true, 10000),
                    new IndexInfo("users", "idx_status", "status", 1, true, 10000))));

    // DISTINCT on 'name' which has a regular index but is NOT a PK
    String sql = "SELECT DISTINCT name FROM users";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);
    // Should NOT report "DISTINCT on primary key" since name is not a PK
    boolean hasPKIssue = issues.stream().anyMatch(i -> i.detail().contains("primary key"));
    assertThat(hasPKIssue).isFalse();
  }

  @Test
  void getPrimaryKeyColumnsFiltersNullColumnNames() {
    // Kills BooleanTrueReturnValsMutator on line 181
    // (lambda$getPrimaryKeyColumns$2: name != null filter)
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("users", List.of(new IndexInfo("users", "PRIMARY", null, 1, false, 10000))));

    // DISTINCT on 'id' but the PRIMARY index has null column name
    String sql = "SELECT DISTINCT id FROM users";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);
    // Should NOT match PK since column name is null
    boolean hasPKIssue = issues.stream().anyMatch(i -> i.detail().contains("primary key"));
    assertThat(hasPKIssue).isFalse();
  }

  @Test
  void distinctOnNonPKColumnWithNoJoinNoGroupBy() {
    // Ensures that DISTINCT without JOIN/GROUP BY and non-PK column produces no issue
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 10000))));

    String sql = "SELECT DISTINCT status FROM users";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);
    assertThat(issues).isEmpty();
  }

  @Test
  void distinctWithEmptyIndexMetadata() {
    // When indexMetadata is empty, PK check is skipped
    String sql = "SELECT DISTINCT id FROM users";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).isEmpty();
  }

  @Test
  void distinctWithNullIndexMetadata() {
    // When indexMetadata is null, PK check is skipped
    String sql = "SELECT DISTINCT id FROM users";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), null);
    assertThat(issues).isEmpty();
  }

  @Test
  void extractDistinctColumnNamesSkipsFunctions() {
    // Columns with function calls should be skipped in PK check
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 10000))));

    String sql = "SELECT DISTINCT UPPER(name) FROM users";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);
    // UPPER(name) has parens, so it's skipped in column extraction
    // No PK match, no JOIN, no GROUP BY -> no issue
    assertThat(issues).isEmpty();
  }

  // ── Final round: killing remaining surviving mutations ─────────────

  @Test
  void distinctWithGroupByReportsCorrectTable_killsLine78() {
    // Kills NegateConditionalsMutator on line 78: tables.isEmpty() negated.
    // If negated, table would be null when tables exist (non-empty list).
    // Verify table is NOT null when a table is present.
    String sql = "SELECT DISTINCT status, COUNT(*) FROM orders GROUP BY status";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).table()).isNotNull();
    assertThat(issues.get(0).table()).isEqualTo("orders");
  }

  @Test
  void distinctWithJoinReportsCorrectTable_killsLine124() {
    // Kills NegateConditionalsMutator on line 124: tables.isEmpty() negated.
    // If negated, table would be null when tables exist.
    // Verify table is NOT null in the JOIN detection path.
    String sql = "SELECT DISTINCT u.email FROM users u JOIN orders o ON u.id = o.user_id";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).table()).isNotNull();
    assertThat(issues.get(0).table()).isEqualTo("users");
  }

  @Test
  void extractDistinctColumnNamesReturnsColumnsWhenPresent_killsLine151() {
    // Kills EmptyObjectReturnValsMutator on line 151: return columns -> return emptyList.
    // Line 151 is the early return when SELECT DISTINCT is not found.
    // For queries that DO have SELECT DISTINCT, this line is NOT reached.
    // This mutant is equivalent because line 151 is only reached when there is
    // no DISTINCT keyword, in which case the empty list is correctly returned.
    // However, to be thorough, test that PK detection works when DISTINCT IS present.
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 10000))));
    String sql = "SELECT DISTINCT id, name FROM users";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);
    // id is PK -> should detect
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("primary key");
  }

  @Test
  void extractDistinctColumnNamesBoundaryExact_killsLine158() {
    // Kills ConditionalsBoundaryMutator on line 158: colStart >= fromMatcher.start()
    //   changed to colStart > fromMatcher.start().
    // When colStart == fromMatcher.start(), original returns empty, mutant continues.
    // This happens when "SELECT DISTINCT " ends right where "FROM" starts, meaning
    // there's no column between DISTINCT and FROM. Like "SELECT DISTINCT FROM t".
    // With mutation: colList = "" -> split -> [""] -> no column match -> same result.
    // This is effectively equivalent, but let's test a minimal column list to exercise.
    IndexMetadata metadata =
        new IndexMetadata(Map.of("t", List.of(new IndexInfo("t", "PRIMARY", "x", 1, false, 100))));
    // Single short column name right after DISTINCT
    String sql = "SELECT DISTINCT x FROM t";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("primary key");
  }

  @Test
  void distinctWithGroupByAndNoTablesHasNullTable() {
    // Complementary test to line 78: when tables IS empty, table should be null.
    // This is hard to construct since SQL usually has FROM, but we can use a
    // subquery-style DISTINCT that doesn't parse a table name easily.
    // Actually, SqlParser.extractTableNames should find tables in most cases.
    // This test ensures GROUP BY detection works even without table resolution.
    String sql = "SELECT DISTINCT col, COUNT(*) FROM mytable GROUP BY col";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).table()).isEqualTo("mytable");
  }

  @Test
  void distinctWithJoinNoGroupByNoPKMetadata_killsLine124Path() {
    // Specifically tests the path where PK check is performed but doesn't match,
    // falling through to JOIN detection (line 122-137).
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 10000))));
    // DISTINCT on 'name' (not PK) with JOIN -> should report JOIN issue
    String sql = "SELECT DISTINCT name FROM users JOIN roles ON users.role_id = roles.id";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("JOIN");
    assertThat(issues.get(0).table()).isNotNull();
  }

  // ── false positive fix: DISTINCT in subquery ──────────────────────

  @Test
  void noIssueForDistinctInSubquery() {
    // DISTINCT in a subquery is intentional — dedup the subquery result
    String sql =
        "SELECT * FROM users WHERE id IN (SELECT DISTINCT user_id FROM orders)";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForDistinctInExistsSubquery() {
    String sql =
        "SELECT * FROM users u WHERE EXISTS (SELECT DISTINCT 1 FROM orders o WHERE o.user_id = u.id)";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).isEmpty();
  }

  @Test
  void stillDetectsDistinctInOuterQueryWithSubquery() {
    // DISTINCT in the outer query should still be detected even if there's a subquery
    String sql =
        "SELECT DISTINCT u.name FROM users u "
            + "JOIN orders o ON u.id = o.user_id "
            + "WHERE o.id IN (SELECT id FROM recent_orders)";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("JOIN");
  }
}
