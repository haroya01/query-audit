package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CorrelatedSubqueryDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final CorrelatedSubqueryDetector detector = new CorrelatedSubqueryDetector();

  @Test
  void detectsCorrelatedSubqueryInSelectClause() {
    String sql =
        "SELECT u.id, u.name, "
            + "(SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count "
            + "FROM users u";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.CORRELATED_SUBQUERY);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    assertThat(issues.get(0).detail()).contains("Correlated subquery");
    assertThat(issues.get(0).suggestion()).contains("LEFT JOIN");
  }

  @Test
  void detectsCorrelatedSubqueryReferencingTableName() {
    String sql =
        "SELECT u.id, "
            + "(SELECT MAX(o.amount) FROM orders o WHERE o.user_id = users.id) AS max_amount "
            + "FROM users";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.CORRELATED_SUBQUERY);
  }

  @Test
  void noIssueForUncorrelatedSubqueryInSelect() {
    String sql =
        "SELECT u.id, " + "(SELECT COUNT(*) FROM orders) AS total_orders " + "FROM users u";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForSimpleSelectWithoutSubquery() {
    String sql = "SELECT u.id, u.name FROM users u WHERE u.active = true";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForSubqueryInWhereClause() {
    // Subquery in WHERE, not in SELECT -- this detector only targets SELECT clause
    String sql =
        "SELECT u.id FROM users u "
            + "WHERE u.id IN (SELECT o.user_id FROM orders o WHERE o.amount > 100)";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void deduplicatesSameNormalizedQuery() {
    String sql =
        "SELECT u.id, "
            + "(SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) "
            + "FROM users u";

    List<Issue> issues = detector.evaluate(List.of(record(sql), record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
  }

  @Test
  void noIssueForNullSql() {
    QueryRecord nullRecord = new QueryRecord(null, null, 0L, 0L, null, 0);
    List<Issue> issues = detector.evaluate(List.of(nullRecord), EMPTY_INDEX);
    assertThat(issues).isEmpty();
  }

  @Test
  void detectsMultipleCorrelatedSubqueriesInSelect() {
    String sql =
        "SELECT u.id, "
            + "(SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count, "
            + "(SELECT SUM(p.amount) FROM payments p WHERE p.user_id = u.id) AS total_paid "
            + "FROM users u";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1); // One issue per query, not per subquery
    assertThat(issues.get(0).type()).isEqualTo(IssueType.CORRELATED_SUBQUERY);
  }

  // ── Mutation-killing tests: evaluate method ────────────────────────

  @Test
  void noIssueForNonSelectQuery() {
    // Kills negated conditional on line 56 (tables.isEmpty() check)
    // When table extraction returns empty, table should be null
    String sql = "UPDATE users SET name = 'x'";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).isEmpty();
  }

  @Test
  void tableIsExtractedWhenPresent() {
    // Kills negated conditional on line 56: table = tables.isEmpty() ? null : tables.get(0)
    String sql =
        "SELECT u.id, "
            + "(SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS cnt "
            + "FROM users u";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    // The table should be extracted (not null)
    assertThat(issues.get(0).table()).isNotNull();
  }

  // ── Mutation-killing tests: findOuterFromIndex ─────────────────────

  @Test
  void findOuterFromIndexSkipsFromInsideSubquery() {
    // Kills boundary mutations on lines 114, 117 and negate conditionals on lines 115, 117
    // Tests that FROM inside a subquery in SELECT is not treated as outer FROM
    String sql = "SELECT (SELECT 1 FROM dual) FROM users u WHERE u.id = 1";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).isEmpty(); // No correlated subquery
  }

  @Test
  void findOuterFromWithNoSpaceBeforeFrom() {
    // Kills math mutator on line 116 (i-1 boundary) and boundary on line 114
    // Test FROM at position 0 or preceded by non-alphanumeric
    String sql = "SELECT x\nFROM users u";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWhenFromIsAtEndOfString() {
    // Kills boundary on line 114 (i + 4 <= upper.length())
    // and PrimitiveReturnsMutator on line 122 (replaced int return with 0)
    // A SQL that has no FROM at all
    boolean result = detector.hasCorrelatedSubqueryInSelect("SELECT 1");
    assertThat(result).isFalse();
  }

  @Test
  void findOuterFromIndexReturnsCorrectPosition() {
    // Kills PrimitiveReturnsMutator line 122: replaced int return with 0
    // Ensures the correct FROM index is found (not 0)
    String sql = "SELECT u.id FROM users u";
    // If findOuterFromIndex returned 0 instead of the real index,
    // selectClause would be empty and no subqueries would be found
    boolean result = detector.hasCorrelatedSubqueryInSelect(sql);
    assertThat(result).isFalse();
  }

  // ── Mutation-killing tests: extractSubqueries ──────────────────────

  @Test
  void extractSubqueriesWithWhitespaceBeforeSelect() {
    // Kills boundary mutations on lines 155, 158, 163, 166
    // Tests subquery extraction when there's whitespace between ( and SELECT
    String sql =
        "SELECT u.id, (  SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) " + "FROM users u";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
  }

  @Test
  void extractSubqueriesWithNestedParens() {
    // Kills boundary mutations on lines 163, 166 (depth tracking in paren matching)
    String sql =
        "SELECT u.id, "
            + "(SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id AND o.amount > (1+2)) "
            + "FROM users u";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
  }

  @Test
  void extractSubqueriesNonSelectParenNotExtracted() {
    // Kills negated conditional on line 155 (ahead < selectClause.length())
    // Parenthesized expression that is NOT a SELECT should not be treated as subquery
    String sql = "SELECT u.id, (u.a + u.b) AS total FROM users u";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).isEmpty();
  }

  // ── Mutation-killing tests: hasCorrelatedSubqueryInSelect ──────────

  @Test
  void hasCorrelatedSubqueryReturnsFalseWhenNoOuterFrom() {
    // Kills boundary mutation on line 82 (outerFromIdx < 0 vs outerFromIdx <= 0)
    // and BooleanTrueReturnValsMutator on line 83
    boolean result = detector.hasCorrelatedSubqueryInSelect("SELECT 1 + 2");
    assertThat(result).isFalse();
  }

  @Test
  void hasCorrelatedSubqueryReturnsTrueForCorrelatedCase() {
    // Kills BooleanTrueReturnValsMutator on line 83 by verifying true is returned
    String sql = "SELECT (SELECT COUNT(*) FROM orders o WHERE o.uid = u.id) FROM users u";
    boolean result = detector.hasCorrelatedSubqueryInSelect(sql);
    assertThat(result).isTrue();
  }

  // ── Mutation-killing tests: isCorrelated ───────────────────────────

  @Test
  void isCorrelatedReturnsFalseWhenWhereReferencesOnlyInnerTable() {
    // Kills negated conditional on line 192 (outerAliases.contains check)
    String sql =
        "SELECT u.id, "
            + "(SELECT COUNT(*) FROM orders o WHERE o.status = 'active') "
            + "FROM users u";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForNullNormalizedSql() {
    // A QueryRecord with null normalizedSql should be skipped
    QueryRecord nullNorm = new QueryRecord("SELECT 1 FROM t", null, 0L, 0L, null, 0);
    List<Issue> issues = detector.evaluate(List.of(nullNorm), EMPTY_INDEX);
    assertThat(issues).isEmpty();
  }

  // ── Additional boundary-killing tests ──────────────────────────────

  @Test
  void findOuterFromAtExactEndOfString() {
    // Kills boundary on line 114 (i + 4 <= vs < upper.length())
    // and line 117 (i + 4 >= sql.length())
    // "FROM" is exactly at the end of the string
    boolean result = detector.hasCorrelatedSubqueryInSelect("SELECT 1 FROM");
    // No table after FROM, but findOuterFromIndex should find it
    // With no alias/table after FROM, no subquery -> false
    assertThat(result).isFalse();
  }

  @Test
  void findOuterFromPrecededByNonAlphanumeric() {
    // Kills negate on line 115 (i == 0 || !isLetterOrDigit(i-1))
    // FROM preceded by various non-alphanumeric characters
    boolean result = detector.hasCorrelatedSubqueryInSelect("SELECT\tFROM users");
    assertThat(result).isFalse();
  }

  @Test
  void findOuterFromFollowedByDigitShouldNotMatch() {
    // Kills negate on line 117 (i+4 >= length || !isLetterOrDigit(i+4))
    // "FROM2" should not match as FROM keyword
    boolean result = detector.hasCorrelatedSubqueryInSelect("SELECT 1 FROM2 x");
    assertThat(result).isFalse();
  }

  @Test
  void findOuterFromAtPositionZero() {
    // Kills negate on line 115: i == 0 case
    // FROM at the very beginning
    boolean result = detector.hasCorrelatedSubqueryInSelect("FROM users");
    assertThat(result).isFalse();
  }

  @Test
  void findOuterFromReturnNegativeOneForNoFrom() {
    // Kills PrimitiveReturnsMutator on line 122 (replaced int return with 0)
    // A string that starts with "S" - if return were 0, it would split at index 0
    // causing incorrect behavior
    boolean result = detector.hasCorrelatedSubqueryInSelect("SELECT (SELECT 1)");
    assertThat(result).isFalse();
  }

  @Test
  void extractSubqueriesWithOpenParenAtEnd() {
    // Kills boundary on line 155 (ahead < selectClause.length())
    // Open paren at the very end of select clause
    boolean result = detector.hasCorrelatedSubqueryInSelect("SELECT x, ( FROM users");
    assertThat(result).isFalse();
  }

  @Test
  void extractSubqueriesSelectExactlyAtBoundary() {
    // Kills boundary on line 158 (ahead + 6 <= upper.length())
    // The word after ( is exactly "SELECT" with nothing after
    // This tests the boundary: ahead + 6 == upper.length()
    boolean result = detector.hasCorrelatedSubqueryInSelect("SELECT (SELECT FROM users");
    // Incomplete subquery, unmatched parens
    assertThat(result).isFalse();
  }

  @Test
  void extractSubqueriesDepthTrackingCorrect() {
    // Kills boundary on lines 163, 166 (depth tracking)
    // Nested parentheses inside the subquery
    String sql = "SELECT (SELECT COUNT(*) FROM (SELECT 1) sub WHERE sub.id = u.id) FROM users u";
    boolean result = detector.hasCorrelatedSubqueryInSelect(sql);
    assertThat(result).isTrue();
  }

  @Test
  void hasCorrelatedSubqueryNegativeFromIndex() {
    // Kills boundary on line 82 (outerFromIdx < 0 vs outerFromIdx <= 0)
    // No FROM in query at all
    boolean result = detector.hasCorrelatedSubqueryInSelect("SELECT 1 + 2 + 3");
    assertThat(result).isFalse();
  }

  @Test
  void hasCorrelatedSubqueryFromAtIndex0() {
    // Kills boundary on line 82: if fromIdx == 0, selectClause is empty
    // FROM at the very start means empty SELECT clause -> no subqueries
    boolean result = detector.hasCorrelatedSubqueryInSelect("FROM users u");
    assertThat(result).isFalse();
  }

  // ── False positive prevention tests ──────────────────────────────────

  @Test
  void noIssueForSubqueryReferencingOwnTableWithSameNameAsOuterTable() {
    // False positive: subquery references its own "users" table, not the outer "users" alias
    String sql =
        "SELECT u.id, "
            + "(SELECT COUNT(*) FROM users WHERE users.active = true) AS active_count "
            + "FROM users u";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForUncorrelatedSelfReferencingSubquery() {
    // Subquery uses the same table name but does not correlate with the outer query
    String sql =
        "SELECT o.id, "
            + "(SELECT AVG(amount) FROM orders WHERE status = 'COMPLETED') AS avg_amount "
            + "FROM orders o";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void stillDetectsCorrelatedSubqueryWithDifferentInnerTable() {
    // Subquery uses a different table but references the outer alias — truly correlated
    String sql =
        "SELECT u.id, "
            + "(SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count "
            + "FROM users u";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.CORRELATED_SUBQUERY);
  }
}
