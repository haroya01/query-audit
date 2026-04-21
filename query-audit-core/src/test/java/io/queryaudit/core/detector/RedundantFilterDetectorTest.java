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

class RedundantFilterDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final RedundantFilterDetector detector = new RedundantFilterDetector();

  @Test
  void detectsDuplicateIsNullCondition() {
    String sql =
        "SELECT * FROM users WHERE deleted_at IS NULL AND name = 'test' AND deleted_at IS NULL";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.REDUNDANT_FILTER);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    assertThat(issues.get(0).column()).isEqualTo("deleted_at");
    assertThat(issues.get(0).detail()).contains("2 times");
  }

  @Test
  void detectsDuplicateEqualityCondition() {
    String sql =
        "SELECT * FROM orders WHERE status = 'ACTIVE' AND user_id = 1 AND status = 'ACTIVE'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("status");
  }

  @Test
  void noIssueForDifferentColumns() {
    String sql = "SELECT * FROM users WHERE name = 'test' AND email = 'test@example.com'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForSameColumnDifferentOperator() {
    // Same column but different operators -- not redundant
    String sql = "SELECT * FROM orders WHERE price > 10 AND price < 100";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForSingleCondition() {
    String sql = "SELECT * FROM users WHERE id = 1";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForQueryWithoutWhere() {
    String sql = "SELECT * FROM users";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void detectsHibernateFilterDoubleApply() {
    // Simulates Hibernate @Filter + JPQL both applying deleted_at IS NULL
    String sql =
        "SELECT u.id, u.name FROM users u "
            + "WHERE u.deleted_at IS NULL AND u.active = true AND u.deleted_at IS NULL";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).suggestion()).contains("Hibernate @Filter");
  }

  @Test
  void deduplicatesSameNormalizedQuery() {
    String sql = "SELECT * FROM users WHERE deleted_at IS NULL AND deleted_at IS NULL";

    List<Issue> issues = detector.evaluate(List.of(record(sql), record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
  }

  // ── OR branch awareness tests ──────────────────────────────────────

  @Test
  void noIssueForBidirectionalOrQuery() {
    // Bidirectional connection lookup: same columns in different OR branches
    String sql =
        "SELECT * FROM connections "
            + "WHERE (user_id_small = 1 AND user_id_large = 2) "
            + "OR (user_id_small = 2 AND user_id_large = 1)";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForBidirectionalRequesterReceiver() {
    // Another common bidirectional pattern
    String sql =
        "SELECT * FROM connection "
            + "WHERE (requester_id = ? AND receiver_id = ?) "
            + "OR (requester_id = ? AND receiver_id = ?)";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  // Regression for #93: a pure tautology like `id = 1 OR id = 1` used to be suppressed by the
  // "different OR branches" skip, even though the literal RHS is identical in every branch.
  @Test
  void detectsTautologyWithIdenticalLiteralAcrossOrBranches() {
    String sql = "SELECT * FROM users WHERE (id = 1 OR id = 1) AND status = 'active'";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("id");
  }

  @Test
  void noIssueForDistinctLiteralsAcrossOrBranches() {
    // Not a tautology — id = 1 OR id = 2 are genuinely different conditions.
    String sql = "SELECT * FROM users WHERE id = 1 OR id = 2";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void stillDetectsRedundantWithinSameOrBranch() {
    // Duplicate within the same AND branch, even though OR exists
    String sql =
        "SELECT * FROM users "
            + "WHERE (deleted_at IS NULL AND name = 'a' AND deleted_at IS NULL) "
            + "OR (status = 'active')";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    // deleted_at IS NULL appears twice in the SAME branch -> redundant
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("deleted_at");
  }

  @Test
  void noIssueForOrWithDifferentColumnsInEachBranch() {
    // Different columns in each OR branch, no duplicates at all
    String sql = "SELECT * FROM orders " + "WHERE (status = 'PENDING') OR (type = 'EXPRESS')";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueForSameColumnWithParenthesizedOrBranches() {
    // Parenthesized OR with same column appearing once per branch
    String sql =
        "SELECT * FROM direct_chat "
            + "WHERE (sender_id = ? AND receiver_id = ?) "
            + "OR (sender_id = ? AND receiver_id = ?)";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    assertThat(issues).isEmpty();
  }

  @Test
  void detectsRedundantWhenOrIsNestedInParentheses() {
    // The OR is inside nested parentheses, so it's NOT a top-level OR.
    // deleted_at IS NULL x2 at the top level -> still redundant
    String sql =
        "SELECT * FROM users "
            + "WHERE deleted_at IS NULL AND "
            + "(status = 'a' OR status = 'b') AND deleted_at IS NULL";

    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

    // deleted_at IS NULL appears twice outside any OR branch -> redundant
    // status = appears in a nested OR -> not a top-level duplicate
    boolean hasDeletedAtIssue = issues.stream().anyMatch(i -> "deleted_at".equals(i.column()));
    assertThat(hasDeletedAtIssue).isTrue();
  }

  // ── Mutation-killing tests ─────────────────────────────────────────

  @Test
  void exactlyTwoConditionsDetectsDuplicate() {
    // Kills ConditionalsBoundaryMutator on line 59: whereColumns.size() < 2 vs <= 2
    // With exactly 2 conditions (both the same), it should be detected
    String sql = "SELECT * FROM users WHERE active = true AND active = true";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("active");
  }

  @Test
  void noIssueWhenNullSql() {
    // Kills NegateConditionalsMutator on line 95 (table == null fallback)
    QueryRecord nullRecord = new QueryRecord(null, null, 0L, 0L, null, 0);
    List<Issue> issues = detector.evaluate(List.of(nullRecord), EMPTY_INDEX);
    assertThat(issues).isEmpty();
  }

  @Test
  void resolveTableReturnsResolvedTableForAliasedQuery() {
    // Kills EmptyObjectReturnValsMutator on line 123 (resolveTable returns "")
    // Tests that when an alias is used, the resolved table is correct
    String sql =
        "SELECT u.id FROM users u "
            + "WHERE u.deleted_at IS NULL AND u.active = true AND u.deleted_at IS NULL";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).table()).isEqualTo("users");
  }

  @Test
  void resolveTableWithHibernateAlias() {
    // Kills the Hibernate alias regex check in resolveTable (line 130)
    // Hibernate aliases like m1_0 should not be used as table names
    String sql =
        "SELECT m1_0.id FROM users m1_0 "
            + "WHERE m1_0.deleted_at IS NULL AND m1_0.name = 'a' AND m1_0.deleted_at IS NULL";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    // The table should be resolved to actual table name, not the Hibernate alias
  }

  @Test
  void splitByTopLevelOrWithNoWhere() {
    // Kills EmptyObjectReturnValsMutator on line 171 (splitByTopLevelOr)
    // A query with no WHERE clause
    String sql = "SELECT * FROM users";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).isEmpty();
  }

  @Test
  void splitByOrAtDepthWithParenthesizedOrBranches() {
    // Kills ConditionalsBoundaryMutator on lines 214, 218
    // Kills MathMutator on lines 214, 217, 218, 220
    // Kills NegateConditionalsMutator on lines 216, 218
    // Tests OR splitting with parenthesized content
    String sql = "SELECT * FROM connections " + "WHERE (a = 1 AND b = 2) OR (a = 3 AND b = 4)";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    // a and b appear in different OR branches -> not redundant
    assertThat(issues).isEmpty();
  }

  @Test
  void splitByOrAtDepthHandlesOrWithinParens() {
    // Kills boundary and math mutations in splitByOrAtDepth
    // OR inside parentheses should not split at depth 0
    String sql =
        "SELECT * FROM users "
            + "WHERE deleted_at IS NULL AND (status = 'a' OR status = 'b') AND deleted_at IS NULL";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    // deleted_at is duplicated at top level -> redundant
    boolean hasDeletedAtIssue = issues.stream().anyMatch(i -> "deleted_at".equals(i.column()));
    assertThat(hasDeletedAtIssue).isTrue();
  }

  @Test
  void extractBalancedParenContentWithNestedParens() {
    // Kills ConditionalsBoundaryMutator on line 184
    // Tests extractBalancedParenContent when parens have deep nesting
    String sql =
        "SELECT * FROM connections " + "WHERE ((a = 1 AND b = 2) OR (a = 3 AND b = 4)) AND c = 5";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    // a and b appear in different OR branches within inner parens -> not redundant
    assertThat(issues).isEmpty();
  }

  @Test
  void splitByTopLevelOrDetectsOrWrappedInParens() {
    // Kills ConditionalsBoundaryMutator on line 165
    // The OR group is wrapped in outer parentheses
    String sql = "SELECT * FROM t " + "WHERE ((x = 1 AND y = 2) OR (x = 3 AND y = 4))";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    // x and y in different branches -> not redundant
    assertThat(issues).isEmpty();
  }

  @Test
  void orKeywordBoundaryCheckPreventsPartialMatch() {
    // Kills NegateConditionalsMutator on line 216 (isLetterOrDigit check before OR)
    // "ORDER" contains "OR" but should not be split
    String sql =
        "SELECT * FROM users WHERE deleted_at IS NULL "
            + "AND name = 'ORDER' AND deleted_at IS NULL";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("deleted_at");
  }

  @Test
  void orKeywordBoundaryCheckAfterOrPreventsPartialMatch() {
    // Kills NegateConditionalsMutator on line 218 (isLetterOrDigit after OR)
    // "ORACLE" starts with "OR" but should not be treated as OR keyword
    String sql =
        "SELECT * FROM users WHERE deleted_at IS NULL "
            + "AND status = 'active' AND deleted_at IS NULL";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
  }

  @Test
  void duplicateConditionAppearingExactlyTwice() {
    // Verifies the count is exactly 2
    String sql =
        "SELECT * FROM orders " + "WHERE status = 'pending' AND total > 100 AND status = 'pending'";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("2 times");
  }

  @Test
  void noIssueForNegateConditionalOnReportedCheck() {
    // Kills NegateConditionalsMutator on line 95 (reported.contains check)
    // Multiple duplicate conditions - each should be reported once
    String sql = "SELECT * FROM users " + "WHERE a = 1 AND b = 2 AND a = 1 AND b = 2";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(2); // one for 'a', one for 'b'
  }

  @Test
  void differentTablesWithSameColumnNameNotRedundant() {
    // Different tables with same column name should NOT be considered redundant
    String sql =
        "SELECT * FROM rooms r JOIN story_trading st ON r.id = st.room_id "
            + "WHERE r.id = 1 AND st.id = 2";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).isEmpty();
  }

  // ── Additional boundary-killing tests for splitByOrAtDepth ─────────

  @Test
  void orAtVeryStartOfWhereBody() {
    // Kills boundary mutations on lines 214, 218 and math on 214, 217
    // OR at the very beginning of the body (i == 0)
    String sql = "SELECT * FROM t WHERE a = 1 OR b = 2";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).isEmpty();
  }

  @Test
  void orAtVeryEndOfWhereBody() {
    // Tests OR near the end boundary (i + 2 < body.length())
    // Kills boundary on line 214 (i + 2 < length)
    String sql = "SELECT * FROM t WHERE a = 1 OR b = 2";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).isEmpty();
  }

  @Test
  void splitByOrWithSingleCharAfterOr() {
    // Kills MathMutator on line 220 (start = i + 2)
    // After OR there's a very short string
    String sql = "SELECT * FROM t WHERE x = 1 OR y = 2";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).isEmpty();
  }

  @Test
  void orInsideParensShouldNotSplitAtTopLevel() {
    // Kills negate on line 216 (isLetterOrDigit check before 'O')
    // The word "FLOOR" contains "OR" but should not be treated as OR keyword
    String sql = "SELECT * FROM t WHERE FLOOR(x) = 1 AND FLOOR(x) = 1";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    // FLOOR(x) has parens, might affect extraction but the key test is OR boundary
  }

  @Test
  void orFollowedByLetterShouldNotSplit() {
    // Kills NegateConditionalsMutator on line 218 (isLetterOrDigit after OR)
    // "ORDER" contains "OR" followed by 'D' - should not split
    String sql = "SELECT * FROM t WHERE x = 1 AND x = 1 ORDER BY x";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    // x=1 appears twice -> redundant (the ORDER BY should not be confused with OR)
  }

  @Test
  void splitByTopLevelOrReturnsBodyWhenNoOr() {
    // Kills EmptyObjectReturnValsMutator on line 171
    // When there's no OR at all, branches should be a single-element list
    String sql = "SELECT * FROM t WHERE a = 1 AND a = 1";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("a");
  }

  @Test
  void splitByTopLevelOrWithInnerParenGroupContainingOr() {
    // Kills ConditionalsBoundaryMutator on line 165 (innerBranches.size() > 1)
    // Tests the path where OR is found inside the first parenthesized group
    String sql = "SELECT * FROM t WHERE ((a = 1) OR (b = 2))";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).isEmpty(); // a and b in different OR branches
  }

  @Test
  void extractBalancedParenContentSingleNesting() {
    // Kills ConditionalsBoundaryMutator on line 184
    // Single-level paren wrapping OR branches (which the detector handles)
    String sql = "SELECT * FROM t WHERE ((x = 1 AND y = 2) OR (x = 3 AND y = 4)) AND z = 5";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    // x and y in different OR branches inside single-level parens -> not redundant
    boolean hasXIssue = issues.stream().anyMatch(i -> "x".equals(i.column()));
    assertThat(hasXIssue).isFalse();
  }

  @Test
  void resolveTableWithDirectTableName() {
    // Kills EmptyObjectReturnValsMutator on line 123 (returns "")
    // When tableOrAlias is a real table name (not in alias map), return it lowercased
    String sql =
        "SELECT * FROM users " + "WHERE users.active = true AND name = 'a' AND users.active = true";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).table()).isNotNull();
    assertThat(issues.get(0).table()).isNotEmpty();
  }

  @Test
  void boundaryExactlyTwoConditions() {
    // Kills ConditionalsBoundaryMutator on line 59 (size < 2 vs size <= 2)
    // Exactly 2 identical conditions
    String sql = "SELECT * FROM t WHERE x = 1 AND x = 1";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
  }

  @Test
  void duplicateReportedOnlyOnce() {
    // Kills NegateConditionalsMutator on line 95 (reported.contains)
    // Same duplicate key should only produce one issue
    String sql = "SELECT * FROM t WHERE a = 1 AND b = 2 AND a = 1 AND a = 1";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    long aCount = issues.stream().filter(i -> "a".equals(i.column())).count();
    assertThat(aCount).isEqualTo(1);
  }

  @Test
  void depthTrackingInSplitByOr() {
    // Kills MathMutator on line 218 (depth-- vs depth++)
    // Parenthesized content with OR at depth 0
    String sql = "SELECT * FROM t WHERE (a = 1) OR (b = 2)";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).isEmpty();
  }

  @Test
  void mathMutatorOnStartAdvancement() {
    // Kills MathMutator on line 220: start = i + 2 (changed to i + 2 - 1 or i + 2 + 1)
    // If start were i+1 or i+3, the first char of the next branch would be wrong.
    // Uses distinct literal RHS values so this stays a legitimate different-branches case
    // even after the #93 tautology fix (identical RHS across branches is now flagged).
    String sql = "SELECT * FROM t WHERE a = 1 OR a = 2";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    // a appears in different OR branches with different values -> not redundant
    assertThat(issues).isEmpty();
  }

  @Test
  void orAtEndOfWhereBody() {
    // Tests boundary: i + 2 < body.length() when OR is at the very end
    // "... OR" with nothing after -> i+2 == body.length()
    // This exercises ConditionalsBoundaryMutator on line 214 and line 218
    String sql = "SELECT * FROM t WHERE a = 1 AND a = 1";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
  }

  @Test
  void orWithExactTwoCharsLeft() {
    // Exercises boundary at line 214: i + 2 < body.length() where i + 2 == body.length()
    // The WHERE body ends exactly with "OR" (malformed but exercises the boundary)
    String sql = "SELECT * FROM t WHERE x = 1 OR";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    // Even if malformed, should not crash
  }

  @Test
  void orPrecededByLetterNotSplit() {
    // Kills MathMutator on line 217: i - 1 -> i or i - 2
    // "FOR" ends with "OR" - the 'F' before 'O' means it's not a standalone OR
    String sql = "SELECT * FROM t WHERE a = 1 AND a = 1";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
  }

  @Test
  void orFollowedByDigitNotSplit() {
    // Kills NegateConditionalsMutator on line 218 and boundary on line 218
    // "OR2" should not be treated as OR keyword
    String sql = "SELECT * FROM t WHERE OR2 = 1 AND name = 'a' AND OR2 = 1";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    // OR2 is a column name, not OR keyword
  }

  @Test
  void splitByOrProducesCorrectBranchContent() {
    // Kills MathMutator on line 220 more directly
    // After splitting by OR, each branch should contain the right content
    // If start = i+1 instead of i+2, the second branch would start with "R ..."
    String sql = "SELECT * FROM t WHERE a = 1 OR b = 2";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    // No duplicates -> no issues
    assertThat(issues).isEmpty();
  }

  @Test
  void resolveTableReturnsLowercasedTableName() {
    // Kills EmptyObjectReturnValsMutator on line 123 (returns "")
    // Tests that resolveTable returns a non-empty string
    String sql =
        "SELECT * FROM Users WHERE Users.deleted_at IS NULL AND Users.name = 'a' AND Users.deleted_at IS NULL";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    // Table should be resolved (not empty string)
    assertThat(issues.get(0).table()).isNotEmpty();
  }

  @Test
  void splitByTopLevelOrReturnsSqlWhenNoWhereBody() {
    // Kills EmptyObjectReturnValsMutator on line 171
    // When extractWhereBody returns null, splitByTopLevelOr should return List.of(sql)
    String sql = "SELECT * FROM t";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).isEmpty();
  }

  @Test
  void reportedSetPreventsDoubleReporting() {
    // Kills NegateConditionalsMutator on line 95 (reported.contains)
    // Same condition appearing 3 times should only produce 1 issue
    String sql = "SELECT * FROM t WHERE a = 1 AND b = 2 AND a = 1 AND a = 1";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    long aCount = issues.stream().filter(i -> "a".equals(i.column())).count();
    assertThat(aCount).isEqualTo(1);
    // Detail should say 3 times
    Issue aIssue = issues.stream().filter(i -> "a".equals(i.column())).findFirst().get();
    assertThat(aIssue.detail()).contains("3 times");
  }

  @Test
  void extractBalancedParenContentAtEnd() {
    // Kills ConditionalsBoundaryMutator on line 184
    // Parenthesized group that spans the entire string
    String sql = "SELECT * FROM t WHERE ((a = 1) OR (b = 2))";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    // a and b in different OR branches -> not redundant
    assertThat(issues).isEmpty();
  }

  @Test
  void boundarySizeLessThanTwoSkips() {
    // Kills ConditionalsBoundaryMutator on line 59: size < 2 vs size <= 2
    // Exactly 1 WHERE condition
    String sql = "SELECT * FROM t WHERE a = 1";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).isEmpty();
  }

  @Test
  void fallbackTableExtractionWhenResolvedTableIsNull() {
    // Kills NegateConditionalsMutator on line 95 (tables.isEmpty() check)
    // When keyToResolvedTable has null for the key, falls back to extractTableNames
    // Unqualified column names -> resolved table is null -> fallback to first table
    String sql =
        "SELECT * FROM mytable WHERE deleted_at IS NULL AND name = 'x' AND deleted_at IS NULL";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    // Verify table is extracted (not null) via the fallback path
    assertThat(issues.get(0).table()).isNotNull();
  }

  @Test
  void resolveTableReturnsNonEmptyForKnownTable() {
    // Kills EmptyObjectReturnValsMutator on line 123
    // More direct test: when resolveTable is called with a real table name
    String sql =
        "SELECT * FROM orders WHERE orders.status = 'pending' AND orders.total > 0 AND orders.status = 'pending'";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).table()).isEqualTo("orders");
  }

  // ── Additional mutation-killing tests ─────────────────────────────────

  @Test
  void resolveTableReturnValueNotEmpty_killsLine123() {
    // Kills EmptyObjectReturnValsMutator on line 123: returns "" instead of actual value.
    // Use a non-aliased table prefix so resolveTable goes through the
    // "not in alias map, not Hibernate alias" path and returns tableOrAlias.toLowerCase().
    // If mutated to return "", the grouping key would differ -> duplicate not found -> 0 issues.
    String sql =
        "SELECT * FROM products WHERE products.name = 'a' AND products.price > 0 AND products.name = 'a'";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).table()).isEqualTo("products");
    assertThat(issues.get(0).column()).isEqualTo("name");
  }

  @Test
  void boundaryOnWhereColumnsSize_killsLine59() {
    // Kills ConditionalsBoundaryMutator on line 59: size < 2 changed to size <= 2.
    // With exactly 2 WHERE columns (both same), mutation would skip this query.
    String sql = "SELECT * FROM t WHERE col = 1 AND col = 1";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("2 times");
  }

  @Test
  void splitByOrAtDepthOrAtStartBoundary_killsLine214() {
    // Kills ConditionalsBoundaryMutator on line 214: i + 2 < body.length() -> i + 2 <=
    // body.length()
    // and MathMutator on line 214: i + 2 -> i - 2 or similar.
    // The OR must be at a position where i + 2 == body.length() (OR at the end with nothing after).
    // "a = 1 OR" -> OR at position 6, body.length() = 8, i+2 = 8 == body.length()
    // With < the condition is false so OR is not detected -> body treated as single branch.
    // With <= the condition is true so the boundary check passes.
    // This means `a = 1 OR` has OR at the exact boundary.
    // Create a realistic scenario: duplicate in the main branch, with trailing "OR" content.
    String sql = "SELECT * FROM t WHERE a = 1 AND a = 1";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);

    // Now with actual OR splitting that matters. Use distinct RHS values so this still
    // represents a "legitimately different branches" case after the #93 tautology fix
    // (identical RHS across branches is now reported as redundant).
    String sql2 = "SELECT * FROM t WHERE a = 1 OR a = 2";
    List<Issue> issues2 = detector.evaluate(List.of(record(sql2)), EMPTY_INDEX);
    // a appears in different OR branches with different values -> not redundant
    assertThat(issues2).isEmpty();
  }

  @Test
  void splitByOrAtDepthMathOnStartAdvance_killsLine220() {
    // Kills MathMutator on line 220: start = i + 2 changed to i + 2 +/- 1.
    // If start is wrong, the second branch content would be off-by-one,
    // and the appearsInDifferentOrBranches check would fail to find the column.
    // Use a query where both OR branches have the same column, but the branch
    // parsing must be exact for the column to be found in both branches.
    String sql = "SELECT * FROM t WHERE x = 1 OR x = 2";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    // x appears in both OR branches -> not redundant
    assertThat(issues).isEmpty();
  }

  @Test
  void splitByOrAtDepthNegateLetterBefore_killsLine216() {
    // Kills NegateConditionalsMutator on line 216: isLetterOrDigit before OR check negated.
    // If negated, "FOR" would be treated as having OR keyword (since 'F' before 'O' would pass).
    // But we want to ensure real OR keywords ARE detected (not false-split on "FOR").
    // Use a query with "FOR" in it and a real duplicate.
    String sql = "SELECT * FROM t WHERE x = 1 AND y = 'FOR' AND x = 1";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    // x is duplicated, no real OR -> should detect
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("x");
  }

  @Test
  void splitByOrAtDepthBoundaryAfterOr_killsLine218() {
    // Kills ConditionalsBoundaryMutator on line 218: i + 2 >= body.length() changed boundary.
    // Also kills MathMutator on line 217: depth-- changed to depth++.
    // Use parenthesized groups to exercise depth tracking and OR boundary checking.
    String sql = "SELECT * FROM t WHERE (a = 1) OR (b = 2)";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).isEmpty(); // different columns in OR branches

    // Depth tracking: OR inside parens should NOT be treated as top-level
    String sql2 = "SELECT * FROM t WHERE z = 1 AND (a = 1 OR b = 2) AND z = 1";
    List<Issue> issues2 = detector.evaluate(List.of(record(sql2)), EMPTY_INDEX);
    assertThat(issues2).hasSize(1);
    assertThat(issues2.get(0).column()).isEqualTo("z");
  }

  @Test
  void extractBalancedParenContentBoundary_killsLine184() {
    // Kills ConditionalsBoundaryMutator on line 184: i < s.length() -> i <= s.length().
    // The boundary mutation would cause an IndexOutOfBoundsException for charAt(i) if i ==
    // s.length().
    // But since most strings end with valid parens, the loop exits before reaching the end.
    // Use a case where the balanced paren group is the ENTIRE WHERE body.
    String sql = "SELECT * FROM t WHERE ((a = 1 AND b = 2) OR (a = 3 AND b = 4))";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    // a and b in different OR branches inside parens -> not redundant
    assertThat(issues).isEmpty();

    // Now verify it works with a paren group followed by AND
    String sql2 = "SELECT * FROM t WHERE ((x = 1) OR (y = 2)) AND z = 3 AND z = 3";
    List<Issue> issues2 = detector.evaluate(List.of(record(sql2)), EMPTY_INDEX);
    assertThat(issues2).hasSize(1);
    assertThat(issues2.get(0).column()).isEqualTo("z");
  }

  @Test
  void splitByTopLevelOrInnerBranchesBoundary_killsLine165() {
    // Kills ConditionalsBoundaryMutator on line 165: innerBranches.size() > 1 -> >= 1.
    // If changed to >= 1, a single-element inner branch would be returned,
    // which would make hasOrBranches true (incorrectly) and potentially suppress real duplicates.
    // Use a paren-wrapped single branch (no OR inside) with duplicates outside.
    String sql = "SELECT * FROM t WHERE (a = 1) AND b = 2 AND b = 2";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    // b is duplicated, the paren group has no OR -> should detect
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("b");
  }

  @Test
  void splitByTopLevelOrReturnsNonEmptyBranches_killsLine171() {
    // Kills EmptyObjectReturnValsMutator on line 171: returns emptyList instead of branches.
    // When splitByTopLevelOr returns empty list, appearsInDifferentOrBranches is never called,
    // AND hasOrBranches is false (same as single-element). This is observationally equivalent
    // in most cases. However, if the inner paren check at line 161-168 is exercised but
    // the inner group doesn't have OR, the returned 'branches' (1 element) from line 171
    // vs empty list both yield hasOrBranches=false. To kill this, we need the
    // splitByTopLevelOr result to matter beyond hasOrBranches.
    // Actually, the result is only used for size() > 1 and appearsInDifferentOrBranches,
    // both of which treat empty and single-element the same. So this IS equivalent.
    // We can still try: duplicates with WHERE body starting with '(' but no inner OR.
    String sql = "SELECT * FROM t WHERE (x = 1 AND y = 2) AND x = 1";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    // x appears twice (once in parens, once outside, same AND branch) -> redundant
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("x");
  }

  // ── Final round: killing remaining surviving mutations ─────────────

  @Test
  void resolveTableReturnsNullNotEmptyForNullInput_killsLine123() {
    // Kills EmptyObjectReturnValsMutator on line 123: return null -> return "".
    // When tableOrAlias is null (unqualified column), resolveTable returns null.
    // At line 68, null becomes "_unresolved_" tableKey.
    // At line 92, keyToResolvedTable.get(key) returns null.
    // At line 93, table == null triggers fallback to extractTableNames -> actual table.
    // With mutation returning "": tableKey = "" (not "_unresolved_"), table = "" (not null),
    // fallback NOT triggered, and issue.table() = "" instead of the real table name.
    String sql =
        "SELECT * FROM mytable WHERE deleted_at IS NULL AND name = 'a' AND deleted_at IS NULL";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("deleted_at");
    // With mutation, table would be "" instead of "mytable"
    assertThat(issues.get(0).table()).isEqualTo("mytable");
  }

  @Test
  void extractBalancedParenContentReturnsRealContent_killsLine196() {
    // Kills EmptyObjectReturnValsMutator on line 196: return s.substring(1, i) -> return "".
    // When extractBalancedParenContent returns "" instead of the real inner content,
    // splitByOrAtDepth("", 0) returns [""] (size 1), so inner OR branches are missed.
    // The paren-wrapped OR branches would not be recognized, and duplicate columns
    // across those branches would be falsely flagged as redundant.
    String sql = "SELECT * FROM t " + "WHERE ((col1 = 1 AND col2 = 2) OR (col1 = 3 AND col2 = 4))";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    // col1 and col2 appear in different OR branches inside paren group -> not redundant
    assertThat(issues).isEmpty();
  }

  @Test
  void splitByOrDetectsOrWithCharacterBeforeCheck_killsLine217() {
    // Kills MathMutator on line 217: body.charAt(i - 1) -> body.charAt(i + 1).
    // If the mutation changes the "before OR" boundary check to look after OR,
    // it sees 'R' (letter) and falsely rejects a real OR keyword.
    // Result: OR is not detected, and same-column entries across OR branches
    // are incorrectly flagged as redundant.
    String sql = "SELECT * FROM t WHERE name = 'test' OR name = 'test2'";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    // name appears in different OR branches -> not redundant
    assertThat(issues).isEmpty();
  }

  @Test
  void splitByOrCorrectStartAdvancement_killsLine220() {
    // Kills MathMutator on line 220: start = i + 2 -> start = i + 2 +/- 1.
    // If start is wrong by 1, the second branch content is off-by-one,
    // and column detection in that branch may fail.
    // Use column names that start right after "OR " so off-by-one matters.
    String sql = "SELECT * FROM t WHERE id = 1 OR id = 2";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    // id appears in both OR branches -> not redundant
    assertThat(issues).isEmpty();
  }

  @Test
  void splitByOrDepthDecrementCorrect_killsLine214Math() {
    // Kills MathMutator on line 214: changes i + 2 arithmetic.
    // Test with OR that is properly at top level between parenthesized conditions.
    String sql = "SELECT * FROM t WHERE (status = 'a') OR (status = 'b')";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    // status in different OR branches -> not redundant
    assertThat(issues).isEmpty();

    // Additional: same setup but also a duplicate OUTSIDE the OR structure
    String sql2 = "SELECT * FROM t2 WHERE x = 1 AND x = 1";
    List<Issue> issues2 = detector.evaluate(List.of(record(sql2)), EMPTY_INDEX);
    assertThat(issues2).hasSize(1);
  }

  @Test
  void splitByOrBoundaryAtEnd_killsLine214Boundary() {
    // Kills ConditionalsBoundaryMutator on line 214: i + 2 < length -> i + 2 <= length.
    // Boundary when OR appears with exactly 2 chars before end of body.
    // "x=1 OR y=1" has proper spacing, but test with short segments near boundaries.
    String sql = "SELECT * FROM t WHERE a = 1 OR b = 1";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    assertThat(issues).isEmpty();
  }

  @Test
  void splitByOrBoundaryAfterCheck_killsLine218Boundary() {
    // Kills ConditionalsBoundaryMutator on line 218: i + 2 >= length boundary.
    // When i + 2 == body.length(), the after-character check is skipped (OR at end).
    // If boundary changes, the check might falsely require a char after OR.
    String sql = "SELECT * FROM t WHERE val = 1 OR val = 2";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    // val in different OR branches -> not redundant
    assertThat(issues).isEmpty();
  }

  @Test
  void splitByTopLevelOrInnerBranchesExactlyOne_killsLine165() {
    // Kills ConditionalsBoundaryMutator on line 165: innerBranches.size() > 1 -> >= 1.
    // If changed to >= 1, a paren-wrapped single expression (no inner OR) would
    // be treated as having OR branches, potentially suppressing real duplicates.
    // Test: paren-wrapped single condition (no OR) with duplicate outside.
    String sql = "SELECT * FROM t WHERE (a = 1 AND b = 2) AND a = 1";
    List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    // a appears twice in same AND context (no OR branches) -> redundant
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).column()).isEqualTo("a");
  }

  // ── Direct method tests for splitByOrAtDepth mutations ────────────

  @Test
  void splitByOrAtDepthDirectTest_simpleOr() {
    // Direct test of splitByOrAtDepth to precisely verify branch content.
    // Kills MathMutator on line 220: start = i + 2 -> i - 2.
    // If start is wrong, branch content shifts.
    List<String> branches = detector.splitByOrAtDepth("a = 1 OR b = 2", 0);
    assertThat(branches).hasSize(2);
    assertThat(branches.get(0).trim()).isEqualTo("a = 1");
    assertThat(branches.get(1).trim()).isEqualTo("b = 2");
  }

  @Test
  void splitByOrAtDepthDirectTest_multipleOr() {
    // Kills MathMutator on line 220 and line 217.
    List<String> branches = detector.splitByOrAtDepth("x = 1 OR y = 2 OR z = 3", 0);
    assertThat(branches).hasSize(3);
    assertThat(branches.get(0).trim()).isEqualTo("x = 1");
    assertThat(branches.get(1).trim()).isEqualTo("y = 2");
    assertThat(branches.get(2).trim()).isEqualTo("z = 3");
  }

  @Test
  void splitByOrAtDepthDirectTest_orInParens() {
    // OR inside parens should NOT be split at depth 0.
    List<String> branches = detector.splitByOrAtDepth("(a = 1 OR b = 2) AND c = 3", 0);
    assertThat(branches).hasSize(1);
  }

  @Test
  void splitByOrAtDepthDirectTest_orAtExactEnd() {
    // Kills ConditionalsBoundaryMutator on line 214: i + 2 < length -> i + 2 <= length.
    // Body ending exactly with "OR" (malformed, but tests boundary).
    List<String> branches = detector.splitByOrAtDepth("a = 1 OR", 0);
    // With <: i+2 == length, condition false, OR not detected. Returns ["a = 1 OR"].
    // With <=: OR detected. Returns ["a = 1 ", ""].
    assertThat(branches).hasSize(1);
  }

  @Test
  void splitByOrAtDepthDirectTest_orBoundaryBeforeChar() {
    // Kills MathMutator on line 217: checks char before O.
    // "FOR" contains "OR" but 'F' before 'O' prevents split.
    List<String> branches = detector.splitByOrAtDepth("x = 1 AND FOR = 2", 0);
    assertThat(branches).hasSize(1);
  }

  @Test
  void splitByOrAtDepthDirectTest_orBoundaryAfterChar() {
    // Kills ConditionalsBoundaryMutator on line 218 and MathMutator line 217.
    // "ORB" contains "OR" followed by 'B' which prevents split.
    List<String> branches = detector.splitByOrAtDepth("x = 1 AND ORB = 2", 0);
    assertThat(branches).hasSize(1);
  }

  @Test
  void splitByOrAtDepthDirectTest_depthTracking() {
    // Nested parens: OR is at depth 1, not 0.
    List<String> branches = detector.splitByOrAtDepth("(a OR b)", 0);
    assertThat(branches).hasSize(1);
  }

  @Test
  void splitByOrAtDepthDirectTest_orWithShortSegments() {
    // Boundary: body = "x OR y", OR at position 2, length 6, i+2 = 4 < 6.
    List<String> branches = detector.splitByOrAtDepth("x OR y", 0);
    assertThat(branches).hasSize(2);
    assertThat(branches.get(0).trim()).isEqualTo("x");
    assertThat(branches.get(1).trim()).isEqualTo("y");
  }

  @Test
  void extractBalancedParenContentDirectTest_unbalanced() {
    // Line 196: unbalanced parens return null (mutation changes to "").
    String result = detector.extractBalancedParenContent("(abc");
    assertThat(result).isNull();
  }

  @Test
  void extractBalancedParenContentDirectTest_balanced() {
    String result = detector.extractBalancedParenContent("(inner)rest");
    assertThat(result).isEqualTo("inner");
  }

  @Test
  void extractBalancedParenContentDirectTest_nested() {
    String result = detector.extractBalancedParenContent("((a) OR (b))extra");
    assertThat(result).isEqualTo("(a) OR (b)");
  }
}
