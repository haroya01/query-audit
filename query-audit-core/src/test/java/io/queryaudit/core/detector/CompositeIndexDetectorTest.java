package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CompositeIndexDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final CompositeIndexDetector detector = new CompositeIndexDetector();

  /**
   * Kills EmptyObjectReturnValsMutator on line 28: replaced return value with
   * Collections.emptyList. When indexMetadata is null or empty, evaluate returns early with empty
   * list. But when there IS metadata and an issue IS detected, the returned list must NOT be empty.
   */
  @Test
  void evaluateReturnsNonEmptyListWhenIssueDetected_killsLine28() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "idx_status_created", "status", 1, true, 100),
                    new IndexInfo("orders", "idx_status_created", "created_at", 2, true, 100))));

    List<Issue> issues =
        detector.evaluate(
            List.of(record("SELECT * FROM orders WHERE created_at > '2024-01-01'")), metadata);

    assertThat(issues).isNotEmpty();
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.COMPOSITE_INDEX_LEADING_COLUMN);
  }

  /**
   * Kills NegateConditionalsMutator on line 130: nonLeadingUsedColumn != null negated. If negated,
   * the suggestion would say "the used column" instead of the actual column name.
   */
  @Test
  void suggestionContainsActualColumnName_killsLine130() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "idx_status_created", "status", 1, true, 100),
                    new IndexInfo("orders", "idx_status_created", "created_at", 2, true, 100))));

    List<Issue> issues =
        detector.evaluate(
            List.of(record("SELECT * FROM orders WHERE created_at > '2024-01-01'")), metadata);

    assertThat(issues).hasSize(1);
    // The suggestion should contain the actual column name, not "the used column"
    assertThat(issues.get(0).suggestion()).contains("created_at");
    assertThat(issues.get(0).suggestion()).doesNotContain("the used column");
  }

  /**
   * Kills BooleanTrueReturnValsMutator on line 98: lambda$evaluate$1 always returns true. If the
   * filter in anyNonLeadingUsed always returns true (even for null columnName), it might find a
   * match when it shouldn't. Test with functional index (null columnName).
   */
  @Test
  void functionalIndexWithNullColumnName_killsLine98() {
    // A composite index where the non-leading column has null columnName (functional index)
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "idx_func", "status", 1, true, 100),
                    new IndexInfo(
                        "orders", "idx_func", null, 2, true, 100) // functional index column
                    )));

    // Query uses a column NOT in the composite index
    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM orders WHERE total > 100")), metadata);

    // No non-leading column matches "total", so no issue should be reported.
    // If mutation makes filter return true for null columnName, it would incorrectly match.
    assertThat(issues).isEmpty();
  }

  /**
   * Kills BooleanTrueReturnValsMutator on line 104: lambda$evaluate$3 always returns true. If the
   * filter always returns true, it would find a non-leading column even when none matches the WHERE
   * columns, leading to a false positive.
   */
  @Test
  void noMatchingNonLeadingColumn_killsLine104() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "idx_a_b", "col_a", 1, true, 100),
                    new IndexInfo("orders", "idx_a_b", "col_b", 2, true, 100))));

    // WHERE uses col_c which is NOT in the composite index at all
    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM orders WHERE col_c = 'x'")), metadata);

    // Neither leading nor non-leading column matches -> no issue
    assertThat(issues).isEmpty();
  }

  /**
   * Kills ConditionalsBoundaryMutator on line 159: cols.size() > 1 changed to >= 1. In
   * hasStandaloneIndex, when checking if a column is the leading column of another composite index,
   * the check `cols.size() > 1` ensures we only look at multi-column indexes for the "leading
   * column" check. If changed to >= 1, single-column indexes would also enter the "leading column"
   * path.
   */
  @Test
  void hasStandaloneIndexBoundary_killsLine159() {
    // Create a scenario where the boundary matters:
    // composite index (a, b) and a single-column index on b.
    // The single-column index on b should be found via the cols.size() == 1 check,
    // NOT the cols.size() > 1 (leading column) check.
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "t",
                List.of(
                    new IndexInfo("t", "idx_a_b", "a", 1, true, 100),
                    new IndexInfo("t", "idx_a_b", "b", 2, true, 100),
                    // Single-column index on b
                    new IndexInfo("t", "idx_b", "b", 1, true, 100))));

    // Query WHERE b = 1 (non-leading column of idx_a_b, but covered by idx_b)
    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM t WHERE b = 1")), metadata);

    // Should NOT report because idx_b covers column b standalone
    assertThat(issues).isEmpty();
  }

  /**
   * Kills ConditionalsBoundaryMutator on line 231: aliasToTable.size() <= 2 changed boundary. In
   * resolveTable, when tableOrAlias is null AND aliasToTable has <= 2 entries, it falls back to the
   * first value. If changed to < 2, single-entry alias maps would not trigger the fallback.
   */
  @Test
  void resolveTableFallbackWithSmallAliasMap_killsLine231() {
    // Use a query with a single table and alias where the WHERE column
    // has no table qualifier. The alias map will have exactly 1 entry (or 2 with the table itself).
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "idx_status_date", "status", 1, true, 100),
                    new IndexInfo("orders", "idx_status_date", "created_at", 2, true, 100))));

    // Unqualified column reference with single table -> resolveTable should fall back
    List<Issue> issues =
        detector.evaluate(
            List.of(record("SELECT * FROM orders WHERE created_at > '2024-01-01'")), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).table()).isEqualTo("orders");
  }
}
