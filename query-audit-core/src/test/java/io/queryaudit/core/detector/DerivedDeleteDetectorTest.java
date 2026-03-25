package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DerivedDeleteDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private final DerivedDeleteDetector detector = new DerivedDeleteDetector();

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  // ── Positive: Issues detected ───────────────────────────────────────

  @Nested
  @DisplayName("Detects derived delete pattern")
  class PositiveCases {

    @Test
    @DisplayName("Detects SELECT + 3 individual DELETEs (deleteByStatus pattern)")
    void detectsDeleteByStatusPattern() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT o.id, o.status, o.total FROM orders o WHERE o.status = 'CANCELLED'"),
              record("DELETE FROM orders WHERE id = 1"),
              record("DELETE FROM orders WHERE id = 2"),
              record("DELETE FROM orders WHERE id = 3"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.DERIVED_DELETE_LOADS_ENTITIES);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
      assertThat(issues.get(0).table()).isEqualTo("orders");
    }

    @Test
    @DisplayName("Detects pattern with many individual DELETEs")
    void detectsWithManyDeletes() {
      List<QueryRecord> queries = new ArrayList<>();
      queries.add(record("SELECT u.id, u.name FROM users u WHERE u.active = false"));
      for (int i = 1; i <= 10; i++) {
        queries.add(record("DELETE FROM users WHERE id = " + i));
      }

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).detail()).contains("10 individual DELETEs");
    }

    @Test
    @DisplayName("Detail includes table name")
    void detailIncludesTableName() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT id FROM orders WHERE status = 'EXPIRED'"),
              record("DELETE FROM orders WHERE id = 1"),
              record("DELETE FROM orders WHERE id = 2"),
              record("DELETE FROM orders WHERE id = 3"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues.get(0).detail()).contains("orders");
    }

    @Test
    @DisplayName("Suggestion mentions @Modifying @Query")
    void suggestionMentionsModifyingQuery() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT id FROM orders WHERE status = 'EXPIRED'"),
              record("DELETE FROM orders WHERE id = 1"),
              record("DELETE FROM orders WHERE id = 2"),
              record("DELETE FROM orders WHERE id = 3"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues.get(0).suggestion()).contains("@Modifying");
      assertThat(issues.get(0).suggestion()).contains("bulk");
    }

    @Test
    @DisplayName("Allows interleaved SELECTs (Hibernate loading associations)")
    void allowsInterleavedSelects() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT o.id, o.status FROM orders o WHERE o.status = 'CANCELLED'"),
              record("DELETE FROM orders WHERE id = 1"),
              record("SELECT a.id FROM addresses a WHERE a.order_id = 1"),
              record("DELETE FROM orders WHERE id = 2"),
              record("SELECT a.id FROM addresses a WHERE a.order_id = 2"),
              record("DELETE FROM orders WHERE id = 3"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).detail()).contains("3 individual DELETEs");
    }
  }

  // ── Negative: No issues ─────────────────────────────────────────────

  @Nested
  @DisplayName("No issue cases")
  class NegativeCases {

    @Test
    @DisplayName("No issue when fewer than 3 DELETEs follow SELECT")
    void noIssueWithFewDeletes() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT id FROM orders WHERE status = 'CANCELLED'"),
              record("DELETE FROM orders WHERE id = 1"),
              record("DELETE FROM orders WHERE id = 2"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue when SELECT has no WHERE clause")
    void noIssueWithoutWhereInSelect() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT id FROM orders"),
              record("DELETE FROM orders WHERE id = 1"),
              record("DELETE FROM orders WHERE id = 2"),
              record("DELETE FROM orders WHERE id = 3"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue when DELETEs are for different table")
    void noIssueWithDifferentTable() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT id FROM orders WHERE status = 'CANCELLED'"),
              record("DELETE FROM order_items WHERE id = 1"),
              record("DELETE FROM order_items WHERE id = 2"),
              record("DELETE FROM order_items WHERE id = 3"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for bulk DELETE (single statement)")
    void noIssueWithBulkDelete() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT id FROM orders WHERE status = 'CANCELLED'"),
              record("DELETE FROM orders WHERE status = 'CANCELLED'"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue when DELETE has complex WHERE (not PK)")
    void noIssueWithComplexDeleteWhere() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT id FROM orders WHERE status = 'CANCELLED'"),
              record("DELETE FROM orders WHERE id = 1 AND version = 3"),
              record("DELETE FROM orders WHERE id = 2 AND version = 1"),
              record("DELETE FROM orders WHERE id = 3 AND version = 2"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Empty query list")
    void emptyQueryList() {
      List<Issue> issues = detector.evaluate(List.of(), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Only DELETE queries without preceding SELECT")
    void onlyDeletes() {
      List<QueryRecord> queries =
          List.of(
              record("DELETE FROM orders WHERE id = 1"),
              record("DELETE FROM orders WHERE id = 2"),
              record("DELETE FROM orders WHERE id = 3"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }

  // ── Edge Cases ──────────────────────────────────────────────────────

  @Nested
  @DisplayName("Edge Cases")
  class EdgeCases {

    @Test
    @DisplayName("Custom threshold works")
    void customThreshold() {
      DerivedDeleteDetector strict = new DerivedDeleteDetector(2);

      List<QueryRecord> queries =
          List.of(
              record("SELECT id FROM orders WHERE status = 'CANCELLED'"),
              record("DELETE FROM orders WHERE id = 1"),
              record("DELETE FROM orders WHERE id = 2"));

      List<Issue> issues = strict.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Same table flagged only once")
    void sameTableFlaggedOnce() {
      List<QueryRecord> queries = new ArrayList<>();
      // First batch
      queries.add(record("SELECT id FROM orders WHERE status = 'CANCELLED'"));
      queries.add(record("DELETE FROM orders WHERE id = 1"));
      queries.add(record("DELETE FROM orders WHERE id = 2"));
      queries.add(record("DELETE FROM orders WHERE id = 3"));
      // Second batch for same table
      queries.add(record("SELECT id FROM orders WHERE status = 'EXPIRED'"));
      queries.add(record("DELETE FROM orders WHERE id = 10"));
      queries.add(record("DELETE FROM orders WHERE id = 20"));
      queries.add(record("DELETE FROM orders WHERE id = 30"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Multiple tables produce separate issues")
    void multipleTablesProduceSeparateIssues() {
      List<QueryRecord> queries = new ArrayList<>();
      queries.add(record("SELECT id FROM orders WHERE status = 'CANCELLED'"));
      queries.add(record("DELETE FROM orders WHERE id = 1"));
      queries.add(record("DELETE FROM orders WHERE id = 2"));
      queries.add(record("DELETE FROM orders WHERE id = 3"));
      queries.add(record("SELECT id FROM users WHERE active = false"));
      queries.add(record("DELETE FROM users WHERE id = 10"));
      queries.add(record("DELETE FROM users WHERE id = 20"));
      queries.add(record("DELETE FROM users WHERE id = 30"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(2);
    }

    @Test
    @DisplayName("No false positive when DELETEs have parameterized WHERE on PK")
    void noFalsePositiveWithParameterizedDeletes() {
      // This actually IS the derived delete pattern (parameterized), should still detect
      List<QueryRecord> queries =
          List.of(
              record("SELECT id FROM orders WHERE status = 'CANCELLED'"),
              record("DELETE FROM orders WHERE id = ?"),
              record("DELETE FROM orders WHERE id = ?"),
              record("DELETE FROM orders WHERE id = ?"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Interrupted by INSERT stops counting")
    void interruptedByInsert() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT id FROM orders WHERE status = 'CANCELLED'"),
              record("DELETE FROM orders WHERE id = 1"),
              record("DELETE FROM orders WHERE id = 2"),
              record("INSERT INTO audit_log (action) VALUES ('delete')"),
              record("DELETE FROM orders WHERE id = 3"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      // Only 2 DELETEs before the INSERT interrupted
      assertThat(issues).isEmpty();
    }
  }
}
