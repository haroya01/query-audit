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

class WriteAmplificationDetectorTest {

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private final WriteAmplificationDetector detector = new WriteAmplificationDetector();

  @Test
  void detectsTableWithTooManyIndexes() {
    // 7 indexes on one table (exceeds threshold of 6)
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "PRIMARY", "id", 1, false, 10000),
                    new IndexInfo("orders", "idx_user_id", "user_id", 1, true, 5000),
                    new IndexInfo("orders", "idx_status", "status", 1, true, 10),
                    new IndexInfo("orders", "idx_created_at", "created_at", 1, true, 10000),
                    new IndexInfo("orders", "idx_total", "total", 1, true, 8000),
                    new IndexInfo("orders", "idx_shipping", "shipping_address", 1, true, 9000),
                    new IndexInfo("orders", "idx_payment", "payment_method", 1, true, 100),
                    new IndexInfo("orders", "idx_tracking", "tracking_number", 1, true, 9500))));

    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM orders WHERE user_id = 1")), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.WRITE_AMPLIFICATION);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    assertThat(issues.get(0).table()).isEqualTo("orders");
    assertThat(issues.get(0).detail()).contains("orders");
    assertThat(issues.get(0).detail()).contains("8"); // 8 distinct index names
    assertThat(issues.get(0).suggestion()).contains("Review if all");
  }

  @Test
  void noIssueWhenIndexCountBelowThreshold() {
    // 3 indexes -- well below threshold
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "users",
                List.of(
                    new IndexInfo("users", "PRIMARY", "id", 1, false, 10000),
                    new IndexInfo("users", "idx_email", "email", 1, false, 10000),
                    new IndexInfo("users", "idx_name", "name", 1, true, 5000))));

    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM users WHERE id = 1")), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void countsCompositeIndexAsOneIndex() {
    // 6 distinct indexes (composite counts as 1) -- exactly at threshold, no issue
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "products",
                List.of(
                    new IndexInfo("products", "PRIMARY", "id", 1, false, 10000),
                    new IndexInfo("products", "idx_cat_name", "category_id", 1, true, 100),
                    new IndexInfo("products", "idx_cat_name", "name", 2, true, 5000),
                    new IndexInfo("products", "idx_price", "price", 1, true, 8000),
                    new IndexInfo("products", "idx_sku", "sku", 1, false, 10000),
                    new IndexInfo("products", "idx_brand", "brand", 1, true, 500),
                    new IndexInfo("products", "idx_created", "created_at", 1, true, 10000))));

    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM products WHERE id = 1")), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithEmptyMetadata() {
    List<Issue> issues =
        detector.evaluate(
            List.of(record("SELECT * FROM orders WHERE id = 1")), new IndexMetadata(Map.of()));

    assertThat(issues).isEmpty();
  }

  @Test
  void noIssueWithNullMetadata() {
    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM orders WHERE id = 1")), null);

    assertThat(issues).isEmpty();
  }

  @Test
  void noDuplicateIssuesForSameTable() {
    // Multiple queries referencing the same table should produce only one issue
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "PRIMARY", "id", 1, false, 10000),
                    new IndexInfo("orders", "idx_1", "col1", 1, true, 5000),
                    new IndexInfo("orders", "idx_2", "col2", 1, true, 5000),
                    new IndexInfo("orders", "idx_3", "col3", 1, true, 5000),
                    new IndexInfo("orders", "idx_4", "col4", 1, true, 5000),
                    new IndexInfo("orders", "idx_5", "col5", 1, true, 5000),
                    new IndexInfo("orders", "idx_6", "col6", 1, true, 5000),
                    new IndexInfo("orders", "idx_7", "col7", 1, true, 5000))));

    List<Issue> issues =
        detector.evaluate(
            List.of(
                record("SELECT * FROM orders WHERE col1 = 1"),
                record("SELECT * FROM orders WHERE col2 = 2")),
            metadata);

    assertThat(issues).hasSize(1);
  }

  @Test
  void skipsTablesWithNoIndexInfo() {
    // Table referenced in query but not present in metadata
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "other_table",
                List.of(new IndexInfo("other_table", "PRIMARY", "id", 1, false, 100))));

    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM orders WHERE id = 1")), metadata);

    assertThat(issues).isEmpty();
  }

  @Test
  void excludesAutoGeneratedFkIndexes() {
    // 9 total indexes but 3 are auto-generated FK/CONSTRAINT indexes from H2.
    // After filtering: 6 user-defined indexes = at threshold, no issue.
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "PRIMARY", "id", 1, false, 10000),
                    new IndexInfo("orders", "idx_user_id", "user_id", 1, true, 5000),
                    new IndexInfo("orders", "idx_status", "status", 1, true, 10),
                    new IndexInfo("orders", "idx_created_at", "created_at", 1, true, 10000),
                    new IndexInfo("orders", "idx_total", "total", 1, true, 8000),
                    new IndexInfo("orders", "idx_shipping", "shipping_address", 1, true, 9000),
                    new IndexInfo("orders", "FK_orders_user", "user_id", 1, true, 5000),
                    new IndexInfo("orders", "CONSTRAINT_123", "status", 1, true, 10),
                    new IndexInfo("orders", "SYS_IDX_001", "created_at", 1, true, 10000))));

    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM orders WHERE user_id = 1")), metadata);

    // 6 user-defined indexes <= threshold of 6, no issue
    assertThat(issues).isEmpty();
  }

  @Test
  void stillDetectsExcessiveUserDefinedIndexes() {
    // 8 user-defined indexes + 2 auto-generated = 10 total.
    // After filtering auto-generated: 8 > 6 threshold -> issue.
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "PRIMARY", "id", 1, false, 10000),
                    new IndexInfo("orders", "idx_user_id", "user_id", 1, true, 5000),
                    new IndexInfo("orders", "idx_status", "status", 1, true, 10),
                    new IndexInfo("orders", "idx_created_at", "created_at", 1, true, 10000),
                    new IndexInfo("orders", "idx_total", "total", 1, true, 8000),
                    new IndexInfo("orders", "idx_shipping", "shipping_address", 1, true, 9000),
                    new IndexInfo("orders", "idx_payment", "payment_method", 1, true, 100),
                    new IndexInfo("orders", "idx_tracking", "tracking_number", 1, true, 9500),
                    new IndexInfo("orders", "FK_orders_user", "user_id", 1, true, 5000),
                    new IndexInfo("orders", "CONSTRAINT_456", "status", 1, true, 10))));

    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM orders WHERE user_id = 1")), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("8"); // 8 user-defined indexes
  }

  @Test
  void customThreshold() {
    WriteAmplificationDetector customDetector = new WriteAmplificationDetector(3);

    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "users",
                List.of(
                    new IndexInfo("users", "PRIMARY", "id", 1, false, 10000),
                    new IndexInfo("users", "idx_email", "email", 1, false, 10000),
                    new IndexInfo("users", "idx_name", "name", 1, true, 5000),
                    new IndexInfo("users", "idx_phone", "phone", 1, true, 5000))));

    List<Issue> issues =
        customDetector.evaluate(List.of(record("SELECT * FROM users WHERE id = 1")), metadata);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).detail()).contains("4");
  }

  // ── Mutation-killing tests ──────────────────────────────────────────

  /**
   * Kills EmptyObjectReturnValsMutator on line 43: replaced return with Collections.emptyList. When
   * indexMetadata is valid and issues ARE detected, the returned list must NOT be empty.
   */
  @Test
  void evaluateReturnsNonEmptyWhenIssueDetected_killsLine43() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "PRIMARY", "id", 1, false, 10000),
                    new IndexInfo("orders", "idx_1", "c1", 1, true, 5000),
                    new IndexInfo("orders", "idx_2", "c2", 1, true, 5000),
                    new IndexInfo("orders", "idx_3", "c3", 1, true, 5000),
                    new IndexInfo("orders", "idx_4", "c4", 1, true, 5000),
                    new IndexInfo("orders", "idx_5", "c5", 1, true, 5000),
                    new IndexInfo("orders", "idx_6", "c6", 1, true, 5000),
                    new IndexInfo("orders", "idx_7", "c7", 1, true, 5000))));

    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM orders WHERE id = 1")), metadata);

    assertThat(issues).isNotEmpty();
    assertThat(issues.get(0).type()).isEqualTo(IssueType.WRITE_AMPLIFICATION);
    assertThat(issues.get(0).table()).isEqualTo("orders");
  }

  /**
   * Kills BooleanTrueReturnValsMutator on line 72: filter lambda always returns true. If the
   * isAutoGeneratedIndex filter always returns true (passes everything through), auto-generated
   * indexes like FK_ would be counted, inflating the count. With this test: 4 user-defined + 3
   * auto-generated = 7 total. If filter is correct: 4 user-defined <= 6 threshold -> no issue. If
   * filter always true: 7 total > 6 threshold -> issue (wrong!).
   */
  @Test
  void autoGeneratedIndexFilterCorrectlyExcludes_killsLine72() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    // 4 user-defined indexes
                    new IndexInfo("orders", "PRIMARY", "id", 1, false, 10000),
                    new IndexInfo("orders", "idx_user", "user_id", 1, true, 5000),
                    new IndexInfo("orders", "idx_status", "status", 1, true, 10),
                    new IndexInfo("orders", "idx_date", "created_at", 1, true, 10000),
                    // 3 auto-generated indexes (should be excluded)
                    new IndexInfo("orders", "FK_orders_user", "user_id", 1, true, 5000),
                    new IndexInfo("orders", "CONSTRAINT_abc", "status", 1, true, 10),
                    new IndexInfo("orders", "SYS_IDX_001", "created_at", 1, true, 10000))));

    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM orders WHERE user_id = 1")), metadata);

    // 4 user-defined <= 6 threshold -> no issue
    // If mutation makes filter always return true, all 7 pass -> 7 > 6 -> issue (wrong!)
    assertThat(issues).isEmpty();
  }

  @Test
  void excludesFkIdxAutoGeneratedIndexes() {
    // FKIDX_ prefix should be auto-generated and excluded
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "PRIMARY", "id", 1, false, 10000),
                    new IndexInfo("orders", "idx_1", "c1", 1, true, 5000),
                    new IndexInfo("orders", "idx_2", "c2", 1, true, 5000),
                    new IndexInfo("orders", "idx_3", "c3", 1, true, 5000),
                    new IndexInfo("orders", "idx_4", "c4", 1, true, 5000),
                    new IndexInfo("orders", "idx_5", "c5", 1, true, 5000),
                    new IndexInfo("orders", "FKIDX_orders_user", "user_id", 1, true, 5000))));

    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM orders WHERE id = 1")), metadata);

    // 6 user-defined (PRIMARY + 5 idx_) <= 6 threshold -> no issue
    assertThat(issues).isEmpty();
  }

  /**
   * Additional test: null index name should be filtered out by the filter lambda. Kills the name !=
   * null filter being replaced with always-true.
   */
  @Test
  void nullIndexNameIsFilteredOut_killsLine72() {
    IndexMetadata metadata =
        new IndexMetadata(
            Map.of(
                "orders",
                List.of(
                    new IndexInfo("orders", "PRIMARY", "id", 1, false, 10000),
                    new IndexInfo("orders", "idx_1", "c1", 1, true, 5000),
                    new IndexInfo("orders", "idx_2", "c2", 1, true, 5000),
                    new IndexInfo("orders", "idx_3", "c3", 1, true, 5000),
                    new IndexInfo("orders", "idx_4", "c4", 1, true, 5000),
                    new IndexInfo("orders", "idx_5", "c5", 1, true, 5000),
                    new IndexInfo("orders", "idx_6", "c6", 1, true, 5000),
                    new IndexInfo("orders", null, "c7", 1, true, 5000) // null index name
                    )));

    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM orders WHERE id = 1")), metadata);

    // 7 named user-defined indexes (including PRIMARY) > 6 threshold -> issue
    // The null-named one should be excluded, leaving 7 > 6 -> issue
    assertThat(issues).hasSize(1);
  }
}
