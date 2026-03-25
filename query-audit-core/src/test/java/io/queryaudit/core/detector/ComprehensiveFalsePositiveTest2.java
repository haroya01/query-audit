package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.regression.QueryCountRegressionDetector;
import io.queryaudit.core.regression.QueryCounts;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive false-positive verification test suite (Part 2). Covers detectors #16-#29:
 * SlowQuery, UnboundedResultSet, WriteAmplification, ImplicitTypeConversion, UnionWithoutAll,
 * CoveringIndex, OrderByLimitWithoutIndex, LargeInList, DistinctMisuse, NullComparison,
 * HavingMisuse, RangeLock, LazyLoadNPlusOne, and QueryCountRegression.
 */
class ComprehensiveFalsePositiveTest2 {

  // ── Helper methods ──────────────────────────────────────────────────

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private static QueryRecord record(String sql, long executionTimeNanos) {
    return new QueryRecord(sql, executionTimeNanos, System.currentTimeMillis(), "");
  }

  private static List<QueryRecord> queries(String... sqls) {
    List<QueryRecord> list = new ArrayList<>();
    for (String sql : sqls) {
      list.add(record(sql));
    }
    return list;
  }

  private static IndexInfo indexInfo(String table, String indexName, String column, int seq) {
    return new IndexInfo(table, indexName, column, seq, true, 1000);
  }

  private static IndexInfo primaryKey(String table, String column) {
    return new IndexInfo(table, "PRIMARY", column, 1, false, 1000);
  }

  private static IndexMetadata indexMetadata(String table, IndexInfo... indexes) {
    return new IndexMetadata(Map.of(table, List.of(indexes)));
  }

  private static IndexMetadata multiTableIndexMetadata(
      Map<String, List<IndexInfo>> indexesByTable) {
    return new IndexMetadata(indexesByTable);
  }

  private static LazyLoadTracker.LazyLoadRecord lazyLoadRecord(
      String role, String ownerEntity, String ownerId) {
    return new LazyLoadTracker.LazyLoadRecord(
        role, ownerEntity, ownerId, System.currentTimeMillis());
  }

  // ── 16. SlowQueryDetector ───────────────────────────────────────────

  @Nested
  @DisplayName("16. SlowQueryDetector")
  class SlowQueryDetectorTests {

    private final SlowQueryDetector detector = new SlowQueryDetector();

    @Test
    @DisplayName("TP: 600ms query triggers WARNING (median-based, threshold 500ms)")
    void truePositive_warningLevel() {
      long nanos = TimeUnit.MILLISECONDS.toNanos(600);
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE name = 'test'", nanos)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.SLOW_QUERY);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    }

    @Test
    @DisplayName("TP: 4000ms query triggers ERROR (median-based, threshold 3000ms)")
    void truePositive_errorLevel() {
      long nanos = TimeUnit.MILLISECONDS.toNanos(4000);
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM orders WHERE status = 'ACTIVE'", nanos)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.SLOW_QUERY);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
    }

    @Test
    @DisplayName("TN: 200ms query does NOT trigger (below 500ms threshold, avoids H2/CI noise)")
    void trueNegative_h2CiNoise() {
      long nanos = TimeUnit.MILLISECONDS.toNanos(200);
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE name = 'test'", nanos)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: One-off spike ignored when median is low")
    void trueNegative_oneOffSpike() {
      // 5 executions: 4 fast + 1 spike. Median = 50ms -> no issue
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record("SELECT * FROM users WHERE id = 1", TimeUnit.MILLISECONDS.toNanos(50)),
                  record("SELECT * FROM users WHERE id = 2", TimeUnit.MILLISECONDS.toNanos(50)),
                  record("SELECT * FROM users WHERE id = 3", TimeUnit.MILLISECONDS.toNanos(50)),
                  record("SELECT * FROM users WHERE id = 4", TimeUnit.MILLISECONDS.toNanos(50)),
                  record("SELECT * FROM users WHERE id = 5", TimeUnit.MILLISECONDS.toNanos(2000))),
              EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: 10ms query does not trigger")
    void trueNegative_fastQuery() {
      long nanos = TimeUnit.MILLISECONDS.toNanos(10);
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE id = 1", nanos)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: 0 execution time (mock/test) does not trigger")
    void trueNegative_zeroExecutionTime() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE id = 1", 0L)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }

  // ── 17. UnboundedResultSetDetector ───────────────────────────────────

  @Nested
  @DisplayName("17. UnboundedResultSetDetector")
  class UnboundedResultSetDetectorTests {

    private final UnboundedResultSetDetector detector = new UnboundedResultSetDetector();

    @Test
    @DisplayName("TP: SELECT without LIMIT triggers WARNING")
    void truePositive_noLimit() {
      List<Issue> issues =
          detector.evaluate(
              queries("SELECT * FROM orders WHERE user_id = ? AND status = 'ACTIVE'"), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.UNBOUNDED_RESULT_SET);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    }

    @Test
    @DisplayName("TN: SELECT with LIMIT does not trigger")
    void trueNegative_withLimit() {
      List<Issue> issues =
          detector.evaluate(
              queries("SELECT * FROM orders WHERE user_id = ? LIMIT 20"), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: Aggregate query (COUNT) does not trigger")
    void trueNegative_aggregate() {
      List<Issue> issues = detector.evaluate(queries("SELECT COUNT(*) FROM orders"), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: Primary key lookup does not trigger")
    void trueNegative_pkLookup() {
      List<Issue> issues =
          detector.evaluate(queries("SELECT * FROM orders WHERE id = ?"), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: FOR UPDATE does not trigger")
    void trueNegative_forUpdate() {
      List<Issue> issues =
          detector.evaluate(queries("SELECT * FROM orders FOR UPDATE"), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: No FROM clause (SELECT 1) does not trigger")
    void trueNegative_noFrom() {
      List<Issue> issues = detector.evaluate(queries("SELECT 1"), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }

  // ── 18. WriteAmplificationDetector ──────────────────────────────────

  @Nested
  @DisplayName("18. WriteAmplificationDetector")
  class WriteAmplificationDetectorTests {

    private final WriteAmplificationDetector detector = new WriteAmplificationDetector();

    @Test
    @DisplayName("TP: Table with 7+ indexes triggers WARNING")
    void truePositive_tooManyIndexes() {
      List<IndexInfo> indexes = new ArrayList<>();
      for (int i = 1; i <= 7; i++) {
        indexes.add(indexInfo("orders", "idx_" + i, "col_" + i, 1));
      }
      IndexMetadata metadata = new IndexMetadata(Map.of("orders", indexes));

      List<Issue> issues =
          detector.evaluate(queries("SELECT * FROM orders WHERE id = 1"), metadata);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.WRITE_AMPLIFICATION);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    }

    @Test
    @DisplayName("TN: Table with 3 indexes does not trigger")
    void trueNegative_fewIndexes() {
      List<IndexInfo> indexes =
          List.of(
              indexInfo("orders", "idx_1", "col_1", 1),
              indexInfo("orders", "idx_2", "col_2", 1),
              indexInfo("orders", "idx_3", "col_3", 1));
      IndexMetadata metadata = new IndexMetadata(Map.of("orders", indexes));

      List<Issue> issues =
          detector.evaluate(queries("SELECT * FROM orders WHERE id = 1"), metadata);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: No index metadata does not trigger")
    void trueNegative_noMetadata() {
      List<Issue> issues =
          detector.evaluate(queries("SELECT * FROM orders WHERE id = 1"), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }

  // ── 19. ImplicitTypeConversionDetector ──────────────────────────────

  @Nested
  @DisplayName("19. ImplicitTypeConversionDetector")
  class ImplicitTypeConversionDetectorTests {

    private final ImplicitTypeConversionDetector detector = new ImplicitTypeConversionDetector();

    @Test
    @DisplayName("TP: String column compared to numeric literal")
    void truePositive_stringColumnNumericLiteral() {
      List<Issue> issues =
          detector.evaluate(queries("SELECT * FROM users WHERE user_name = 123"), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.IMPLICIT_TYPE_CONVERSION);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    }

    @Test
    @DisplayName("TP: phone_code column compared to numeric literal")
    void truePositive_phoneCode() {
      List<Issue> issues =
          detector.evaluate(queries("SELECT * FROM users WHERE phone_code = 82"), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.IMPLICIT_TYPE_CONVERSION);
    }

    @Test
    @DisplayName("TN: String column compared to quoted string does not trigger")
    void trueNegative_quotedString() {
      List<Issue> issues =
          detector.evaluate(queries("SELECT * FROM users WHERE user_name = '123'"), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: id column with numeric literal does not trigger")
    void trueNegative_idColumn() {
      List<Issue> issues =
          detector.evaluate(queries("SELECT * FROM users WHERE user_id = 123"), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: Non-string-indicator column does not trigger")
    void trueNegative_nonStringColumn() {
      List<Issue> issues =
          detector.evaluate(queries("SELECT * FROM users WHERE count = 5"), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }

  // ── 20. UnionWithoutAllDetector ─────────────────────────────────────

  @Nested
  @DisplayName("20. UnionWithoutAllDetector")
  class UnionWithoutAllDetectorTests {

    private final UnionWithoutAllDetector detector = new UnionWithoutAllDetector();

    @Test
    @DisplayName("TP: UNION without ALL triggers INFO")
    void truePositive_unionWithoutAll() {
      List<Issue> issues =
          detector.evaluate(queries("SELECT id FROM a UNION SELECT id FROM b"), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.UNION_WITHOUT_ALL);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    @Test
    @DisplayName("TN: UNION ALL does not trigger")
    void trueNegative_unionAll() {
      List<Issue> issues =
          detector.evaluate(queries("SELECT id FROM a UNION ALL SELECT id FROM b"), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: No UNION at all does not trigger")
    void trueNegative_noUnion() {
      List<Issue> issues = detector.evaluate(queries("SELECT * FROM a"), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }

  // ── 21. CoveringIndexDetector ───────────────────────────────────────

  @Nested
  @DisplayName("21. CoveringIndexDetector")
  class CoveringIndexDetectorTests {

    private final CoveringIndexDetector detector = new CoveringIndexDetector();

    @Test
    @DisplayName("TP: SELECT columns not in index suggests covering index")
    void truePositive_coveringIndexOpportunity() {
      IndexMetadata metadata =
          indexMetadata("users", indexInfo("users", "idx_status", "status", 1));

      List<Issue> issues =
          detector.evaluate(queries("SELECT name, email FROM users WHERE status = ?"), metadata);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.COVERING_INDEX_OPPORTUNITY);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    @Test
    @DisplayName("TN: SELECT columns already in index does not trigger")
    void trueNegative_alreadyCovered() {
      IndexMetadata metadata =
          indexMetadata("users", indexInfo("users", "idx_status", "status", 1));

      List<Issue> issues =
          detector.evaluate(queries("SELECT status FROM users WHERE status = ?"), metadata);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: SELECT * does not trigger (too many columns)")
    void trueNegative_selectAll() {
      IndexMetadata metadata =
          indexMetadata("users", indexInfo("users", "idx_status", "status", 1));

      List<Issue> issues =
          detector.evaluate(queries("SELECT * FROM users WHERE status = ?"), metadata);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: No index metadata does not trigger")
    void trueNegative_noMetadata() {
      List<Issue> issues =
          detector.evaluate(queries("SELECT name, email FROM users WHERE status = ?"), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }

  // ── 22. OrderByLimitWithoutIndexDetector ─────────────────────────────

  @Nested
  @DisplayName("22. OrderByLimitWithoutIndexDetector")
  class OrderByLimitWithoutIndexDetectorTests {

    private final OrderByLimitWithoutIndexDetector detector =
        new OrderByLimitWithoutIndexDetector();

    @Test
    @DisplayName("TP: ORDER BY + LIMIT without index on ORDER BY column")
    void truePositive_noIndexOnOrderByColumn() {
      IndexMetadata metadata = indexMetadata("orders", primaryKey("orders", "id"));

      List<Issue> issues =
          detector.evaluate(queries("SELECT * FROM orders ORDER BY created_at LIMIT 10"), metadata);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.ORDER_BY_LIMIT_WITHOUT_INDEX);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    }

    @Test
    @DisplayName("TN: ORDER BY + LIMIT WITH index on ORDER BY column does not trigger")
    void trueNegative_withIndex() {
      IndexMetadata metadata =
          indexMetadata(
              "orders",
              primaryKey("orders", "id"),
              indexInfo("orders", "idx_created_at", "created_at", 1));

      List<Issue> issues =
          detector.evaluate(queries("SELECT * FROM orders ORDER BY created_at LIMIT 10"), metadata);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: ORDER BY on PK does not trigger")
    void trueNegative_orderByPK() {
      IndexMetadata metadata = indexMetadata("orders", primaryKey("orders", "id"));

      List<Issue> issues =
          detector.evaluate(queries("SELECT * FROM orders ORDER BY id LIMIT 10"), metadata);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: ORDER BY without LIMIT does not trigger")
    void trueNegative_noLimit() {
      IndexMetadata metadata = indexMetadata("orders", primaryKey("orders", "id"));

      List<Issue> issues =
          detector.evaluate(queries("SELECT * FROM orders ORDER BY created_at"), metadata);

      assertThat(issues).isEmpty();
    }
  }

  // ── 23. LargeInListDetector ─────────────────────────────────────────

  @Nested
  @DisplayName("23. LargeInListDetector")
  class LargeInListDetectorTests {

    private final LargeInListDetector detector = new LargeInListDetector();

    @Test
    @DisplayName("TP: IN clause with 150 items triggers WARNING")
    void truePositive_largeInList() {
      String values =
          String.join(
              ",", IntStream.rangeClosed(1, 150).mapToObj(String::valueOf).toArray(String[]::new));
      String sql = "SELECT * FROM orders WHERE id IN (" + values + ")";

      List<Issue> issues = detector.evaluate(queries(sql), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.LARGE_IN_LIST);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    }

    @Test
    @DisplayName("TN: IN clause with 3 items does not trigger")
    void trueNegative_smallInList() {
      List<Issue> issues =
          detector.evaluate(queries("SELECT * FROM orders WHERE id IN (1, 2, 3)"), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: IN clause with single parameter does not trigger")
    void trueNegative_singleParameter() {
      List<Issue> issues =
          detector.evaluate(queries("SELECT * FROM orders WHERE id IN (?)"), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }

  // ── 24. DistinctMisuseDetector ──────────────────────────────────────

  @Nested
  @DisplayName("24. DistinctMisuseDetector")
  class DistinctMisuseDetectorTests {

    private final DistinctMisuseDetector detector = new DistinctMisuseDetector();

    @Test
    @DisplayName("TP: DISTINCT with JOIN triggers WARNING")
    void truePositive_distinctWithJoin() {
      List<Issue> issues =
          detector.evaluate(
              queries("SELECT DISTINCT a.id FROM a JOIN b ON a.id = b.a_id"), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.DISTINCT_MISUSE);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    }

    @Test
    @DisplayName("TP: DISTINCT with GROUP BY triggers WARNING")
    void truePositive_distinctWithGroupBy() {
      List<Issue> issues =
          detector.evaluate(
              queries("SELECT DISTINCT status, COUNT(*) FROM orders GROUP BY status"), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.DISTINCT_MISUSE);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    }

    @Test
    @DisplayName("TN: Legitimate DISTINCT does not trigger")
    void trueNegative_legitimateDistinct() {
      List<Issue> issues =
          detector.evaluate(queries("SELECT DISTINCT email FROM users"), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: No DISTINCT does not trigger")
    void trueNegative_noDistinct() {
      List<Issue> issues = detector.evaluate(queries("SELECT * FROM orders"), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }

  // ── 25. NullComparisonDetector ──────────────────────────────────────

  @Nested
  @DisplayName("25. NullComparisonDetector")
  class NullComparisonDetectorTests {

    private final NullComparisonDetector detector = new NullComparisonDetector();

    @Test
    @DisplayName("TP: = NULL triggers ERROR")
    void truePositive_equalsNull() {
      List<Issue> issues =
          detector.evaluate(queries("SELECT * FROM orders WHERE status = NULL"), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.NULL_COMPARISON);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
    }

    @Test
    @DisplayName("TP: != NULL triggers ERROR")
    void truePositive_notEqualsNull() {
      List<Issue> issues =
          detector.evaluate(queries("SELECT * FROM orders WHERE status != NULL"), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.NULL_COMPARISON);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
    }

    @Test
    @DisplayName("TP: <> NULL triggers ERROR")
    void truePositive_diamondNotEqualsNull() {
      List<Issue> issues =
          detector.evaluate(queries("SELECT * FROM orders WHERE status <> NULL"), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.NULL_COMPARISON);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
    }

    @Test
    @DisplayName("TN: IS NULL does not trigger")
    void trueNegative_isNull() {
      List<Issue> issues =
          detector.evaluate(queries("SELECT * FROM orders WHERE status IS NULL"), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: IS NOT NULL does not trigger")
    void trueNegative_isNotNull() {
      List<Issue> issues =
          detector.evaluate(queries("SELECT * FROM orders WHERE status IS NOT NULL"), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: COALESCE with NULL does not trigger")
    void trueNegative_coalesce() {
      // COALESCE(status, NULL) does not have a WHERE comparison pattern
      List<Issue> issues =
          detector.evaluate(
              queries("SELECT COALESCE(status, NULL) FROM orders WHERE id = 1"), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }

  // ── 26. HavingMisuseDetector ────────────────────────────────────────

  @Nested
  @DisplayName("26. HavingMisuseDetector")
  class HavingMisuseDetectorTests {

    private final HavingMisuseDetector detector = new HavingMisuseDetector();

    @Test
    @DisplayName("TP: HAVING on non-aggregate column triggers WARNING")
    void truePositive_havingOnNonAggregate() {
      List<Issue> issues =
          detector.evaluate(
              queries("SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING dept = 'sales'"),
              EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.HAVING_MISUSE);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    }

    @Test
    @DisplayName("TN: HAVING with aggregate function does not trigger")
    void trueNegative_havingWithAggregate() {
      List<Issue> issues =
          detector.evaluate(
              queries("SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING COUNT(*) > 5"),
              EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: No HAVING does not trigger")
    void trueNegative_noHaving() {
      List<Issue> issues = detector.evaluate(queries("SELECT * FROM emp"), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }

  // ── 27. RangeLockDetector ───────────────────────────────────────────

  @Nested
  @DisplayName("27. RangeLockDetector")
  class RangeLockDetectorTests {

    private final RangeLockDetector detector = new RangeLockDetector();

    @Test
    @DisplayName("TP: Range FOR UPDATE without index triggers WARNING")
    void truePositive_rangeLockNoIndex() {
      IndexMetadata metadata = indexMetadata("orders", primaryKey("orders", "id"));

      List<Issue> issues =
          detector.evaluate(
              queries("SELECT * FROM orders WHERE created_at > '2024-01-01' FOR UPDATE"), metadata);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.RANGE_LOCK_RISK);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    }

    @Test
    @DisplayName("TN: Range FOR UPDATE WITH index does not trigger")
    void trueNegative_rangeLockWithIndex() {
      IndexMetadata metadata =
          indexMetadata(
              "orders",
              primaryKey("orders", "id"),
              indexInfo("orders", "idx_created_at", "created_at", 1));

      List<Issue> issues =
          detector.evaluate(
              queries("SELECT * FROM orders WHERE created_at > '2024-01-01' FOR UPDATE"), metadata);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: Equality FOR UPDATE does not trigger (not range)")
    void trueNegative_equalityForUpdate() {
      IndexMetadata metadata = indexMetadata("orders", primaryKey("orders", "id"));

      List<Issue> issues =
          detector.evaluate(queries("SELECT * FROM orders WHERE id = 1 FOR UPDATE"), metadata);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: Range without FOR UPDATE does not trigger")
    void trueNegative_noForUpdate() {
      IndexMetadata metadata = indexMetadata("orders", primaryKey("orders", "id"));

      List<Issue> issues =
          detector.evaluate(
              queries("SELECT * FROM orders WHERE created_at > '2024-01-01'"), metadata);

      assertThat(issues).isEmpty();
    }
  }

  // ── 28. LazyLoadNPlusOneDetector ────────────────────────────────────

  @Nested
  @DisplayName("28. LazyLoadNPlusOneDetector")
  class LazyLoadNPlusOneDetectorTests {

    private final LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector();

    @Test
    @DisplayName("TP: 5 records with same role, different owner IDs triggers ERROR")
    void truePositive_nPlusOneDetected() {
      List<LazyLoadTracker.LazyLoadRecord> records =
          List.of(
              lazyLoadRecord("com.example.Order.items", "com.example.Order", "1"),
              lazyLoadRecord("com.example.Order.items", "com.example.Order", "2"),
              lazyLoadRecord("com.example.Order.items", "com.example.Order", "3"),
              lazyLoadRecord("com.example.Order.items", "com.example.Order", "4"),
              lazyLoadRecord("com.example.Order.items", "com.example.Order", "5"));

      List<Issue> issues = detector.evaluate(records);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.N_PLUS_ONE);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
    }

    @Test
    @DisplayName("TN: 2 records (below threshold) does not trigger")
    void trueNegative_belowThreshold() {
      List<LazyLoadTracker.LazyLoadRecord> records =
          List.of(
              lazyLoadRecord("com.example.Order.items", "com.example.Order", "1"),
              lazyLoadRecord("com.example.Order.items", "com.example.Order", "2"));

      List<Issue> issues = detector.evaluate(records);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: 5 records same role, same owner ID does not trigger (reload, not N+1)")
    void trueNegative_sameOwnerReload() {
      List<LazyLoadTracker.LazyLoadRecord> records =
          List.of(
              lazyLoadRecord("com.example.Order.items", "com.example.Order", "1"),
              lazyLoadRecord("com.example.Order.items", "com.example.Order", "1"),
              lazyLoadRecord("com.example.Order.items", "com.example.Order", "1"),
              lazyLoadRecord("com.example.Order.items", "com.example.Order", "1"),
              lazyLoadRecord("com.example.Order.items", "com.example.Order", "1"));

      List<Issue> issues = detector.evaluate(records);

      assertThat(issues).isEmpty();
    }
  }

  // ── 29. QueryCountRegressionDetector ────────────────────────────────

  @Nested
  @DisplayName("29. QueryCountRegressionDetector")
  class QueryCountRegressionDetectorTests {

    private final QueryCountRegressionDetector detector = new QueryCountRegressionDetector();

    @Test
    @DisplayName("TP: Baseline 10 -> current 25 (2.5x, +15) triggers WARNING")
    void truePositive_warningRegression() {
      QueryCounts baseline = new QueryCounts(10, 0, 0, 0, 10);
      QueryCounts current = new QueryCounts(25, 0, 0, 0, 25);

      List<Issue> issues = detector.detect("TestClass", "testMethod", current, baseline);

      assertThat(issues).isNotEmpty();
      assertThat(issues.get(0).type()).isEqualTo(IssueType.QUERY_COUNT_REGRESSION);
      assertThat(issues.get(0).severity()).isIn(Severity.WARNING, Severity.ERROR);
    }

    @Test
    @DisplayName("TP: Baseline 5 -> current 50 (10x) triggers ERROR")
    void truePositive_errorRegression() {
      QueryCounts baseline = new QueryCounts(5, 0, 0, 0, 5);
      QueryCounts current = new QueryCounts(50, 0, 0, 0, 50);

      List<Issue> issues = detector.detect("TestClass", "testMethod", current, baseline);

      assertThat(issues).isNotEmpty();
      assertThat(issues.get(0).type()).isEqualTo(IssueType.QUERY_COUNT_REGRESSION);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
    }

    @Test
    @DisplayName("TN: Baseline 10 -> current 12 (+2, 1.2x) does not trigger")
    void trueNegative_smallIncrease() {
      QueryCounts baseline = new QueryCounts(10, 0, 0, 0, 10);
      QueryCounts current = new QueryCounts(12, 0, 0, 0, 12);

      List<Issue> issues = detector.detect("TestClass", "testMethod", current, baseline);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: Decrease from baseline does not trigger (improvement)")
    void trueNegative_decrease() {
      QueryCounts baseline = new QueryCounts(20, 0, 0, 0, 20);
      QueryCounts current = new QueryCounts(10, 0, 0, 0, 10);

      List<Issue> issues = detector.detect("TestClass", "testMethod", current, baseline);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("TN: No baseline (first run) does not trigger")
    void trueNegative_noBaseline() {
      QueryCounts current = new QueryCounts(50, 0, 0, 0, 50);

      List<Issue> issues = detector.detect("TestClass", "testMethod", current, null);

      assertThat(issues).isEmpty();
    }
  }
}
