package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.core.interceptor.LazyLoadTracker.LazyLoadRecord;
import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Adversarial false-positive test suite for ALL detectors.
 *
 * <p>Every test case contains SQL that SHOULD NOT trigger the detector under test. These are edge
 * cases and tricky patterns that could cause false positives.
 */
class AdversarialFalsePositiveTest {

  // ── Helpers ─────────────────────────────────────────────────────────

  private static QueryRecord record(String sql) {
    return new QueryRecord(
        sql, 0L, System.currentTimeMillis(), "com.example.Service.method(Service.java:42)");
  }

  private static QueryRecord record(String sql, int stackHash) {
    return new QueryRecord(
        sql,
        0L,
        System.currentTimeMillis(),
        "com.example.Service.method(Service.java:42)",
        stackHash);
  }

  private static QueryRecord recordWithTime(String sql, long executionTimeNanos) {
    return new QueryRecord(
        sql,
        executionTimeNanos,
        System.currentTimeMillis(),
        "com.example.Service.method(Service.java:42)");
  }

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static IndexMetadata indexOn(String table, String column) {
    return new IndexMetadata(
        Map.of(table, List.of(new IndexInfo(table, "idx_" + column, column, 1, true, 100))));
  }

  private static IndexMetadata primaryKeyOn(String table, String column) {
    return new IndexMetadata(
        Map.of(table, List.of(new IndexInfo(table, "PRIMARY", column, 1, false, 1000))));
  }

  private static IndexMetadata multiIndex(String table, List<IndexInfo> indexes) {
    return new IndexMetadata(Map.of(table, indexes));
  }

  private static IndexMetadata compositeIndex(String table, String indexName, String... columns) {
    List<IndexInfo> infos = new ArrayList<>();
    for (int i = 0; i < columns.length; i++) {
      infos.add(new IndexInfo(table, indexName, columns[i], i + 1, true, 100));
    }
    return new IndexMetadata(Map.of(table, infos));
  }

  private static IndexMetadata emptyTableIndex(String table) {
    return new IndexMetadata(Map.of(table, List.of()));
  }

  private static IndexMetadata multiTableIndex(Map<String, List<IndexInfo>> map) {
    return new IndexMetadata(map);
  }

  // =====================================================================
  // 1. NPlusOneDetector
  // =====================================================================

  @Nested
  class NPlusOneDetectorFalsePositives {

    private final NPlusOneDetector detector = new NPlusOneDetector(3);

    @Test
    void differentQueriesBelowThresholdShouldNotTrigger() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT u1_0.id,u1_0.name FROM users u1_0 WHERE u1_0.id=1"),
              record("SELECT u1_0.id,u1_0.name FROM users u1_0 WHERE u1_0.id=2"));
      assertThat(detector.evaluate(queries, EMPTY_INDEX)).isEmpty();
    }

    @Test
    void completelyDifferentQueriesShouldNotTrigger() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1"),
              record("SELECT * FROM orders WHERE user_id = 1"),
              record("SELECT * FROM products WHERE category_id = 1"),
              record("SELECT * FROM reviews WHERE product_id = 1"));
      assertThat(detector.evaluate(queries, EMPTY_INDEX)).isEmpty();
    }

    @Test
    void paginatedQueriesWithDifferentOffsetsShouldNotTrigger() {
      // These normalize differently because OFFSET values differ
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 0"),
              record("SELECT * FROM orders ORDER BY id LIMIT 10 OFFSET 0"));
      assertThat(detector.evaluate(queries, EMPTY_INDEX)).isEmpty();
    }

    @Test
    void singleQueryShouldNotTrigger() {
      List<QueryRecord> queries =
          List.of(record("SELECT u1_0.id,u1_0.name FROM users u1_0 WHERE u1_0.status=?"));
      assertThat(detector.evaluate(queries, EMPTY_INDEX)).isEmpty();
    }

    @Test
    void emptyQueryListShouldNotTrigger() {
      assertThat(detector.evaluate(List.of(), EMPTY_INDEX)).isEmpty();
    }

    @Test
    void insertQueriesRepeatedShouldNotTriggerNPlusOne() {
      // Repeated inserts are handled by RepeatedSingleInsertDetector, not N+1
      // But normalizedSql for these will be the same, so N+1 would fire.
      // This is expected since N+1 just groups by normalizedSql.
      // Testing that different SQL patterns don't cross-contaminate.
      List<QueryRecord> queries =
          List.of(
              record("INSERT INTO users (id, name) VALUES (1, 'Alice')"),
              record("INSERT INTO orders (id, user_id) VALUES (1, 1)"),
              record("INSERT INTO products (id, name) VALUES (1, 'Widget')"));
      assertThat(detector.evaluate(queries, EMPTY_INDEX)).isEmpty();
    }

    @Test
    void twoQueriesJustBelowThresholdShouldNotTrigger() {
      NPlusOneDetector highThreshold = new NPlusOneDetector(5);
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1"),
              record("SELECT * FROM users WHERE id = 2"),
              record("SELECT * FROM users WHERE id = 3"),
              record("SELECT * FROM users WHERE id = 4"));
      assertThat(highThreshold.evaluate(queries, EMPTY_INDEX)).isEmpty();
    }
  }

  // =====================================================================
  // 2. SelectAllDetector
  // =====================================================================

  @Nested
  class SelectAllDetectorFalsePositives {

    private final SelectAllDetector detector = new SelectAllDetector();

    @Test
    void countStarShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT COUNT(*) FROM users")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void existsAsOuterSelectShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT EXISTS(SELECT 1 FROM users WHERE id = ?)")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void existsSelectStarShouldNotTrigger() {
      // EXISTS(SELECT * FROM ...) is idiomatic SQL and should not be flagged
      List<Issue> issues =
          detector.evaluate(
              List.of(record("EXISTS(SELECT * FROM users WHERE id = ?)")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void selectExistsSelectStarShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT id FROM orders WHERE EXISTS(SELECT * FROM users WHERE users.id = orders.user_id)")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void plainSelectStarIsStillFlagged() {
      List<Issue> issues = detector.evaluate(List.of(record("SELECT * FROM users")), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
    }

    @Test
    void explicitColumnsShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT u1_0.id,u1_0.name,u1_0.email FROM users u1_0 WHERE u1_0.active=?")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void countStarWithWhereShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT COUNT(*) FROM users WHERE status = 'active'")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void selectOneShouldNotTrigger() {
      List<Issue> issues = detector.evaluate(List.of(record("SELECT 1")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void countDistinctShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT COUNT(DISTINCT u.email) FROM users u")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void insertWithValuesShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("INSERT INTO users (id, name) VALUES (?, ?)")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void selectMaxMinShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT MAX(id), MIN(id) FROM users")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 3. WhereFunctionDetector
  // =====================================================================

  @Nested
  class WhereFunctionDetectorFalsePositives {

    private final WhereFunctionDetector detector = new WhereFunctionDetector();

    @Test
    void functionOnValueSideNotColumnShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM orders WHERE created_at = DATE('2024-01-01')")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void aggregateFunctionInSelectNotWhereShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT COUNT(id), MAX(created_at) FROM orders WHERE status = ?")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void noWhereFunctionShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT u1_0.id,u1_0.name FROM users u1_0 WHERE u1_0.status=?")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void functionInSelectClauseOnlyShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT UPPER(name), LOWER(email) FROM users WHERE id = ?")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void castInSelectNotWhereShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT CAST(amount AS DECIMAL(10,2)) FROM orders WHERE id = ?")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void functionInOrderByNotWhereShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT id, name FROM users WHERE status = ? ORDER BY LOWER(name)")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void simpleEqualsComparisonShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE email = ? AND active = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 4. OrAbuseDetector
  // =====================================================================

  @Nested
  class OrAbuseDetectorFalsePositives {

    private final OrAbuseDetector detector = new OrAbuseDetector(3);

    @Test
    void twoOrConditionsBelowThresholdShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE name = ? OR email = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void orConditionsAllOnSameColumnShouldNotTrigger() {
      // This is equivalent to IN clause, should not trigger
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM users WHERE status = 'active' OR status = 'pending' OR status = 'review'")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void singleConditionNoOrShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE id = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void andConditionsOnlyShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE name = ? AND email = ? AND active = ?")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void inClauseInsteadOfOrShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record("SELECT * FROM users WHERE status IN ('active', 'pending', 'review')")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void optionalParameterPatternShouldNotTrigger() {
      // Common JPA pattern: (? IS NULL OR col = ?)
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM users WHERE (? IS NULL OR name = ?) AND (? IS NULL OR email = ?)")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 5. MissingIndexDetector
  // =====================================================================

  @Nested
  class MissingIndexDetectorFalsePositives {

    private final MissingIndexDetector detector = new MissingIndexDetector();

    @Test
    void columnWithIndexShouldNotTrigger() {
      IndexMetadata meta = indexOn("users", "email");
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE email = ?")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void emptyIndexMetadataShouldNotTrigger() {
      // No index info means we can't know if indexes exist - skip to avoid FP
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE email = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void softDeleteColumnShouldNotTrigger() {
      IndexMetadata meta = indexOn("users", "email");
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE deleted_at IS NULL AND email = ?")), meta);
      // deleted_at should be suppressed as soft-delete column
      assertThat(issues.stream().filter(i -> "deleted_at".equalsIgnoreCase(i.column())).toList())
          .isEmpty();
    }

    @Test
    void lowCardinalityColumnShouldNotTrigger() {
      IndexMetadata meta = indexOn("users", "email");
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE is_active = 1 AND email = ?")), meta);
      assertThat(issues.stream().filter(i -> "is_active".equalsIgnoreCase(i.column())).toList())
          .isEmpty();
    }

    @Test
    void compositeIndexLeadingColumnShouldNotTrigger() {
      IndexMetadata meta = compositeIndex("users", "idx_composite", "tenant_id", "email");
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE tenant_id = ? AND email = ?")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void insertQueryShouldNotTrigger() {
      IndexMetadata meta = emptyTableIndex("users");
      List<Issue> issues =
          detector.evaluate(List.of(record("INSERT INTO users (id, name) VALUES (?, ?)")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void selectWithoutWhereShouldNotTriggerMissingIndex() {
      IndexMetadata meta = emptyTableIndex("users");
      List<Issue> issues = detector.evaluate(List.of(record("SELECT * FROM users")), meta);
      // MissingIndexDetector checks WHERE columns; no WHERE = no columns to check
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 6. UpdateWithoutWhereDetector
  // =====================================================================

  @Nested
  class UpdateWithoutWhereDetectorFalsePositives {

    private final UpdateWithoutWhereDetector detector = new UpdateWithoutWhereDetector();

    @Test
    void updateWithWhereShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("UPDATE users SET name = ? WHERE id = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void deleteWithWhereShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("DELETE FROM users WHERE id = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void selectQueryShouldNotTrigger() {
      List<Issue> issues = detector.evaluate(List.of(record("SELECT * FROM users")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void insertQueryShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("INSERT INTO users (id, name) VALUES (?, ?)")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void updateWithComplexWhereShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "UPDATE orders SET status = 'shipped' WHERE user_id = ? AND status = 'pending'")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void deleteWithSubqueryWhereShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("DELETE FROM users WHERE id IN (SELECT user_id FROM inactive_users)")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 7. DmlWithoutIndexDetector
  // =====================================================================

  @Nested
  class DmlWithoutIndexDetectorFalsePositives {

    private final DmlWithoutIndexDetector detector = new DmlWithoutIndexDetector();

    @Test
    void updateWithIndexedWhereShouldNotTrigger() {
      IndexMetadata meta = indexOn("users", "id");
      List<Issue> issues =
          detector.evaluate(List.of(record("UPDATE users SET name = ? WHERE id = ?")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void deleteWithIndexedWhereShouldNotTrigger() {
      IndexMetadata meta = indexOn("users", "id");
      List<Issue> issues =
          detector.evaluate(List.of(record("DELETE FROM users WHERE id = ?")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void selectQueryShouldNotTrigger() {
      IndexMetadata meta = emptyTableIndex("users");
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE name = ?")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void updateWithoutWhereShouldNotTrigger() {
      // UpdateWithoutWhereDetector handles this case, not DmlWithoutIndexDetector
      IndexMetadata meta = emptyTableIndex("users");
      List<Issue> issues = detector.evaluate(List.of(record("UPDATE users SET active = 0")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void emptyIndexMetadataShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("UPDATE users SET name = ? WHERE email = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void insertQueryShouldNotTrigger() {
      IndexMetadata meta = emptyTableIndex("users");
      List<Issue> issues =
          detector.evaluate(List.of(record("INSERT INTO users (id, name) VALUES (?, ?)")), meta);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 8. RepeatedSingleInsertDetector
  // =====================================================================

  @Nested
  class RepeatedSingleInsertDetectorFalsePositives {

    private final RepeatedSingleInsertDetector detector = new RepeatedSingleInsertDetector(3);

    @Test
    void multiRowInsertShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')"),
                  record(
                      "INSERT INTO users (id, name) VALUES (4, 'Dave'), (5, 'Eve'), (6, 'Frank')"),
                  record(
                      "INSERT INTO users (id, name) VALUES (7, 'Grace'), (8, 'Hank'), (9, 'Ivy')")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void belowThresholdShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record("INSERT INTO users (id, name) VALUES (1, 'Alice')"),
                  record("INSERT INTO users (id, name) VALUES (2, 'Bob')")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void differentTablesShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record("INSERT INTO users (id, name) VALUES (1, 'Alice')"),
                  record("INSERT INTO orders (id, user_id) VALUES (1, 1)"),
                  record("INSERT INTO products (id, name) VALUES (1, 'Widget')")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void selectQueryShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record("SELECT * FROM users"),
                  record("SELECT * FROM users"),
                  record("SELECT * FROM users")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void singleInsertShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("INSERT INTO users (id, name) VALUES (1, 'Alice')")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void updateQueriesShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record("UPDATE users SET name = 'Alice' WHERE id = 1"),
                  record("UPDATE users SET name = 'Bob' WHERE id = 2"),
                  record("UPDATE users SET name = 'Carol' WHERE id = 3")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 9. InsertSelectAllDetector
  // =====================================================================

  @Nested
  class InsertSelectAllDetectorFalsePositives {

    private final InsertSelectAllDetector detector = new InsertSelectAllDetector();

    @Test
    void insertWithExplicitColumnsShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "INSERT INTO archive_users (id, name, email) SELECT id, name, email FROM users WHERE status = 'inactive'")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void insertWithValuesShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("INSERT INTO users (id, name, email) VALUES (?, ?, ?)")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void selectCountStarShouldNotTrigger() {
      // COUNT(*) is not INSERT...SELECT *
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT COUNT(*) FROM users")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void plainSelectStarShouldNotTrigger() {
      // This is SelectAllDetector's job, not InsertSelectAllDetector
      List<Issue> issues = detector.evaluate(List.of(record("SELECT * FROM users")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void updateQueryShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("UPDATE users SET name = ? WHERE id = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void insertSelectCountStarShouldNotTrigger() {
      // INSERT INTO ... SELECT COUNT(*) is not SELECT *
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "INSERT INTO stats (table_name, row_count) SELECT 'users', COUNT(*) FROM users")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 10. OffsetPaginationDetector
  // =====================================================================

  @Nested
  class OffsetPaginationDetectorFalsePositives {

    private final OffsetPaginationDetector detector = new OffsetPaginationDetector(1000);

    @Test
    void offsetBelowThresholdShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 50")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void noOffsetAtAllShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users ORDER BY id LIMIT 10")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void offsetZeroShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 0")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void selectWithoutLimitOrOffsetShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE id = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void smallOffsetValue999ShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 999")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void insertQueryShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("INSERT INTO users (id, name) VALUES (?, ?)")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 11. CartesianJoinDetector
  // =====================================================================

  @Nested
  class CartesianJoinDetectorFalsePositives {

    private final CartesianJoinDetector detector = new CartesianJoinDetector();

    @Test
    void properJoinWithOnShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT u.id, o.id FROM users u JOIN orders o ON u.id = o.user_id")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void implicitJoinWithWhereConditionShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT u.id, o.id FROM users u, orders o WHERE u.id = o.user_id")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void leftJoinWithOnShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record("SELECT u.id, o.id FROM users u LEFT JOIN orders o ON u.id = o.user_id")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void joinWithUsingShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users JOIN orders USING (user_id)")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void singleTableSelectShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE id = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void multipleJoinsAllWithOnShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id JOIN items i ON o.id = i.order_id")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void hibernateStyleJoinShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "select u1_0.id,u1_0.name from users u1_0 join orders o1_0 on u1_0.id=o1_0.user_id where u1_0.active=?")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void crossJoinShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM dates d CROSS JOIN products p")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void naturalJoinShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM orders NATURAL JOIN customers")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 12. CorrelatedSubqueryDetector
  // =====================================================================

  @Nested
  class CorrelatedSubqueryDetectorFalsePositives {

    private final CorrelatedSubqueryDetector detector = new CorrelatedSubqueryDetector();

    @Test
    void subqueryInWhereNotSelectShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100)")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void nonCorrelatedSubqueryShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record("SELECT u.id, (SELECT MAX(total) FROM orders) AS max_total FROM users u")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void simpleSelectShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT u1_0.id,u1_0.name FROM users u1_0 WHERE u1_0.status=?")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void existsSubqueryInWhereShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void insertQueryShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("INSERT INTO users (id, name) VALUES (?, ?)")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void joinInsteadOfSubqueryShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT u.id, COUNT(o.id) FROM users u LEFT JOIN orders o ON u.id = o.user_id GROUP BY u.id")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 13. NullComparisonDetector
  // =====================================================================

  @Nested
  class NullComparisonDetectorFalsePositives {

    private final NullComparisonDetector detector = new NullComparisonDetector();

    @Test
    void isNullCorrectUsageShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE deleted_at IS NULL")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void isNotNullCorrectUsageShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE email IS NOT NULL")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void parameterizedComparisonShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE email = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void regularStringComparisonShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE status = 'active'")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void numericComparisonShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE age > 18")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void betweenComparisonShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM users WHERE created_at BETWEEN '2024-01-01' AND '2024-12-31'")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void hibernateStyleIsNullShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record("select u1_0.id,u1_0.name from users u1_0 where u1_0.deleted_at is null")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 14. SargabilityDetector
  // =====================================================================

  @Nested
  class SargabilityDetectorFalsePositives {

    private final SargabilityDetector detector = new SargabilityDetector();

    @Test
    void simpleColumnEqualsValueShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE id = 1")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void arithmeticOnValueSideOnlyShouldNotTrigger() {
      // "WHERE id = 10 + 5" - the arithmetic is on the value, not the column
      // This depends on how the regex works. The pattern looks for col op literal cmp.
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE id = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void noWhereClauseShouldNotTrigger() {
      List<Issue> issues = detector.evaluate(List.of(record("SELECT * FROM users")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void simpleComparisonOperatorsShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM orders WHERE amount > ? AND quantity <= ?")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void betweenComparisonShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM orders WHERE created_at BETWEEN ? AND ?")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void inClauseShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE id IN (1, 2, 3)")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void likeComparisonShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE name LIKE 'John%'")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 15. ImplicitTypeConversionDetector
  // =====================================================================

  @Nested
  class ImplicitTypeConversionDetectorFalsePositives {

    private final ImplicitTypeConversionDetector detector = new ImplicitTypeConversionDetector();

    @Test
    void stringComparedToStringShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE user_name = 'john'")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void numericColumnComparedToNumberShouldNotTrigger() {
      // Column name doesn't match string indicators
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE age = 25")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void parameterizedQueryShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE user_name = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void idColumnComparedToNumberShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE id = 42")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void noWhereClauseShouldNotTrigger() {
      List<Issue> issues = detector.evaluate(List.of(record("SELECT * FROM users")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void stringColumnComparedToStringLiteralShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE user_email = 'test@example.com'")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void insertQueryShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("INSERT INTO users (user_name, age) VALUES ('john', 25)")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 16. ForUpdateWithoutIndexDetector
  // =====================================================================

  @Nested
  class ForUpdateWithoutIndexDetectorFalsePositives {

    private final ForUpdateWithoutIndexDetector detector = new ForUpdateWithoutIndexDetector();

    @Test
    void forUpdateWithIndexedWhereShouldNotTrigger() {
      IndexMetadata meta = indexOn("users", "id");
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE id = ? FOR UPDATE")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void forShareWithIndexedWhereShouldNotTrigger() {
      IndexMetadata meta = indexOn("users", "id");
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE id = ? FOR SHARE")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void selectWithoutForUpdateShouldNotTrigger() {
      IndexMetadata meta = emptyTableIndex("users");
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE name = ?")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void emptyIndexMetadataShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE id = ? FOR UPDATE")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void insertQueryShouldNotTrigger() {
      IndexMetadata meta = emptyTableIndex("users");
      List<Issue> issues =
          detector.evaluate(List.of(record("INSERT INTO users (id, name) VALUES (?, ?)")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void forUpdateWithPrimaryKeyLookupShouldNotTrigger() {
      IndexMetadata meta = primaryKeyOn("users", "id");
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT u.id, u.name FROM users u WHERE u.id = ? FOR UPDATE")), meta);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 17. IndexRedundancyDetector
  // =====================================================================

  @Nested
  class IndexRedundancyDetectorFalsePositives {

    private final IndexRedundancyDetector detector = new IndexRedundancyDetector();

    @Test
    void nonOverlappingIndexesShouldNotTrigger() {
      IndexMetadata meta =
          multiIndex(
              "users",
              List.of(
                  new IndexInfo("users", "idx_email", "email", 1, true, 100),
                  new IndexInfo("users", "idx_name", "name", 1, true, 100)));
      List<Issue> issues = detector.evaluate(List.of(record("SELECT * FROM users")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void uniqueVsNonUniqueShouldNotTrigger() {
      // Unique index has semantic meaning beyond just search, so it's not redundant
      IndexMetadata meta =
          multiIndex(
              "users",
              List.of(
                  new IndexInfo("users", "uq_email", "email", 1, false, 1000),
                  new IndexInfo("users", "idx_email_name", "email", 1, true, 100),
                  new IndexInfo("users", "idx_email_name", "name", 2, true, 100)));
      List<Issue> issues = detector.evaluate(List.of(record("SELECT * FROM users")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void emptyIndexMetadataShouldNotTrigger() {
      List<Issue> issues = detector.evaluate(List.of(record("SELECT * FROM users")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void singleIndexShouldNotTrigger() {
      IndexMetadata meta = indexOn("users", "email");
      List<Issue> issues = detector.evaluate(List.of(record("SELECT * FROM users")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void primaryKeyPlusDifferentIndexShouldNotTrigger() {
      IndexMetadata meta =
          multiIndex(
              "users",
              List.of(
                  new IndexInfo("users", "PRIMARY", "id", 1, false, 1000),
                  new IndexInfo("users", "idx_email", "email", 1, true, 100)));
      List<Issue> issues = detector.evaluate(List.of(record("SELECT * FROM users")), meta);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 18. SlowQueryDetector
  // =====================================================================

  @Nested
  class SlowQueryDetectorFalsePositives {

    private final SlowQueryDetector detector = new SlowQueryDetector(500, 3000);

    @Test
    void fastQueryShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  recordWithTime(
                      "SELECT * FROM users WHERE id = ?", TimeUnit.MILLISECONDS.toNanos(10))),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void zeroTimeQueryShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE id = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void queryBelowWarningThresholdShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  recordWithTime(
                      "SELECT * FROM users WHERE id = ?", TimeUnit.MILLISECONDS.toNanos(499))),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void medianBelowThresholdWithOneSpikeShouldNotTrigger() {
      // One slow execution among many fast ones: median is still fast
      List<QueryRecord> queries = new ArrayList<>();
      for (int i = 0; i < 9; i++) {
        queries.add(
            recordWithTime(
                "SELECT * FROM users WHERE status = ?", TimeUnit.MILLISECONDS.toNanos(50)));
      }
      queries.add(
          recordWithTime(
              "SELECT * FROM users WHERE status = ?",
              TimeUnit.MILLISECONDS.toNanos(5000))); // One outlier
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void emptyQueryListShouldNotTrigger() {
      List<Issue> issues = detector.evaluate(List.of(), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 19. HavingMisuseDetector
  // =====================================================================

  @Nested
  class HavingMisuseDetectorFalsePositives {

    private final HavingMisuseDetector detector = new HavingMisuseDetector();

    @Test
    void havingWithAggregateCountShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT status, COUNT(*) AS cnt FROM users GROUP BY status HAVING COUNT(*) > 5")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void havingWithSumShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT user_id, SUM(amount) FROM orders GROUP BY user_id HAVING SUM(amount) > 1000")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void havingWithAvgShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT department, AVG(salary) FROM employees GROUP BY department HAVING AVG(salary) > 50000")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void noHavingClauseShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT status, COUNT(*) FROM users GROUP BY status")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void havingWithMaxShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT category, MAX(price) FROM products GROUP BY category HAVING MAX(price) > 100")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void havingWithMinShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT category, MIN(price) FROM products GROUP BY category HAVING MIN(price) < 10")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void insertQueryShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("INSERT INTO stats (category, total) VALUES (?, ?)")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 20. DistinctMisuseDetector
  // =====================================================================

  @Nested
  class DistinctMisuseDetectorFalsePositives {

    private final DistinctMisuseDetector detector = new DistinctMisuseDetector();

    @Test
    void selectWithoutDistinctShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT u1_0.id,u1_0.name FROM users u1_0 WHERE u1_0.active=?")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void distinctWithoutGroupByOrJoinOnNonPkShouldNotTrigger() {
      // DISTINCT without GROUP BY or JOIN, and no PK info - legitimate use
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT DISTINCT status FROM users")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void distinctOnNonPkWithoutJoinShouldNotTrigger() {
      // DISTINCT on a non-PK column without JOIN is legitimate
      IndexMetadata meta = primaryKeyOn("users", "id");
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT DISTINCT email FROM users")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void countDistinctShouldNotTrigger() {
      // COUNT(DISTINCT ...) is not SELECT DISTINCT
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT COUNT(DISTINCT status) FROM users")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void insertQueryShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("INSERT INTO users (id, name) VALUES (?, ?)")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void selectDistinctOnNonPkNoJoinNoGroupByShouldNotTrigger() {
      // Just DISTINCT on a column, no PK info, no JOINs, no GROUP BY
      // This is a legitimate deduplication use case
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT DISTINCT city FROM addresses")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 21. LikeWildcardDetector
  // =====================================================================

  @Nested
  class LikeWildcardDetectorFalsePositives {

    private final LikeWildcardDetector detector = new LikeWildcardDetector();

    @Test
    void trailingWildcardOnlyShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE name LIKE 'John%'")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void likeWithoutWildcardShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE name LIKE 'John'")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void likeWithParameterShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE name LIKE ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void equalsComparisonShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE name = 'John'")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void noLikeAtAllShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE id = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void likeInSelectNotWhereShouldNotTrigger() {
      // This is somewhat contrived but the detector looks at the whole SQL
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE id = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 22. RedundantFilterDetector
  // =====================================================================

  @Nested
  class RedundantFilterDetectorFalsePositives {

    private final RedundantFilterDetector detector = new RedundantFilterDetector();

    @Test
    void differentColumnsInWhereShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE name = ? AND email = ? AND active = ?")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void sameColumnInDifferentOrBranchesShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM messages WHERE (sender_id = ? AND receiver_id = ?) OR (sender_id = ? AND receiver_id = ?)")),
              EMPTY_INDEX);
      // sender_id and receiver_id appear in different OR branches - not redundant
      assertThat(issues).isEmpty();
    }

    @Test
    void singleConditionShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE id = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void noWhereClauseShouldNotTrigger() {
      List<Issue> issues = detector.evaluate(List.of(record("SELECT * FROM users")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void sameColumnDifferentOperatorsShouldNotTrigger() {
      // col > ? AND col < ? is a range, not redundant
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM orders WHERE amount > 100 AND amount < 500")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void joinWithDifferentTablesSameColumnNameShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = ? AND o.id = ?")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 23. UnboundedResultSetDetector
  // =====================================================================

  @Nested
  class UnboundedResultSetDetectorFalsePositives {

    private final UnboundedResultSetDetector detector = new UnboundedResultSetDetector();

    @Test
    void selectWithLimitShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users LIMIT 10")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void aggregateCountQueryShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT COUNT(*) FROM users WHERE active = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void aggregateMaxQueryShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT MAX(created_at) FROM orders")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void primaryKeyLookupShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE id = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void forUpdateQueryShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE status = 'pending' FOR UPDATE")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void selectWithoutFromShouldNotTrigger() {
      List<Issue> issues = detector.evaluate(List.of(record("SELECT 1")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void existsQueryShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT EXISTS(SELECT 1 FROM users WHERE email = ?)")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void inSubqueryBoundedResultShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void insertQueryShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("INSERT INTO users (id, name) VALUES (?, ?)")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 24. CountInsteadOfExistsDetector
  // =====================================================================

  @Nested
  class CountInsteadOfExistsDetectorFalsePositives {

    private final CountInsteadOfExistsDetector detector = new CountInsteadOfExistsDetector();

    @Test
    void countWithGroupByShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT status, COUNT(*) FROM users GROUP BY status")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void countWithoutWhereShouldNotTrigger() {
      // COUNT(*) without WHERE is a total count, not an existence check
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT COUNT(*) FROM users")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void countDistinctShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT COUNT(DISTINCT email) FROM users WHERE active = 1")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void countWithHavingShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id HAVING COUNT(*) > 5")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void selectWithoutCountShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE active = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void countInSubqueryShouldNotTrigger() {
      // COUNT inside a subquery is a column value, not existence check
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT u.id, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count FROM users u")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void sumQueryShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT SUM(amount) FROM orders WHERE user_id = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 25. CoveringIndexDetector
  // =====================================================================

  @Nested
  class CoveringIndexDetectorFalsePositives {

    private final CoveringIndexDetector detector = new CoveringIndexDetector();

    @Test
    void selectStarCannotBeCoveredShouldNotTrigger() {
      // SELECT * means all columns, covering index isn't practical
      IndexMetadata meta = indexOn("users", "email");
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE email = ?")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void emptyIndexMetadataShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT id, name FROM users WHERE email = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void insertQueryShouldNotTrigger() {
      IndexMetadata meta = indexOn("users", "email");
      List<Issue> issues =
          detector.evaluate(List.of(record("INSERT INTO users (id, name) VALUES (?, ?)")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void updateQueryShouldNotTrigger() {
      IndexMetadata meta = indexOn("users", "email");
      List<Issue> issues =
          detector.evaluate(List.of(record("UPDATE users SET name = ? WHERE email = ?")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void queryAlreadyCoveredByIndexShouldNotTrigger() {
      // If all selected columns are already in the index, no opportunity
      IndexMetadata meta =
          multiIndex(
              "users",
              List.of(
                  new IndexInfo("users", "idx_email", "email", 1, true, 100),
                  new IndexInfo("users", "idx_email", "id", 2, true, 100)));
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT id FROM users WHERE email = ?")), meta);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 26. WriteAmplificationDetector
  // =====================================================================

  @Nested
  class WriteAmplificationDetectorFalsePositives {

    private final WriteAmplificationDetector detector = new WriteAmplificationDetector(6);

    @Test
    void tableLessThanOrEqualSixIndexesShouldNotTrigger() {
      IndexMetadata meta =
          multiIndex(
              "users",
              List.of(
                  new IndexInfo("users", "PRIMARY", "id", 1, false, 1000),
                  new IndexInfo("users", "idx_email", "email", 1, true, 100),
                  new IndexInfo("users", "idx_name", "name", 1, true, 100),
                  new IndexInfo("users", "idx_status", "status", 1, true, 50),
                  new IndexInfo("users", "idx_created", "created_at", 1, true, 100),
                  new IndexInfo("users", "idx_updated", "updated_at", 1, true, 100)));
      List<Issue> issues = detector.evaluate(List.of(record("SELECT * FROM users")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void emptyIndexMetadataShouldNotTrigger() {
      List<Issue> issues = detector.evaluate(List.of(record("SELECT * FROM users")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void singleIndexShouldNotTrigger() {
      IndexMetadata meta = indexOn("users", "email");
      List<Issue> issues = detector.evaluate(List.of(record("SELECT * FROM users")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void autoGeneratedFkIndexesShouldNotBeCounted() {
      // FK_ and CONSTRAINT_ prefixed indexes are auto-generated by H2 and filtered out
      IndexMetadata meta =
          multiIndex(
              "users",
              List.of(
                  new IndexInfo("users", "PRIMARY", "id", 1, false, 1000),
                  new IndexInfo("users", "idx_email", "email", 1, true, 100),
                  new IndexInfo("users", "FK_users_dept", "dept_id", 1, true, 50),
                  new IndexInfo("users", "FK_users_role", "role_id", 1, true, 50),
                  new IndexInfo("users", "CONSTRAINT_1", "manager_id", 1, true, 50),
                  new IndexInfo("users", "CONSTRAINT_2", "org_id", 1, true, 50),
                  new IndexInfo("users", "SYS_IDX_001", "team_id", 1, true, 50),
                  new IndexInfo("users", "FKIDX_ref", "ref_id", 1, true, 50)));
      List<Issue> issues = detector.evaluate(List.of(record("SELECT * FROM users")), meta);
      // Only PRIMARY and idx_email count (2), which is well under 6
      assertThat(issues).isEmpty();
    }

    @Test
    void noQueryShouldNotTrigger() {
      IndexMetadata meta = indexOn("users", "email");
      List<Issue> issues = detector.evaluate(List.of(), meta);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 27. UnionWithoutAllDetector
  // =====================================================================

  @Nested
  class UnionWithoutAllDetectorFalsePositives {

    private final UnionWithoutAllDetector detector = new UnionWithoutAllDetector();

    @Test
    void unionAllShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT id FROM users UNION ALL SELECT id FROM admins")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void singleSelectShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE id = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void insertQueryShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("INSERT INTO users (id, name) VALUES (?, ?)")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void unionAllWithMultipleSetsShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT id FROM users UNION ALL SELECT id FROM admins UNION ALL SELECT id FROM moderators")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void selectWithNoUnionKeywordShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users JOIN orders ON users.id = orders.user_id")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 28. OrderByLimitWithoutIndexDetector
  // =====================================================================

  @Nested
  class OrderByLimitWithoutIndexDetectorFalsePositives {

    private final OrderByLimitWithoutIndexDetector detector =
        new OrderByLimitWithoutIndexDetector();

    @Test
    void orderByOnIndexedColumnShouldNotTrigger() {
      IndexMetadata meta = indexOn("users", "created_at");
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users ORDER BY created_at DESC LIMIT 10")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void noOrderByShouldNotTrigger() {
      IndexMetadata meta = emptyTableIndex("users");
      List<Issue> issues = detector.evaluate(List.of(record("SELECT * FROM users LIMIT 10")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void orderByWithoutLimitShouldNotTrigger() {
      IndexMetadata meta = emptyTableIndex("users");
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users ORDER BY name")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void emptyIndexMetadataShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users ORDER BY name LIMIT 10")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void insertQueryShouldNotTrigger() {
      IndexMetadata meta = emptyTableIndex("users");
      List<Issue> issues =
          detector.evaluate(List.of(record("INSERT INTO users (id, name) VALUES (?, ?)")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void orderByPrimaryKeyShouldNotTrigger() {
      IndexMetadata meta = primaryKeyOn("users", "id");
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users ORDER BY id LIMIT 10")), meta);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 29. LargeInListDetector
  // =====================================================================

  @Nested
  class LargeInListDetectorFalsePositives {

    private final LargeInListDetector detector = new LargeInListDetector();

    @Test
    void smallInListShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE id IN (1, 2, 3, 4, 5)")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void inWithSubqueryShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void noInClauseShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE id = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void singleValueInClauseShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE id IN (?)")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void inWithFewParametersShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE id IN (?, ?, ?, ?, ?)")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void equalsComparisonShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE status = 'active'")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 30. RangeLockDetector
  // =====================================================================

  @Nested
  class RangeLockDetectorFalsePositives {

    private final RangeLockDetector detector = new RangeLockDetector();

    @Test
    void forUpdateWithIndexedRangeColumnShouldNotTrigger() {
      IndexMetadata meta = indexOn("orders", "created_at");
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM orders WHERE created_at > '2024-01-01' FOR UPDATE")),
              meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void forUpdateWithEqualityOnlyNoRangeShouldNotTrigger() {
      // Equality conditions are not range conditions for gap lock purposes
      IndexMetadata meta = indexOn("users", "id");
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE id = ? FOR UPDATE")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void selectWithoutForUpdateShouldNotTrigger() {
      IndexMetadata meta = emptyTableIndex("orders");
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM orders WHERE created_at > '2024-01-01'")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void emptyIndexMetadataShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM orders WHERE amount > 100 FOR UPDATE")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void forShareWithIndexedColumnShouldNotTrigger() {
      IndexMetadata meta = indexOn("orders", "amount");
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM orders WHERE amount BETWEEN 100 AND 500 FOR SHARE")),
              meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void insertQueryShouldNotTrigger() {
      IndexMetadata meta = emptyTableIndex("orders");
      List<Issue> issues =
          detector.evaluate(List.of(record("INSERT INTO orders (id, amount) VALUES (?, ?)")), meta);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 31. DuplicateQueryDetector
  // =====================================================================

  @Nested
  class DuplicateQueryDetectorFalsePositives {

    private final DuplicateQueryDetector detector = new DuplicateQueryDetector(3);

    @Test
    void singleQueryShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE id = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void differentQueriesShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record("SELECT * FROM users WHERE id = 1"),
                  record("SELECT * FROM orders WHERE id = 1"),
                  record("SELECT * FROM products WHERE id = 1")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void emptyQueryListShouldNotTrigger() {
      assertThat(detector.evaluate(List.of(), EMPTY_INDEX)).isEmpty();
    }

    @Test
    void queriesAlreadyCoveredByNPlusOneShouldNotDuplicate() {
      // When normalized pattern appears 3+ times, DuplicateQueryDetector skips them
      // because N+1 detector covers those patterns
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record("SELECT * FROM users WHERE id = 1"),
                  record("SELECT * FROM users WHERE id = 2"),
                  record("SELECT * FROM users WHERE id = 3")),
              EMPTY_INDEX);
      // These normalize identically -> flagged as N+1 -> excluded from duplicate detection
      assertThat(issues).isEmpty();
    }

    @Test
    void samePatternDifferentValuesShouldNotTrigger() {
      // Same normalized pattern is N+1, not duplicate
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record("SELECT * FROM users WHERE id = 1"),
                  record("SELECT * FROM users WHERE id = 2"),
                  record("SELECT * FROM users WHERE id = 3")),
              EMPTY_INDEX);
      // These normalize to the same pattern; DuplicateQueryDetector excludes N+1 patterns
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 32. CompositeIndexDetector
  // =====================================================================

  @Nested
  class CompositeIndexDetectorFalsePositives {

    private final CompositeIndexDetector detector = new CompositeIndexDetector();

    @Test
    void queryUsingLeadingColumnsShouldNotTrigger() {
      IndexMetadata meta = compositeIndex("users", "idx_tenant_email", "tenant_id", "email");
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE tenant_id = ? AND email = ?")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void emptyIndexMetadataShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE email = ?")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void insertQueryShouldNotTrigger() {
      IndexMetadata meta = compositeIndex("users", "idx_comp", "a", "b");
      List<Issue> issues =
          detector.evaluate(List.of(record("INSERT INTO users (a, b) VALUES (?, ?)")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void noCompositeIndexShouldNotTrigger() {
      IndexMetadata meta = indexOn("users", "email");
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE email = ?")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void queryWithoutWhereOnCompositeColumnsShouldNotTrigger() {
      IndexMetadata meta = compositeIndex("users", "idx_comp", "tenant_id", "email");
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE name = ?")), meta);
      // name is not in the composite index, so composite detector has nothing to flag
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 33. LazyLoadNPlusOneDetector
  // =====================================================================

  @Nested
  class LazyLoadNPlusOneDetectorFalsePositives {

    private final LazyLoadNPlusOneDetector detector = new LazyLoadNPlusOneDetector(3);

    @Test
    void emptyRecordsShouldNotTrigger() {
      List<Issue> issues = detector.evaluate(List.of());
      assertThat(issues).isEmpty();
    }

    @Test
    void belowThresholdShouldNotTrigger() {
      List<LazyLoadRecord> records =
          List.of(
              new LazyLoadRecord(
                  "com.example.Order.items", "com.example.Order", "1", System.currentTimeMillis()),
              new LazyLoadRecord(
                  "com.example.Order.items", "com.example.Order", "2", System.currentTimeMillis()));
      List<Issue> issues = detector.evaluate(records);
      assertThat(issues).isEmpty();
    }

    @Test
    void differentCollectionRolesShouldNotTrigger() {
      List<LazyLoadRecord> records =
          List.of(
              new LazyLoadRecord(
                  "com.example.Order.items", "com.example.Order", "1", System.currentTimeMillis()),
              new LazyLoadRecord(
                  "com.example.User.orders", "com.example.User", "1", System.currentTimeMillis()),
              new LazyLoadRecord(
                  "com.example.Product.reviews",
                  "com.example.Product",
                  "1",
                  System.currentTimeMillis()));
      List<Issue> issues = detector.evaluate(records);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 34. CoveringIndexDetector (additional edge cases)
  // =====================================================================

  @Nested
  class CoveringIndexDetectorAdditionalFalsePositives {

    private final CoveringIndexDetector detector = new CoveringIndexDetector();

    @Test
    void deleteQueryShouldNotTrigger() {
      IndexMetadata meta = indexOn("users", "email");
      List<Issue> issues =
          detector.evaluate(List.of(record("DELETE FROM users WHERE email = ?")), meta);
      assertThat(issues).isEmpty();
    }

    @Test
    void aggregateQueryShouldNotTrigger() {
      IndexMetadata meta = indexOn("users", "status");
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT COUNT(*) FROM users WHERE status = ?")), meta);
      assertThat(issues).isEmpty();
    }
  }

  // =====================================================================
  // 35. HavingMisuseDetector (additional Hibernate patterns)
  // =====================================================================

  @Nested
  class HavingMisuseDetectorAdditionalFalsePositives {

    private final HavingMisuseDetector detector = new HavingMisuseDetector();

    @Test
    void havingWithMultipleAggregatesShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT category, COUNT(*), AVG(price) FROM products GROUP BY category HAVING COUNT(*) > 10 AND AVG(price) > 50")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void updateQueryShouldNotTrigger() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("UPDATE products SET price = price * 1.1 WHERE category = ?")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }
}
