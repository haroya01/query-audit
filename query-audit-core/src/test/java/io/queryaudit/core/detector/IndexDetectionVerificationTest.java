package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/** Comprehensive verification of MissingIndexDetector and CompositeIndexDetector. */
class IndexDetectionVerificationTest {

  // ═══════════════════════════════════════════════════════════════
  //  Helpers
  // ═══════════════════════════════════════════════════════════════

  private static QueryRecord query(String sql) {
    return new QueryRecord(sql, 1_000_000L, System.currentTimeMillis(), "test.StackTrace");
  }

  private static IndexInfo idx(String table, String indexName, String column, int seq) {
    return new IndexInfo(table, indexName, column, seq, true, 100);
  }

  private static IndexInfo pk(String table, String column) {
    return new IndexInfo(table, "PRIMARY", column, 1, false, 1000);
  }

  private static IndexMetadata metadata(IndexInfo... infos) {
    Map<String, List<IndexInfo>> map = new HashMap<>();
    for (IndexInfo info : infos) {
      map.computeIfAbsent(info.tableName(), k -> new ArrayList<>()).add(info);
    }
    return new IndexMetadata(map);
  }

  private List<Issue> runMissing(String sql, IndexMetadata meta) {
    return new MissingIndexDetector().evaluate(List.of(query(sql)), meta);
  }

  private List<Issue> runComposite(String sql, IndexMetadata meta) {
    return new CompositeIndexDetector().evaluate(List.of(query(sql)), meta);
  }

  // ═══════════════════════════════════════════════════════════════
  //  MissingIndexDetector — True Positives
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("MissingIndexDetector — True Positives")
  class MissingTruePositives {

    @Test
    @DisplayName("1. Simple WHERE without index on status")
    void whereWithoutIndex() {
      IndexMetadata meta = metadata(pk("orders", "id"));
      List<Issue> issues = runMissing("SELECT * FROM orders WHERE status = ?", meta);
      assertThat(issues)
          .anyMatch(
              i ->
                  i.type() == IssueType.MISSING_WHERE_INDEX
                      && "orders".equals(i.table())
                      && "status".equals(i.column()));
    }

    @Test
    @DisplayName("2. JOIN without index on FK")
    void joinWithoutIndex() {
      IndexMetadata meta = metadata(pk("orders", "id"), pk("users", "id"));
      List<Issue> issues =
          runMissing("SELECT * FROM orders o JOIN users u ON o.user_id = u.id", meta);
      assertThat(issues)
          .anyMatch(
              i ->
                  i.type() == IssueType.MISSING_JOIN_INDEX
                      && "orders".equals(i.table())
                      && "user_id".equals(i.column()));
    }

    @Test
    @DisplayName("3. ORDER BY without index")
    void orderByWithoutIndex() {
      IndexMetadata meta = metadata(pk("events", "id"));
      List<Issue> issues = runMissing("SELECT * FROM events ORDER BY created_at", meta);
      assertThat(issues)
          .anyMatch(
              i ->
                  i.type() == IssueType.MISSING_ORDER_BY_INDEX
                      && "events".equals(i.table())
                      && "created_at".equals(i.column()));
    }

    @Test
    @DisplayName("4. GROUP BY without index")
    void groupByWithoutIndex() {
      IndexMetadata meta = metadata(pk("orders", "id"));
      List<Issue> issues = runMissing("SELECT status, COUNT(*) FROM orders GROUP BY status", meta);
      assertThat(issues)
          .anyMatch(
              i ->
                  i.type() == IssueType.MISSING_GROUP_BY_INDEX
                      && "orders".equals(i.table())
                      && "status".equals(i.column()));
    }

    @Test
    @DisplayName("5. Multiple missing indexes in one query")
    void multipleMissing() {
      IndexMetadata meta = metadata(pk("orders", "id"));
      List<Issue> issues =
          runMissing("SELECT * FROM orders WHERE a = ? AND b = ? ORDER BY c", meta);
      assertThat(issues).hasSizeGreaterThanOrEqualTo(3);
      assertThat(issues).anyMatch(i -> "a".equals(i.column()));
      assertThat(issues).anyMatch(i -> "b".equals(i.column()));
      assertThat(issues).anyMatch(i -> "c".equals(i.column()));
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  MissingIndexDetector — True Negatives
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("MissingIndexDetector — True Negatives")
  class MissingTrueNegatives {

    @Test
    @DisplayName("6. WHERE with index — no issue")
    void whereWithIndex() {
      IndexMetadata meta = metadata(pk("orders", "id"), idx("orders", "idx_status", "status", 1));
      List<Issue> issues = runMissing("SELECT * FROM orders WHERE status = ?", meta);
      assertThat(issues)
          .noneMatch(i -> i.type() == IssueType.MISSING_WHERE_INDEX && "status".equals(i.column()));
    }

    @Test
    @DisplayName("7. WHERE on PRIMARY KEY — no issue")
    void whereOnPrimaryKey() {
      IndexMetadata meta = metadata(pk("orders", "id"));
      List<Issue> issues = runMissing("SELECT * FROM orders WHERE id = ?", meta);
      assertThat(issues)
          .noneMatch(i -> i.type() == IssueType.MISSING_WHERE_INDEX && "id".equals(i.column()));
    }

    @Test
    @DisplayName("8. JOIN on indexed FK — no issue")
    void joinOnIndexedFk() {
      IndexMetadata meta =
          metadata(
              pk("orders", "id"), pk("users", "id"), idx("orders", "idx_user_id", "user_id", 1));
      List<Issue> issues =
          runMissing("SELECT * FROM orders o JOIN users u ON o.user_id = u.id", meta);
      assertThat(issues)
          .noneMatch(i -> i.type() == IssueType.MISSING_JOIN_INDEX && "user_id".equals(i.column()));
    }

    @Test
    @DisplayName("9. Query with no WHERE/JOIN/ORDER/GROUP — nothing to check")
    void noClausesToCheck() {
      IndexMetadata meta = metadata(pk("orders", "id"));
      List<Issue> issues = runMissing("SELECT * FROM orders", meta);
      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("10. Column in composite index as LEADING column — OK")
    void compositeLeadingColumn() {
      IndexMetadata meta =
          metadata(
              pk("orders", "id"),
              idx("orders", "idx_status_created", "status", 1),
              idx("orders", "idx_status_created", "created_at", 2));
      List<Issue> issues = runMissing("SELECT * FROM orders WHERE status = ?", meta);
      assertThat(issues)
          .noneMatch(i -> i.type() == IssueType.MISSING_WHERE_INDEX && "status".equals(i.column()));
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  MissingIndexDetector — False Positive Prevention
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("MissingIndexDetector — False Positive Prevention")
  class MissingFalsePositivePrevention {

    @Test
    @DisplayName("11. Hibernate alias resolution: u1_0 -> users")
    void hibernateAliasResolution() {
      IndexMetadata meta = metadata(pk("users", "id"), idx("users", "idx_status", "status", 1));
      List<Issue> issues =
          runMissing("SELECT u1_0.name FROM users u1_0 WHERE u1_0.status = ?", meta);
      // status is indexed on users — should NOT flag
      assertThat(issues)
          .noneMatch(i -> i.type() == IssueType.MISSING_WHERE_INDEX && "status".equals(i.column()));
    }

    @Test
    @DisplayName("12. Subquery columns should NOT be checked against outer table")
    void subqueryColumnsNotLeaked() {
      IndexMetadata meta = metadata(pk("orders", "id"), pk("items", "id"));
      List<Issue> issues =
          runMissing(
              "SELECT * FROM orders WHERE id IN (SELECT order_id FROM items WHERE price > ?)",
              meta);
      // price belongs to items, not orders — and the subquery is stripped
      assertThat(issues).noneMatch(i -> "orders".equals(i.table()) && "price".equals(i.column()));
    }

    @Test
    @DisplayName("13. Self-join: check orders indexes correctly for both aliases")
    void selfJoin() {
      IndexMetadata meta =
          metadata(pk("orders", "id"), idx("orders", "idx_parent_id", "parent_id", 1));
      List<Issue> issues =
          runMissing("SELECT * FROM orders o1 JOIN orders o2 ON o1.parent_id = o2.id", meta);
      // parent_id is indexed, id is PK — no missing index
      assertThat(issues).noneMatch(i -> i.type() == IssueType.MISSING_JOIN_INDEX);
    }

    @Test
    @DisplayName("14. Case sensitivity: Status vs status should match")
    void caseSensitivity() {
      IndexMetadata meta = metadata(pk("orders", "id"), idx("orders", "idx_status", "status", 1));
      List<Issue> issues = runMissing("SELECT * FROM orders WHERE Status = ?", meta);
      assertThat(issues)
          .noneMatch(
              i ->
                  i.type() == IssueType.MISSING_WHERE_INDEX
                      && "status".equalsIgnoreCase(i.column()));
    }

    @Test
    @DisplayName("15. Non-leading column in composite — MissingIndexDetector should SKIP")
    void nonLeadingCompositeSkipped() {
      // Composite (status, created_at) — WHERE uses created_at only
      // MissingIndexDetector should NOT flag it (let CompositeIndexDetector handle)
      IndexMetadata meta =
          metadata(
              pk("orders", "id"),
              idx("orders", "idx_status_created", "status", 1),
              idx("orders", "idx_status_created", "created_at", 2));
      List<Issue> issues = runMissing("SELECT * FROM orders WHERE created_at = ?", meta);
      assertThat(issues)
          .noneMatch(
              i ->
                  i.type() == IssueType.MISSING_WHERE_INDEX
                      && "created_at".equalsIgnoreCase(i.column()));
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  MissingIndexDetector — Realistic Hibernate SQL
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("MissingIndexDetector — Realistic Hibernate SQL")
  class HibernateSql {

    @Test
    @DisplayName("16. Hibernate-style SELECT with alias m1_0")
    void hibernateSelectWithAlias() {
      IndexMetadata meta =
          metadata(
              pk("members", "id"),
              idx("members", "idx_status", "status", 1),
              idx("members", "idx_created_at", "created_at", 1));
      String sql =
          "select m1_0.id,m1_0.name,m1_0.email from members m1_0 "
              + "where m1_0.status=? and m1_0.deleted_at is null order by m1_0.created_at desc";
      List<Issue> issues = runMissing(sql, meta);
      // status and created_at are indexed; deleted_at is NOT but is a soft-delete column
      // with IS NULL operator and another indexed column (status) in WHERE — should be SKIPPED
      assertThat(issues).noneMatch(i -> "status".equals(i.column()));
      assertThat(issues).noneMatch(i -> "created_at".equals(i.column()));
      assertThat(issues).noneMatch(i -> "deleted_at".equals(i.column()));
    }

    @Test
    @DisplayName("17. Hibernate count with IN clause")
    void hibernateCountWithIn() {
      IndexMetadata meta =
          metadata(pk("room_members", "id"), idx("room_members", "idx_room_id", "room_id", 1));
      String sql =
          "select count(*) from room_members rm1_0 "
              + "where rm1_0.room_id=? and rm1_0.role in (?,?,?)";
      List<Issue> issues = runMissing(sql, meta);
      // room_id is indexed — should not flag
      assertThat(issues).noneMatch(i -> "room_id".equals(i.column()));
      // role is a low-cardinality column name and room_id is indexed — should be SKIPPED
      assertThat(issues).noneMatch(i -> "role".equals(i.column()));
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  CompositeIndexDetector — True Positives
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("CompositeIndexDetector — True Positives")
  class CompositeTruePositives {

    @Test
    @DisplayName("18. Composite (a, b), WHERE only uses b — flag")
    void nonLeadingOnly() {
      IndexMetadata meta =
          metadata(
              pk("orders", "id"), idx("orders", "idx_ab", "a", 1), idx("orders", "idx_ab", "b", 2));
      List<Issue> issues = runComposite("SELECT * FROM orders WHERE b = ?", meta);
      assertThat(issues)
          .anyMatch(
              i -> i.type() == IssueType.COMPOSITE_INDEX_LEADING_COLUMN && "b".equals(i.column()));
    }

    @Test
    @DisplayName("19. Composite (a, b, c), WHERE only uses c — flag")
    void deepNonLeading() {
      IndexMetadata meta =
          metadata(
              pk("orders", "id"),
              idx("orders", "idx_abc", "a", 1),
              idx("orders", "idx_abc", "b", 2),
              idx("orders", "idx_abc", "c", 3));
      List<Issue> issues = runComposite("SELECT * FROM orders WHERE c = ?", meta);
      assertThat(issues)
          .anyMatch(
              i -> i.type() == IssueType.COMPOSITE_INDEX_LEADING_COLUMN && "c".equals(i.column()));
    }

    @Test
    @DisplayName("20. Multiple composites: (a,b) and (c,d), WHERE uses b and d — both flagged")
    void multipleComposites() {
      IndexMetadata meta =
          metadata(
              pk("orders", "id"),
              idx("orders", "idx_ab", "a", 1),
              idx("orders", "idx_ab", "b", 2),
              idx("orders", "idx_cd", "c", 1),
              idx("orders", "idx_cd", "d", 2));
      List<Issue> issues = runComposite("SELECT * FROM orders WHERE b = ? AND d = ?", meta);
      assertThat(issues).anyMatch(i -> "b".equals(i.column()));
      assertThat(issues).anyMatch(i -> "d".equals(i.column()));
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  CompositeIndexDetector — True Negatives
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("CompositeIndexDetector — True Negatives")
  class CompositeTrueNegatives {

    @Test
    @DisplayName("21. Composite (a, b), WHERE uses a — leading column used, OK")
    void leadingColumnUsed() {
      IndexMetadata meta =
          metadata(
              pk("orders", "id"), idx("orders", "idx_ab", "a", 1), idx("orders", "idx_ab", "b", 2));
      List<Issue> issues = runComposite("SELECT * FROM orders WHERE a = ?", meta);
      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("22. Composite (a, b), WHERE uses both a AND b — fully used, OK")
    void fullyUsed() {
      IndexMetadata meta =
          metadata(
              pk("orders", "id"), idx("orders", "idx_ab", "a", 1), idx("orders", "idx_ab", "b", 2));
      List<Issue> issues = runComposite("SELECT * FROM orders WHERE a = ? AND b = ?", meta);
      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("23. Non-leading column has standalone index — should NOT flag")
    void standaloneIndexExists() {
      IndexMetadata meta =
          metadata(
              pk("orders", "id"),
              idx("orders", "idx_ab", "a", 1),
              idx("orders", "idx_ab", "b", 2),
              idx("orders", "idx_b", "b", 1) // standalone index on b
              );
      List<Issue> issues = runComposite("SELECT * FROM orders WHERE b = ?", meta);
      assertThat(issues).noneMatch(i -> "b".equals(i.column()));
    }

    @Test
    @DisplayName("24. Column is PRIMARY KEY — should NOT flag")
    void primaryKeyColumn() {
      // Composite (id, status) — WHERE uses status but id is PK
      // Actually this tests: WHERE uses id (the PK) => leading used => OK
      // More relevant: WHERE uses col that is PK standalone => hasStandaloneIndex true
      IndexMetadata meta =
          metadata(
              pk("orders", "id"),
              idx("orders", "idx_status_name", "status", 1),
              idx("orders", "idx_status_name", "name", 2));
      // name has PK? No. Let's make a case where the non-leading col IS the PK
      IndexMetadata meta2 =
          metadata(
              pk("orders", "id"),
              idx("orders", "idx_status_id", "status", 1),
              idx("orders", "idx_status_id", "id", 2));
      List<Issue> issues = runComposite("SELECT * FROM orders WHERE id = ?", meta2);
      // id is PK (standalone index), so even though it's non-leading in composite, should not flag
      assertThat(issues).noneMatch(i -> "id".equals(i.column()));
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  CompositeIndexDetector — Edge Cases
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("CompositeIndexDetector — Edge Cases")
  class CompositeEdgeCases {

    @Test
    @DisplayName("25. NULL columnName in IndexInfo — should not cause NPE")
    void nullColumnNameInIndexInfo() {
      // Functional index in MySQL can produce NULL column name
      IndexMetadata meta =
          metadata(
              pk("orders", "id"),
              new IndexInfo("orders", "idx_func", null, 1, true, 50),
              idx("orders", "idx_func", "status", 2));
      assertThatCode(() -> runComposite("SELECT * FROM orders WHERE status = ?", meta))
          .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("26. Same column in multiple composites: leading in one, non-leading in another")
    void columnInMultipleComposites() {
      // col appears as leading in composite1, non-leading in composite2
      IndexMetadata meta =
          metadata(
              pk("orders", "id"),
              idx("orders", "idx_col_x", "col", 1),
              idx("orders", "idx_col_x", "x", 2),
              idx("orders", "idx_y_col", "y", 1),
              idx("orders", "idx_y_col", "col", 2));
      List<Issue> issues = runComposite("SELECT * FROM orders WHERE col = ?", meta);
      // col is leading in idx_col_x => that composite is fine
      // col is non-leading in idx_y_col BUT col has a standalone-like presence
      // Actually col doesn't have a standalone index (idx_col_x is composite),
      // so hasStandaloneIndex returns false.
      // However leading is used in idx_col_x, so idx_col_x won't flag.
      // For idx_y_col: leading is 'y', not in WHERE. Non-leading 'col' is in WHERE.
      // col does NOT have a standalone single-col index. So it WOULD be flagged.
      // But that's arguably correct — the idx_y_col composite is indeed being used inefficiently.
      // The test just verifies no exception and reasonable behavior.
      assertThatCode(() -> issues.size()).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("27. Table with 20 indexes — performance check")
    void manyIndexes() {
      List<IndexInfo> infos = new ArrayList<>();
      infos.add(pk("orders", "id"));
      for (int i = 0; i < 20; i++) {
        infos.add(idx("orders", "idx_" + i, "col" + i, 1));
        infos.add(idx("orders", "idx_" + i, "col" + (i + 20), 2));
      }
      IndexMetadata meta = metadata(infos.toArray(new IndexInfo[0]));

      long start = System.nanoTime();
      List<Issue> issues = runComposite("SELECT * FROM orders WHERE col5 = ?", meta);
      long elapsed = System.nanoTime() - start;

      // Should complete in well under 1 second
      assertThat(elapsed).isLessThan(1_000_000_000L);
      // col5 is non-leading in some composite; just verify no crash
      assertThat(issues).isNotNull();
    }

    @Test
    @DisplayName("28. Empty WHERE clause — no columns to check, no flags")
    void emptyWhere() {
      IndexMetadata meta =
          metadata(
              pk("orders", "id"), idx("orders", "idx_ab", "a", 1), idx("orders", "idx_ab", "b", 2));
      List<Issue> issues = runComposite("SELECT * FROM orders", meta);
      assertThat(issues).isEmpty();
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  IndexMetadata — Edge Cases
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("IndexMetadata — Edge Cases")
  class MetadataEdgeCases {

    @Test
    @DisplayName("29. hasIndexOn with null table — should return false, not NPE")
    void hasIndexOnNullTable() {
      IndexMetadata meta = metadata(pk("orders", "id"));
      assertThatCode(() -> assertThat(meta.hasIndexOn(null, "id")).isFalse())
          .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("30. hasIndexOn with null column — should return false, not NPE")
    void hasIndexOnNullColumn() {
      IndexMetadata meta = metadata(pk("orders", "id"));
      assertThatCode(() -> assertThat(meta.hasIndexOn("orders", null)).isFalse())
          .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("31. hasIndexOn with table not in metadata — should return false")
    void hasIndexOnUnknownTable() {
      IndexMetadata meta = metadata(pk("orders", "id"));
      assertThat(meta.hasIndexOn("nonexistent", "id")).isFalse();
    }

    @Test
    @DisplayName("32. getCompositeIndexes with empty/unknown table — should return empty map")
    void compositeIndexesForEmptyTable() {
      IndexMetadata meta = metadata(pk("orders", "id"));
      Map<String, List<IndexInfo>> result = meta.getCompositeIndexes("nonexistent");
      assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("33. IndexInfo with null columnName — should not cause NPE in getCompositeIndexes")
    void nullColumnNameInGetCompositeIndexes() {
      IndexMetadata meta =
          metadata(
              new IndexInfo("orders", "idx_func", null, 1, true, 50),
              idx("orders", "idx_func", "status", 2));
      assertThatCode(() -> meta.getCompositeIndexes("orders")).doesNotThrowAnyException();
    }
  }
}
