package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.baseline.Baseline;
import io.queryaudit.core.baseline.BaselineEntry;
import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive edge case tests for all detectors. Focuses on false positive scenarios and boundary
 * conditions.
 */
class EdgeCaseDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private static QueryRecord record(String sql, String stackTrace) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), stackTrace);
  }

  // ══════════════════════════════════════════════════════════════════════
  // NPlusOneDetector edge cases (SQL-level, INFO-only)
  //
  // After migration to Hibernate event-based N+1 detection, the SQL-level
  // NPlusOneDetector reports ALL repeated query patterns as INFO only.
  // ══════════════════════════════════════════════════════════════════════

  @Nested
  class NPlusOneEdgeCases {

    private final NPlusOneDetector detector = new NPlusOneDetector(3);

    /** Helper: creates a filler query at the given index (unique SQL). */
    private static QueryRecord filler(int index) {
      return new QueryRecord(
          "SELECT * FROM filler_table_" + index + " WHERE id = 1",
          0L,
          System.currentTimeMillis(),
          "");
    }

    /**
     * Helper: builds a query list with target queries at specific positions, gaps filled with
     * unique filler queries.
     */
    private static List<QueryRecord> buildQueryList(
        String targetSql, List<Integer> positions, int totalSize) {
      List<QueryRecord> queries = new ArrayList<>();
      for (int i = 0; i < totalSize; i++) {
        if (positions.contains(i)) {
          queries.add(record("SELECT * FROM users WHERE id = " + i));
        } else {
          queries.add(filler(i));
        }
      }
      return queries;
    }

    // --- Test 1: 3 consecutive queries -> INFO (SQL-level always INFO) ---
    @Test
    void consecutiveQueries_atThreshold_shouldBeInfo() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1"),
              record("SELECT * FROM users WHERE id = 2"),
              record("SELECT * FROM users WHERE id = 3"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    // --- Test 2: 5 consecutive queries -> INFO (SQL-level always INFO) ---
    @Test
    void fiveConsecutiveQueries_shouldBeInfo() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1"),
              record("SELECT * FROM users WHERE id = 2"),
              record("SELECT * FROM users WHERE id = 3"),
              record("SELECT * FROM users WHERE id = 4"),
              record("SELECT * FROM users WHERE id = 5"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    // --- Test 3: Spread out queries (gap > 5) -> INFO ---
    @Test
    void spreadOutQueries_shouldBeInfo() {
      // positions [0, 20, 40] -> gaps = [19, 19] -> medianGap=19 -> INFO
      List<QueryRecord> queries =
          buildQueryList("SELECT * FROM users WHERE id = ?", List.of(0, 20, 40), 41);

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    // --- Test 4: Empty/null stack traces with consecutive queries -> INFO ---
    @Test
    void emptyStackTraces_consecutive_shouldBeInfo() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1", ""),
              record("SELECT * FROM users WHERE id = 2", ""),
              record("SELECT * FROM users WHERE id = 3", ""));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    @Test
    void nullStackTraces_consecutive_shouldBeInfo() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1", (String) null),
              record("SELECT * FROM users WHERE id = 2", (String) null),
              record("SELECT * FROM users WHERE id = 3", (String) null));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    // --- Test 5: Same pattern 2x -> below threshold ---
    @Test
    void samePattern2x_belowThreshold_shouldNotDetect() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1"),
              record("SELECT * FROM users WHERE id = 2"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    // --- Test 6: Interleaved with gap of 1 -> INFO ---
    @Test
    void interleavedWithGapOf1_shouldBeInfo() {
      // target at [0, 2, 4], fillers at [1, 3]
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1"), // pos 0
              filler(1), // pos 1
              record("SELECT * FROM users WHERE id = 2"), // pos 2
              filler(3), // pos 3
              record("SELECT * FROM users WHERE id = 3") // pos 4
              );

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    // --- Test 7: Gap of 4 -> INFO ---
    @Test
    void gapOf4_shouldBeInfo() {
      // target at [0, 5, 10]
      List<QueryRecord> queries =
          buildQueryList("SELECT * FROM users WHERE id = ?", List.of(0, 5, 10), 11);

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    // --- Test 8: Burst + spread -> high median -> INFO ---
    @Test
    void burstPlusSpread_shouldBeInfo() {
      // positions [0, 1, 2, 50, 100] -> gaps = [0, 0, 47, 49] -> median = (0+47)/2 = 23.5 -> INFO
      List<QueryRecord> queries =
          buildQueryList("SELECT * FROM users WHERE id = ?", List.of(0, 1, 2, 50, 100), 101);

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    // --- Test 9: Stack trace content is irrelevant ---
    @Test
    void handlerTrace_consecutive_shouldBeInfo() {
      String handlerTrace =
          "com.app.controller.AuthController.handleLogin\n"
              + "com.app.filter.SecurityFilter.doFilter";
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1", handlerTrace),
              record("SELECT * FROM users WHERE id = 2", handlerTrace),
              record("SELECT * FROM users WHERE id = 3", handlerTrace),
              record("SELECT * FROM users WHERE id = 4", handlerTrace),
              record("SELECT * FROM users WHERE id = 5", handlerTrace));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      // SQL-level always INFO
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    // --- Test 10: Setup/cleanup traces are irrelevant when queries are spread out ---
    @Test
    void setupTrace_spreadOut_shouldBeInfo() {
      // Spread out queries, even with "normal" stack traces
      List<QueryRecord> queries =
          buildQueryList("SELECT * FROM users WHERE id = ?", List.of(0, 30, 60), 61);

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }
  }

  // ══════════════════════════════════════════════════════════════════════
  // SelectAllDetector edge cases
  // ══════════════════════════════════════════════════════════════════════

  @Nested
  class SelectAllEdgeCases {

    private final SelectAllDetector detector = new SelectAllDetector();

    // --- Test 12: "SELECT * FROM users" ---
    @Test
    void selectStarFromUsers_shouldDetect() {
      List<Issue> issues = detector.evaluate(List.of(record("SELECT * FROM users")), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.SELECT_ALL);
    }

    // --- Test 13: "SELECT u.* FROM users u" ---
    @Test
    void selectAliasStar_shouldDetect() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT u.* FROM users u")), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.SELECT_ALL);
    }

    // --- Test 14: "SELECT COUNT(*) FROM users" ---
    @Test
    void countStar_shouldNotDetect() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT COUNT(*) FROM users")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    // --- Test 15: "SELECT EXISTS(SELECT * FROM ...)" ---
    @Test
    void existsSelectStar_shouldNotDetect() {
      // EXISTS(SELECT * ...) is idiomatic SQL — the inner SELECT * should not trigger
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT EXISTS(SELECT * FROM users WHERE id = 1)")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    // --- Test 16: lowercase "select * from users where id = ?" ---
    @Test
    void lowercaseSelectStar_shouldDetect() {
      List<Issue> issues =
          detector.evaluate(List.of(record("select * from users where id = ?")), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
    }

    // --- Test 17: Hibernate-generated specific columns ---
    @Test
    void hibernateGeneratedSpecificColumns_shouldNotDetect() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("select u1_0.id, u1_0.name from users u1_0")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void selectStarDeduplication_samePatternDifferentValues() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record("SELECT * FROM users WHERE id = 1"),
                  record("SELECT * FROM users WHERE id = 2"),
                  record("SELECT * FROM users WHERE id = 3")),
              EMPTY_INDEX);
      // All normalize to the same pattern — only 1 issue
      assertThat(issues).hasSize(1);
    }
  }

  // ══════════════════════════════════════════════════════════════════════
  // WhereFunctionDetector edge cases
  // ══════════════════════════════════════════════════════════════════════

  @Nested
  class WhereFunctionEdgeCases {

    private final WhereFunctionDetector detector = new WhereFunctionDetector();

    // --- Test 18: DATE(created_at) ---
    @Test
    void dateFunctionOnColumn_shouldDetect() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM orders WHERE DATE(created_at) = '2024-01-01'")),
              EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).column()).isEqualTo("created_at");
    }

    // --- Test 19: LOWER(email) ---
    @Test
    void lowerFunctionOnColumn_shouldDetect() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE LOWER(email) = ?")), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).column()).isEqualTo("email");
    }

    // --- Test 20: function on value, not column ---
    @Test
    void functionOnValueNotColumn_shouldNotDetect() {
      // "WHERE created_at > DATE_SUB(NOW(), INTERVAL 1 DAY)"
      // DATE_SUB is not in the recognized function list (DATE, LOWER, UPPER, YEAR, MONTH, TRIM,
      // SUBSTRING, CAST)
      // And NOW() — also not recognized. This should NOT detect.
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM orders WHERE created_at > DATE_SUB(NOW(), INTERVAL 1 DAY)")),
              EMPTY_INDEX);
      // DATE_SUB is not in the function list, so no detection
      assertThat(issues).isEmpty();
    }

    // --- Test 21: subquery in WHERE ---
    @Test
    void subqueryInWhere_shouldNotDetectAsFunction() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)")),
              EMPTY_INDEX);
      // No function wrapping a column in the WHERE clause
      assertThat(issues).isEmpty();
    }

    // --- Test 22: COALESCE wrapping column (now index-safe) ---
    @Test
    void coalesceIsIndexSafe_notFlagged() {
      // COALESCE is index-safe in MySQL 8.0.13+ and should not be flagged
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE COALESCE(nickname, 'unknown') = ?")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void castFunctionOnColumn_shouldDetect() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE CAST(age AS VARCHAR) = '30'")),
              EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).detail()).contains("CAST");
    }

    @Test
    void trimFunctionOnColumn_shouldDetect() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE TRIM(name) = 'John'")), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).column()).isEqualTo("name");
    }
  }

  // ══════════════════════════════════════════════════════════════════════
  // OrAbuseDetector edge cases
  // ══════════════════════════════════════════════════════════════════════

  @Nested
  class OrAbuseEdgeCases {

    // --- Test 23: 3 ORs with threshold 3 ---
    @Test
    void threeOrsWithThreshold3_shouldDetect() {
      OrAbuseDetector detector = new OrAbuseDetector(3);
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE a = 1 OR b = 2 OR c = 3 OR d = 4")),
              EMPTY_INDEX);
      // 3 OR tokens in that query → count = 3, threshold = 3 → detected
      assertThat(issues).hasSize(1);
    }

    // --- Test 24: below threshold ---
    @Test
    void twoOrsWithThreshold3_shouldNotDetect() {
      OrAbuseDetector detector = new OrAbuseDetector(3);
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE a = 1 OR b = 2")), EMPTY_INDEX);
      // 1 OR token → count = 1, below threshold
      assertThat(issues).isEmpty();
    }

    // --- Test 25: IN clause, not OR ---
    @Test
    void inClauseNotOr_shouldNotDetect() {
      OrAbuseDetector detector = new OrAbuseDetector(3);
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE a IN (1, 2, 3)")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    // --- Test 26: OR inside parentheses ---
    @Test
    void twoOrsInsideParensWithThreshold3_shouldNotDetect() {
      OrAbuseDetector detector = new OrAbuseDetector(3);
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE (a = 1 OR a = 2) AND b = 3")), EMPTY_INDEX);
      // 1 OR → below threshold of 3
      assertThat(issues).isEmpty();
    }

    // --- Test 27: 4 ORs on same column (equivalent to IN) ---
    @Test
    void fourOrsSameColumn_shouldNotDetect() {
      OrAbuseDetector detector = new OrAbuseDetector(3);
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM users WHERE status = 'A' OR status = 'B' OR status = 'C' OR status = 'D'")),
              EMPTY_INDEX);
      // All ORs are on the same column (status) -- equivalent to IN clause,
      // MySQL optimizes identically, so this is NOT OR abuse.
      assertThat(issues).isEmpty();
    }

    @Test
    void orInsideSubqueryNotCounted() {
      OrAbuseDetector detector = new OrAbuseDetector(3);
      // Subquery is removed by removeSubqueries, so ORs inside are not counted
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM users WHERE id IN (SELECT id FROM t WHERE a = 1 OR b = 2 OR c = 3 OR d = 4)")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // ══════════════════════════════════════════════════════════════════════
  // OffsetPaginationDetector edge cases
  // ══════════════════════════════════════════════════════════════════════

  @Nested
  class OffsetPaginationEdgeCases {

    // --- Test 28: large offset ---
    @Test
    void largeOffset_shouldDetect() {
      OffsetPaginationDetector detector = new OffsetPaginationDetector(1000);
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users LIMIT 10 OFFSET 5000")), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).detail()).contains("5000");
    }

    // --- Test 29: small offset ---
    @Test
    void smallOffset_shouldNotDetect() {
      OffsetPaginationDetector detector = new OffsetPaginationDetector(1000);
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users LIMIT 10 OFFSET 10")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    // --- Test 30: no offset ---
    @Test
    void limitOnly_shouldNotDetect() {
      OffsetPaginationDetector detector = new OffsetPaginationDetector(1000);
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users LIMIT 10")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    // --- Test 31: MySQL comma format ---
    @Test
    void mySqlCommaFormatLargeOffset_shouldDetect() {
      OffsetPaginationDetector detector = new OffsetPaginationDetector(1000);
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users LIMIT 5000, 10")), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
    }

    @Test
    void offsetExactlyAtThreshold_shouldDetect() {
      OffsetPaginationDetector detector = new OffsetPaginationDetector(1000);
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users LIMIT 10 OFFSET 1000")), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
    }

    @Test
    void offsetJustBelowThreshold_shouldNotDetect() {
      OffsetPaginationDetector detector = new OffsetPaginationDetector(1000);
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users LIMIT 10 OFFSET 999")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // ══════════════════════════════════════════════════════════════════════
  // MissingIndexDetector edge cases
  // ══════════════════════════════════════════════════════════════════════

  @Nested
  class MissingIndexEdgeCases {

    private final MissingIndexDetector detector = new MissingIndexDetector();

    // --- Test 32: WHERE column has index ---
    @Test
    void whereColumnHasIndex_shouldNotDetect() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "users",
                  List.of(
                      new IndexInfo("users", "PRIMARY", "id", 1, false, 100),
                      new IndexInfo("users", "idx_email", "email", 1, true, 100))));

      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE email = 'test@example.com'")), metadata);
      assertThat(issues.stream().filter(i -> i.type() == IssueType.MISSING_WHERE_INDEX)).isEmpty();
    }

    // --- Test 33: WHERE column has no index ---
    @Test
    void whereColumnNoIndex_shouldDetect() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 100))));

      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE email = 'test@example.com'")), metadata);
      assertThat(issues)
          .anyMatch(i -> i.type() == IssueType.MISSING_WHERE_INDEX && "email".equals(i.column()));
    }

    // --- Test 34: Hibernate alias "u1_0" should be resolved, not reported ---
    @Test
    void hibernateAlias_shouldResolveToActualTable() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 100))));

      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT u1_0.id, u1_0.name FROM users u1_0 WHERE u1_0.email = 'test@example.com'")),
              metadata);
      // The alias u1_0 resolves to "users" table via aliasToTable mapping
      assertThat(issues)
          .anyMatch(
              i ->
                  i.type() == IssueType.MISSING_WHERE_INDEX
                      && "users".equals(i.table())
                      && "email".equals(i.column()));
      // Should NOT report "u1_0" as the table
      assertThat(issues).noneMatch(i -> "u1_0".equals(i.table()));
    }

    // --- Test 35: Subquery columns should not be checked against outer table ---
    @Test
    void subqueryColumns_shouldNotReportAgainstOuterTable() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 100)),
                  "orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 100))));

      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)")),
              metadata);
      // Subqueries are removed by SqlParser.removeSubqueries, so "user_id" from the
      // inner query should NOT be checked against the outer "users" table
      assertThat(
              issues.stream()
                  .filter(
                      i ->
                          i.type() == IssueType.MISSING_WHERE_INDEX
                              && "user_id".equals(i.column())))
          .isEmpty();
    }

    // --- Test 37: Multiple WHERE conditions ---
    @Test
    void multipleWhereConditions_shouldReportBothMissingIndexes() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 100))));

      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record("SELECT * FROM users WHERE name = 'John' AND email = 'test@example.com'")),
              metadata);

      List<Issue> missingWhereIndexes =
          issues.stream().filter(i -> i.type() == IssueType.MISSING_WHERE_INDEX).toList();
      assertThat(missingWhereIndexes).extracting(Issue::column).contains("name", "email");
    }

    @Test
    void nonSelectQuery_shouldBeSkipped() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 100))));

      List<Issue> issues =
          detector.evaluate(
              List.of(record("UPDATE users SET name = 'John' WHERE email = 'test@example.com'")),
              metadata);
      assertThat(issues).isEmpty();
    }

    @Test
    void nullIndexMetadata_shouldReturnEmpty() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE email = 'test@example.com'")), null);
      assertThat(issues).isEmpty();
    }
  }

  // ══════════════════════════════════════════════════════════════════════
  // CompositeIndexDetector edge cases
  // ══════════════════════════════════════════════════════════════════════

  @Nested
  class CompositeIndexEdgeCases {

    private final CompositeIndexDetector detector = new CompositeIndexDetector();

    // --- Test 38: Composite (a, b), WHERE uses only b ---
    @Test
    void compositeIndex_whereUsesOnlyNonLeading_shouldDetect() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "orders",
                  List.of(
                      new IndexInfo("orders", "idx_a_b", "a", 1, true, 100),
                      new IndexInfo("orders", "idx_a_b", "b", 2, true, 100))));

      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM orders WHERE b = 'value'")), metadata);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.COMPOSITE_INDEX_LEADING_COLUMN);
    }

    // --- Test 39: Composite (a, b), WHERE uses a ---
    @Test
    void compositeIndex_whereUsesLeading_shouldNotDetect() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "orders",
                  List.of(
                      new IndexInfo("orders", "idx_a_b", "a", 1, true, 100),
                      new IndexInfo("orders", "idx_a_b", "b", 2, true, 100))));

      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM orders WHERE a = 'value'")), metadata);
      assertThat(issues).isEmpty();
    }

    // --- Test 40: Composite (a, b), WHERE uses both a AND b ---
    @Test
    void compositeIndex_whereUsesBothLeadingAndNonLeading_shouldNotDetect() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "orders",
                  List.of(
                      new IndexInfo("orders", "idx_a_b", "a", 1, true, 100),
                      new IndexInfo("orders", "idx_a_b", "b", 2, true, 100))));

      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM orders WHERE a = 'x' AND b = 'y'")), metadata);
      assertThat(issues).isEmpty();
    }

    // --- Test 41: Composite (a, b, c), WHERE uses only c ---
    @Test
    void threeColumnCompositeIndex_whereUsesOnlyThird_shouldDetect() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "orders",
                  List.of(
                      new IndexInfo("orders", "idx_a_b_c", "a", 1, true, 100),
                      new IndexInfo("orders", "idx_a_b_c", "b", 2, true, 100),
                      new IndexInfo("orders", "idx_a_b_c", "c", 3, true, 100))));

      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM orders WHERE c = 'value'")), metadata);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.COMPOSITE_INDEX_LEADING_COLUMN);
      assertThat(issues.get(0).detail()).contains("a"); // leading column mentioned
    }

    @Test
    void compositeIndex_nonSelectQuery_shouldBeSkipped() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "orders",
                  List.of(
                      new IndexInfo("orders", "idx_a_b", "a", 1, true, 100),
                      new IndexInfo("orders", "idx_a_b", "b", 2, true, 100))));

      List<Issue> issues =
          detector.evaluate(
              List.of(record("UPDATE orders SET status = 'done' WHERE b = 'value'")), metadata);
      assertThat(issues).isEmpty();
    }
  }

  // ══════════════════════════════════════════════════════════════════════
  // LikeWildcardDetector edge cases
  // ══════════════════════════════════════════════════════════════════════

  @Nested
  class LikeWildcardEdgeCases {

    private final LikeWildcardDetector detector = new LikeWildcardDetector();

    // --- Test 42: LIKE '%test' ---
    @Test
    void likeLeadingWildcard_shouldDetect() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE name LIKE '%test'")), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.LIKE_LEADING_WILDCARD);
    }

    // --- Test 43: LIKE '%test%' ---
    @Test
    void likeLeadingAndTrailingWildcard_shouldDetect() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE name LIKE '%test%'")), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
    }

    // --- Test 44: LIKE 'test%' ---
    @Test
    void likeTrailingWildcardOnly_shouldNotDetect() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE name LIKE 'test%'")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    // --- Test 45: LIKE ? (parameterized) ---
    // Post-#91: parameterized LIKE is reported at INFO severity because the runtime binding
    // may begin with '%'. See LikeWildcardDetectorTest#infoIssueForParameterizedLike.
    @Test
    void likeParameterized_emitsInfo() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE name LIKE ?")), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(io.queryaudit.core.model.Severity.INFO);
    }

    // --- Test 46: LIKE '%' ---
    @Test
    void likeWildcardOnly_shouldDetect() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE name LIKE '%'")), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
    }

    @Test
    void likeCaseInsensitive_shouldDetect() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE name like '%test'")), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
    }

    @Test
    void nullSql_shouldNotDetect() {
      // QueryRecord with null sql yields null normalizedSql, which is skipped
      List<Issue> issues =
          detector.evaluate(List.of(new QueryRecord(null, null, 0L, 0L, null, 0)), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // ══════════════════════════════════════════════════════════════════════
  // CartesianJoinDetector edge cases
  // ══════════════════════════════════════════════════════════════════════

  @Nested
  class CartesianJoinEdgeCases {

    private final CartesianJoinDetector detector = new CartesianJoinDetector();

    // --- Test 47: JOIN with ON clause ---
    @Test
    void joinWithOnClause_shouldNotDetect() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM a JOIN b ON a.id = b.aid")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    // --- Test 48: JOIN without ON ---
    @Test
    void joinWithoutOn_shouldDetect() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM a JOIN b WHERE a.id = 1")), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.CARTESIAN_JOIN);
    }

    // --- Test 49: FROM a, b WHERE a.id = b.aid ---
    @Test
    void implicitJoinWithWhereCondition_shouldNotDetect() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM a, b WHERE a.id = b.aid")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    // --- Test 50: FROM a, b without WHERE ---
    @Test
    void implicitJoinWithoutWhere_shouldDetect() {
      List<Issue> issues = detector.evaluate(List.of(record("SELECT * FROM a, b")), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.CARTESIAN_JOIN);
    }

    @Test
    void joinWithOnClause_multipleJoins_allWithOn_shouldNotDetect() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM a JOIN b ON a.id = b.aid JOIN c ON b.id = c.bid")),
              EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void crossJoinExplicit_shouldNotDetect() {
      // CROSS JOIN is an intentional Cartesian product and should not be flagged
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM a CROSS JOIN b")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // ══════════════════════════════════════════════════════════════════════
  // Baseline edge cases
  // ══════════════════════════════════════════════════════════════════════

  @Nested
  class BaselineEdgeCases {

    // --- Test 51: Issue matches baseline entry ---
    @Test
    void issueMatchesBaselineEntry_shouldBeAcknowledged() {
      List<BaselineEntry> baseline =
          List.of(
              new BaselineEntry(
                  "n-plus-one", "users", null, null, "dev@example.com", "Independent calls"));

      Issue issue =
          new Issue(
              IssueType.N_PLUS_ONE,
              Severity.ERROR,
              "select * from users where id = ?",
              "users",
              null,
              "Same query pattern executed 5 times",
              "Use JOIN FETCH");

      assertThat(Baseline.isAcknowledged(baseline, issue)).isTrue();
    }

    // --- Test 52: Issue does NOT match baseline entry ---
    @Test
    void issueDoesNotMatchBaseline_shouldNotBeAcknowledged() {
      List<BaselineEntry> baseline =
          List.of(
              new BaselineEntry("n-plus-one", "orders", null, null, "dev@example.com", "Known"));

      Issue issue =
          new Issue(
              IssueType.N_PLUS_ONE,
              Severity.ERROR,
              "select * from users where id = ?",
              "users",
              null,
              "Same query pattern executed 5 times",
              "Use JOIN FETCH");

      assertThat(Baseline.isAcknowledged(baseline, issue)).isFalse();
    }

    // --- Test 53: Baseline entry with null table = wildcard ---
    @Test
    void baselineWithNullTable_matchesAnyTable() {
      List<BaselineEntry> baseline =
          List.of(
              new BaselineEntry("n-plus-one", null, null, null, "dev@example.com", "All N+1 OK"));

      Issue issueUsers =
          new Issue(
              IssueType.N_PLUS_ONE,
              Severity.ERROR,
              "select * from users where id = ?",
              "users",
              null,
              "detail",
              "suggestion");
      Issue issueOrders =
          new Issue(
              IssueType.N_PLUS_ONE,
              Severity.ERROR,
              "select * from orders where id = ?",
              "orders",
              null,
              "detail",
              "suggestion");

      assertThat(Baseline.isAcknowledged(baseline, issueUsers)).isTrue();
      assertThat(Baseline.isAcknowledged(baseline, issueOrders)).isTrue();
    }

    // --- Test 54: Baseline entry with specific table + column ---
    @Test
    void baselineWithSpecificTableAndColumn_onlyMatchesThatCombination() {
      List<BaselineEntry> baseline =
          List.of(
              new BaselineEntry(
                  "missing-where-index",
                  "users",
                  "deleted_at",
                  null,
                  "dev@example.com",
                  "Soft delete, low cardinality"));

      Issue matchingIssue =
          new Issue(
              IssueType.MISSING_WHERE_INDEX,
              Severity.ERROR,
              "select * from users where deleted_at is null",
              "users",
              "deleted_at",
              "detail",
              "suggestion");
      Issue wrongColumn =
          new Issue(
              IssueType.MISSING_WHERE_INDEX,
              Severity.ERROR,
              "select * from users where email = ?",
              "users",
              "email",
              "detail",
              "suggestion");
      Issue wrongTable =
          new Issue(
              IssueType.MISSING_WHERE_INDEX,
              Severity.ERROR,
              "select * from orders where deleted_at is null",
              "orders",
              "deleted_at",
              "detail",
              "suggestion");

      assertThat(Baseline.isAcknowledged(baseline, matchingIssue)).isTrue();
      assertThat(Baseline.isAcknowledged(baseline, wrongColumn)).isFalse();
      assertThat(Baseline.isAcknowledged(baseline, wrongTable)).isFalse();
    }

    @Test
    void nullBaseline_shouldReturnFalse() {
      Issue issue =
          new Issue(
              IssueType.N_PLUS_ONE,
              Severity.ERROR,
              "select * from users where id = ?",
              "users",
              null,
              "detail",
              "suggestion");
      assertThat(Baseline.isAcknowledged(null, issue)).isFalse();
    }

    @Test
    void emptyBaseline_shouldReturnFalse() {
      Issue issue =
          new Issue(
              IssueType.N_PLUS_ONE,
              Severity.ERROR,
              "select * from users where id = ?",
              "users",
              null,
              "detail",
              "suggestion");
      assertThat(Baseline.isAcknowledged(List.of(), issue)).isFalse();
    }

    @Test
    void nullIssue_shouldReturnFalse() {
      List<BaselineEntry> baseline =
          List.of(new BaselineEntry("n-plus-one", null, null, null, "dev", "reason"));
      assertThat(Baseline.isAcknowledged(baseline, null)).isFalse();
    }

    @Test
    void findMatch_returnsEntryWhenMatched() {
      BaselineEntry entry = new BaselineEntry("n-plus-one", "users", null, null, "dev", "reason");
      List<BaselineEntry> baseline = List.of(entry);

      Issue issue =
          new Issue(
              IssueType.N_PLUS_ONE,
              Severity.ERROR,
              "select * from users where id = ?",
              "users",
              null,
              "detail",
              "suggestion");
      assertThat(Baseline.findMatch(baseline, issue)).isEqualTo(entry);
    }

    @Test
    void findMatch_returnsNullWhenNotMatched() {
      List<BaselineEntry> baseline =
          List.of(new BaselineEntry("select-all", "users", null, null, "dev", "reason"));

      Issue issue =
          new Issue(
              IssueType.N_PLUS_ONE,
              Severity.ERROR,
              "select * from users where id = ?",
              "users",
              null,
              "detail",
              "suggestion");
      assertThat(Baseline.findMatch(baseline, issue)).isNull();
    }
  }
}
