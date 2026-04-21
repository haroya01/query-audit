package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.queryaudit.core.baseline.BaselineEntry;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Adversarial stress tests for every detector. Each test targets a specific boundary condition,
 * null-safety path, or performance envelope. Bugs found during this analysis are documented inline;
 * fixes are applied to the source.
 */
class DetectorStressTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  // ── helpers ─────────────────────────────────────────────────────────

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private static QueryRecord record(String sql, String stackTrace) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), stackTrace);
  }

  private static QueryRecord recordWithNormalizedOverride(
      String sql, String normalizedSql, String stackTrace) {
    int hash = stackTrace == null ? 0 : stackTrace.hashCode();
    return new QueryRecord(sql, normalizedSql, 0L, System.currentTimeMillis(), stackTrace, hash);
  }

  // ══════════════════════════════════════════════════════════════════════
  //  NPlusOneDetector
  // ══════════════════════════════════════════════════════════════════════

  @Nested
  class NPlusOneStress {

    private final NPlusOneDetector detector = new NPlusOneDetector(3);

    /**
     * Exactly at threshold (3) with consecutive queries. SQL-level detector always reports INFO.
     */
    @Test
    void exactlyAtThreshold_consecutive_isInfo() {
      String trace =
          "com.myapp.controller.UserController.getUsers(UserController.java:42)\n"
              + "com.myapp.service.UserService.findAll(UserService.java:20)";
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1", trace),
              record("SELECT * FROM users WHERE id = 2", trace),
              record("SELECT * FROM users WHERE id = 3", trace));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      // SQL-level always INFO
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    /** 100 consecutive identical queries — INFO at SQL level. */
    @Test
    void hundredIdenticalQueries_consecutive_isInfo() {
      String trace = "com.myapp.repository.OrderRepository.findByUserId(OrderRepository.java:15)";
      List<QueryRecord> queries = new ArrayList<>();
      for (int i = 0; i < 100; i++) {
        queries.add(record("SELECT * FROM orders WHERE user_id = " + i, trace));
      }

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
      assertThat(issues.get(0).detail()).contains("100 times");
    }

    /** Queries with null SQL — should be silently skipped. */
    @Test
    void nullSql_doesNotCrash() {
      // QueryRecord with null sql produces null normalizedSql
      QueryRecord nullSqlRecord =
          new QueryRecord(null, null, 0L, System.currentTimeMillis(), "trace", "trace".hashCode());
      List<QueryRecord> queries = List.of(nullSqlRecord, nullSqlRecord, nullSqlRecord);

      assertThatCode(() -> detector.evaluate(queries, EMPTY_INDEX)).doesNotThrowAnyException();
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty(); // all skipped because normalizedSql == null
    }

    /** Empty string SQL normalizes to empty string — forms a group. */
    @Test
    void emptySql_doesNotCrash() {
      List<QueryRecord> queries = List.of(record(""), record(""), record(""));
      assertThatCode(() -> detector.evaluate(queries, EMPTY_INDEX)).doesNotThrowAnyException();
    }

    /** Whitespace-only SQL normalizes to empty-ish string. */
    @Test
    void whitespaceSql_doesNotCrash() {
      List<QueryRecord> queries = List.of(record("   "), record("   "), record("   "));
      assertThatCode(() -> detector.evaluate(queries, EMPTY_INDEX)).doesNotThrowAnyException();
    }

    /** Single query should never detect N+1. */
    @Test
    void singleQuery_neverDetects() {
      List<QueryRecord> queries = List.of(record("SELECT * FROM users WHERE id = 1"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    /** All queries unique — should never detect. */
    @Test
    void allUnique_neverDetects() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1"),
              record("SELECT name FROM orders WHERE status = 'active'"),
              record("SELECT count(*) FROM products"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    /** 10 different patterns, each appearing 2 times (below threshold). */
    @Test
    void tenPatterns_eachBelowThreshold_noneDetected() {
      List<QueryRecord> queries = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        // Each pattern appears exactly 2 times; threshold is 3
        queries.add(record("SELECT * FROM table" + i + " WHERE id = 1"));
        queries.add(record("SELECT * FROM table" + i + " WHERE id = 2"));
      }
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    /** Stack trace with special characters. */
    @Test
    void stackTraceWithSpecialChars_doesNotCrash() {
      String trace =
          "com.app.MyClass$Inner.method(MyClass.java:10)\n"
              + "\tat com.app.Other.<init>(Other.java:5)\n"
              + "\tat com.app.Tab\there(Tab.java:1)";
      List<QueryRecord> queries = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        queries.add(record("SELECT * FROM users WHERE id = " + i, trace));
      }
      assertThatCode(() -> detector.evaluate(queries, EMPTY_INDEX)).doesNotThrowAnyException();
    }

    /** Very long stack trace (100+ frames). */
    @Test
    void veryLongStackTrace_doesNotCrash() {
      StringBuilder trace = new StringBuilder();
      for (int i = 0; i < 150; i++) {
        if (i > 0) trace.append("\n");
        trace
            .append("com.app.deep.Level")
            .append(i)
            .append(".method(Level")
            .append(i)
            .append(".java:1)");
      }
      String longTrace = trace.toString();
      List<QueryRecord> queries = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        queries.add(record("SELECT * FROM users WHERE id = " + i, longTrace));
      }
      assertThatCode(() -> detector.evaluate(queries, EMPTY_INDEX)).doesNotThrowAnyException();
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    /** Null normalizedSql records should be skipped cleanly. */
    @Test
    void nullNormalizedSql_skippedCleanly() {
      QueryRecord r =
          new QueryRecord(
              "SELECT 1", null, 0L, System.currentTimeMillis(), "trace", "trace".hashCode());
      List<QueryRecord> queries = List.of(r, r, r, r);
      assertThatCode(() -> detector.evaluate(queries, EMPTY_INDEX)).doesNotThrowAnyException();
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // ══════════════════════════════════════════════════════════════════════
  //  MissingIndexDetector
  // ══════════════════════════════════════════════════════════════════════

  @Nested
  class MissingIndexStress {

    private final MissingIndexDetector detector = new MissingIndexDetector();

    /** Query with positional parameter "WHERE ? = 1" — should not crash. */
    @Test
    void positionalParameter_doesNotCrash() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 100))));
      List<QueryRecord> queries = List.of(record("SELECT * FROM users WHERE ? = 1"));
      assertThatCode(() -> detector.evaluate(queries, metadata)).doesNotThrowAnyException();
    }

    /** All columns indexed — should detect nothing. */
    @Test
    void allColumnsIndexed_detectsNothing() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "users",
                  List.of(
                      new IndexInfo("users", "PRIMARY", "id", 1, false, 100),
                      new IndexInfo("users", "idx_email", "email", 1, true, 100),
                      new IndexInfo("users", "idx_name", "name", 1, true, 100))));
      List<QueryRecord> queries =
          List.of(record("SELECT id FROM users WHERE email = 'x' AND name = 'y'"));
      List<Issue> issues = detector.evaluate(queries, metadata);
      // Filter only MISSING_WHERE_INDEX issues for the columns we care about
      List<Issue> whereIssues =
          issues.stream().filter(i -> i.type() == IssueType.MISSING_WHERE_INDEX).toList();
      assertThat(whereIssues).isEmpty();
    }

    /**
     * BUG FOUND: Table not in IndexMetadata but metadata is non-empty. hasIndexOn returns false for
     * unknown tables, causing false positives. The detector should skip tables that are not tracked
     * in metadata. FIX: Added hasTable() check in MissingIndexDetector before flagging.
     */
    @Test
    void tableNotInMetadata_doesNotFalsePositive() {
      // metadata knows about "orders" but NOT "users"
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 100))));
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM users WHERE email = 'test@example.com'"));
      List<Issue> issues = detector.evaluate(queries, metadata);
      // Should NOT report missing index on users.email because metadata has no info on "users"
      List<Issue> whereIssues =
          issues.stream()
              .filter(i -> i.type() == IssueType.MISSING_WHERE_INDEX && "users".equals(i.table()))
              .toList();
      assertThat(whereIssues).isEmpty();
    }

    /** Query with 20 WHERE conditions — should check all without crashing. */
    @Test
    void twentyWhereConditions_doesNotCrash() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of("t", List.of(new IndexInfo("t", "PRIMARY", "id", 1, false, 100))));
      StringBuilder sb = new StringBuilder("SELECT * FROM t WHERE ");
      for (int i = 0; i < 20; i++) {
        if (i > 0) sb.append(" AND ");
        sb.append("col").append(i).append(" = ").append(i);
      }
      List<QueryRecord> queries = List.of(record(sb.toString()));
      assertThatCode(() -> detector.evaluate(queries, metadata)).doesNotThrowAnyException();
      List<Issue> issues = detector.evaluate(queries, metadata);
      // Should report missing indexes for col0..col19 (none are indexed)
      long missingCount =
          issues.stream().filter(i -> i.type() == IssueType.MISSING_WHERE_INDEX).count();
      assertThat(missingCount).isGreaterThanOrEqualTo(1);
    }

    /**
     * Subquery: inner table columns should NOT be checked against outer table indexes.
     * SqlParser.removeSubqueries should handle this.
     */
    @Test
    void subquery_innerTableNotCheckedAgainstOuter() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 100)),
                  "other_table",
                      List.of(new IndexInfo("other_table", "PRIMARY", "id", 1, false, 100))));
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id IN (SELECT id FROM other_table WHERE col = 1)"));
      List<Issue> issues = detector.evaluate(queries, metadata);
      // "col" is in the subquery's WHERE on other_table — but subquery should be removed
      // so "col" should NOT appear as a missing index on "users"
      List<Issue> userColIssues =
          issues.stream()
              .filter(
                  i ->
                      i.type() == IssueType.MISSING_WHERE_INDEX
                          && "users".equals(i.table())
                          && "col".equals(i.column()))
              .toList();
      assertThat(userColIssues).isEmpty();
    }

    /** Self-join: FROM t a JOIN t b ON a.id = b.parent_id */
    @Test
    void selfJoin_doesNotCrash() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "t",
                  List.of(
                      new IndexInfo("t", "PRIMARY", "id", 1, false, 100),
                      new IndexInfo("t", "idx_parent", "parent_id", 1, true, 100))));
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM t a JOIN t b ON a.id = b.parent_id"));
      assertThatCode(() -> detector.evaluate(queries, metadata)).doesNotThrowAnyException();
    }

    /** null IndexMetadata — should not crash. */
    @Test
    void nullIndexMetadata_doesNotCrash() {
      List<QueryRecord> queries = List.of(record("SELECT * FROM users WHERE email = 'test'"));
      assertThatCode(() -> detector.evaluate(queries, null)).doesNotThrowAnyException();
      List<Issue> issues = detector.evaluate(queries, null);
      assertThat(issues).isEmpty();
    }
  }

  // ══════════════════════════════════════════════════════════════════════
  //  CompositeIndexDetector
  // ══════════════════════════════════════════════════════════════════════

  @Nested
  class CompositeIndexStress {

    private final CompositeIndexDetector detector = new CompositeIndexDetector();

    /** Table with only single-column indexes — should detect nothing. */
    @Test
    void onlySingleColumnIndexes_detectsNothing() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "users",
                  List.of(
                      new IndexInfo("users", "PRIMARY", "id", 1, false, 100),
                      new IndexInfo("users", "idx_email", "email", 1, true, 100))));
      List<QueryRecord> queries = List.of(record("SELECT * FROM users WHERE email = 'test'"));
      List<Issue> issues = detector.evaluate(queries, metadata);
      assertThat(issues).isEmpty();
    }

    /** Table with 10 composite indexes — should check all. */
    @Test
    void tenCompositeIndexes_checksAll() {
      List<IndexInfo> indexes = new ArrayList<>();
      indexes.add(new IndexInfo("t", "PRIMARY", "id", 1, false, 100));
      for (int i = 0; i < 10; i++) {
        String indexName = "idx_comp_" + i;
        indexes.add(new IndexInfo("t", indexName, "leading_" + i, 1, true, 100));
        indexes.add(new IndexInfo("t", indexName, "trailing_" + i, 2, true, 100));
      }
      IndexMetadata metadata = new IndexMetadata(Map.of("t", indexes));

      // Query uses trailing_5 but not leading_5
      List<QueryRecord> queries = List.of(record("SELECT * FROM t WHERE trailing_5 = 'x'"));
      List<Issue> issues = detector.evaluate(queries, metadata);
      assertThat(issues)
          .anyMatch(
              i ->
                  i.type() == IssueType.COMPOSITE_INDEX_LEADING_COLUMN
                      && i.detail().contains("leading_5"));
    }

    /** Composite index where ALL columns are used in WHERE — should detect nothing. */
    @Test
    void allColumnsUsed_detectsNothing() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "orders",
                  List.of(
                      new IndexInfo("orders", "idx_comp", "status", 1, true, 100),
                      new IndexInfo("orders", "idx_comp", "created_at", 2, true, 100),
                      new IndexInfo("orders", "idx_comp", "type", 3, true, 100))));
      List<QueryRecord> queries =
          List.of(
              record(
                  "SELECT * FROM orders WHERE status = 'A' AND created_at > '2024-01-01' AND type = 'B'"));
      List<Issue> issues = detector.evaluate(queries, metadata);
      assertThat(issues).isEmpty();
    }

    /** Empty IndexMetadata — should not crash. */
    @Test
    void emptyMetadata_doesNotCrash() {
      List<QueryRecord> queries = List.of(record("SELECT * FROM t WHERE col = 1"));
      assertThatCode(() -> detector.evaluate(queries, EMPTY_INDEX)).doesNotThrowAnyException();
      assertThat(detector.evaluate(queries, EMPTY_INDEX)).isEmpty();
    }

    /** null IndexMetadata — should not crash. */
    @Test
    void nullMetadata_doesNotCrash() {
      List<QueryRecord> queries = List.of(record("SELECT * FROM t WHERE col = 1"));
      assertThatCode(() -> detector.evaluate(queries, null)).doesNotThrowAnyException();
      assertThat(detector.evaluate(queries, null)).isEmpty();
    }
  }

  // ══════════════════════════════════════════════════════════════════════
  //  SelectAllDetector
  // ══════════════════════════════════════════════════════════════════════

  @Nested
  class SelectAllStress {

    private final SelectAllDetector detector = new SelectAllDetector();

    /** 1000 identical SELECT * queries — should report only once (dedup). */
    @Test
    void thousandIdentical_reportsOnce() {
      List<QueryRecord> queries = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {
        queries.add(record("SELECT * FROM users WHERE id = " + i));
      }
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      long selectAllCount = issues.stream().filter(i -> i.type() == IssueType.SELECT_ALL).count();
      assertThat(selectAllCount).isEqualTo(1);
    }

    /** SELECT * with very long table name. */
    @Test
    void veryLongTableName_doesNotCrash() {
      String longTable = "a".repeat(500);
      List<QueryRecord> queries = List.of(record("SELECT * FROM " + longTable));
      assertThatCode(() -> detector.evaluate(queries, EMPTY_INDEX)).doesNotThrowAnyException();
    }

    /** 100 queries, only 1 has SELECT * — should detect that 1. */
    @Test
    void hundredQueries_oneSelectAll_detected() {
      List<QueryRecord> queries = new ArrayList<>();
      for (int i = 0; i < 99; i++) {
        queries.add(record("SELECT id, name FROM users WHERE id = " + i));
      }
      queries.add(record("SELECT * FROM orders WHERE id = 1"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.SELECT_ALL);
    }

    /** null sql in query — should not crash. */
    @Test
    void nullSql_doesNotCrash() {
      QueryRecord r =
          new QueryRecord(null, null, 0L, System.currentTimeMillis(), "", "".hashCode());
      List<QueryRecord> queries = List.of(r);
      assertThatCode(() -> detector.evaluate(queries, EMPTY_INDEX)).doesNotThrowAnyException();
    }
  }

  // ══════════════════════════════════════════════════════════════════════
  //  WhereFunctionDetector
  // ══════════════════════════════════════════════════════════════════════

  @Nested
  class WhereFunctionStress {

    private final WhereFunctionDetector detector = new WhereFunctionDetector();

    /** Nested functions: UPPER(LOWER(TRIM(name))) */
    @Test
    void nestedFunctions_detected() {
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM users WHERE UPPER(LOWER(TRIM(name))) = 'TEST'"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      // Should detect at least one function wrapping a column
      assertThat(issues).isNotEmpty();
      assertThat(issues).allMatch(i -> i.type() == IssueType.WHERE_FUNCTION);
    }

    /**
     * Unknown function: custom_function(col) — the detector only checks a known list (DATE, LOWER,
     * UPPER, YEAR, MONTH, TRIM, SUBSTRING, CAST) so custom_function should NOT be detected.
     */
    @Test
    void unknownFunction_notDetected() {
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM users WHERE custom_function(col) = 1"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    /**
     * Function on VALUE, not column: "WHERE col = UPPER('test')". The regex-based parser extracts
     * functions in the WHERE clause body, so UPPER('test') -> column captured is 'test' (after
     * quote replacement in normalize... but detectWhereFunctions works on raw SQL). Actually, the
     * regex captures the next \w+ after the function's opening paren. For UPPER('test'), after '('
     * the next chars are "'test'" which is NOT \w+, so the regex won't match. This is correct.
     */
    @Test
    void functionOnValue_notOnColumn_notDetected() {
      List<QueryRecord> queries = List.of(record("SELECT * FROM users WHERE col = UPPER('test')"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      // UPPER('test') — the ' before test means \w+ won't match
      assertThat(issues).isEmpty();
    }

    /** null sql — should not crash. */
    @Test
    void nullSql_doesNotCrash() {
      QueryRecord r =
          new QueryRecord(null, null, 0L, System.currentTimeMillis(), "", "".hashCode());
      assertThatCode(() -> detector.evaluate(List.of(r), EMPTY_INDEX)).doesNotThrowAnyException();
    }
  }

  // ══════════════════════════════════════════════════════════════════════
  //  CartesianJoinDetector
  // ══════════════════════════════════════════════════════════════════════

  @Nested
  class CartesianJoinStress {

    private final CartesianJoinDetector detector = new CartesianJoinDetector();

    /**
     * "FROM a, b, c WHERE a.id = b.aid AND b.id = c.bid" Has WHERE conditions linking all tables —
     * the implicit join check only fires when there is NO WHERE clause at all, so this is NOT
     * detected.
     */
    @Test
    void implicitJoinWithWhere_allLinked_notDetected() {
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM a, b, c WHERE a.id = b.aid AND b.id = c.bid"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    /**
     * "FROM a, b, c WHERE a.id = 1" — only a is filtered, b and c are cartesian. Current
     * implementation: implicit cartesian only detected when NO WHERE clause. Since there IS a WHERE
     * clause, this is NOT detected. This is a known limitation of the regex-based approach (not a
     * crash bug).
     */
    @Test
    void implicitJoinWithPartialWhere_knownLimitation_notDetected() {
      List<QueryRecord> queries = List.of(record("SELECT * FROM a, b, c WHERE a.id = 1"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      // Known limitation: the detector does not analyze whether WHERE links all tables
      assertThat(issues).isEmpty();
    }

    /** CROSS JOIN — explicit cross join is intentional and should NOT be flagged. */
    @Test
    void explicitCrossJoin_notDetected() {
      List<QueryRecord> queries = List.of(record("SELECT * FROM a CROSS JOIN b"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    /** INNER JOIN with ON + CROSS JOIN — cross join is intentional, should not be flagged. */
    @Test
    void innerJoinWithOn_plusCrossJoin_notDetected() {
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM a INNER JOIN b ON a.id = b.aid CROSS JOIN c"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    /** Implicit join without any WHERE clause — detected. */
    @Test
    void implicitJoinNoWhere_detected() {
      List<QueryRecord> queries = List.of(record("SELECT * FROM a, b"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.CARTESIAN_JOIN);
    }

    /** null sql — should not crash. */
    @Test
    void nullSql_doesNotCrash() {
      QueryRecord r =
          new QueryRecord(null, null, 0L, System.currentTimeMillis(), "", "".hashCode());
      assertThatCode(() -> detector.evaluate(List.of(r), EMPTY_INDEX)).doesNotThrowAnyException();
    }
  }

  // ══════════════════════════════════════════════════════════════════════
  //  LikeWildcardDetector
  // ══════════════════════════════════════════════════════════════════════

  @Nested
  class LikeWildcardStress {

    private final LikeWildcardDetector detector = new LikeWildcardDetector();

    /**
     * Dynamic LIKE with CONCAT — parameterised, so the regex-based detector won't see a literal '%'
     * and should NOT detect.
     */
    @Test
    void dynamicLikeConcat_notDetected() {
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM users WHERE name LIKE CONCAT('%', ?, '%')"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      // CONCAT uses parentheses, not a string literal starting with '%'
      // The regex looks for LIKE '%...' — CONCAT('%',...) has a quote
      // but it's LIKE CONCAT... not LIKE '%.  So not detected.
      assertThat(issues).isEmpty();
    }

    /** NOT LIKE '%test' — still has LIKE '%...' so should detect. */
    @Test
    void notLike_leadingWildcard_detected() {
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM users WHERE name NOT LIKE '%test'"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.LIKE_LEADING_WILDCARD);
    }

    /** Empty LIKE: LIKE '' — no leading wildcard. */
    @Test
    void emptyLike_notDetected() {
      List<QueryRecord> queries = List.of(record("SELECT * FROM users WHERE name LIKE ''"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    /** null sql — should not crash. */
    @Test
    void nullSql_doesNotCrash() {
      QueryRecord r =
          new QueryRecord(null, null, 0L, System.currentTimeMillis(), "", "".hashCode());
      assertThatCode(() -> detector.evaluate(List.of(r), EMPTY_INDEX)).doesNotThrowAnyException();
    }
  }

  // ══════════════════════════════════════════════════════════════════════
  //  OrAbuseDetector
  // ══════════════════════════════════════════════════════════════════════

  @Nested
  class OrAbuseStress {

    private final OrAbuseDetector detector = new OrAbuseDetector(3);

    /** 50 OR conditions — should detect with high count. */
    @Test
    void fiftyOrConditions_detected() {
      StringBuilder sb = new StringBuilder("SELECT * FROM t WHERE a = 1");
      for (int i = 1; i < 50; i++) {
        sb.append(" OR col").append(i).append(" = ").append(i);
      }
      List<QueryRecord> queries = List.of(record(sb.toString()));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.OR_ABUSE);
      // The detail should contain the OR count (49 ORs for 50 conditions)
      assertThat(issues.get(0).detail()).contains("49");
    }

    /**
     * OR in CASE WHEN — the current implementation counts ALL \bOR\b in the WHERE body, including
     * those inside CASE expressions. This is a known limitation of the regex-based approach.
     */
    @Test
    void orInCaseWhen_knownLimitation_counted() {
      List<QueryRecord> queries =
          List.of(
              record(
                  "SELECT * FROM t WHERE CASE WHEN a = 1 OR b = 2 OR c = 3 OR d = 4 THEN 1 END = 1"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      // Known limitation: OR inside CASE is still counted
      // With 3 ORs at threshold 3, it will be detected
      assertThat(issues).hasSize(1);
    }

    /** null sql — should not crash. */
    @Test
    void nullSql_doesNotCrash() {
      QueryRecord r =
          new QueryRecord(null, null, 0L, System.currentTimeMillis(), "", "".hashCode());
      assertThatCode(() -> detector.evaluate(List.of(r), EMPTY_INDEX)).doesNotThrowAnyException();
    }
  }

  // ══════════════════════════════════════════════════════════════════════
  //  QueryAuditAnalyzer
  // ══════════════════════════════════════════════════════════════════════

  @Nested
  class AnalyzerStress {

    /** Empty query list — should return empty report. */
    @Test
    void emptyQueryList_emptyReport() {
      QueryAuditAnalyzer analyzer = newAnalyzer(QueryAuditConfig.defaults());
      QueryAuditReport report = analyzer.analyze("test", List.of(), EMPTY_INDEX);
      assertThat(report.getConfirmedIssues()).isEmpty();
      assertThat(report.getInfoIssues()).isEmpty();
      assertThat(report.getTotalQueryCount()).isEqualTo(0);
    }

    /** null query list — should return empty report. */
    @Test
    void nullQueryList_emptyReport() {
      QueryAuditAnalyzer analyzer = newAnalyzer(QueryAuditConfig.defaults());
      QueryAuditReport report = analyzer.analyze("test", null, EMPTY_INDEX);
      assertThat(report.getConfirmedIssues()).isEmpty();
      assertThat(report.getAllQueries()).isEmpty();
    }

    /** All queries suppressed — should return empty issues. */
    @Test
    void allQueriesSuppressed_emptyIssues() {
      QueryAuditConfig config = QueryAuditConfig.builder().addSuppressQuery("SELECT").build();
      QueryAuditAnalyzer analyzer = newAnalyzer(config);
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1"),
              record("SELECT * FROM users WHERE id = 2"),
              record("SELECT * FROM users WHERE id = 3"));
      QueryAuditReport report = analyzer.analyze("test", queries, EMPTY_INDEX);
      assertThat(report.getConfirmedIssues()).isEmpty();
    }

    /** Disabled config — should return empty report. */
    @Test
    void disabledConfig_emptyReport() {
      QueryAuditConfig config = QueryAuditConfig.builder().enabled(false).build();
      QueryAuditAnalyzer analyzer = newAnalyzer(config);
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1"),
              record("SELECT * FROM users WHERE id = 2"),
              record("SELECT * FROM users WHERE id = 3"));
      QueryAuditReport report = analyzer.analyze("test", queries, EMPTY_INDEX);
      assertThat(report.getConfirmedIssues()).isEmpty();
      assertThat(report.getInfoIssues()).isEmpty();
    }

    /** Baseline acknowledges all issues — confirmed should be empty. */
    @Test
    void baselineAcknowledgesAll_confirmedEmpty() {
      List<BaselineEntry> baseline =
          List.of(
              new BaselineEntry("n-plus-one", null, null, null, "dev", "ok"),
              new BaselineEntry("select-all", null, null, null, "dev", "ok"),
              new BaselineEntry("unbounded-result-set", null, null, null, "dev", "ok"));
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), baseline);

      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1"),
              record("SELECT * FROM users WHERE id = 2"),
              record("SELECT * FROM users WHERE id = 3"),
              record("SELECT * FROM users WHERE id = 4"),
              record("SELECT * FROM users WHERE id = 5"));
      QueryAuditReport report = analyzer.analyze("test", queries, EMPTY_INDEX);
      assertThat(report.getConfirmedIssues()).isEmpty();
      assertThat(report.getAcknowledgedIssues()).isNotEmpty();
    }

    /** 4-arg analyze with testClass. */
    @Test
    void fourArgAnalyze_withTestClass() {
      QueryAuditAnalyzer analyzer = newAnalyzer(QueryAuditConfig.defaults());
      QueryAuditReport report = analyzer.analyze("MyTest", "myMethod", List.of(), EMPTY_INDEX);
      assertThat(report.getTestClass()).isEqualTo("MyTest");
      assertThat(report.getTestName()).isEqualTo("myMethod");
    }

    /** 4-arg analyze null queries. */
    @Test
    void fourArgAnalyze_nullQueries() {
      QueryAuditAnalyzer analyzer = newAnalyzer(QueryAuditConfig.defaults());
      QueryAuditReport report = analyzer.analyze("MyTest", "myMethod", null, EMPTY_INDEX);
      assertThat(report.getConfirmedIssues()).isEmpty();
    }

    private QueryAuditAnalyzer newAnalyzer(QueryAuditConfig config) {
      return new QueryAuditAnalyzer(config, List.of());
    }
  }

  // ══════════════════════════════════════════════════════════════════════
  //  Thread safety (QueryInterceptor is CopyOnWriteArrayList-based;
  //  here we test that detectors themselves don't break on concurrent input)
  // ══════════════════════════════════════════════════════════════════════

  @Nested
  class ThreadSafety {

    /**
     * Run 100 concurrent analyses. Detectors are stateless (instance state is only final fields),
     * so the real concern is that the analyze method doesn't throw ConcurrentModificationException
     * on shared input lists.
     */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void concurrentAnalyses_noConcurrentModificationException() throws Exception {
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());

      List<QueryRecord> sharedQueries = new ArrayList<>();
      for (int i = 0; i < 50; i++) {
        sharedQueries.add(record("SELECT * FROM users WHERE id = " + i));
      }
      List<QueryRecord> immutableQueries = Collections.unmodifiableList(sharedQueries);

      int threadCount = 100;
      ExecutorService executor = Executors.newFixedThreadPool(10);
      CountDownLatch latch = new CountDownLatch(threadCount);
      CopyOnWriteArrayList<Throwable> errors = new CopyOnWriteArrayList<>();

      for (int t = 0; t < threadCount; t++) {
        final int threadId = t;
        executor.submit(
            () -> {
              try {
                QueryAuditReport report =
                    analyzer.analyze("thread-" + threadId, immutableQueries, EMPTY_INDEX);
                assertThat(report).isNotNull();
              } catch (Throwable e) {
                errors.add(e);
              } finally {
                latch.countDown();
              }
            });
      }

      latch.await();
      executor.shutdown();
      assertThat(errors).as("Concurrent analysis errors").isEmpty();
    }
  }

  // ══════════════════════════════════════════════════════════════════════
  //  Memory / performance
  // ══════════════════════════════════════════════════════════════════════

  @Nested
  class Performance {

    /** 10,000 queries — CI-budget 30s. */
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void tenThousandQueries_completesInTime() {
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());

      List<QueryRecord> queries = new ArrayList<>();
      for (int i = 0; i < 10_000; i++) {
        queries.add(record("SELECT * FROM users WHERE id = " + i));
      }

      long start = System.nanoTime();
      QueryAuditReport report = analyzer.analyze("perfTest", queries, EMPTY_INDEX);
      long elapsedMs = (System.nanoTime() - start) / 1_000_000;

      assertThat(report).isNotNull();
      // 25s budget for CI headroom; JSqlParser path is ~2x the regex baseline and GitHub
      // Actions runners are ~2x slower than local. Pays for #54 / #102 / #103 correctness.
      assertThat(elapsedMs).as("Analysis of 10k queries should finish in < 25s").isLessThan(25_000);
    }

    /** 1,000 unique patterns — should not OOM. */
    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void thousandUniquePatterns_noOOM() {
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());

      List<QueryRecord> queries = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {
        queries.add(record("SELECT * FROM table_" + i + " WHERE col_" + i + " = " + i));
      }

      QueryAuditReport report = analyzer.analyze("uniquePatternTest", queries, EMPTY_INDEX);
      assertThat(report).isNotNull();
      assertThat(report.getUniquePatternCount()).isEqualTo(1000);
    }
  }

  // ══════════════════════════════════════════════════════════════════════
  //  OffsetPaginationDetector stress
  // ══════════════════════════════════════════════════════════════════════

  @Nested
  class OffsetPaginationStress {

    private final OffsetPaginationDetector detector = new OffsetPaginationDetector(1000);

    /** null sql — should not crash. */
    @Test
    void nullSql_doesNotCrash() {
      QueryRecord r =
          new QueryRecord(null, null, 0L, System.currentTimeMillis(), "", "".hashCode());
      assertThatCode(() -> detector.evaluate(List.of(r), EMPTY_INDEX)).doesNotThrowAnyException();
    }

    /** Very large offset value. */
    @Test
    void veryLargeOffset_detected() {
      List<QueryRecord> queries = List.of(record("SELECT * FROM t LIMIT 10 OFFSET 999999999"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
    }
  }
}
