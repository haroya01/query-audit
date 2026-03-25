package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * PostgreSQL-specific false positive evidence tests.
 *
 * <p>Each test documents a PostgreSQL idiom or feature that interacts with query-audit detectors.
 * Tests are annotated with external references (PostgreSQL official docs, academic papers) proving
 * the pattern is legitimate. Results are categorised as:
 *
 * <ul>
 *   <li><b>TP</b> (True Positive): Detector correctly flags an anti-pattern, even if PostgreSQL
 *       has a workaround (e.g., expression indexes). The flag is still valid general advice.
 *   <li><b>TN</b> (True Negative): Detector correctly does NOT flag a legitimate PostgreSQL pattern.
 * </ul>
 */
class PostgreSqlFalsePositiveEvidenceTest {

  // ── Helper methods ──────────────────────────────────────────────────

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static List<Issue> issuesOfType(List<Issue> issues, IssueType type) {
    return issues.stream().filter(i -> i.type() == type).toList();
  }

  // =====================================================================
  // 1. WhereFunctionDetector
  // =====================================================================

  @Nested
  @DisplayName("WhereFunctionDetector — PostgreSQL patterns")
  class WhereFunctionDetectorTests {

    private final WhereFunctionDetector detector = new WhereFunctionDetector();

    @Test
    @DisplayName(
        "TP: LOWER(email) in WHERE is flagged — Ref: https://www.postgresql.org/docs/current/indexes-expressional.html")
    void tp_lowerInWhere_flaggedDespiteExpressionIndex() {
      // PostgreSQL supports expression indexes: CREATE INDEX idx ON t (LOWER(email)).
      // With such an index, WHERE LOWER(email) = ? IS sargable in PostgreSQL.
      // However, the detector correctly flags this because:
      //   1) It cannot know whether the expression index exists at analysis time.
      //   2) The flag serves as a reminder to verify the expression index is in place.
      // This is a known false positive when a PG expression index is present.
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE LOWER(email) = 'test@example.com'")),
              EMPTY_INDEX);

      List<Issue> whereFunctionIssues = issuesOfType(issues, IssueType.WHERE_FUNCTION);
      assertThat(whereFunctionIssues)
          .as("LOWER() in WHERE should be flagged (expression index not detectable)")
          .isNotEmpty();
      assertThat(whereFunctionIssues.get(0).severity()).isEqualTo(Severity.ERROR);
    }

    @Test
    @DisplayName(
        "TP: EXTRACT(YEAR FROM created_at) in WHERE is flagged — Ref: https://www.postgresql.org/docs/current/functions-datetime.html")
    void tp_extractYearInWhere_flagged() {
      // EXTRACT(YEAR FROM created_at) = 2024 is common in PostgreSQL for year filtering.
      // The detector correctly flags this because a range scan
      // (WHERE created_at >= '2024-01-01' AND created_at < '2025-01-01') is more efficient,
      // as it can use a B-tree index on created_at directly.
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM events WHERE EXTRACT(YEAR FROM created_at) = 2024")),
              EMPTY_INDEX);

      List<Issue> whereFunctionIssues = issuesOfType(issues, IssueType.WHERE_FUNCTION);
      assertThat(whereFunctionIssues)
          .as("EXTRACT() in WHERE should be flagged — range scan is preferred")
          .isNotEmpty();
    }

    @Test
    @DisplayName(
        "TN: JSONB arrow operators in WHERE are not functions — Ref: https://www.postgresql.org/docs/current/functions-json.html")
    void tn_jsonbArrowOperatorsNotFlagged() {
      // PostgreSQL JSONB operators ->> and -> are operators, not function calls.
      // The WhereFunctionDetector should not treat them as wrapping functions.
      // Example: WHERE data->>'name' = 'test' uses the JSONB text extraction operator.
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record("SELECT * FROM documents WHERE data->>'name' = 'test'")),
              EMPTY_INDEX);

      List<Issue> whereFunctionIssues = issuesOfType(issues, IssueType.WHERE_FUNCTION);
      assertThat(whereFunctionIssues)
          .as("JSONB arrow operators (->>) should not trigger WHERE_FUNCTION detection")
          .isEmpty();
    }
  }

  // =====================================================================
  // 2. SelectAllDetector
  // =====================================================================

  @Nested
  @DisplayName("SelectAllDetector — PostgreSQL RETURNING *")
  class SelectAllDetectorTests {

    private final SelectAllDetector detector = new SelectAllDetector();

    @Test
    @DisplayName(
        "TN: INSERT ... RETURNING * is not SELECT * — Ref: https://www.postgresql.org/docs/current/dml-returning.html")
    void tn_returningStarNotFlagged() {
      // PostgreSQL's RETURNING * clause is used in INSERT/UPDATE/DELETE statements
      // to return all columns of the affected rows. This is NOT a SELECT * anti-pattern.
      // RETURNING * is useful when the caller needs all generated columns (e.g., serial IDs,
      // default values, trigger-computed columns) without knowing the schema at compile time.
      List<Issue> issues =
          detector.evaluate(
              List.of(record("INSERT INTO users (name) VALUES ('test') RETURNING *")),
              EMPTY_INDEX);

      List<Issue> selectAllIssues = issuesOfType(issues, IssueType.SELECT_ALL);
      assertThat(selectAllIssues)
          .as("RETURNING * is PostgreSQL-specific and should not trigger SELECT_ALL")
          .isEmpty();
    }
  }

  // =====================================================================
  // 3. NotInSubqueryDetector
  // =====================================================================

  @Nested
  @DisplayName("NotInSubqueryDetector — PostgreSQL NOT NULL awareness")
  class NotInSubqueryDetectorTests {

    private final NotInSubqueryDetector detector = new NotInSubqueryDetector();

    @Test
    @DisplayName(
        "TP: NOT IN (SELECT ... WHERE id IS NOT NULL) still flagged — Ref: https://www.postgresql.org/docs/current/functions-subquery.html")
    void tp_notInWithNotNullConstraint_stillFlagged() {
      // In PostgreSQL, NOT IN (SELECT id FROM t WHERE id IS NOT NULL) is safe from the
      // NULL-row problem because the IS NOT NULL filter ensures no NULLs in the subquery result.
      // However, the detector uses a simple regex (NOT IN (SELECT ...) and does not analyze
      // the subquery's WHERE clause for NULL safety. This is a known limitation.
      // The recommendation to use NOT EXISTS is still valid as a defensive best practice.
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM orders WHERE customer_id NOT IN "
                          + "(SELECT id FROM customers WHERE id IS NOT NULL)")),
              EMPTY_INDEX);

      List<Issue> notInIssues = issuesOfType(issues, IssueType.NOT_IN_SUBQUERY);
      assertThat(notInIssues)
          .as(
              "NOT IN (SELECT ...) is flagged regardless of IS NOT NULL filter — "
                  + "known limitation: detector does not check nullability")
          .isNotEmpty();
      assertThat(notInIssues.get(0).severity()).isEqualTo(Severity.ERROR);
    }
  }

  // =====================================================================
  // 4. CorrelatedSubqueryDetector
  // =====================================================================

  @Nested
  @DisplayName("CorrelatedSubqueryDetector — PostgreSQL LATERAL JOIN alternative")
  class CorrelatedSubqueryDetectorTests {

    private final CorrelatedSubqueryDetector detector = new CorrelatedSubqueryDetector();

    @Test
    @DisplayName(
        "TP: Correlated subquery in SELECT clause is flagged — Ref: https://www.postgresql.org/docs/current/queries-table-expressions.html#QUERIES-LATERAL")
    void tp_correlatedSubqueryInSelect_flagged() {
      // In PostgreSQL, LATERAL JOINs are the idiomatic replacement for correlated subqueries.
      // A correlated subquery in the SELECT clause executes once per outer row (O(N)).
      // The detector correctly flags this pattern. The suggested rewrite would be:
      //   SELECT o.id, i.cnt FROM orders o
      //   LEFT JOIN LATERAL (SELECT COUNT(*) AS cnt FROM items i WHERE i.order_id = o.id) i ON true
      // or equivalently:
      //   SELECT o.id, COUNT(i.id) FROM orders o LEFT JOIN items i ON i.order_id = o.id GROUP BY o.id
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT o.id, (SELECT COUNT(*) FROM items i WHERE i.order_id = o.id) FROM orders o")),
              EMPTY_INDEX);

      List<Issue> correlatedIssues = issuesOfType(issues, IssueType.CORRELATED_SUBQUERY);
      assertThat(correlatedIssues)
          .as("Correlated subquery in SELECT should be flagged — LATERAL JOIN is preferred in PG")
          .isNotEmpty();
      assertThat(correlatedIssues.get(0).severity()).isEqualTo(Severity.WARNING);
    }
  }

  // =====================================================================
  // 5. UnionWithoutAllDetector
  // =====================================================================

  @Nested
  @DisplayName("UnionWithoutAllDetector — PostgreSQL ENUM-like values")
  class UnionWithoutAllDetectorTests {

    private final UnionWithoutAllDetector detector = new UnionWithoutAllDetector();

    @Test
    @DisplayName(
        "TP: UNION for ENUM-like value set is flagged as INFO — Ref: https://www.postgresql.org/docs/current/queries-union.html")
    void tp_unionForEnumValues_flaggedAsInfo() {
      // In PostgreSQL, UNION (without ALL) is sometimes used to generate ENUM-like value sets:
      //   SELECT 'ACTIVE' AS status UNION SELECT 'INACTIVE'
      // UNION (without ALL) is semantically correct here because deduplication is desired.
      // The detector flags it as INFO severity, which is acceptable — it cannot infer intent
      // and the performance overhead of DISTINCT sort on small literal sets is negligible.
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT 'ACTIVE' AS status UNION SELECT 'INACTIVE'")),
              EMPTY_INDEX);

      List<Issue> unionIssues = issuesOfType(issues, IssueType.UNION_WITHOUT_ALL);
      assertThat(unionIssues)
          .as("UNION without ALL is flagged as INFO even for ENUM-like values — acceptable")
          .isNotEmpty();
      assertThat(unionIssues.get(0).severity()).isEqualTo(Severity.INFO);
    }
  }

  // =====================================================================
  // 6. OffsetPaginationDetector
  // =====================================================================

  @Nested
  @DisplayName("OffsetPaginationDetector — PostgreSQL small OFFSET")
  class OffsetPaginationDetectorTests {

    private final OffsetPaginationDetector detector = new OffsetPaginationDetector();

    @Test
    @DisplayName(
        "TN: Small OFFSET (page 2) is not flagged — Ref: https://www.postgresql.org/docs/current/queries-limit.html")
    void tn_smallOffsetNotFlagged() {
      // PostgreSQL LIMIT/OFFSET syntax: SELECT * FROM t ORDER BY id LIMIT 20 OFFSET 20
      // An OFFSET of 20 (page 2) is well below the default threshold of 1000.
      // The detector only flags OFFSET values >= 1000 (literal) or parameterized OFFSET (?).
      // Small OFFSETs are perfectly fine for early pages.
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM products ORDER BY id LIMIT 20 OFFSET 20")),
              EMPTY_INDEX);

      List<Issue> offsetIssues = issuesOfType(issues, IssueType.OFFSET_PAGINATION);
      assertThat(offsetIssues)
          .as("OFFSET 20 is below the 1000 threshold and should not be flagged")
          .isEmpty();
    }
  }

  // =====================================================================
  // 7. SargabilityDetector
  // =====================================================================

  @Nested
  @DisplayName("SargabilityDetector — PostgreSQL array operators")
  class SargabilityDetectorTests {

    private final SargabilityDetector detector = new SargabilityDetector();

    @Test
    @DisplayName(
        "TN: Array containment operator @> is not arithmetic — Ref: https://www.postgresql.org/docs/current/functions-array.html")
    void tn_arrayContainmentOperatorNotFlagged() {
      // PostgreSQL array containment: WHERE tags @> ARRAY['urgent']
      // The @> operator checks if the left array contains all elements of the right array.
      // This is sargable with a GIN index: CREATE INDEX idx ON t USING GIN (tags).
      // The SargabilityDetector only looks for arithmetic operators (+, -, *, /) on columns,
      // so @> should not be detected as non-sargable.
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM tickets WHERE tags @> ARRAY['urgent']")),
              EMPTY_INDEX);

      List<Issue> sargabilityIssues = issuesOfType(issues, IssueType.NON_SARGABLE_EXPRESSION);
      assertThat(sargabilityIssues)
          .as("PostgreSQL array operator @> should not trigger non-sargable detection")
          .isEmpty();
    }
  }

  // =====================================================================
  // 8. CartesianJoinDetector
  // =====================================================================

  @Nested
  @DisplayName("CartesianJoinDetector — PostgreSQL GENERATE_SERIES")
  class CartesianJoinDetectorTests {

    private final CartesianJoinDetector detector = new CartesianJoinDetector();

    @Test
    @DisplayName(
        "TN: CROSS JOIN with generate_series is intentional — Ref: https://www.postgresql.org/docs/current/functions-srf.html")
    void tn_crossJoinWithGenerateSeriesNotFlagged() {
      // In PostgreSQL, CROSS JOIN with generate_series() is a common data-generation pattern:
      //   SELECT * FROM generate_series(1, 10) AS g CROSS JOIN t
      // CROSS JOIN is explicitly excluded from CartesianJoinDetector because it indicates
      // the developer intentionally wants the Cartesian product. The detector only flags
      // implicit Cartesian joins (FROM a, b without WHERE) and JOINs missing ON/USING.
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM generate_series(1, 10) AS g CROSS JOIN products p")),
              EMPTY_INDEX);

      List<Issue> cartesianIssues = issuesOfType(issues, IssueType.CARTESIAN_JOIN);
      assertThat(cartesianIssues)
          .as("Explicit CROSS JOIN with generate_series() should not be flagged")
          .isEmpty();
    }
  }

  // =====================================================================
  // 9. ImplicitTypeConversionDetector
  // =====================================================================

  @Nested
  @DisplayName("ImplicitTypeConversionDetector — PostgreSQL strict typing")
  class ImplicitTypeConversionDetectorTests {

    private final ImplicitTypeConversionDetector detector = new ImplicitTypeConversionDetector();

    @Test
    @DisplayName(
        "TP: String column compared to number is flagged — Ref: https://www.postgresql.org/docs/current/typeconv.html")
    void tp_stringColumnComparedToNumber_flaggedForMySql() {
      // PostgreSQL is stricter about type conversion than MySQL. In PostgreSQL,
      // WHERE varchar_col = 123 would raise an ERROR (no implicit varchar->integer cast).
      // In MySQL, the same query silently converts and disables index usage.
      // The detector flags this pattern, which is correct for MySQL but would actually
      // be rejected at runtime in PostgreSQL. This is documented as MySQL-specific behavior.
      // The column name "user_name" contains the "_name" indicator, triggering detection.
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE user_name = 123")), EMPTY_INDEX);

      List<Issue> conversionIssues = issuesOfType(issues, IssueType.IMPLICIT_TYPE_CONVERSION);
      assertThat(conversionIssues)
          .as(
              "String column = number is flagged (correct for MySQL; "
                  + "PostgreSQL would reject this query entirely)")
          .isNotEmpty();
      assertThat(conversionIssues.get(0).severity()).isEqualTo(Severity.WARNING);
    }
  }

  // =====================================================================
  // 10. HavingMisuseDetector
  // =====================================================================

  @Nested
  @DisplayName("HavingMisuseDetector — PostgreSQL non-aggregate HAVING")
  class HavingMisuseDetectorTests {

    private final HavingMisuseDetector detector = new HavingMisuseDetector();

    @Test
    @DisplayName(
        "TP: Non-aggregate HAVING condition is flagged — Ref: https://www.postgresql.org/docs/current/sql-select.html#SQL-HAVING")
    void tp_nonAggregateHaving_flagged() {
      // PostgreSQL allows HAVING conditions that reference GROUP BY columns without
      // aggregate functions: SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING dept = 'sales'
      // While syntactically valid in both MySQL and PostgreSQL, this is semantically
      // equivalent to WHERE dept = 'sales' ... GROUP BY dept, which is more efficient
      // because it filters BEFORE aggregation rather than after.
      // The detector correctly flags this as WARNING.
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING dept = 'sales'")),
              EMPTY_INDEX);

      List<Issue> havingIssues = issuesOfType(issues, IssueType.HAVING_MISUSE);
      assertThat(havingIssues)
          .as("Non-aggregate HAVING condition should be flagged — WHERE is more efficient")
          .isNotEmpty();
      assertThat(havingIssues.get(0).severity()).isEqualTo(Severity.WARNING);
    }
  }

  // =====================================================================
  // 11. LikeWildcardDetector
  // =====================================================================

  @Nested
  @DisplayName("LikeWildcardDetector — PostgreSQL pg_trgm extension")
  class LikeWildcardDetectorTests {

    private final LikeWildcardDetector detector = new LikeWildcardDetector();

    @Test
    @DisplayName(
        "TP: LIKE '%test%' is flagged despite pg_trgm — Ref: https://www.postgresql.org/docs/current/pgtrgm.html")
    void tp_likeLeadingWildcard_flaggedDespitePgTrgm() {
      // In PostgreSQL, the pg_trgm extension enables GIN/GiST indexes that support
      // LIKE '%pattern%' queries efficiently. With:
      //   CREATE INDEX idx ON t USING GIN (col gin_trgm_ops);
      // the query LIKE '%test%' can use the trigram index instead of a full table scan.
      // However, the detector cannot know whether pg_trgm is installed or the GIN index exists.
      // It correctly flags this as a known limitation — the warning is still useful as a
      // reminder to verify the pg_trgm index is in place.
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM articles WHERE title LIKE '%test%'")),
              EMPTY_INDEX);

      List<Issue> likeIssues = issuesOfType(issues, IssueType.LIKE_LEADING_WILDCARD);
      assertThat(likeIssues)
          .as(
              "LIKE with leading wildcard is flagged — known limitation: "
                  + "pg_trgm GIN index makes this sargable but detector cannot verify")
          .isNotEmpty();
      assertThat(likeIssues.get(0).severity()).isEqualTo(Severity.WARNING);
    }
  }

  // =====================================================================
  // 12. ForUpdateWithoutTimeoutDetector
  // =====================================================================

  @Nested
  @DisplayName("ForUpdateWithoutTimeoutDetector — PostgreSQL NOWAIT / SKIP LOCKED")
  class ForUpdateWithoutTimeoutDetectorTests {

    private final ForUpdateWithoutTimeoutDetector detector =
        new ForUpdateWithoutTimeoutDetector();

    @Test
    @DisplayName(
        "TN: FOR UPDATE NOWAIT is not flagged — Ref: https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE")
    void tn_forUpdateNowaitNotFlagged() {
      // PostgreSQL's FOR UPDATE NOWAIT immediately raises an error if the row is locked,
      // preventing indefinite blocking. This is the idiomatic PostgreSQL pattern for
      // pessimistic locking with timeout protection.
      // The detector correctly recognizes NOWAIT and does not flag the query.
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM accounts WHERE id = 1 FOR UPDATE NOWAIT")),
              EMPTY_INDEX);

      List<Issue> forUpdateIssues = issuesOfType(issues, IssueType.FOR_UPDATE_WITHOUT_TIMEOUT);
      assertThat(forUpdateIssues)
          .as("FOR UPDATE NOWAIT should not be flagged — NOWAIT prevents indefinite blocking")
          .isEmpty();
    }

    @Test
    @DisplayName(
        "TN: FOR UPDATE SKIP LOCKED is not flagged — Ref: https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE")
    void tn_forUpdateSkipLockedNotFlagged() {
      // PostgreSQL's FOR UPDATE SKIP LOCKED silently skips rows that are already locked.
      // This is commonly used for queue-like processing patterns where workers pick up
      // unlocked rows without blocking on locked ones.
      // The detector correctly recognizes SKIP LOCKED and does not flag the query.
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM job_queue WHERE status = 'pending' ORDER BY created_at LIMIT 1 FOR UPDATE SKIP LOCKED")),
              EMPTY_INDEX);

      List<Issue> forUpdateIssues = issuesOfType(issues, IssueType.FOR_UPDATE_WITHOUT_TIMEOUT);
      assertThat(forUpdateIssues)
          .as(
              "FOR UPDATE SKIP LOCKED should not be flagged — "
                  + "SKIP LOCKED is a non-blocking locking strategy")
          .isEmpty();
    }

    @Test
    @DisplayName(
        "TP: Bare FOR UPDATE without NOWAIT/SKIP LOCKED is flagged — Ref: https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE")
    void tp_bareForUpdate_flagged() {
      // A bare FOR UPDATE without NOWAIT or SKIP LOCKED will block indefinitely if the
      // target row is locked by another transaction. This is the anti-pattern the detector
      // is designed to catch. Both MySQL and PostgreSQL have the same blocking behavior.
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM accounts WHERE id = 1 FOR UPDATE")),
              EMPTY_INDEX);

      List<Issue> forUpdateIssues = issuesOfType(issues, IssueType.FOR_UPDATE_WITHOUT_TIMEOUT);
      assertThat(forUpdateIssues)
          .as("Bare FOR UPDATE should be flagged — may block indefinitely")
          .isNotEmpty();
      assertThat(forUpdateIssues.get(0).severity()).isEqualTo(Severity.WARNING);
    }
  }
}
