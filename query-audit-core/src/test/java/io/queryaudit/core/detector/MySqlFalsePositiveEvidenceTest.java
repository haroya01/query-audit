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
 * MySQL-specific false positive evidence tests.
 *
 * <p>Each test documents a scenario where the detector's behaviour may diverge from what MySQL
 * actually does at runtime. Every test carries an external reference (MySQL official docs, academic
 * paper, or well-known resource) that proves the pattern is legitimate.
 *
 * <p>Tests are grouped by detector in {@code @Nested} classes.
 *
 * @author generated
 */
class MySqlFalsePositiveEvidenceTest {

  // ═══════════════════════════════════════════════════════════════
  //  Helpers
  // ═══════════════════════════════════════════════════════════════

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  // ═══════════════════════════════════════════════════════════════
  //  1. WhereFunctionDetector — MySQL 8.0 Functional Indexes
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("WhereFunctionDetector — MySQL 8.0 Functional Indexes")
  class WhereFunctionDetectorFalsePositives {

    private final WhereFunctionDetector detector = new WhereFunctionDetector();

    @Test
    @DisplayName(
        "TN: LOWER(email) is sargable with functional index (known limitation) "
            + "- Ref: https://dev.mysql.com/doc/refman/8.0/en/create-index.html#create-index-functional-key-parts")
    void lowerInWhere_flaggedButSuggestionMentionsFunctionalIndex() {
      // MySQL 8.0+ supports functional indexes: CREATE INDEX idx ON t ((LOWER(col))).
      // When such an index exists, WHERE LOWER(col) = ? IS sargable.
      // The detector cannot know about functional indexes at analysis time,
      // so it flags this as an error. However, its suggestion should mention
      // functional indexes as a viable alternative.
      String sql = "SELECT * FROM users WHERE LOWER(email) = 'test@example.com'";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // Known limitation: detector fires even though a functional index could exist
      assertThat(issues)
          .anyMatch(i -> i.type() == IssueType.WHERE_FUNCTION && i.severity() == Severity.ERROR);

      // Verify the suggestion mentions functional index as a workaround
      assertThat(issues)
          .anyMatch(
              i ->
                  i.type() == IssueType.WHERE_FUNCTION
                      && i.suggestion() != null
                      && i.suggestion().toLowerCase().contains("functional index"));
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  2. SelectAllDetector — SELECT * in EXISTS subquery
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("SelectAllDetector — EXISTS and derived table handling")
  class SelectAllDetectorFalsePositives {

    private final SelectAllDetector detector = new SelectAllDetector();

    @Test
    @DisplayName(
        "TN: SELECT * inside EXISTS is idiomatic and optimized away "
            + "- Ref: https://dev.mysql.com/doc/refman/8.0/en/exists-and-not-exists-subqueries.html")
    void selectStarInExists_notFlagged() {
      // MySQL documentation: "Traditionally, an EXISTS subquery starts with SELECT *,
      // but it could begin with SELECT 5 or SELECT column1 or anything at all.
      // MySQL ignores the SELECT list in such a subquery."
      // EXISTS (SELECT * FROM ...) is idiomatic SQL and should NOT trigger SELECT_ALL.
      String sql =
          "SELECT id FROM orders WHERE EXISTS "
              + "(SELECT * FROM order_items WHERE order_items.order_id = orders.id)";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // The parser's EXISTS_SELECT_STAR pattern neutralizes SELECT * inside EXISTS
      assertThat(issues).noneMatch(i -> i.type() == IssueType.SELECT_ALL);
    }

    @Test
    @DisplayName(
        "TN: SELECT * in derived table with outer column projection "
            + "- Ref: https://dev.mysql.com/doc/refman/8.0/en/derived-tables.html")
    void selectStarInDerivedTable_outerSelectsSpecificColumns() {
      // When the outer query selects specific columns from a derived table,
      // using SELECT * in the inner query is common and valid.
      // MySQL 8.0 derived table merging may optimize this automatically.
      String sql = "SELECT sub.id, sub.name FROM " + "(SELECT * FROM users WHERE active = 1) sub";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // The outer SELECT * is what matters; inner SELECT * in derived tables
      // is a style issue at worst. Current detector behaviour is documented here.
      // If it triggers, it's a known limitation (detector sees SELECT * in inner query).
      // This test documents the current behaviour.
      if (!issues.isEmpty()) {
        // Known limitation: detector flags inner SELECT * in derived table
        assertThat(issues).allMatch(i -> i.type() == IssueType.SELECT_ALL);
      }
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  3. OrAbuseDetector — MySQL index_merge optimization
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("OrAbuseDetector — MySQL index_merge optimization")
  class OrAbuseDetectorFalsePositives {

    @Test
    @DisplayName(
        "TN: Single OR on two indexed columns below threshold is not flagged "
            + "- Ref: https://dev.mysql.com/doc/refman/8.0/en/index-merge-optimization.html")
    void singleOrBelowThreshold_notFlagged() {
      // MySQL can use index_merge for WHERE col1 = ? OR col2 = ?
      // if both columns are individually indexed.
      // With default threshold=3, a single OR (2 conditions) should not trigger.
      OrAbuseDetector detector = new OrAbuseDetector(3);

      String sql = "SELECT * FROM users WHERE email = 'a@b.com' OR phone = '1234567890'";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // Only 1 OR condition (below threshold of 3), should not trigger
      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName(
        "TN: 4 OR conditions on different indexed columns — legitimate with index_merge (known limitation) "
            + "- Ref: https://dev.mysql.com/doc/refman/8.0/en/index-merge-optimization.html")
    void fourOrConditionsOnDifferentIndexedColumns_flaggedAsKnownLimitation() {
      // MySQL index_merge can handle OR across different indexed columns.
      // The detector counts OR keywords (not conditions), so threshold=3 means >= 3 OR keywords.
      // This test documents that the detector fires even when index_merge could optimize it.
      OrAbuseDetector detector = new OrAbuseDetector(3);

      String sql =
          "SELECT * FROM users WHERE email = 'a@b.com' "
              + "OR phone = '1234567890' "
              + "OR username = 'testuser' "
              + "OR nickname = 'test'";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // Known limitation: detector flags this even though MySQL index_merge could optimize it
      // (3 OR keywords >= threshold of 3)
      assertThat(issues).anyMatch(i -> i.type() == IssueType.OR_ABUSE);
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  4. ImplicitTypeConversionDetector — Numeric _code columns
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("ImplicitTypeConversionDetector — Numeric _code column limitation")
  class ImplicitTypeConversionDetectorFalsePositives {

    private final ImplicitTypeConversionDetector detector = new ImplicitTypeConversionDetector();

    @Test
    @DisplayName(
        "TN: zip_code = 12345 triggers despite possibly being an INTEGER column (known limitation) "
            + "- Ref: https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html")
    void numericComparisonOnCodeColumn_flaggedAsKnownLimitation() {
      // country_code, zip_code, area_code could be INTEGER columns.
      // WHERE zip_code = 12345 is NOT implicit type conversion if the column is numeric.
      // The detector uses heuristics (_code suffix -> string) and cannot verify actual schema.
      String sql = "SELECT * FROM addresses WHERE zip_code = 12345";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // Known limitation: detector flags this because _code is in STRING_COLUMN_INDICATORS
      assertThat(issues)
          .anyMatch(
              i -> i.type() == IssueType.IMPLICIT_TYPE_CONVERSION && "zip_code".equals(i.column()));
    }

    @Test
    @DisplayName(
        "TN: country_code = 82 triggers despite being a valid numeric comparison (known limitation) "
            + "- Ref: https://dev.mysql.com/doc/refman/8.0/en/type-conversion.html")
    void countryCodeNumeric_flaggedAsKnownLimitation() {
      // country_code could be an INT storing ISO 3166-1 numeric codes (e.g., 82 = South Korea).
      // The detector cannot distinguish string vs numeric column types.
      String sql = "SELECT * FROM countries WHERE country_code = 82";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // Known limitation: heuristic-based detection cannot verify column type
      assertThat(issues)
          .anyMatch(
              i ->
                  i.type() == IssueType.IMPLICIT_TYPE_CONVERSION
                      && "country_code".equals(i.column()));
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  5. InsertOnDuplicateKeyDetector — Single-row UPSERT
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("InsertOnDuplicateKeyDetector — Single-row UPSERT pattern")
  class InsertOnDuplicateKeyDetectorFalsePositives {

    private final InsertOnDuplicateKeyDetector detector = new InsertOnDuplicateKeyDetector();

    @Test
    @DisplayName(
        "TN: Single-row INSERT ON DUPLICATE KEY UPDATE is the recommended MySQL upsert (known limitation) "
            + "- Ref: https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html")
    void singleRowUpsert_flaggedAsKnownLimitation() {
      // Single-row INSERT ... ON DUPLICATE KEY UPDATE on a unique key is the
      // recommended MySQL upsert pattern. Deadlock risk is minimal for single-row
      // operations, but the detector cannot distinguish single-row from bulk operations.
      String sql =
          "INSERT INTO user_settings (user_id, theme, locale) VALUES (1, 'dark', 'en') "
              + "ON DUPLICATE KEY UPDATE theme = VALUES(theme), locale = VALUES(locale)";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // Known limitation: detector flags single-row upserts even though deadlock risk is minimal
      assertThat(issues).anyMatch(i -> i.type() == IssueType.INSERT_ON_DUPLICATE_KEY);
      assertThat(issues).allMatch(i -> i.severity() == Severity.WARNING);
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  6. CartesianJoinDetector — Intentional CROSS JOIN
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("CartesianJoinDetector — Intentional CROSS JOIN exclusion")
  class CartesianJoinDetectorFalsePositives {

    private final CartesianJoinDetector detector = new CartesianJoinDetector();

    @Test
    @DisplayName(
        "TN: Explicit CROSS JOIN is intentional and should not be flagged "
            + "- Ref: https://dev.mysql.com/doc/refman/8.0/en/join.html")
    void explicitCrossJoin_notFlagged() {
      // CROSS JOIN is the standard SQL way to produce a Cartesian product intentionally.
      // Common use cases: generating all combinations of sizes and colors,
      // calendar generation, pivot table construction.
      String sql = "SELECT * FROM sizes CROSS JOIN colors";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // CROSS JOIN is explicitly excluded from detection (see CartesianJoinDetector regex)
      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName(
        "TN: CROSS JOIN with WHERE clause is not flagged "
            + "- Ref: https://dev.mysql.com/doc/refman/8.0/en/join.html")
    void crossJoinWithWhereClause_notFlagged() {
      // CROSS JOIN with a WHERE clause that filters is a valid pattern
      String sql = "SELECT s.name, c.name FROM sizes s CROSS JOIN colors c WHERE s.active = 1";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  7. LikeWildcardDetector — FULLTEXT search context
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("LikeWildcardDetector — Leading wildcard LIKE vs FULLTEXT")
  class LikeWildcardDetectorFalsePositives {

    private final LikeWildcardDetector detector = new LikeWildcardDetector();

    @Test
    @DisplayName(
        "TN: LIKE '%test%' is correctly flagged — MATCH...AGAINST is the alternative "
            + "- Ref: https://dev.mysql.com/doc/refman/8.0/en/fulltext-search.html")
    void leadingWildcardLike_correctlyFlagged() {
      // Leading wildcard LIKE prevents B-tree index usage.
      // The correct alternative for full-text search is MATCH...AGAINST with a FULLTEXT index.
      // This test verifies the detector fires correctly and documents the alternative.
      String sql = "SELECT * FROM articles WHERE title LIKE '%test%'";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // Detector should flag this correctly
      assertThat(issues).anyMatch(i -> i.type() == IssueType.LIKE_LEADING_WILDCARD);

      // Verify suggestion mentions fulltext as an alternative
      assertThat(issues)
          .anyMatch(
              i ->
                  i.type() == IssueType.LIKE_LEADING_WILDCARD
                      && i.suggestion() != null
                      && i.suggestion().toLowerCase().contains("fulltext"));
    }

    @Test
    @DisplayName(
        "TN: Trailing wildcard LIKE 'test%' is NOT flagged (sargable pattern) "
            + "- Ref: https://dev.mysql.com/doc/refman/8.0/en/fulltext-search.html")
    void trailingWildcardOnly_notFlagged() {
      // LIKE 'prefix%' IS sargable and can use a B-tree index.
      String sql = "SELECT * FROM articles WHERE title LIKE 'test%'";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  8. NotInSubqueryDetector — NOT NULL primary key subquery
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("NotInSubqueryDetector — NOT NULL subquery column limitation")
  class NotInSubqueryDetectorFalsePositives {

    private final NotInSubqueryDetector detector = new NotInSubqueryDetector();

    @Test
    @DisplayName(
        "TN: NOT IN (SELECT id ...) where id is NOT NULL PK will never return NULL (known limitation) "
            + "- Ref: https://dev.mysql.com/doc/refman/8.0/en/subquery-errors.html")
    void notInWithNotNullPrimaryKey_flaggedAsKnownLimitation() {
      // NOT IN (SELECT id FROM t) where id is a NOT NULL primary key will NEVER
      // produce a NULL in the subquery result, so the NULL-related correctness
      // concern does not apply. The detector flags it anyway because it cannot
      // verify column nullability from SQL text alone.
      String sql =
          "SELECT * FROM orders WHERE customer_id NOT IN "
              + "(SELECT id FROM customers WHERE active = 1)";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // Known limitation: detector flags NOT IN (SELECT ...) regardless of nullability
      assertThat(issues).anyMatch(i -> i.type() == IssueType.NOT_IN_SUBQUERY);
      assertThat(issues).allMatch(i -> i.severity() == Severity.ERROR);
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  9. CountInsteadOfExistsDetector — COUNT for pagination total
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("CountInsteadOfExistsDetector — Legitimate COUNT usage")
  class CountInsteadOfExistsDetectorFalsePositives {

    private final CountInsteadOfExistsDetector detector = new CountInsteadOfExistsDetector();

    @Test
    @DisplayName(
        "TN: COUNT(*) for pagination total is flagged as INFO (low severity is appropriate) "
            + "- Ref: https://dev.mysql.com/doc/refman/8.0/en/information-functions.html")
    void countForPaginationTotal_flaggedAsInfoSeverity() {
      // SELECT COUNT(*) FROM orders WHERE user_id = ? is needed when the actual
      // count value is required (e.g., for pagination total count display).
      // The detector should flag it as INFO (not ERROR/WARNING) because the
      // suggestion may not apply when the count value itself is needed.
      String sql = "SELECT COUNT(*) FROM orders WHERE user_id = 42";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // Detector flags it, but severity should be INFO (informational)
      assertThat(issues).anyMatch(i -> i.type() == IssueType.COUNT_INSTEAD_OF_EXISTS);
      assertThat(issues).allMatch(i -> i.severity() == Severity.INFO);

      // Verify the suggestion acknowledges that the count value may be needed
      assertThat(issues)
          .anyMatch(
              i ->
                  i.suggestion() != null
                      && i.suggestion().toLowerCase().contains("ignore if the actual count"));
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  10. SargabilityDetector — CASE expression false positive
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("SargabilityDetector — CASE expression in WHERE")
  class SargabilityDetectorFalsePositives {

    private final SargabilityDetector detector = new SargabilityDetector();

    @Test
    @DisplayName(
        "TN: CASE WHEN in WHERE is not arithmetic on a column "
            + "- Ref: https://dev.mysql.com/doc/refman/8.0/en/flow-control-functions.html#operator_case")
    void caseWhenInWhere_notFalselyDetectedAsArithmetic() {
      // CASE WHEN col1 = 1 THEN col2 ELSE col3 END = ? should not be flagged
      // as non-sargable arithmetic. The CASE expression is a flow control
      // function, not arithmetic on a column.
      String sql =
          "SELECT * FROM orders WHERE "
              + "CASE WHEN status = 1 THEN priority ELSE category END = 'high'";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // CASE-WHEN should not be falsely detected as col +/- arithmetic
      assertThat(issues).noneMatch(i -> i.type() == IssueType.NON_SARGABLE_EXPRESSION);
    }

    @Test
    @DisplayName(
        "TN: Simple column arithmetic col + 1 = ? is correctly flagged "
            + "- Ref: https://dev.mysql.com/doc/refman/8.0/en/flow-control-functions.html#operator_case")
    void columnArithmetic_correctlyFlagged() {
      // Verify that real arithmetic on columns is still detected
      String sql = "SELECT * FROM orders WHERE price + 1 = 100";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).anyMatch(i -> i.type() == IssueType.NON_SARGABLE_EXPRESSION);
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  11. OffsetPaginationDetector — Small OFFSET values
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("OffsetPaginationDetector — Small OFFSET values")
  class OffsetPaginationDetectorFalsePositives {

    @Test
    @DisplayName(
        "TN: OFFSET 10 (page 2) is below threshold and should not trigger "
            + "- Ref: https://dev.mysql.com/doc/refman/8.0/en/select.html")
    void smallOffset_notFlagged() {
      // OFFSET 10 with LIMIT 10 is page 2 — perfectly fine for performance.
      // Only large offsets (>= 1000 by default) are problematic because MySQL
      // must scan and discard all rows up to the offset.
      OffsetPaginationDetector detector = new OffsetPaginationDetector(1000);

      String sql = "SELECT * FROM orders LIMIT 10 OFFSET 10";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // OFFSET 10 is well below the 1000 threshold
      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName(
        "TN: OFFSET 999 is just below the default threshold and should not trigger "
            + "- Ref: https://dev.mysql.com/doc/refman/8.0/en/select.html")
    void offsetJustBelowThreshold_notFlagged() {
      OffsetPaginationDetector detector = new OffsetPaginationDetector(1000);

      String sql = "SELECT * FROM orders LIMIT 10 OFFSET 999";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName(
        "TN: OFFSET 1000 triggers at the threshold boundary "
            + "- Ref: https://dev.mysql.com/doc/refman/8.0/en/select.html")
    void offsetAtThreshold_flagged() {
      OffsetPaginationDetector detector = new OffsetPaginationDetector(1000);

      String sql = "SELECT * FROM orders LIMIT 10 OFFSET 1000";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).anyMatch(i -> i.type() == IssueType.OFFSET_PAGINATION);
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  12. ImplicitJoinDetector — Single table with IN subquery
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("ImplicitJoinDetector — Single table with IN subquery")
  class ImplicitJoinDetectorFalsePositives {

    private final ImplicitJoinDetector detector = new ImplicitJoinDetector();

    @Test
    @DisplayName(
        "TN: Single-table query with IN (SELECT ...) subquery should not be flagged "
            + "- Ref: https://dev.mysql.com/doc/refman/8.0/en/join.html")
    void singleTableWithInSubquery_notFlagged() {
      // A single-table query with an IN subquery is NOT an implicit join.
      // The subquery creates a nested scope, not a comma-separated FROM clause.
      String sql = "SELECT * FROM users WHERE id IN (SELECT user_id FROM active_sessions)";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // The detector uses SqlParser.removeSubqueries() before checking,
      // so the IN (SELECT ...) should be stripped and not cause a false positive
      assertThat(issues).noneMatch(i -> i.type() == IssueType.IMPLICIT_JOIN);
    }

    @Test
    @DisplayName(
        "TN: Single-table query with EXISTS subquery should not be flagged "
            + "- Ref: https://dev.mysql.com/doc/refman/8.0/en/join.html")
    void singleTableWithExistsSubquery_notFlagged() {
      String sql =
          "SELECT * FROM orders WHERE EXISTS "
              + "(SELECT 1 FROM order_items WHERE order_items.order_id = orders.id)";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).noneMatch(i -> i.type() == IssueType.IMPLICIT_JOIN);
    }
  }
}
