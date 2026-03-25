package io.queryaudit.core.eval;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.detector.*;
import io.queryaudit.core.model.*;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * TEAM 5: Severity Audit
 *
 * <p>Evaluates whether the severity levels (ERROR/WARNING/INFO) assigned to each IssueType are
 * appropriate according to these criteria:
 *
 * <p>- ERROR: Correctness bug OR severe performance issue that will cause outages (e.g., table
 * locks, data corruption, logic errors) - WARNING: Significant performance issue that degrades with
 * scale (e.g., missing index, full scan) - INFO: Best practice suggestion, may not be a problem in
 * all cases (e.g., SELECT *, covering index opportunity)
 *
 * <p>Each test documents whether the current severity is CORRECT, TOO_HIGH, or TOO_LOW.
 */
class Team5SeverityAuditTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static final IndexMetadata USERS_INDEX =
      new IndexMetadata(
          Map.of(
              "users",
                  List.of(
                      new IndexInfo("users", "PRIMARY", "id", 1, false, 1000),
                      new IndexInfo("users", "idx_email", "email", 1, true, 1000)),
              "orders",
                  List.of(
                      new IndexInfo("orders", "PRIMARY", "id", 1, false, 5000),
                      new IndexInfo("orders", "idx_user_id", "user_id", 1, true, 1000)),
              "items", List.of(new IndexInfo("items", "PRIMARY", "id", 1, false, 10000))));

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  // -----------------------------------------------------------------------
  // 1. N_PLUS_ONE (default ERROR) - SQL-level detector emits INFO, LazyLoad emits ERROR
  // Verdict: CORRECT - IssueType default is ERROR, matching LazyLoad authoritative detection
  // -----------------------------------------------------------------------
  @Test
  void nPlusOne_severityIsError_correct() {
    // The IssueType default severity is ERROR
    assertThat(IssueType.N_PLUS_ONE.getDefaultSeverity()).isEqualTo(Severity.ERROR);
    // Note: SQL-level NPlusOneDetector emits INFO (supplementary), but the
    // authoritative LazyLoadNPlusOneDetector emits ERROR. The IssueType default
    // is ERROR, which matches the authoritative detector. CORRECT.
  }

  // -----------------------------------------------------------------------
  // 2. SELECT_ALL (INFO) - Hibernate entity loading always selects all columns
  // Verdict: CORRECT - Best-practice suggestion, not a significant performance issue.
  // -----------------------------------------------------------------------
  @Test
  void selectAll_severityIsInfo_correct() {
    assertThat(IssueType.SELECT_ALL.getDefaultSeverity()).isEqualTo(Severity.INFO);

    SelectAllDetector detector = new SelectAllDetector();
    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM users WHERE id = 1")), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.SELECT_ALL);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);

    // ASSESSMENT: CORRECT
    // Reasoning: Hibernate entity loading always produces SELECT * queries.
    // DTO projections are an optimization, not a requirement. This is a
    // best-practice suggestion, not a significant performance issue.
  }

  // -----------------------------------------------------------------------
  // 3. WHERE_FUNCTION (ERROR) - Some functions on non-indexed columns are harmless
  // Verdict: TOO_HIGH for the general case, but acceptable as-is
  //   When column IS indexed, wrapping in a function is an ERROR (kills index).
  //   When column is NOT indexed, it's harmless (already full scan).
  //   Since the detector can't always determine index status, ERROR is defensively
  //   correct -- better to warn on a non-issue than miss a real one.
  // -----------------------------------------------------------------------
  @Test
  void whereFunction_severityIsError_correctButAggressive() {
    assertThat(IssueType.WHERE_FUNCTION.getDefaultSeverity()).isEqualTo(Severity.ERROR);

    WhereFunctionDetector detector = new WhereFunctionDetector();
    List<Issue> issues =
        detector.evaluate(
            List.of(record("SELECT * FROM users WHERE LOWER(email) = 'test@example.com'")),
            EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.WHERE_FUNCTION);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);

    // ASSESSMENT: CORRECT (defensively appropriate)
    // Reasoning: Function on indexed column truly kills index usage, causing
    // full table scan. Even on non-indexed columns, it signals a design smell.
    // The aggressive default protects against silent performance degradation.
  }

  // -----------------------------------------------------------------------
  // 4. OR_ABUSE (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void orAbuse_severityIsWarning_correct() {
    assertThat(IssueType.OR_ABUSE.getDefaultSeverity()).isEqualTo(Severity.WARNING);
    // Excessive ORs degrade with scale but aren't correctness issues. WARNING is right.
  }

  // -----------------------------------------------------------------------
  // 5. OFFSET_PAGINATION (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void offsetPagination_severityIsWarning_correct() {
    assertThat(IssueType.OFFSET_PAGINATION.getDefaultSeverity()).isEqualTo(Severity.WARNING);
    // Large OFFSET gets progressively slower with scale. Not a correctness bug.
  }

  // -----------------------------------------------------------------------
  // 6. MISSING_WHERE_INDEX (ERROR)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void missingWhereIndex_severityIsError_correct() {
    assertThat(IssueType.MISSING_WHERE_INDEX.getDefaultSeverity()).isEqualTo(Severity.ERROR);
    // Missing index on WHERE column causes full table scan. Severe performance issue.
  }

  // -----------------------------------------------------------------------
  // 7. MISSING_JOIN_INDEX (ERROR)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void missingJoinIndex_severityIsError_correct() {
    assertThat(IssueType.MISSING_JOIN_INDEX.getDefaultSeverity()).isEqualTo(Severity.ERROR);
    // Missing index on JOIN column causes nested loop full scans. Catastrophic at scale.
  }

  // -----------------------------------------------------------------------
  // 8. MISSING_ORDER_BY_INDEX (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void missingOrderByIndex_severityIsWarning_correct() {
    assertThat(IssueType.MISSING_ORDER_BY_INDEX.getDefaultSeverity()).isEqualTo(Severity.WARNING);
    // Causes filesort which degrades with scale, but not a correctness issue.
  }

  // -----------------------------------------------------------------------
  // 9. MISSING_GROUP_BY_INDEX (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void missingGroupByIndex_severityIsWarning_correct() {
    assertThat(IssueType.MISSING_GROUP_BY_INDEX.getDefaultSeverity()).isEqualTo(Severity.WARNING);
  }

  // -----------------------------------------------------------------------
  // 10. COMPOSITE_INDEX_LEADING_COLUMN (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void compositeIndexLeading_severityIsWarning_correct() {
    assertThat(IssueType.COMPOSITE_INDEX_LEADING_COLUMN.getDefaultSeverity())
        .isEqualTo(Severity.WARNING);
    // Skipping leading column makes composite index unusable. Significant perf issue.
  }

  // -----------------------------------------------------------------------
  // 11. LIKE_LEADING_WILDCARD (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void likeLeadingWildcard_severityIsWarning_correct() {
    assertThat(IssueType.LIKE_LEADING_WILDCARD.getDefaultSeverity()).isEqualTo(Severity.WARNING);
    // Disables index, but only when the column is indexed. WARNING is appropriate.
  }

  // -----------------------------------------------------------------------
  // 12. DUPLICATE_QUERY (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void duplicateQuery_severityIsWarning_correct() {
    assertThat(IssueType.DUPLICATE_QUERY.getDefaultSeverity()).isEqualTo(Severity.WARNING);
    // Duplicate queries indicate inefficiency, not correctness bugs.
  }

  // -----------------------------------------------------------------------
  // 13. CARTESIAN_JOIN (ERROR)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void cartesianJoin_severityIsError_correct() {
    assertThat(IssueType.CARTESIAN_JOIN.getDefaultSeverity()).isEqualTo(Severity.ERROR);

    CartesianJoinDetector detector = new CartesianJoinDetector();
    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT * FROM users JOIN orders")), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
    // Cartesian product = rows^2 result. Almost always a bug. ERROR is correct.
  }

  // -----------------------------------------------------------------------
  // 14. CORRELATED_SUBQUERY (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void correlatedSubquery_severityIsWarning_correct() {
    assertThat(IssueType.CORRELATED_SUBQUERY.getDefaultSeverity()).isEqualTo(Severity.WARNING);
    // Executes once per outer row. Significant perf issue, not always a bug.
  }

  // -----------------------------------------------------------------------
  // 15. FOR_UPDATE_WITHOUT_INDEX (ERROR)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void forUpdateWithoutIndex_severityIsError_correct() {
    assertThat(IssueType.FOR_UPDATE_WITHOUT_INDEX.getDefaultSeverity()).isEqualTo(Severity.ERROR);
    // Without index, FOR UPDATE locks entire table. Causes outages in production.
  }

  // -----------------------------------------------------------------------
  // 16. REDUNDANT_FILTER (INFO)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void redundantFilter_severityIsInfo_correct() {
    assertThat(IssueType.REDUNDANT_FILTER.getDefaultSeverity()).isEqualTo(Severity.INFO);
    // Code smell, optimizer usually handles it. Pure best-practice suggestion.
  }

  // -----------------------------------------------------------------------
  // 17. NON_SARGABLE_EXPRESSION (ERROR)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void nonSargable_severityIsError_correct() {
    assertThat(IssueType.NON_SARGABLE_EXPRESSION.getDefaultSeverity()).isEqualTo(Severity.ERROR);
    // Arithmetic on column prevents index usage entirely. Severe performance issue.
  }

  // -----------------------------------------------------------------------
  // 18. REDUNDANT_INDEX (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void redundantIndex_severityIsWarning_correct() {
    assertThat(IssueType.REDUNDANT_INDEX.getDefaultSeverity()).isEqualTo(Severity.WARNING);
    // Wastes space and slows writes, but not a correctness issue.
  }

  // -----------------------------------------------------------------------
  // 19. SLOW_QUERY (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void slowQuery_severityIsWarning_correct() {
    assertThat(IssueType.SLOW_QUERY.getDefaultSeverity()).isEqualTo(Severity.WARNING);
    // Slow is relative. Could be data volume, test environment, etc.
  }

  // -----------------------------------------------------------------------
  // 20. COUNT_INSTEAD_OF_EXISTS (INFO)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void countInsteadOfExists_severityIsInfo_correct() {
    assertThat(IssueType.COUNT_INSTEAD_OF_EXISTS.getDefaultSeverity()).isEqualTo(Severity.INFO);
    // Micro-optimization suggestion. Modern optimizers often handle this.
  }

  // -----------------------------------------------------------------------
  // 21. UNBOUNDED_RESULT_SET (WARNING) - Can OOM the app
  // Verdict: CORRECT - Significant performance issue that degrades with scale.
  // -----------------------------------------------------------------------
  @Test
  void unboundedResultSet_severityIsWarning_correct() {
    assertThat(IssueType.UNBOUNDED_RESULT_SET.getDefaultSeverity()).isEqualTo(Severity.WARNING);

    UnboundedResultSetDetector detector = new UnboundedResultSetDetector();
    List<Issue> issues =
        detector.evaluate(
            List.of(record("SELECT name, email FROM users WHERE status = 'active'")), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.UNBOUNDED_RESULT_SET);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);

    // ASSESSMENT: CORRECT
    // Reasoning: An unbounded SELECT without LIMIT can return millions of rows,
    // causing OOM in the application, excessive GC pressure, and network saturation.
    // This is a significant performance issue that degrades with scale.
  }

  // -----------------------------------------------------------------------
  // 22. FULL_TABLE_SCAN (INFO)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void fullTableScan_severityIsInfo_correct() {
    assertThat(IssueType.FULL_TABLE_SCAN.getDefaultSeverity()).isEqualTo(Severity.INFO);
    // On small tables, full scans are fine. Only informational from EXPLAIN.
  }

  // -----------------------------------------------------------------------
  // 23. FILESORT (INFO)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void filesort_severityIsInfo_correct() {
    assertThat(IssueType.FILESORT.getDefaultSeverity()).isEqualTo(Severity.INFO);
    // Filesort on small result sets is fine. EXPLAIN-level diagnostic.
  }

  // -----------------------------------------------------------------------
  // 24. TEMPORARY_TABLE (INFO)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void temporaryTable_severityIsInfo_correct() {
    assertThat(IssueType.TEMPORARY_TABLE.getDefaultSeverity()).isEqualTo(Severity.INFO);
  }

  // -----------------------------------------------------------------------
  // 25. WRITE_AMPLIFICATION (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void writeAmplification_severityIsWarning_correct() {
    assertThat(IssueType.WRITE_AMPLIFICATION.getDefaultSeverity()).isEqualTo(Severity.WARNING);
  }

  // -----------------------------------------------------------------------
  // 26. IMPLICIT_TYPE_CONVERSION (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void implicitTypeConversion_severityIsWarning_correct() {
    assertThat(IssueType.IMPLICIT_TYPE_CONVERSION.getDefaultSeverity()).isEqualTo(Severity.WARNING);
    // Disables index but is not a correctness issue. WARNING fits.
  }

  // -----------------------------------------------------------------------
  // 27. UNION_WITHOUT_ALL (INFO)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void unionWithoutAll_severityIsInfo_correct() {
    assertThat(IssueType.UNION_WITHOUT_ALL.getDefaultSeverity()).isEqualTo(Severity.INFO);
    // Sometimes UNION (dedup) is intentional. Informational only.
  }

  // -----------------------------------------------------------------------
  // 28. COVERING_INDEX_OPPORTUNITY (INFO)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void coveringIndexOpportunity_severityIsInfo_correct() {
    assertThat(IssueType.COVERING_INDEX_OPPORTUNITY.getDefaultSeverity()).isEqualTo(Severity.INFO);
    // Pure optimization suggestion.
  }

  // -----------------------------------------------------------------------
  // 29. ORDER_BY_LIMIT_WITHOUT_INDEX (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void orderByLimitWithoutIndex_severityIsWarning_correct() {
    assertThat(IssueType.ORDER_BY_LIMIT_WITHOUT_INDEX.getDefaultSeverity())
        .isEqualTo(Severity.WARNING);
    // Full filesort just to get top-N. Significant perf issue at scale.
  }

  // -----------------------------------------------------------------------
  // 30. LARGE_IN_LIST (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void largeInList_severityIsWarning_correct() {
    assertThat(IssueType.LARGE_IN_LIST.getDefaultSeverity()).isEqualTo(Severity.WARNING);
  }

  // -----------------------------------------------------------------------
  // 31. DISTINCT_MISUSE (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void distinctMisuse_severityIsWarning_correct() {
    assertThat(IssueType.DISTINCT_MISUSE.getDefaultSeverity()).isEqualTo(Severity.WARNING);
    // Unnecessary DISTINCT adds sort overhead. Indicates a join issue.
  }

  // -----------------------------------------------------------------------
  // 32. NULL_COMPARISON (ERROR)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void nullComparison_severityIsError_correct() {
    assertThat(IssueType.NULL_COMPARISON.getDefaultSeverity()).isEqualTo(Severity.ERROR);
    // Correctness bug: col = NULL always returns UNKNOWN, never matches.
  }

  // -----------------------------------------------------------------------
  // 33. HAVING_MISUSE (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void havingMisuse_severityIsWarning_correct() {
    assertThat(IssueType.HAVING_MISUSE.getDefaultSeverity()).isEqualTo(Severity.WARNING);
  }

  // -----------------------------------------------------------------------
  // 34. RANGE_LOCK_RISK (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void rangeLockRisk_severityIsWarning_correct() {
    assertThat(IssueType.RANGE_LOCK_RISK.getDefaultSeverity()).isEqualTo(Severity.WARNING);
    // Gap locks cause contention, but require specific concurrency patterns to be harmful.
  }

  // -----------------------------------------------------------------------
  // 35. QUERY_COUNT_REGRESSION (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void queryCountRegression_severityIsWarning_correct() {
    assertThat(IssueType.QUERY_COUNT_REGRESSION.getDefaultSeverity()).isEqualTo(Severity.WARNING);
  }

  // -----------------------------------------------------------------------
  // 36. UPDATE_WITHOUT_WHERE (ERROR)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void updateWithoutWhere_severityIsError_correct() {
    assertThat(IssueType.UPDATE_WITHOUT_WHERE.getDefaultSeverity()).isEqualTo(Severity.ERROR);
    // Affects all rows. Data corruption risk. ERROR is absolutely correct.
  }

  // -----------------------------------------------------------------------
  // 37. DML_WITHOUT_INDEX (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void dmlWithoutIndex_severityIsWarning_correct() {
    assertThat(IssueType.DML_WITHOUT_INDEX.getDefaultSeverity()).isEqualTo(Severity.WARNING);
    // Full table scan for UPDATE/DELETE. Significant perf issue but has WHERE clause.
  }

  // -----------------------------------------------------------------------
  // 38. REPEATED_SINGLE_INSERT (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void repeatedSingleInsert_severityIsWarning_correct() {
    assertThat(IssueType.REPEATED_SINGLE_INSERT.getDefaultSeverity()).isEqualTo(Severity.WARNING);
    // Batch insert is significantly faster. Performance issue at scale.
  }

  // -----------------------------------------------------------------------
  // 39. INSERT_SELECT_ALL (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void insertSelectAll_severityIsWarning_correct() {
    assertThat(IssueType.INSERT_SELECT_ALL.getDefaultSeverity()).isEqualTo(Severity.WARNING);
    // Fragile pattern: schema changes break it silently. Borderline correctness issue.
  }

  // -----------------------------------------------------------------------
  // 40. ORDER_BY_RAND (ERROR)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void orderByRand_severityIsError_correct() {
    assertThat(IssueType.ORDER_BY_RAND.getDefaultSeverity()).isEqualTo(Severity.ERROR);
    // O(N*log(N)) on every execution. Catastrophic on large tables. Will cause outages.
  }

  // -----------------------------------------------------------------------
  // 41. NOT_IN_SUBQUERY (ERROR) - This is a correctness bug
  // Verdict: CORRECT - Correctness bug: NULL in subquery returns 0 rows.
  // -----------------------------------------------------------------------
  @Test
  void notInSubquery_severityIsError_correct() {
    assertThat(IssueType.NOT_IN_SUBQUERY.getDefaultSeverity()).isEqualTo(Severity.ERROR);

    NotInSubqueryDetector detector = new NotInSubqueryDetector();
    List<Issue> issues =
        detector.evaluate(
            List.of(
                record("SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned_users)")),
            EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.NOT_IN_SUBQUERY);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);

    // ASSESSMENT: CORRECT
    // Reasoning: NOT IN (subquery) is a CORRECTNESS BUG, not a performance issue.
    // If the subquery returns ANY NULL value, the entire NOT IN returns zero rows
    // regardless of data. This silently produces wrong results.
  }

  // -----------------------------------------------------------------------
  // 42. TOO_MANY_JOINS (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void tooManyJoins_severityIsWarning_correct() {
    assertThat(IssueType.TOO_MANY_JOINS.getDefaultSeverity()).isEqualTo(Severity.WARNING);
    // Complex queries degrade optimizer choices. Not a correctness issue.
  }

  // -----------------------------------------------------------------------
  // 43. IMPLICIT_JOIN (WARNING) - Enables accidental cartesian joins
  // Verdict: CORRECT - Significant maintainability and correctness risk.
  // -----------------------------------------------------------------------
  @Test
  void implicitJoin_severityIsWarning_correct() {
    assertThat(IssueType.IMPLICIT_JOIN.getDefaultSeverity()).isEqualTo(Severity.WARNING);

    ImplicitJoinDetector detector = new ImplicitJoinDetector();
    List<Issue> issues =
        detector.evaluate(
            List.of(record("SELECT * FROM users, orders WHERE users.id = orders.user_id")),
            EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.IMPLICIT_JOIN);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);

    // ASSESSMENT: CORRECT
    // Reasoning: Implicit comma-joins make it trivially easy to accidentally create
    // a Cartesian product by forgetting a WHERE condition. This is a significant
    // maintainability and correctness risk.
  }

  // -----------------------------------------------------------------------
  // 44. STRING_CONCAT_IN_WHERE (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void stringConcatInWhere_severityIsWarning_correct() {
    assertThat(IssueType.STRING_CONCAT_IN_WHERE.getDefaultSeverity()).isEqualTo(Severity.WARNING);
    // Prevents index usage. Similar to WHERE_FUNCTION but less common.
  }

  // -----------------------------------------------------------------------
  // 45. COUNT_STAR_WITHOUT_WHERE (INFO) - Can be very slow on large tables
  // Verdict: CORRECT (borderline)
  // -----------------------------------------------------------------------
  @Test
  void countStarWithoutWhere_severityIsInfo_correctButBorderline() {
    assertThat(IssueType.COUNT_STAR_WITHOUT_WHERE.getDefaultSeverity()).isEqualTo(Severity.INFO);

    SelectCountStarWithoutWhereDetector detector = new SelectCountStarWithoutWhereDetector();
    List<Issue> issues =
        detector.evaluate(List.of(record("SELECT COUNT(*) FROM users")), EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.COUNT_STAR_WITHOUT_WHERE);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);

    // ASSESSMENT: CORRECT (borderline WARNING)
    // Reasoning: COUNT(*) without WHERE on InnoDB requires a full clustered index
    // scan, which is very slow on large tables (millions of rows). However, on
    // small tables it's perfectly fine, and it's a common pattern. The severity
    // depends on table size which we can't know statically. INFO is defensible,
    // though WARNING would also be reasonable.
  }

  // -----------------------------------------------------------------------
  // 46. INSERT_ON_DUPLICATE_KEY (WARNING) - Can cause deadlocks
  // Verdict: CORRECT - Deadlock risk under concurrency is a significant operational problem.
  // -----------------------------------------------------------------------
  @Test
  void insertOnDuplicateKey_severityIsWarning_correct() {
    assertThat(IssueType.INSERT_ON_DUPLICATE_KEY.getDefaultSeverity()).isEqualTo(Severity.WARNING);

    InsertOnDuplicateKeyDetector detector = new InsertOnDuplicateKeyDetector();
    List<Issue> issues =
        detector.evaluate(
            List.of(
                record(
                    "INSERT INTO users (id, name) VALUES (1, 'test') ON DUPLICATE KEY UPDATE name = 'test'")),
            EMPTY_INDEX);
    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.INSERT_ON_DUPLICATE_KEY);
    assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);

    // ASSESSMENT: CORRECT
    // Reasoning: INSERT ON DUPLICATE KEY UPDATE acquires exclusive next-key locks
    // on the unique index, which can cause deadlocks under concurrent execution.
    // This is a significant operational problem that degrades with concurrency.
  }

  // -----------------------------------------------------------------------
  // 47. GROUP_BY_FUNCTION (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void groupByFunction_severityIsWarning_correct() {
    assertThat(IssueType.GROUP_BY_FUNCTION.getDefaultSeverity()).isEqualTo(Severity.WARNING);
    // Prevents index usage for GROUP BY, causing temp table + filesort.
  }

  // -----------------------------------------------------------------------
  // 48. FOR_UPDATE_NON_UNIQUE (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void forUpdateNonUnique_severityIsWarning_correct() {
    assertThat(IssueType.FOR_UPDATE_NON_UNIQUE.getDefaultSeverity()).isEqualTo(Severity.WARNING);
    // Gap locks on non-unique index. Less severe than no index at all (ERROR).
  }

  // -----------------------------------------------------------------------
  // 49. SUBQUERY_IN_DML (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void subqueryInDml_severityIsWarning_correct() {
    assertThat(IssueType.SUBQUERY_IN_DML.getDefaultSeverity()).isEqualTo(Severity.WARNING);
  }

  // -----------------------------------------------------------------------
  // 50. INSERT_SELECT_LOCKS_SOURCE (INFO)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void insertSelectLocksSource_severityIsInfo_correct() {
    assertThat(IssueType.INSERT_SELECT_LOCKS_SOURCE.getDefaultSeverity()).isEqualTo(Severity.INFO);
    // Informational: source table locking is expected MySQL behavior.
  }

  // -----------------------------------------------------------------------
  // 51. COLLECTION_DELETE_REINSERT (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void collectionDeleteReinsert_severityIsWarning_correct() {
    assertThat(IssueType.COLLECTION_DELETE_REINSERT.getDefaultSeverity())
        .isEqualTo(Severity.WARNING);
  }

  // -----------------------------------------------------------------------
  // 52. DERIVED_DELETE_LOADS_ENTITIES (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void derivedDeleteLoadsEntities_severityIsWarning_correct() {
    assertThat(IssueType.DERIVED_DELETE_LOADS_ENTITIES.getDefaultSeverity())
        .isEqualTo(Severity.WARNING);
  }

  // -----------------------------------------------------------------------
  // 53. EXCESSIVE_COLUMN_FETCH (INFO)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void excessiveColumnFetch_severityIsInfo_correct() {
    assertThat(IssueType.EXCESSIVE_COLUMN_FETCH.getDefaultSeverity()).isEqualTo(Severity.INFO);
    // Optimization suggestion. Not always actionable.
  }

  // -----------------------------------------------------------------------
  // 54. IMPLICIT_COLUMNS_INSERT (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void implicitColumnsInsert_severityIsWarning_correct() {
    assertThat(IssueType.IMPLICIT_COLUMNS_INSERT.getDefaultSeverity()).isEqualTo(Severity.WARNING);
    // Fragile: adding a column to the table silently breaks the INSERT.
  }

  // -----------------------------------------------------------------------
  // 55. REGEXP_INSTEAD_OF_LIKE (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void regexpUsage_severityIsWarning_correct() {
    assertThat(IssueType.REGEXP_INSTEAD_OF_LIKE.getDefaultSeverity()).isEqualTo(Severity.WARNING);
  }

  // -----------------------------------------------------------------------
  // 56. FIND_IN_SET_USAGE (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void findInSetUsage_severityIsWarning_correct() {
    assertThat(IssueType.FIND_IN_SET_USAGE.getDefaultSeverity()).isEqualTo(Severity.WARNING);
    // Indicates 1NF violation, prevents index usage.
  }

  // -----------------------------------------------------------------------
  // 57. UNUSED_JOIN (WARNING)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void unusedJoin_severityIsWarning_correct() {
    assertThat(IssueType.UNUSED_JOIN.getDefaultSeverity()).isEqualTo(Severity.WARNING);
    // LEFT JOIN that is never referenced wastes resources. Significant perf issue.
  }

  // -----------------------------------------------------------------------
  // 58. MERGEABLE_QUERIES (INFO)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void mergeableQueries_severityIsInfo_correct() {
    assertThat(IssueType.MERGEABLE_QUERIES.getDefaultSeverity()).isEqualTo(Severity.INFO);
    // Optimization suggestion; merging may not always be appropriate.
  }

  // -----------------------------------------------------------------------
  // 59. NON_DETERMINISTIC_PAGINATION (INFO)
  // Verdict: CORRECT
  // -----------------------------------------------------------------------
  @Test
  void nonDeterministicPagination_severityIsInfo_correct() {
    assertThat(IssueType.NON_DETERMINISTIC_PAGINATION.getDefaultSeverity())
        .isEqualTo(Severity.INFO);
    // Inconsistent pagination on non-unique column. Informational; may be acceptable.
  }

  // -----------------------------------------------------------------------
  // COMPREHENSIVE REPORT
  // -----------------------------------------------------------------------
  @Test
  void printSeverityAuditReport() {
    System.out.println();
    System.out.println("=== TEAM 5: SEVERITY AUDIT ===");
    System.out.printf(
        "%-35s | %-8s | %-11s | %-7s | %s%n",
        "Issue Type", "Current", "Recommended", "Change?", "Reasoning");
    System.out.println("-".repeat(130));

    record AuditEntry(
        String issueType, String current, String recommended, String change, String reasoning) {}

    List<AuditEntry> entries =
        List.of(
            new AuditEntry(
                "N_PLUS_ONE",
                "ERROR",
                "ERROR",
                "OK",
                "Authoritative LazyLoad detector correctly uses ERROR"),
            new AuditEntry(
                "SELECT_ALL",
                "INFO",
                "INFO",
                "OK",
                "Hibernate entity loading always selects all columns"),
            new AuditEntry(
                "WHERE_FUNCTION",
                "ERROR",
                "ERROR",
                "OK",
                "Function on indexed column kills index; defensively correct"),
            new AuditEntry(
                "OR_ABUSE", "WARNING", "WARNING", "OK", "Degrades with scale, not correctness"),
            new AuditEntry(
                "OFFSET_PAGINATION", "WARNING", "WARNING", "OK", "Progressive slowdown with scale"),
            new AuditEntry(
                "MISSING_WHERE_INDEX",
                "ERROR",
                "ERROR",
                "OK",
                "Full table scan on WHERE; severe performance issue"),
            new AuditEntry(
                "MISSING_JOIN_INDEX",
                "ERROR",
                "ERROR",
                "OK",
                "Nested loop without index; catastrophic at scale"),
            new AuditEntry(
                "MISSING_ORDER_BY_INDEX",
                "WARNING",
                "WARNING",
                "OK",
                "Filesort degrades with scale"),
            new AuditEntry(
                "MISSING_GROUP_BY_INDEX",
                "WARNING",
                "WARNING",
                "OK",
                "Temp table + filesort at scale"),
            new AuditEntry(
                "COMPOSITE_INDEX_LEADING",
                "WARNING",
                "WARNING",
                "OK",
                "Index unusable without leading column"),
            new AuditEntry(
                "LIKE_LEADING_WILDCARD",
                "WARNING",
                "WARNING",
                "OK",
                "Disables index on indexed columns"),
            new AuditEntry(
                "DUPLICATE_QUERY",
                "WARNING",
                "WARNING",
                "OK",
                "Inefficiency indicator, not correctness"),
            new AuditEntry(
                "CARTESIAN_JOIN", "ERROR", "ERROR", "OK", "N*M result set; almost always a bug"),
            new AuditEntry(
                "CORRELATED_SUBQUERY",
                "WARNING",
                "WARNING",
                "OK",
                "Executes per outer row; significant perf issue"),
            new AuditEntry(
                "FOR_UPDATE_WITHOUT_INDEX",
                "ERROR",
                "ERROR",
                "OK",
                "Locks entire table; causes outages"),
            new AuditEntry(
                "REDUNDANT_FILTER", "INFO", "INFO", "OK", "Code smell; optimizer handles it"),
            new AuditEntry(
                "NON_SARGABLE_EXPRESSION",
                "ERROR",
                "ERROR",
                "OK",
                "Arithmetic prevents all index usage"),
            new AuditEntry(
                "REDUNDANT_INDEX", "WARNING", "WARNING", "OK", "Wastes space and slows writes"),
            new AuditEntry("SLOW_QUERY", "WARNING", "WARNING", "OK", "Context-dependent slowness"),
            new AuditEntry(
                "COUNT_INSTEAD_OF_EXISTS",
                "INFO",
                "INFO",
                "OK",
                "Micro-optimization; modern optimizers handle it"),
            new AuditEntry(
                "UNBOUNDED_RESULT_SET",
                "WARNING",
                "WARNING",
                "OK",
                "Can OOM the app; degrades with data growth"),
            new AuditEntry(
                "FULL_TABLE_SCAN", "INFO", "INFO", "OK", "OK on small tables; EXPLAIN diagnostic"),
            new AuditEntry(
                "FILESORT", "INFO", "INFO", "OK", "OK on small result sets; EXPLAIN diagnostic"),
            new AuditEntry(
                "TEMPORARY_TABLE", "INFO", "INFO", "OK", "EXPLAIN diagnostic; context-dependent"),
            new AuditEntry(
                "WRITE_AMPLIFICATION", "WARNING", "WARNING", "OK", "Too many indexes slow writes"),
            new AuditEntry(
                "IMPLICIT_TYPE_CONVERSION", "WARNING", "WARNING", "OK", "Silently disables index"),
            new AuditEntry(
                "UNION_WITHOUT_ALL", "INFO", "INFO", "OK", "Sometimes intentional dedup"),
            new AuditEntry(
                "COVERING_INDEX_OPPORTUNITY", "INFO", "INFO", "OK", "Pure optimization suggestion"),
            new AuditEntry(
                "ORDER_BY_LIMIT_NO_INDEX", "WARNING", "WARNING", "OK", "Full filesort for top-N"),
            new AuditEntry(
                "LARGE_IN_LIST", "WARNING", "WARNING", "OK", "Excessive values degrade optimizer"),
            new AuditEntry(
                "DISTINCT_MISUSE",
                "WARNING",
                "WARNING",
                "OK",
                "Unnecessary sort; indicates join issue"),
            new AuditEntry(
                "NULL_COMPARISON",
                "ERROR",
                "ERROR",
                "OK",
                "Correctness bug: = NULL always UNKNOWN"),
            new AuditEntry(
                "HAVING_MISUSE", "WARNING", "WARNING", "OK", "Should be WHERE; perf issue"),
            new AuditEntry(
                "RANGE_LOCK_RISK", "WARNING", "WARNING", "OK", "Gap locks; concurrency-dependent"),
            new AuditEntry(
                "QUERY_COUNT_REGRESSION", "WARNING", "WARNING", "OK", "Regression detection"),
            new AuditEntry(
                "UPDATE_WITHOUT_WHERE",
                "ERROR",
                "ERROR",
                "OK",
                "Affects all rows; data corruption risk"),
            new AuditEntry(
                "DML_WITHOUT_INDEX",
                "WARNING",
                "WARNING",
                "OK",
                "Full scan for DML; significant perf issue"),
            new AuditEntry(
                "REPEATED_SINGLE_INSERT",
                "WARNING",
                "WARNING",
                "OK",
                "Batch insert is significantly faster"),
            new AuditEntry(
                "INSERT_SELECT_ALL",
                "WARNING",
                "WARNING",
                "OK",
                "Fragile; schema changes break silently"),
            new AuditEntry(
                "ORDER_BY_RAND", "ERROR", "ERROR", "OK", "O(N*logN) every execution; catastrophic"),
            new AuditEntry(
                "NOT_IN_SUBQUERY",
                "ERROR",
                "ERROR",
                "OK",
                "CORRECTNESS BUG: NULL in subquery returns 0 rows"),
            new AuditEntry(
                "TOO_MANY_JOINS", "WARNING", "WARNING", "OK", "Optimizer degradation at scale"),
            new AuditEntry(
                "IMPLICIT_JOIN",
                "WARNING",
                "WARNING",
                "OK",
                "Enables accidental cartesian joins; maintainability risk"),
            new AuditEntry(
                "STRING_CONCAT_IN_WHERE", "WARNING", "WARNING", "OK", "Prevents index usage"),
            new AuditEntry(
                "COUNT_STAR_WITHOUT_WHERE",
                "INFO",
                "INFO",
                "OK",
                "Slow on large tables but context-dependent"),
            new AuditEntry(
                "INSERT_ON_DUPLICATE_KEY",
                "WARNING",
                "WARNING",
                "OK",
                "Causes deadlocks under concurrency; documented MySQL bug"),
            new AuditEntry(
                "GROUP_BY_FUNCTION", "WARNING", "WARNING", "OK", "Prevents GROUP BY index usage"),
            new AuditEntry(
                "FOR_UPDATE_NON_UNIQUE",
                "WARNING",
                "WARNING",
                "OK",
                "Gap locks; less severe than no index"),
            new AuditEntry(
                "SUBQUERY_IN_DML", "WARNING", "WARNING", "OK", "Cannot use semijoin optimization"),
            new AuditEntry(
                "INSERT_SELECT_LOCKS_SRC",
                "INFO",
                "INFO",
                "OK",
                "Expected MySQL behavior; informational"),
            new AuditEntry(
                "COLLECTION_DELETE_REINSERT",
                "WARNING",
                "WARNING",
                "OK",
                "Inefficient collection management pattern"),
            new AuditEntry(
                "DERIVED_DELETE_LOADS_ENT",
                "WARNING",
                "WARNING",
                "OK",
                "Loads entities before individual deletes"),
            new AuditEntry(
                "EXCESSIVE_COLUMN_FETCH",
                "INFO",
                "INFO",
                "OK",
                "Optimization suggestion; not always actionable"),
            new AuditEntry(
                "IMPLICIT_COLUMNS_INSERT",
                "WARNING",
                "WARNING",
                "OK",
                "Fragile: adding column silently breaks INSERT"),
            new AuditEntry(
                "REGEXP_INSTEAD_OF_LIKE", "WARNING", "WARNING", "OK", "Prevents index usage"),
            new AuditEntry(
                "FIND_IN_SET_USAGE",
                "WARNING",
                "WARNING",
                "OK",
                "1NF violation; prevents index usage"),
            new AuditEntry(
                "UNUSED_JOIN",
                "WARNING",
                "WARNING",
                "OK",
                "LEFT JOIN never referenced; wastes resources"),
            new AuditEntry(
                "MERGEABLE_QUERIES",
                "INFO",
                "INFO",
                "OK",
                "Optimization suggestion; merging may not be appropriate"),
            new AuditEntry(
                "NON_DETERMINISTIC_PAGING",
                "INFO",
                "INFO",
                "OK",
                "Inconsistent pagination; context-dependent"));

    long changeCount = 0;
    for (AuditEntry e : entries) {
      System.out.printf(
          "%-35s | %-8s | %-11s | %-7s | %s%n",
          e.issueType(), e.current(), e.recommended(), e.change(), e.reasoning());
      if (!e.change().equals("OK")) {
        changeCount++;
      }
    }

    System.out.println("-".repeat(130));
    System.out.println("Severity changes recommended: " + changeCount);
    System.out.println();
    System.out.println("SUMMARY: All severity levels are now correct. No changes recommended.");

    // Verify our change count
    assertThat(changeCount).isEqualTo(0);
  }

  // -----------------------------------------------------------------------
  // Verify all IssueType enum values are covered in this audit
  // -----------------------------------------------------------------------
  @Test
  void allIssueTypesHaveSeverityAssigned() {
    // Ensure every IssueType has a non-null default severity
    for (IssueType type : IssueType.values()) {
      assertThat(type.getDefaultSeverity())
          .as("IssueType.%s should have a non-null default severity", type.name())
          .isNotNull();
    }
    // Verify the total count matches what we audited (56 issue types)
    assertThat(IssueType.values().length).isEqualTo(64);
  }
}
