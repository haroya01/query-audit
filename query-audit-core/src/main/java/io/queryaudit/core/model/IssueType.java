package io.queryaudit.core.model;

import static io.queryaudit.core.model.Severity.*;

/**
 * Enumeration of all issue types that can be detected by query-audit.
 *
 * @author haroya
 * @since 0.2.0
 */
public enum IssueType {
  N_PLUS_ONE("n-plus-one", "N+1 Query detected", ERROR),
  N_PLUS_ONE_SUSPECT("n-plus-one-suspect", "N+1 Query suspected (SQL-level heuristic)", INFO),
  SELECT_ALL("select-all", "SELECT * usage", INFO),
  WHERE_FUNCTION("where-function", "Function usage in WHERE clause disables index", ERROR),
  OR_ABUSE("or-abuse", "Excessive OR conditions in WHERE clause", WARNING),
  OFFSET_PAGINATION("offset-pagination", "Large OFFSET pagination", WARNING),
  MISSING_WHERE_INDEX("missing-where-index", "Missing index on WHERE column", ERROR),
  MISSING_JOIN_INDEX("missing-join-index", "Missing index on JOIN column", ERROR),
  MISSING_ORDER_BY_INDEX("missing-order-by-index", "Missing index on ORDER BY column", WARNING),
  MISSING_GROUP_BY_INDEX("missing-group-by-index", "Missing index on GROUP BY column", WARNING),
  COMPOSITE_INDEX_LEADING_COLUMN(
      "composite-index-leading", "Composite index leading column not used", WARNING),
  LIKE_LEADING_WILDCARD(
      "like-leading-wildcard", "Leading wildcard in LIKE disables index", WARNING),
  /**
   * Reserved for future use. DuplicateQueryDetector exists but is currently disabled because
   * datasource-proxy provides SQL with '?' placeholders, making it impossible to distinguish "same
   * query, same params" from "same query, different params". Not currently emitted by any detector.
   */
  DUPLICATE_QUERY("duplicate-query", "Duplicate exact query detected", WARNING),
  CARTESIAN_JOIN("cartesian-join", "Cartesian JOIN detected (missing ON condition)", ERROR),
  CORRELATED_SUBQUERY("correlated-subquery", "Correlated subquery in SELECT clause", WARNING),
  FOR_UPDATE_WITHOUT_INDEX(
      "for-update-no-index", "FOR UPDATE without index may lock entire table", ERROR),
  REDUNDANT_FILTER("redundant-filter", "Redundant duplicate WHERE condition", INFO),
  NON_SARGABLE_EXPRESSION("non-sargable", "Arithmetic on column prevents index usage", ERROR),
  REDUNDANT_INDEX("redundant-index", "Redundant index (prefix of another index)", WARNING),
  SLOW_QUERY("slow-query", "Slow query detected", WARNING),
  COUNT_INSTEAD_OF_EXISTS(
      "count-instead-of-exists", "COUNT used where EXISTS would be more efficient", INFO),
  UNBOUNDED_RESULT_SET(
      "unbounded-result-set", "SELECT without LIMIT could return unbounded rows", WARNING),
  /** Reserved for future EXPLAIN-based detection. Not currently emitted by any detector. */
  FULL_TABLE_SCAN("full-scan", "Full table scan detected", INFO),
  /** Reserved for future EXPLAIN-based detection. Not currently emitted by any detector. */
  FILESORT("filesort", "Filesort detected", INFO),
  /** Reserved for future EXPLAIN-based detection. Not currently emitted by any detector. */
  TEMPORARY_TABLE("temporary-table", "Temporary table usage", INFO),
  WRITE_AMPLIFICATION("write-amplification", "Too many indexes cause write amplification", WARNING),
  IMPLICIT_TYPE_CONVERSION(
      "implicit-type-conversion", "Implicit type conversion disables index", WARNING),
  UNION_WITHOUT_ALL("union-without-all", "UNION without ALL forces deduplication sort", INFO),
  COVERING_INDEX_OPPORTUNITY(
      "covering-index-opportunity", "Query could benefit from a covering index", INFO),
  ORDER_BY_LIMIT_WITHOUT_INDEX(
      "order-by-limit-no-index", "ORDER BY with LIMIT without index causes full filesort", WARNING),
  LARGE_IN_LIST("large-in-list", "IN clause with too many values", WARNING),
  DISTINCT_MISUSE("distinct-misuse", "Potentially unnecessary DISTINCT usage", WARNING),
  NULL_COMPARISON("null-comparison", "Comparison with NULL using = or != (always UNKNOWN)", ERROR),
  HAVING_MISUSE(
      "having-misuse", "HAVING condition on non-aggregate column should be WHERE", WARNING),
  RANGE_LOCK_RISK(
      "range-lock-risk",
      "Range condition with FOR UPDATE on unindexed column may cause gap locks",
      WARNING),
  QUERY_COUNT_REGRESSION("query-count-regression", "Query count regression detected", WARNING),
  UPDATE_WITHOUT_WHERE(
      "update-without-where", "UPDATE/DELETE without WHERE clause affects all rows", ERROR),
  DML_WITHOUT_INDEX(
      "dml-without-index",
      "UPDATE/DELETE WHERE column has no index causes full table scan",
      WARNING),
  REPEATED_SINGLE_INSERT(
      "repeated-single-insert", "Repeated single-row INSERT should use batch insert", WARNING),
  INSERT_SELECT_ALL(
      "insert-select-all",
      "INSERT with SELECT * is fragile and may transfer unnecessary data",
      WARNING),
  ORDER_BY_RAND("order-by-rand", "ORDER BY RAND() causes full table scan and sort", ERROR),
  NOT_IN_SUBQUERY(
      "not-in-subquery", "NOT IN (subquery) returns empty when subquery contains NULL", ERROR),
  TOO_MANY_JOINS("too-many-joins", "Query has too many JOINs", WARNING),
  IMPLICIT_JOIN("implicit-join", "Implicit comma-separated join syntax", WARNING),
  STRING_CONCAT_IN_WHERE(
      "string-concat-where", "String concatenation in WHERE prevents index usage", WARNING),
  COUNT_STAR_WITHOUT_WHERE(
      "count-star-no-where", "COUNT(*) without WHERE scans entire table", INFO),
  INSERT_ON_DUPLICATE_KEY(
      "insert-on-duplicate-key", "INSERT ON DUPLICATE KEY UPDATE may cause deadlocks", WARNING),
  GROUP_BY_FUNCTION("group-by-function", "Function in GROUP BY prevents index usage", WARNING),
  FOR_UPDATE_NON_UNIQUE(
      "for-update-non-unique", "FOR UPDATE on non-unique index causes gap locks", WARNING),
  SUBQUERY_IN_DML(
      "subquery-in-dml", "Subquery in UPDATE/DELETE cannot use semijoin optimization", WARNING),
  INSERT_SELECT_LOCKS_SOURCE(
      "insert-select-locks-source", "INSERT...SELECT locks source table rows", INFO),
  COLLECTION_DELETE_REINSERT(
      "collection-delete-reinsert",
      "DELETE-all + re-INSERT pattern indicates inefficient collection management",
      WARNING),
  DERIVED_DELETE_LOADS_ENTITIES(
      "derived-delete-loads-entities",
      "Derived delete loads entities before individual deletes",
      WARNING),
  EXCESSIVE_COLUMN_FETCH(
      "excessive-column-fetch", "Query fetches too many columns, consider DTO projection", INFO),
  IMPLICIT_COLUMNS_INSERT(
      "implicit-columns-insert", "INSERT without explicit column list is fragile", WARNING),
  REGEXP_INSTEAD_OF_LIKE("regexp-usage", "REGEXP/RLIKE prevents index usage", WARNING),
  FIND_IN_SET_USAGE(
      "find-in-set", "FIND_IN_SET indicates comma-separated values violating 1NF", WARNING),
  UNUSED_JOIN("unused-join", "LEFT JOIN table is never referenced in query", WARNING),
  MERGEABLE_QUERIES(
      "mergeable-queries", "Multiple queries to same table could be merged into one", INFO),
  NON_DETERMINISTIC_PAGINATION(
      "non-deterministic-pagination",
      "ORDER BY + LIMIT on non-unique column gives inconsistent pagination",
      INFO),
  LIMIT_WITHOUT_ORDER_BY(
      "limit-without-order-by",
      "LIMIT without ORDER BY returns non-deterministic results",
      WARNING),
  WINDOW_FUNCTION_WITHOUT_PARTITION(
      "window-no-partition",
      "Window function without PARTITION BY processes entire table",
      WARNING),
  FOR_UPDATE_WITHOUT_TIMEOUT(
      "for-update-no-timeout",
      "FOR UPDATE without NOWAIT/SKIP LOCKED may block indefinitely",
      WARNING),
  CASE_IN_WHERE("case-in-where", "CASE expression in WHERE prevents index usage", WARNING),
  FORCE_INDEX_HINT(
      "force-index-hint", "FORCE INDEX/USE INDEX hint may become stale as schema evolves", INFO),
  FIND_BY_ID_FOR_ASSOCIATION(
      "find-by-id-for-association",
      "findById() used only for FK association; consider getReferenceById()",
      INFO);

  private final String code;
  private final String description;
  private final Severity defaultSeverity;

  IssueType(String code, String description, Severity defaultSeverity) {
    this.code = code;
    this.description = description;
    this.defaultSeverity = defaultSeverity;
  }

  public String getCode() {
    return code;
  }

  public String getDescription() {
    return description;
  }

  public Severity getDefaultSeverity() {
    return defaultSeverity;
  }
}
