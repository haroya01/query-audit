# Configuration Reference

QueryAudit can be configured at three levels. When multiple levels conflict, the
most specific one wins:

```
method-level @QueryAudit  >  class-level @QueryAudit  >  application.yml  >  built-in defaults
```

---

## application.yml Properties

All properties are optional. The table below lists every supported key under the
`query-audit` prefix.

| Property | Type | Default | Description |
|---|---|---|---|
| `enabled` | `boolean` | `true` | Master switch. When `false`, the `BeanPostProcessor` that wraps DataSources is not created. |
| `fail-on-detection` | `boolean` | `true` | Whether confirmed issues (ERROR/WARNING) should cause the test to fail with an `AssertionError`. |
| `n-plus-one.threshold` | `int` | `3` | Number of times a structurally identical query must repeat before it is flagged as N+1. |
| `offset-pagination.threshold` | `int` | `1000` | The OFFSET value that triggers a warning. |
| `or-clause.threshold` | `int` | `3` | Number of OR conditions in a single WHERE clause before flagging. |
| `suppress-patterns` | `List<String>` | `[]` | Issue codes or qualified patterns to suppress globally. See [Suppressing Issues](suppressing.md). |
| `suppress-queries` | `List<String>` | `[]` | SQL query substrings to suppress (e.g., health-check queries). Case-insensitive substring match. |
| `baseline-path` | `String` | `null` | Path to the query count baseline file. When `null`, uses `.query-audit-baseline` in the working directory. |
| `auto-open-report` | `boolean` | `true` | Whether to automatically open the HTML report in a browser after tests. |
| `max-queries` | `int` | `10000` | Maximum number of queries recorded per test. Prevents OOM when a test generates excessive queries. |
| `report.format` | `String` | `"console"` | Report output format: `console`, `json`, or `html`. |
| `report.output-dir` | `String` | `"build/reports/query-audit"` | Directory for HTML and JSON reports. |
| `report.show-info` | `boolean` | `true` | Whether INFO-level issues appear in the report. |
| `disabled-rules` | `List<String>` | `[]` | Rule codes to completely disable. |
| `severity-overrides` | `Map<String,String>` | `{}` | Override severity per rule code (e.g., `select-all: WARNING`). |
| `large-in-list.threshold` | `int` | `100` | Number of values in IN clause before flagging. |
| `too-many-joins.threshold` | `int` | `5` | Number of JOINs before flagging. |
| `excessive-column.threshold` | `int` | `15` | Number of selected columns before flagging. |
| `repeated-insert.threshold` | `int` | `3` | Number of repeated single-row INSERTs before flagging. |
| `write-amplification.threshold` | `int` | `6` | Number of indexes per table before flagging write amplification. |
| `slow-query.warning-ms` | `long` | `500` | Execution time (ms) that triggers a WARNING-level slow query. |
| `slow-query.error-ms` | `long` | `3000` | Execution time (ms) that triggers an ERROR-level slow query. |

### Full Example

```yaml
query-audit:
  enabled: true
  fail-on-detection: true
  n-plus-one:
    threshold: 3
  offset-pagination:
    threshold: 1000
  or-clause:
    threshold: 3
  large-in-list:
    threshold: 100
  too-many-joins:
    threshold: 5
  excessive-column:
    threshold: 15
  repeated-insert:
    threshold: 3
  write-amplification:
    threshold: 6
  slow-query:
    warning-ms: 500
    error-ms: 3000
  disabled-rules:
    - "select-all"
  severity-overrides:
    unbounded-result-set: ERROR
  suppress-patterns:
    - "select-all"
    - "missing-where-index:config.key"
  suppress-queries:
    - "SELECT 1"
  baseline-path: ""
  auto-open-report: false
  max-queries: 10000
  report:
    format: console
    output-dir: build/reports/query-audit
    show-info: true
```

---

## Common Configurations

Copy-paste these presets for typical use cases.

=== "Strict (recommended for new projects)"

    Best for greenfield projects where you want to catch issues from day one.

    ```yaml
    query-audit:
      enabled: true
      fail-on-detection: true
      n-plus-one:
        threshold: 2
      slow-query:
        warning-ms: 200
        error-ms: 1000
      too-many-joins:
        threshold: 3
      large-in-list:
        threshold: 50
      report:
        format: console
        show-info: true
    ```

=== "Gradual adoption (legacy projects)"

    Start with report-only mode to inventory existing issues without breaking builds.

    ```yaml
    query-audit:
      enabled: true
      fail-on-detection: false          # Report only, no failures
      suppress-patterns:
        - "select-all"                   # Suppress common legacy patterns
        - "offset-pagination"
      report:
        format: console
        show-info: false                 # Hide noise from INFO findings
    ```

=== "CI pipeline"

    Optimized for headless CI environments.

    ```yaml
    query-audit:
      enabled: true
      fail-on-detection: true
      auto-open-report: false            # No browser in CI
      report:
        format: console
        output-dir: build/reports/query-audit
      suppress-queries:
        - "SELECT 1"                     # Health-check queries
        - "SHOW WARNINGS"               # MySQL driver internals
    ```

=== "Performance-focused"

    Tight thresholds for performance-critical services.

    ```yaml
    query-audit:
      enabled: true
      fail-on-detection: true
      n-plus-one:
        threshold: 2                     # Strict N+1 detection
      slow-query:
        warning-ms: 100                  # Flag anything over 100ms
        error-ms: 500                    # Fail on anything over 500ms
      too-many-joins:
        threshold: 3                     # Strict join limit
      large-in-list:
        threshold: 50                    # Tighter IN list limit
      excessive-column:
        threshold: 10                    # Discourage wide queries
    ```

=== "Minimal (N+1 only)"

    Only detect the single most impactful anti-pattern.

    ```yaml
    query-audit:
      enabled: true
      fail-on-detection: true
      disabled-rules:                    # Disable everything except N+1
        - "select-all"
        - "missing-where-index"
        - "offset-pagination"
        # ... add all other rule codes you want to skip
    ```

    !!! tip
        Consider using `@DetectNPlusOne` annotation instead for a cleaner approach.

### Recommended Threshold Values

| Threshold | Conservative | Moderate (default) | Strict |
|---|---|---|---|
| `n-plus-one.threshold` | 5 | 3 | 2 |
| `slow-query.warning-ms` | 1000 | 500 | 200 |
| `slow-query.error-ms` | 5000 | 3000 | 1000 |
| `offset-pagination.threshold` | 5000 | 1000 | 500 |
| `large-in-list.threshold` | 500 | 100 | 50 |
| `too-many-joins.threshold` | 8 | 5 | 3 |
| `excessive-column.threshold` | 25 | 15 | 10 |
| `or-clause.threshold` | 5 | 3 | 2 |

!!! tip "Choosing the right profile"
    - **Conservative**: Use when adopting QueryAudit in a large existing codebase.
      Minimizes false positives at the cost of missing some real issues.
    - **Moderate (default)**: Balanced for most projects. Good starting point.
    - **Strict**: Use for new projects or performance-critical services. May require
      more suppression of intentional patterns.

---

## @QueryAudit Annotation Attributes

See the [Annotations Guide](annotations.md) for detailed usage patterns.

| Attribute | Type | Default | Description |
|---|---|---|---|
| `suppress` | `String[]` | `{}` | Issue codes or qualified patterns to suppress. |
| `failOn` | `IssueType[]` | `{}` (all confirmed) | Restrict which issue types cause a test failure. |
| `nPlusOneThreshold` | `int` | `-1` (use default) | Override the N+1 detection threshold. |
| `failOnDetection` | `boolean` | `true` | Whether to fail the test when confirmed issues are detected. |
| `baselinePath` | `String` | `""` (default) | Path to the baseline file. |
| `autoOpenReport` | `boolean` | `false` | Open HTML report in browser after tests. |

### Annotation-Based Configuration Examples

=== "Annotation only"

    ```java
    @QueryAudit(
        failOnDetection = BooleanOverride.TRUE,
        nPlusOneThreshold = 2,
        suppress = {"select-all"},
        autoOpenReport = BooleanOverride.FALSE
    )
    @SpringBootTest
    class OrderServiceTest { }
    ```

=== "Annotation + application.yml"

    ```java
    // Annotation overrides only what it specifies.
    // Everything else comes from application.yml.
    @QueryAudit(nPlusOneThreshold = 2)
    @SpringBootTest
    class OrderServiceTest { }
    ```

    ```yaml
    # application.yml (base configuration)
    query-audit:
      fail-on-detection: true
      suppress-patterns:
        - "select-all"
      report:
        format: console
    ```

---

## Programmatic Configuration

When using QueryAudit without Spring Boot:

```java
QueryAuditConfig config = QueryAuditConfig.builder()
    .enabled(true)
    .failOnDetection(true)
    .nPlusOneThreshold(5)
    .offsetPaginationThreshold(500)
    .orClauseThreshold(4)
    .largeInListThreshold(100)
    .tooManyJoinsThreshold(5)
    .excessiveColumnThreshold(15)
    .repeatedInsertThreshold(3)
    .writeAmplificationThreshold(6)
    .slowQueryWarningMs(500)
    .slowQueryErrorMs(3000)
    .addDisabledRule("select-all")
    .addSeverityOverride("unbounded-result-set", "ERROR")
    .addSuppressPattern("select-all")
    .addSuppressPattern("missing-where-index:users.email")
    .addSuppressQuery("SELECT 1")       // Suppress health-check queries
    .showInfo(true)
    .maxQueries(10_000)                // Limit recorded queries per test
    .build();
```

### Builder Method Reference

| Method | Type | Default | Description |
|---|---|---|---|
| `enabled(boolean)` | `boolean` | `true` | Master switch. |
| `failOnDetection(boolean)` | `boolean` | `true` | Fail on confirmed issues. |
| `nPlusOneThreshold(int)` | `int` | `3` | N+1 detection threshold. |
| `offsetPaginationThreshold(int)` | `int` | `1000` | OFFSET pagination threshold. |
| `orClauseThreshold(int)` | `int` | `3` | OR clause threshold. |
| `suppressPatterns(Set<String>)` | `Set<String>` | `{}` | Replace all suppress patterns. |
| `addSuppressPattern(String)` | `String` | -- | Add a single suppress pattern. |
| `suppressQueries(Set<String>)` | `Set<String>` | `{}` | Replace all suppressed queries. |
| `addSuppressQuery(String)` | `String` | -- | Add a single suppressed query substring. |
| `showInfo(boolean)` | `boolean` | `true` | Show INFO-level issues. |
| `maxQueries(int)` | `int` | `10000` | Max queries recorded per test. |
| `largeInListThreshold(int)` | `int` | `100` | IN clause value count threshold. |
| `tooManyJoinsThreshold(int)` | `int` | `5` | JOIN count threshold. |
| `excessiveColumnThreshold(int)` | `int` | `15` | Selected column count threshold. |
| `repeatedInsertThreshold(int)` | `int` | `3` | Repeated single-row INSERT threshold. |
| `writeAmplificationThreshold(int)` | `int` | `6` | Index count per table threshold. |
| `slowQueryWarningMs(long)` | `long` | `500` | Slow query WARNING threshold (ms). |
| `slowQueryErrorMs(long)` | `long` | `3000` | Slow query ERROR threshold (ms). |
| `addDisabledRule(String)` | `String` | -- | Disable a specific rule by code. |
| `disabledRules(Set<String>)` | `Set<String>` | `{}` | Replace all disabled rules. |
| `addSeverityOverride(String, String)` | `String, String` | -- | Override severity for a rule code. |
| `severityOverrides(Map<String,String>)` | `Map<String,String>` | `{}` | Replace all severity overrides. |

---

## System Properties

These can be passed via `-D` flags on the command line:

| Property | Description |
|---|---|
| `-DqueryAudit.updateBaseline=true` | Update the query count baseline file after test run |
| `-DqueryAudit.countBaselinePath=path` | Override the query count baseline file path |
| `-Dqueryaudit.autoOpenReport=true` | Force open HTML report in browser |

```bash
./gradlew test -DqueryAudit.updateBaseline=true
```

---

## Issue Types Reference

All 64 issue codes that can be used in `suppress`, `failOn`, and `suppress-patterns`.
Of these, 60 are actively emitted by 57 detection rules. The remaining 4 are disabled or reserved.

### ERROR Severity (11 issue types)

| Code | Enum | Description |
|---|---|---|
| `n-plus-one` | `N_PLUS_ONE` | Hibernate-level authoritative N+1 (same lazy collection/proxy loaded for many distinct owners) |
| `where-function` | `WHERE_FUNCTION` | Function on column in WHERE disables index |
| `missing-where-index` | `MISSING_WHERE_INDEX` | No index on WHERE column |
| `missing-join-index` | `MISSING_JOIN_INDEX` | No index on JOIN column |
| `cartesian-join` | `CARTESIAN_JOIN` | JOIN without ON condition |
| `non-sargable` | `NON_SARGABLE_EXPRESSION` | Arithmetic on column prevents index |
| `null-comparison` | `NULL_COMPARISON` | `= NULL` instead of `IS NULL` (logic bug) |
| `for-update-no-index` | `FOR_UPDATE_WITHOUT_INDEX` | FOR UPDATE without index locks table |
| `update-without-where` | `UPDATE_WITHOUT_WHERE` | UPDATE/DELETE without WHERE |
| `order-by-rand` | `ORDER_BY_RAND` | ORDER BY RAND() causes full table scan and sort |
| `not-in-subquery` | `NOT_IN_SUBQUERY` | NOT IN (subquery) returns empty when subquery contains NULL |

### WARNING Severity (38 issue types + 1 reserved)

| Code | Enum | Description |
|---|---|---|
| `or-abuse` | `OR_ABUSE` | Excessive OR conditions |
| `offset-pagination` | `OFFSET_PAGINATION` | Large OFFSET pagination |
| `missing-order-by-index` | `MISSING_ORDER_BY_INDEX` | No index on ORDER BY column |
| `missing-group-by-index` | `MISSING_GROUP_BY_INDEX` | No index on GROUP BY column |
| `composite-index-leading` | `COMPOSITE_INDEX_LEADING_COLUMN` | Composite index leading column unused |
| `like-leading-wildcard` | `LIKE_LEADING_WILDCARD` | Leading wildcard in LIKE |
| `duplicate-query` | `DUPLICATE_QUERY` | Exact duplicate SQL *(reserved)* |
| `correlated-subquery` | `CORRELATED_SUBQUERY` | Correlated subquery in SELECT |
| `redundant-index` | `REDUNDANT_INDEX` | Redundant index (prefix of another) |
| `slow-query` | `SLOW_QUERY` | Query exceeding time threshold |
| `unbounded-result-set` | `UNBOUNDED_RESULT_SET` | SELECT without LIMIT |
| `write-amplification` | `WRITE_AMPLIFICATION` | Too many indexes on table |
| `implicit-type-conversion` | `IMPLICIT_TYPE_CONVERSION` | Implicit type conversion disables index |
| `order-by-limit-no-index` | `ORDER_BY_LIMIT_WITHOUT_INDEX` | ORDER BY + LIMIT without index |
| `large-in-list` | `LARGE_IN_LIST` | IN clause with too many values |
| `distinct-misuse` | `DISTINCT_MISUSE` | Unnecessary DISTINCT |
| `having-misuse` | `HAVING_MISUSE` | HAVING on non-aggregate column |
| `range-lock-risk` | `RANGE_LOCK_RISK` | Range + FOR UPDATE on unindexed column |
| `query-count-regression` | `QUERY_COUNT_REGRESSION` | Query count regression vs baseline |
| `dml-without-index` | `DML_WITHOUT_INDEX` | UPDATE/DELETE WHERE without index |
| `repeated-single-insert` | `REPEATED_SINGLE_INSERT` | Repeated single-row INSERT |
| `insert-select-all` | `INSERT_SELECT_ALL` | INSERT with SELECT * |
| `insert-on-duplicate-key` | `INSERT_ON_DUPLICATE_KEY` | INSERT ON DUPLICATE KEY UPDATE may cause deadlocks |
| `subquery-in-dml` | `SUBQUERY_IN_DML` | Subquery in UPDATE/DELETE blocks semijoin optimization |
| `implicit-columns-insert` | `IMPLICIT_COLUMNS_INSERT` | INSERT without explicit column list |
| `too-many-joins` | `TOO_MANY_JOINS` | Query has too many JOINs |
| `implicit-join` | `IMPLICIT_JOIN` | Implicit comma-separated join syntax |
| `string-concat-where` | `STRING_CONCAT_IN_WHERE` | String concatenation in WHERE prevents index |
| `unused-join` | `UNUSED_JOIN` | LEFT JOIN table never referenced in query |
| `for-update-non-unique` | `FOR_UPDATE_NON_UNIQUE` | FOR UPDATE on non-unique index causes gap locks |
| `group-by-function` | `GROUP_BY_FUNCTION` | Function in GROUP BY prevents index usage |
| `regexp-usage` | `REGEXP_INSTEAD_OF_LIKE` | REGEXP/RLIKE prevents index usage |
| `find-in-set` | `FIND_IN_SET_USAGE` | FIND_IN_SET indicates comma-separated values violating 1NF |
| `collection-delete-reinsert` | `COLLECTION_DELETE_REINSERT` | DELETE-all + re-INSERT pattern (inefficient collection management) |
| `derived-delete-loads-entities` | `DERIVED_DELETE_LOADS_ENTITIES` | Derived delete loads entities before individual deletes |
| `limit-without-order-by` | `LIMIT_WITHOUT_ORDER_BY` | LIMIT without ORDER BY returns non-deterministic rows |
| `window-no-partition` | `WINDOW_FUNCTION_WITHOUT_PARTITION` | Window function without PARTITION BY operates on entire result set |
| `for-update-no-timeout` | `FOR_UPDATE_WITHOUT_TIMEOUT` | FOR UPDATE without NOWAIT or SKIP LOCKED may block indefinitely |
| `case-in-where` | `CASE_IN_WHERE` | CASE expression in WHERE clause prevents index usage |

### INFO Severity (12 issue types + 3 reserved)

| Code | Enum | Description |
|---|---|---|
| `n-plus-one-suspect` | `N_PLUS_ONE_SUSPECT` | SQL-level heuristic: same normalized query repeated above threshold (suggestive; see `n-plus-one` for authoritative Hibernate detection) |
| `select-all` | `SELECT_ALL` | `SELECT *` usage |
| `redundant-filter` | `REDUNDANT_FILTER` | Duplicate WHERE condition |
| `count-instead-of-exists` | `COUNT_INSTEAD_OF_EXISTS` | COUNT where EXISTS is better |
| `full-scan` | `FULL_TABLE_SCAN` | Full table scan (EXPLAIN-based, reserved) |
| `filesort` | `FILESORT` | Filesort detected (EXPLAIN-based, reserved) |
| `temporary-table` | `TEMPORARY_TABLE` | Temporary table usage (EXPLAIN-based, reserved) |
| `union-without-all` | `UNION_WITHOUT_ALL` | UNION without ALL |
| `covering-index-opportunity` | `COVERING_INDEX_OPPORTUNITY` | Could benefit from covering index |
| `count-star-no-where` | `COUNT_STAR_WITHOUT_WHERE` | COUNT(*) without WHERE scans entire table |
| `insert-select-locks-source` | `INSERT_SELECT_LOCKS_SOURCE` | INSERT...SELECT locks source table rows |
| `excessive-column-fetch` | `EXCESSIVE_COLUMN_FETCH` | Query fetches too many columns |
| `mergeable-queries` | `MERGEABLE_QUERIES` | Multiple queries to same table could be merged |
| `non-deterministic-pagination` | `NON_DETERMINISTIC_PAGINATION` | ORDER BY + LIMIT on non-unique column gives inconsistent results |
| `force-index-hint` | `FORCE_INDEX_HINT` | FORCE/USE/IGNORE INDEX hint overrides optimizer decisions |

---

## Memory Optimization

QueryAudit records every SQL statement executed during a test for analysis.
In large test suites or tests that generate many queries, this can consume
significant heap memory. The following settings help control memory usage.

### Max Queries Per Test (default: 10,000)

Each test records up to 10,000 queries by default. When the limit is reached,
further queries are silently dropped (a single warning is printed to stderr).
This prevents out-of-memory errors when a test unexpectedly generates a very
large number of SQL statements.

```yaml
query-audit:
  max-queries: 5000   # Lower limit for memory-constrained environments
```

Or programmatically:

```java
QueryAuditConfig config = QueryAuditConfig.builder()
    .maxQueries(5000)
    .build();
```

### Automatic String Pooling

QueryAudit automatically deduplicates SQL strings and stack traces in memory.
N+1 queries originate from the same call site and execute the same SQL, so
pooling ensures that identical strings share a single object reference. This
optimization is always active and requires no configuration.

### HTML Report Aggregator Memory

The HTML report aggregator accumulates reports across all test classes. For
test suites with hundreds of test classes, it automatically switches to
lightweight summaries (issues only, no raw query list) after the first 200
reports to prevent unbounded memory growth.

### Recommended JVM Heap for Large Test Suites

| Test suite size | Recommended `-Xmx` | Notes |
|---|---|---|
| < 100 test classes | Default (256m-512m) | No tuning needed |
| 100-500 test classes | 512m-1g | Consider lowering `max-queries` |
| 500+ test classes | 1g-2g | Lower `max-queries` to 5000 or less |

Example Gradle configuration:

```groovy
test {
    jvmArgs '-Xmx1g'
}
```

---

## See Also

- [Annotations Guide](annotations.md) -- Annotation usage and examples
- [Suppressing Issues](suppressing.md) -- How to suppress specific detections
- [Reports](reports.md) -- Report format details and example output
- [CI/CD Integration](ci-cd.md) -- Using configuration in CI pipelines
