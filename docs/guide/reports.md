# Reports

After each audited test method, QueryAudit prints its findings and adds the result to the suite
summary. You can also select one suite-level JSON or HTML artifact for later review.

QueryAudit provides three report formats:

| Reporter | Class | Output | Use Case |
|---|---|---|---|
| **Console** | `ConsoleReporter` | ANSI-colored stdout | Development and local testing |
| **JSON** | `JsonReporter` | Structured JSON file | CI artifacts, dashboards, trend tracking |
| **HTML** | `HtmlReporter` / `HtmlReportAggregator` | Self-contained HTML file | Build artifacts, PR reviews, team sharing |

All three reporters implement the `Reporter` interface from `query-audit-core`.

`query-audit.report.format` selects one suite-level artifact. The default `console` setting creates
no report files. Select `json` for `report.json` or `html` for the browser report. QueryAudit still
prints each test's console diagnostics so failures remain readable in local and CI logs.

When `json` or `html` is selected, that file is part of the audit result. If QueryAudit cannot
write it, the JUnit run fails with the format and target path in the error message. This prevents a
CI job from accepting a run whose report is missing.

---

## Console Report

The default reporter prints a human-readable, ANSI-colored report to standard output
after each test method.

### Example Output

```
------------------------------------------------------------------------
  QUERY AUDIT REPORT
  Test: findRecentOrders_shouldUseIndex
------------------------------------------------------------------------

--- CONFIRMED (100% reliable) ---

  [ERROR] N+1 Query detected
    Query:  SELECT * FROM order_items WHERE order_id = ?
    Target: order_items
    Detail: Query repeated 12 times (threshold: 3)
    Fix:    Use JOIN FETCH, @EntityGraph, or batch loading (IN clause)

  [ERROR] Missing index on WHERE column
    Query:  SELECT * FROM orders WHERE user_id = ? ORDER BY created_at DESC
    Target: orders.user_id
    Detail: Column 'user_id' is used in WHERE clause but has no index
    Fix:    CREATE INDEX idx_orders_user_id ON orders (user_id);

  [WARNING] SELECT * usage
    Query:  SELECT * FROM orders WHERE user_id = ? ORDER BY created_at DESC
    Target: orders
    Detail: SELECT * returns all columns; consider selecting only needed columns

--- INFO (may vary with data volume) ---

  [INFO] Full table scan detected
    Query:  SELECT * FROM config WHERE key = 'app.version'
    Target: config
    Detail: Full table scan on 'config' (small table, may be acceptable)

[OK] 5 queries passed

------------------------------------------------------------------------
  4 unique patterns | 18 total queries | 342 ms total
  2 errors | 1 warning | 1 info | 5 passed
------------------------------------------------------------------------
```

### Configuration

The console reporter is enabled by default. No explicit configuration is needed.

```yaml
query-audit:
  report:
    format: console       # Default value
    show-info: true       # Show or hide INFO-level findings
```

With `format: console`, the suite finalizer prints its summary without creating `index.html` or
`report.json`.

### ANSI Color Coding

The console reporter uses ANSI escape codes to improve readability:

| Element | Color | ANSI Code |
|---|---|---|
| ERROR severity tag and text | Red | `\033[31m` |
| WARNING severity tag and text | Yellow | `\033[33m` |
| INFO severity tag and text | Cyan | `\033[36m` |
| OK / passed count | Green | `\033[32m` |
| Header dividers, section titles | Bold | `\033[1m` |
| SQL text, dim labels | Dim | `\033[2m` |

Colors are automatically disabled when:

- The `NO_COLOR` environment variable is set (following the [no-color.org](https://no-color.org) convention).
- There is no interactive console attached (e.g., when running in a CI pipeline that
  captures output).

!!! note "Forcing colors in CI"
    Some CI systems (GitHub Actions, GitLab CI) do support ANSI colors. If colors
    are disabled but you want them, ensure `System.console()` returns non-null or
    construct `ConsoleReporter` manually with `colorsEnabled = true`.

---

## JSON Report

The JSON reporter produces a structured, machine-readable report suitable for CI artifacts,
dashboards, and downstream processing.

### Enabling JSON Reports

```yaml
query-audit:
  report:
    format: json
    output-dir: build/reports/query-audit
```

This selection writes `report.json` and does not create HTML files.

For plain JUnit without Spring configuration, use
`./gradlew test -DqueryAudit.reportFormat=json`.

### Example Output

The file is a **versioned suite envelope**. `outcome` and `incompleteReasons` describe whether
the run produced a trustworthy verdict, while `reports` keeps the per-test findings and statistics.

```json
{
  "schemaVersion": "1.1.0",
  "outcome": "FAIL",
  "incompleteReasons": [],
  "reports": [
    {
      "testClass": "com.example.OrderServiceTest",
      "testName": "findRecentOrders_shouldUseIndex",
      "summary": {
        "confirmedIssues": 2,
        "infoIssues": 1,
        "acknowledgedIssues": 0,
        "uniquePatterns": 4,
        "totalQueries": 18,
        "executionTimeMs": 342
      },
      "confirmedIssues": [
        {
          "type": "n-plus-one",
          "severity": "ERROR",
          "query": "select * from order_items where order_id = ?",
          "table": "order_items",
          "column": null,
          "detail": "Query repeated 12 times (threshold: 3)",
          "suggestion": "Use JOIN FETCH, @EntityGraph, or batch loading (IN clause)",
          "sourceLocation": "com.example.OrderService.findOrders:42",
          "remediation": {"kind": "batch-fetch", "table": "order_items"}
        },
        {
          "type": "missing-where-index",
          "severity": "ERROR",
          "query": "select * from orders where user_id = ? order by created_at desc",
          "table": "orders",
          "column": "user_id",
          "detail": "Column 'user_id' is used in WHERE clause but has no index",
          "suggestion": "CREATE INDEX idx_orders_user_id ON orders (user_id);",
          "sourceLocation": "com.example.OrderService.findOrders:42",
          "remediation": {"kind": "add-index", "table": "orders", "columns": ["user_id"]}
        }
      ],
      "infoIssues": [],
      "acknowledgedIssues": [],
      "indexMetadata": {
        "orders": [
          {"name": "PRIMARY", "unique": true, "columns": ["id"], "cardinality": 120000}
        ]
      },
      "queries": [
        {
          "sql": "SELECT * FROM orders WHERE user_id = 42 ORDER BY created_at DESC",
          "normalizedSql": "select * from orders where user_id = ? order by created_at desc",
          "executionTimeNanos": 15234000,
          "stackTrace": "com.example.OrderService.findOrders:42"
        }
      ]
    }
  ]
}
```

### JSON Schema

The envelope carries `schemaVersion` (semver) so consumers can detect incompatible input instead
of silently misparsing it. The current version is **1.1.0**. QueryAudit 0.5.x wrote schema 1.0
without a run outcome; the comparator treats those reports as `INCONCLUSIVE` because it cannot
infer a trustworthy `PASS` from the per-test reports alone.

The published JSON Schemas validate both envelope versions. The deprecated Java method
`JsonReporter.toEnvelopeJson(List<QueryAuditReport>)` retains the legacy 1.0 shape because a list
of reports cannot establish a run outcome. New callers should use
`JsonReporter.toRunEnvelopeJson(AuditRunResult)`.

### Run outcomes

The suite outcome uses one precedence rule everywhere: `INCONCLUSIVE > FAIL > PASS`.

| Outcome | Meaning |
|---|---|
| `PASS` | The required audit completed and every configured policy and contract passed. |
| `FAIL` | The audit completed, but `failOnDetection`, `@DetectNPlusOne`, `@ExpectQueries`, `@ExpectMaxQueryCount`, or a recorded query contract failed. |
| `INCONCLUSIVE` | Collection or a required input was incomplete, so the run cannot produce a trustworthy verdict. Any partial findings and statistics remain in `reports`. |

Confirmed findings do not automatically mean `FAIL`. For example, a completed run with
`fail-on-detection: false` can be `PASS` while still reporting findings for review. Conversely,
an incomplete run stays `INCONCLUSIVE` even when its retained queries also show a policy violation.

Each incomplete reason is an object with a stable `code`. The `detail` field is always present and
may be `null` when no additional context is available:

| Code | Current producer |
|---|---|
| `QUERY_LIMIT_REACHED` | JUnit query capture exceeded `max-queries`; retained queries are still analyzed and reported. |
| `DATASOURCE_UNAVAILABLE` | An active JUnit audit could not resolve a `DataSource`. |
| `AUDIT_INITIALIZATION_FAILED` | An active JUnit audit could not install reliable query capture, including unsupported concurrent execution. |
| `AUDIT_ANALYSIS_FAILED` | QueryAudit could not complete analysis for an active test. Earlier per-test reports remain available, but the suite is incomplete. |
| `CONTRACT_UNREADABLE` | The query contract or query-count baseline was unreadable or malformed. |
| `UNSUPPORTED_SCHEMA` | Report comparison received a schema it cannot evaluate safely, including legacy 1.0 input with no outcome. |
| `EXPECTED_TEST_MISSING` | Report comparison can derive this from a baseline-relative missing test. Suite-wide expected-test generation remains part of [#240](https://github.com/haroya01/query-audit/issues/240). |
| `REPORT_WRITE_FAILED` | The selected JSON or HTML artifact could not be written. QueryAudit records the incomplete state and fails the JUnit run; the missing artifact cannot contain its own failure reason. |

Field notes for machine consumers:

- Every finding carries `sourceLocation` (the innermost application stack frame of the
  offending query) and, for high-precision rules, a structured `remediation` hint
  (`kind` + `table` + `columns`) so tooling can act without parsing the prose `suggestion`.
- `indexMetadata` embeds the actual index state (from `SHOW INDEX` / `pg_catalog`) of every
  table referenced by a finding — grouped per index, columns in index order. A report
  consumer can decide on and generate the correct fix without separate database access.
  `null` means no metadata was collected (non-database test).

The stable schema URLs are
[`schema/report-1.0.schema.json`](https://github.com/haroya01/query-audit/blob/main/docs/schema/report-1.0.schema.json)
and
[`schema/report-1.1.schema.json`](https://github.com/haroya01/query-audit/blob/main/docs/schema/report-1.1.schema.json).
[`schema/report.schema.json`](https://github.com/haroya01/query-audit/blob/main/docs/schema/report.schema.json)
always points to the current version.

!!! tip "CI artifact storage"
    Store JSON reports as CI artifacts for trend tracking across builds. Parse them
    with `jq` or feed them into monitoring dashboards.

    ```bash
    # Total confirmed issues across all test methods
    jq '[.reports[].summary.confirmedIssues] | add' build/reports/query-audit/report.json

    # List all detected issue types
    jq '[.reports[].confirmedIssues[].type] | unique' build/reports/query-audit/report.json

    # Find tests with N+1 issues
    jq '.reports[] | select(.confirmedIssues[]? | .type == "n-plus-one") | .testName' \
        build/reports/query-audit/report.json
    ```

---

## Delta Verdict (compare two runs)

Every fix loop ends with the same question: *did my change resolve the finding without
introducing new ones?* The compare command answers it from two `report.json` files alone —
no re-analysis, no database access:

```bash
java -cp query-audit-core-<version>.jar \
    io.queryaudit.core.reporter.ReportComparator before.json after.json verdict.json
```

```
[QueryAudit] compare: PASS; 0 new, 1 resolved, 0 persisting; queries 11 -> 7
  RESOLVED n-plus-one (table: order_items) in OrderServiceTest.findOrders
```

- **Exit contract**: `0` for `PASS`, `1` for `FAIL`, and `2` for `INCONCLUSIVE` or a usage/parse
  error. A candidate run that already has outcome `FAIL` cannot become a successful comparison
  merely because it introduced no new finding.
- **`verdict.json`**: `{outcome, incompleteReasons, newFindings, resolved, persisting, complete,
  missingTests, queryCountDelta, executionTimeMsDelta}` — the termination condition for automated
  fix loops.
- Every test present in the baseline report must also appear in the candidate report. Otherwise,
  `complete` is `false`, `missingTests` identifies the absent tests, and their findings are not
  classified as resolved. With the schema 1.x report fields, tests are matched by
  `testClass|testName`.
- **Matching key**: `testClass|testName|type|normalized-pattern|sourceLocation`, so findings
  survive unrelated refactors as long as the statement shape and call site are stable.
- Only **confirmed** findings participate; INFO advisories don't gate fix loops.
- Schema 1.1+ inputs must carry a valid outcome and a consistent reason list. A valid
  `INCONCLUSIVE` input keeps its partial delta but forces comparison exit code `2`. Legacy schema
  1.0 input is also inconclusive; unsupported major versions produce
  `UNSUPPORTED_SCHEMA`. Pre-envelope reports are rejected with a hint.

As a Gradle task in the consuming project:

```groovy
tasks.register('queryAuditCompare', JavaExec) {
    classpath = configurations.testRuntimeClasspath
    mainClass = 'io.queryaudit.core.reporter.ReportComparator'
    args 'baseline-report.json', 'build/reports/query-audit/report.json', 'build/verdict.json'
}
```

---

## HTML Report

The HTML report aggregator accumulates results across all test classes and produces a
self-contained HTML file at `build/reports/query-audit/index.html` after all tests complete.
The report includes expandable sections, syntax-highlighted SQL, and a visual summary.

### Features

- **Test-level drill-down** -- Expand each test to see its detected issues and queries
- **Filtering** -- Filter by severity, issue type, or test class
- **Search** -- Full-text search across SQL queries and issue descriptions
- **Summary dashboard** -- Overall counts of errors, warnings, and info findings
- **Self-contained** -- Single HTML file with embedded CSS and JavaScript, no external dependencies

### Configuration

```yaml
query-audit:
  report:
    format: html
    output-dir: build/reports/query-audit    # Where to write index.html
  auto-open-report: true                     # Open in browser after tests
```

This selection writes the HTML index and per-class pages and does not create `report.json`.

For plain JUnit without Spring configuration, use
`./gradlew test -DqueryAudit.reportFormat=html`.

Or via annotation:

```java
@QueryAudit(autoOpenReport = true)
```

Or via system property:

```bash
./gradlew test -Dqueryaudit.autoOpenReport=true
```

### Example HTML Report Structure

The generated HTML report contains these sections:

```
+------------------------------------------------------------+
|  QueryAudit Report                                        |
|  Generated: 2026-03-25 14:30:00                            |
+------------------------------------------------------------+
|  Summary: 12 tests | 5 errors | 3 warnings | 2 info       |
+------------------------------------------------------------+
|                                                            |
|  [v] OrderServiceTest                                      |
|      [v] findRecentOrders (2 errors, 1 warning)            |
|          [ERROR] N+1 Query detected                        |
|            Query: SELECT * FROM order_items WHERE ...       |
|          [ERROR] Missing index on WHERE column              |
|            Query: SELECT * FROM orders WHERE user_id = ...  |
|          [WARNING] SELECT * usage                          |
|      [ ] createOrder (0 issues)                            |
|                                                            |
|  [v] UserServiceTest                                       |
|      [v] findActiveUsers (1 warning)                       |
|          [WARNING] Unbounded result set                    |
+------------------------------------------------------------+
```

!!! warning "HTML report timing"
    The root suite finalizer writes the HTML report after all participating test classes finish. If
    the test engine cannot reach finalization, no report is written. Check the test logs for the
    earlier lifecycle failure.

---

## Report Sections Explained

### Header

```
------------------------------------------------------------------------
  QUERY AUDIT REPORT
  Test: findRecentOrders_shouldUseIndex
------------------------------------------------------------------------
```

Shows the name of the test method that was analyzed.

### CONFIRMED (100% reliable)

```
--- CONFIRMED (100% reliable) ---
```

Issues in this section are determined purely from SQL parsing and index metadata --
they do not depend on data volume or query planner behavior. These are the issues
that cause the test to fail when `failOnDetection` is `true`.

Confirmed issues have either **ERROR** or **WARNING** severity:

- **ERROR** -- high-confidence performance problems (N+1, missing WHERE/JOIN index,
  function on indexed column)
- **WARNING** -- likely problems that may be intentional in some cases (SELECT *,
  excessive OR clauses, large OFFSET pagination, missing ORDER BY/GROUP BY index)

### INFO (may vary with data volume)

```
--- INFO (may vary with data volume) ---
```

INFO-level issues come from EXPLAIN analysis and depend on the query planner's
decisions, which can vary with data volume. These are shown for awareness but
never cause a test failure. Examples include full table scans, filesort, and
temporary table usage.

!!! tip
    Set `report.show-info: false` in `application.yml` to hide this section if
    your tests use small datasets where these findings are not actionable.

    The setting applies to console, HTML, and JSON output, including aggregate summary counts.
    It does not disable INFO detectors or change test failure behavior. Confirmed and acknowledged
    findings, captured queries, query totals, timings, and index metadata remain available.

### OK

```
[OK] 5 queries passed
```

Shows how many queries had no detected issues.

### Summary

```
------------------------------------------------------------------------
  4 unique patterns | 18 total queries | 342 ms total
  2 errors | 1 warning | 1 info | 5 passed
------------------------------------------------------------------------
```

The summary footer provides:

- **Unique patterns** -- number of structurally distinct SQL statements (after
  parameter normalization).
- **Total queries** -- total number of SQL statements executed during the test
  (before deduplication).
- **Total time** -- cumulative execution time of all intercepted queries.
- **Breakdown** -- counts by severity plus passed queries.

---

## How to Read the Report Effectively

1. **Start with the summary line.** If it says `0 errors | 0 warnings`, your queries
   are clean.

2. **Focus on CONFIRMED errors first.** These are definite problems -- an N+1 that
   fires 12 times, a WHERE column with no index, etc.

3. **Review warnings.** These may be intentional (e.g., `SELECT *` in a test helper
   that actually needs all columns). If intentional, suppress them with
   `@QueryAudit(suppress = {"select-all"})`.

4. **Glance at INFO.** INFO issues flag things like full table scans that are normal
   on small test datasets. If your test uses realistic data volumes, these may be
   worth investigating.

5. **Look at the Fix suggestion.** QueryAudit provides actionable suggestions like
   `CREATE INDEX` DDL or recommendations to use JOIN FETCH.

---

## See Also

- [Configuration Reference](configuration.md) -- Configure report format and output directory
- [CI/CD Integration](ci-cd.md) -- Upload reports as CI artifacts
- [Suppressing Issues](suppressing.md) -- Suppress intentional findings from reports
