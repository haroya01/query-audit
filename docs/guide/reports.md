# Reports

!!! note "Version scope"
    This page documents the 0.6 report contract implemented on `main`. QueryAudit 0.5 writes both
    HTML and schema 1.0 JSON after a session with at least one completed audited result; the
    differences are called out below.

After each audited test method, QueryAudit prints its findings and adds the result to the suite
summary. You can also select one suite-level JSON or HTML artifact for later review.

QueryAudit provides three report formats:

| Reporter | Class | Output | Use Case |
|---|---|---|---|
| **Console** | `ConsoleReporter` | ANSI-colored stdout | Development and local testing |
| **JSON** | `JsonReporter` | Structured JSON file | CI artifacts, dashboards, trend tracking |
| **HTML** | `HtmlReporter` / `HtmlReportAggregator` | HTML index plus one page per test class | Build artifacts, PR reviews, team sharing |

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
────────────────────────────────────────────────────────────────────────
  QUERY GUARD REPORT
  Test: findRecentOrders_shouldUseIndex
────────────────────────────────────────────────────────────────────────

--- TOP ISSUES BY IMPACT ---

  #1 [ERROR] N+1 Query detected order_items  165 pts
      Fix: Use JOIN FETCH, @EntityGraph, or batch loading (IN clause)

--- CONFIRMED (100% reliable, sorted by priority) ---

  [ERROR] N+1 Query detected
    Query:  select id, order_id, sku from order_items where order_id = ?
    Source: com.example.OrderService.findOrders:42
    Target: order_items
    Detail: Query repeated 3 times (threshold: 3)
    Fix:    Use JOIN FETCH, @EntityGraph, or batch loading (IN clause)


--- INFO (may vary with data volume) ---

  [INFO] SELECT * usage
    Query:  select * from orders where user_id = ?
    Source: com.example.OrderService.findOrders:41
    Target: orders
    Detail: SELECT * usage detected on table 'orders'
    Fix:    Replace SELECT * with an explicit column list to reduce network I/O and enable covering index optimization.


[OK] 2 queries passed

--- Query Patterns ---
  [  3x] select id, order_id, sku from order_items where order_id = ?
  [  1x] select * from orders where user_id = ?

--- Table Access Frequency ---
  order_items  3 queries
  orders       1 queries
────────────────────────────────────────────────────────────────────────
  2 unique patterns | 4 total queries | 342 ms total
  1 error | 1 info | 2 passed
────────────────────────────────────────────────────────────────────────
```

### Configuration

The JUnit extension always writes the console report for an audited test. No reporter selection is
required.

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

For Spring Boot, select JSON in `application.yml`:

```yaml
query-audit:
  report:
    format: json
    output-dir: build/reports/query-audit
```

The JUnit extension then writes one aggregate file to
`build/reports/query-audit/report.json` after the test session completes. This selection does not
create HTML files.

For plain JUnit, Maven can pass the test-JVM property directly with
`mvn test -DqueryAudit.reportFormat=json`. Gradle users should add the
[project-property bridge](ci-cd.md#plain-junit-build-tool-setup) once, then run
`./gradlew test -PqueryAuditReportFormat=json`.

### Example Output

The file is a **versioned suite envelope**. `outcome` and `incompleteReasons` describe whether
the run produced a trustworthy verdict, while `reports` keeps the per-test findings and statistics.

```json
{
  "schemaVersion": "1.4.0",
  "redaction": "REDACTED",
  "outcome": "FAIL",
  "incompleteReasons": [],
  "reports": [
    {
      "testId": "[engine:junit-jupiter]/[class:com.example.OrderServiceTest]/[method:findRecentOrders()]",
      "testClass": "OrderServiceTest",
      "testName": "findRecentOrders_shouldUseIndex",
      "testSelector": {
        "type": "junit-unique-id",
        "value": "[engine:junit-jupiter]/[class:com.example.OrderServiceTest]/[method:findRecentOrders()]"
      },
      "summary": {
        "confirmedIssues": 2,
        "infoIssues": 1,
        "acknowledgedIssues": 0,
        "uniquePatterns": 2,
        "totalQueries": 4,
        "executionTimeMs": 342
      },
      "queryEvidence": { "status": "COMPLETE", "retainedQueries": 4, "omittedQueries": 0 },
      "confirmedIssues": [
        {
          "type": "n-plus-one",
          "severity": "ERROR",
          "query": "select id, order_id, sku from order_items where order_id = ?",
          "table": "order_items",
          "column": null,
          "detail": "N+1 Query detected",
          "suggestion": "Consider a fetch join, entity graph, or batch loading.",
          "sourceLocation": "com.example.OrderService.findOrders:42",
          "remediation": {"kind": "batch-fetch", "table": "order_items"}
        },
        {
          "type": "missing-where-index",
          "severity": "ERROR",
          "query": "select * from orders where user_id = ? order by created_at desc",
          "table": "orders",
          "column": "user_id",
          "detail": "Missing index on WHERE column",
          "suggestion": "Check the query plan before adding an index on the reported columns.",
          "sourceLocation": "com.example.OrderService.findOrders:42",
          "remediation": {"kind": "add-index", "table": "orders", "columns": ["user_id"]}
        }
      ],
      "infoIssues": [
        {
          "type": "select-all",
          "severity": "INFO",
          "query": "select * from orders where user_id = ? order by created_at desc",
          "table": "orders",
          "column": null,
          "detail": "SELECT * usage",
          "suggestion": "Select only the columns required by the caller.",
          "sourceLocation": "com.example.OrderService.findOrders:42",
          "remediation": {"kind": "select-explicit-columns", "table": "orders"}
        }
      ],
      "acknowledgedIssues": [],
      "indexMetadata": {
        "orders": [
          {"name": "PRIMARY", "unique": true, "columns": ["id"], "cardinality": 120000}
        ]
      },
      "queries": [
        {
          "sql": "SELECT * FROM orders WHERE user_id = ? ORDER BY created_at DESC",
          "normalizedSql": "select * from orders where user_id = ? order by created_at desc",
          "executionTimeNanos": 15234000,
          "stackTrace": "com.example.OrderService.findOrders:42"
        },
        {
          "sql": "SELECT id, order_id, sku FROM order_items WHERE order_id = ?",
          "normalizedSql": "select id, order_id, sku from order_items where order_id = ?",
          "executionTimeNanos": 108922000,
          "stackTrace": "com.example.OrderService.findOrders:42"
        },
        {
          "sql": "SELECT id, order_id, sku FROM order_items WHERE order_id = ?",
          "normalizedSql": "select id, order_id, sku from order_items where order_id = ?",
          "executionTimeNanos": 109300000,
          "stackTrace": "com.example.OrderService.findOrders:42"
        },
        {
          "sql": "SELECT id, order_id, sku FROM order_items WHERE order_id = ?",
          "normalizedSql": "select id, order_id, sku from order_items where order_id = ?",
          "executionTimeNanos": 108544000,
          "stackTrace": "com.example.OrderService.findOrders:42"
        }
      ]
    }
  ]
}
```

### JSON Schema

The envelope carries `schemaVersion` (semver) so consumers can detect incompatible input instead
of silently misparsing it. The current version is **1.4.0**. QueryAudit 0.5.x wrote schema 1.0
without a run outcome; the comparator treats those reports as `INCONCLUSIVE` because it cannot
infer a trustworthy `PASS` from the per-test reports alone. Schema 1.1 added run outcomes, and
schema 1.2 adds a stable test identity and reproducible selector to every per-test report.

The published JSON Schemas validate all three envelope versions. The deprecated Java method
`JsonReporter.toEnvelopeJson(List<QueryAuditReport>)` retains the exact legacy 1.0 shape, without
run outcomes or stable identity fields. A list of reports cannot establish whether the audit
completed or its policies passed. New callers should use
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

- `testId` is the machine identity. In JUnit reports it is the opaque value from
  `ExtensionContext.getUniqueId()`, which distinguishes packages, nested classes, overloaded
  methods, and test-template invocations. `testName` remains presentation text and may change
  without changing the ID.
- `testSelector.value` can be passed to JUnit Platform's
  `DiscoverySelectors.selectUniqueId(...)` or the Console Launcher's `--select-unique-id` option.
  Parameterized invocations use JUnit's invocation ordinal (`#1`, `#2`, and so on), so reordering
  or inserting arguments can intentionally change those invocation IDs.
- Reports created directly through `query-audit-core` have no framework selector. Their existing
  constructors derive a deterministic `query-audit:core:v1:<sha256>` ID from the exact
  `testClass` and `testName` inputs. Core callers should pass a fully qualified class name and a
  stable logical test name when they need identity across runs.
- Every finding has a `sourceLocation` field. Its value is the innermost captured application
  frame when one is available, and `null` when capture cannot identify one. High-precision rules
  may also include a structured `remediation` hint (`kind` + optional `table` and `columns`) so
  tooling can act without parsing the prose `suggestion`.
- When database index metadata was collected, `indexMetadata` includes known indexes for finding
  tables, grouped per index with columns in index order. It is `null` when no metadata was attached
  and `{}` when metadata was attached but no reported table has a known index. Consumers should
  treat this as optional context rather than assume that every finding carries complete index
  state.

The stable schema URLs are
[`schema/report-1.0.schema.json`](https://haroya01.github.io/query-audit/schema/report-1.0.schema.json),
[`schema/report-1.1.schema.json`](https://haroya01.github.io/query-audit/schema/report-1.1.schema.json),
[`schema/report-1.2.schema.json`](https://haroya01.github.io/query-audit/schema/report-1.2.schema.json),
[`schema/report-1.3.schema.json`](https://haroya01.github.io/query-audit/schema/report-1.3.schema.json), and
[`schema/report-1.4.schema.json`](https://haroya01.github.io/query-audit/schema/report-1.4.schema.json).
[`schema/report.schema.json`](https://haroya01.github.io/query-audit/schema/report.schema.json)
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

!!! warning "Java API compatibility in 0.6"
    `ReportComparator.Finding` and `ReportComparator.TestRef` now prepend `testId` to their record
    component lists. Their 0.5 six- and two-argument constructors remain and set `testId` to `null`,
    so ordinary constructor calls continue to work. Record patterns and code that reflects on record
    components or canonical constructors must adopt the new seven- and three-component shapes.
    Generated `equals()`, `hashCode()`, and `toString()` methods now include `testId`.

- Every test present in the baseline report must also appear in the candidate report. Otherwise,
  `complete` is `false`, `missingTests` identifies the absent tests, and their findings are not
  classified as resolved. Schema 1.2 reports are matched by `testId`; verdict findings and missing
  test entries carry that ID alongside their display fields.
- **Matching key**: `testId|type|normalized-pattern|sourceLocation`, so findings survive display
  name edits and unrelated refactors as long as the statement shape and call site are stable.
- The comparator accepts schema 1.0 and 1.1 reports and uses an exact `testClass|testName` fallback
  when one side lacks IDs. It rejects an ambiguous legacy match instead of assigning one old test
  to multiple stable IDs. Re-record archived baselines with QueryAudit 0.6 when a suite contains
  duplicate legacy identities. A display name changed before the first schema 1.2 run has no safe
  fallback and is reported as a missing old test plus a new test.
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

The HTML report aggregator accumulates results across all test classes and writes a multi-page
report under `build/reports/query-audit/` after all tests complete. `index.html` links to one detail
page per test class. Each page embeds its own CSS and JavaScript, so the report has no external
runtime dependencies; keep the generated directory together so those links continue to work.

### Features

- **Class overview** -- Compare test, issue, query, duration, and status counts by class
- **Prioritized findings** -- Review cross-test deduplication and the highest-impact findings first
- **Method drill-down** -- Expand a test method to inspect findings, fixes, and captured query detail
- **Review progress** -- Check off findings locally; the browser retains that state for the same report
- **Portable pages** -- CSS and JavaScript are embedded in each generated page

### Opening the Report Locally

Select the HTML format to write the browser report under `build/reports/query-audit/`. Automatic
opening is a separate setting and is disabled when a common CI environment variable is present.

```yaml
query-audit:
  report:
    format: html
    output-dir: build/reports/query-audit    # Where to write index.html
  auto-open-report: true                     # Open in browser after tests
```

This selection writes the HTML index and per-class pages and does not create `report.json`.

For plain JUnit, Maven can select the format with
`mvn test -DqueryAudit.reportFormat=html`. Gradle users should use the
[project-property bridge](ci-cd.md#plain-junit-build-tool-setup) and run
`./gradlew test -PqueryAuditReportFormat=html`.

Or via annotation:

```java
@QueryAudit(autoOpenReport = BooleanOverride.TRUE)
```

For plain JUnit, the equivalent test-JVM system property is
`-Dqueryaudit.autoOpenReport=true`.

### Example HTML Report Structure

The generated directory contains the overview and one page for each participating test class:

```
build/reports/query-audit/
├── index.html
├── OrderServiceTest.html
└── UserServiceTest.html
```

The index shows a class table, a cross-test unique-issue summary, and impact-ranked confirmed
findings when present. A class page shows its totals and expandable method cards. Each method card
contains its findings and the retained query timeline and patterns.

!!! warning "HTML report timing"
    The root suite finalizer writes the HTML report after all participating test classes finish. If
    the test engine cannot reach finalization, no report is written. Check the test logs for the
    earlier lifecycle failure.

---

## Report Sections Explained

### Header

```
────────────────────────────────────────────────────────────────────────
  QUERY GUARD REPORT
  Test: findRecentOrders_shouldUseIndex
────────────────────────────────────────────────────────────────────────
```

Shows the name of the test method that was analyzed.

### CONFIRMED findings

```
--- CONFIRMED (100% reliable, sorted by priority) ---
```

Issues in this section are eligible to fail the test when `failOnDetection` is `true`. They come
from structural SQL checks, database metadata, configured thresholds, or Hibernate events. Review
the evidence against the application semantics before changing a query or schema.

Confirmed issues have either **ERROR** or **WARNING** severity:

- **ERROR** -- high-confidence performance problems (N+1, missing WHERE/JOIN index,
  function on indexed column)
- **WARNING** -- likely problems that may be intentional in some cases (excessive OR clauses,
  large OFFSET pagination, missing ORDER BY/GROUP BY index)

### INFO (may vary with data volume)

```
--- INFO (may vary with data volume) ---
```

INFO findings are advisory and do not fail a test at their default severity. They include
structural or contextual suggestions such as `SELECT *`, `COUNT` where `EXISTS` may suffice, and
covering-index opportunities; runtime heuristics such as suspected N+1 access; and EXPLAIN results
such as full table scans, filesort, and temporary-table use. Some depend on data volume or planner
state, while others need application context before a change is justified.

!!! tip
    Set `report.show-info: false` in `application.yml` to hide this section if
    your tests use small datasets where these findings are not actionable.

    The setting applies to console, HTML, and JSON output, including aggregate summary counts.
    It does not disable INFO detectors or change test failure behavior. Confirmed and acknowledged
    findings, captured queries, query totals, timings, and index metadata remain available.

### OK

```
[OK] 2 queries passed
```

Shows how many queries had no detected issues.

### Summary

```
────────────────────────────────────────────────────────────────────────
  2 unique patterns | 4 total queries | 342 ms total
  1 error | 1 info | 2 passed
────────────────────────────────────────────────────────────────────────
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

1. **Start with the summary line.** `0 errors | 0 warnings` means there are no confirmed findings
   at the default severities; INFO advisories may still deserve review.

2. **Focus on CONFIRMED errors first.** These are the configured actionable findings, such as a
   repeated access pattern or a WHERE column with no matching index in the captured metadata.

3. **Review warnings.** Rules such as large-offset pagination or excessive OR clauses can be
   intentional. Suppress a reviewed case with its rule code or record it in the baseline with a
   reason.

4. **Glance at INFO.** `SELECT *` and other contextual suggestions are INFO by default. EXPLAIN
   advisories such as a full table scan may be normal on a small test dataset and more useful when
   the test has realistic volume and statistics.

5. **Look at the Fix suggestion.** QueryAudit provides actionable suggestions like
   `CREATE INDEX` DDL or recommendations to use JOIN FETCH.

---

## See Also

- [Configuration Reference](configuration.md) -- Configure report format and output directory
- [CI/CD Integration](ci-cd.md) -- Upload reports as CI artifacts
- [Suppressing Issues](suppressing.md) -- Suppress intentional findings from reports

### Query evidence retention

Each test in schema 1.3 includes `queryEvidence` with `status`, `retainedQueries`, and
`omittedQueries`. `COMPLETE` means every captured query record is present (including a test
that executed zero queries). `PARTIAL` means some records are retained; `OMITTED` means the
query list is empty even though queries ran. Consumers must not interpret an empty `queries`
array as proof that no queries executed; use `summary.totalQueries` for the captured count.

The suite aggregator retains full query records for the first 200 reports and compacts later
reports to bound memory use. Compaction preserves findings, test identity, index metadata,
query totals, and timing. It only changes evidence availability, so it does not turn a completed
PASS or FAIL into INCONCLUSIVE. HTML reports also show when query evidence was omitted.

### Machine report redaction

JSON reports, comparison verdicts, and GitHub Actions annotations default to `REDACTED`.
SQL values are replaced with `?`, including numeric, string, national/escaped/Unicode,
hex/bit/octal (including numeric separators), dollar-quoted, date/time, and interval literals. Comments are removed, including
unterminated comments and literals. Where backslash escaping is ambiguous, the report hides
value spans from both interpretations. Double-quoted SQL text is treated conservatively as
potential literal content because its meaning differs between database modes. MySQL backtick
identifiers and unquoted schema identifiers are retained. A `#` starts a redacted comment
even where PostgreSQL could interpret it as an operator; the default favors hiding possible
MySQL comment content. Synthetic `findById` evidence never includes the entity ID.

Raw finding details and suggestions may contain values extracted from SQL without quotes.
Redacted reports replace this prose with the rule description and a safe suggestion; structured
remediation still identifies the action, table, and columns. Incomplete-reason codes remain,
while their free-form details are omitted. Stack evidence keeps up to five application frames,
removes framework frames, and reduces source paths to filenames.

The original in-memory capture, analysis, policy checks, query counts, and outcome are unchanged.
The envelope declares `redaction`; comparing different modes returns INCONCLUSIVE with
`REPORT_REDACTION_MISMATCH`, never a successful resolution. Verdict JSON is redacted by default
even when both input reports used full detail.

For local debugging, explicitly opt in to full detail:

```yaml
query-audit:
  report:
    format: json
    redaction: full
```

Plain JUnit uses `-DqueryAudit.reportRedaction=full`. Core callers can set
`QueryAuditConfig.builder().reportRedaction(ReportRedaction.FULL)` when constructing a
`JsonReporter`, or pass `ReportRedaction.FULL` to `JsonReporter.toRunEnvelopeJson`.
All active contexts in one JUnit run must use the same mode. Unknown values fail configuration.

Full reports can expose SQL values and local paths; keep them out of shared CI artifacts.
Console and HTML diagnostics are not redacted by this setting. The JUnit extension still
prints per-test console output, including in CI, so review job-log access as well as artifact
uploads.
Test identities, display names, schema identifiers, and numeric execution statistics are
structural report data and remain visible. Do not put credentials or customer data in those
identifiers or test names. Redaction reduces accidental disclosure; it is not an anonymizer for
arbitrary application metadata.
