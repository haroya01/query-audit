# Reports

Read the failed policy, inspect its SQL and captured call site, then keep a JSON artifact for CI.

## Read a policy failure

The [runnable read-path test](../getting-started/quickstart.md) permits one SELECT and no writes.
Adding `-PextraWrite=true` produces this budget failure (excerpt):

```text
QueryAudit: readsOnce() exceeded its query budget.
UPDATE: executed 1, expected at most 0.
  UPDATE orders SET status = 'NEW' WHERE id = 1
```

The console lists captured SQL for the violated type. Its first stack frame can be a JDBC proxy.
Use the JSON evidence to find the application caller; this is the same write in the example's
`reports[].queries` (excerpt; line numbers can change):

```json
{
  "sql": "UPDATE orders SET status = ? WHERE id = ?",
  "stackTrace": "example.audit.FirstAuditTest.writeOnReadPath:43\nexample.audit.FirstAuditTest.readsOnce:26"
}
```

JSON redacts literal values and filters stack frames. Application frames appear when capture
retains them. [Snapshot contract failures](contracts.md#enforcement) also show recorded and
executed counts; increases list statements of that type. Fix the extra access and rerun the same
test, or review an intentional [budget](annotations.md#expectqueries) or [contract](contracts.md#updating)
change with the code.

## Keep the result in CI

Select JSON for the audit run, then inspect its outcome and any missing evidence:

```bash
jq '{outcome, incompleteReasons, coverage}' build/reports/query-audit/report.json
```

| Result | Use in CI |
|---|---|
| `PASS` | Accept only with a successful test command and a fresh report |
| `FAIL` | Inspect the failed budget, contract, or enabled finding policy in the test log |
| `INCONCLUSIVE` | Restore missing capture, required tests, or inputs before accepting the run |

Use the [first CI check](first-ci-check.md) for the full gate and artifact setup. Add an
[expected-test manifest](audit-coverage.md) to require specific audits. To compare runs, use the
[compare command](#delta-verdict-compare-two-runs); incompatible settings or missing audit evidence
cannot establish a successful fix. Total count changes are reported for context; budgets and
contracts enforce query counts.

## Report formats

!!! note "Version scope"
    This page documents QueryAudit 0.6 and JSON schema 1.6. QueryAudit 0.5 writes both HTML
    and schema 1.0 JSON after a session with at least one completed audited result; the
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

A finding includes its SQL, source location when captured, and a suggested investigation. Example
excerpt from the console report:

```text
  [ERROR] N+1 Query detected
    Query:  select id, order_id, sku from order_items where order_id = ?
    Source: com.example.OrderService.findOrders:42
    Target: order_items
    Detail: Query repeated 3 times (threshold: 3)
    Fix:    Use JOIN FETCH, @EntityGraph, or batch loading (IN clause)

--- Query Patterns ---
  [  3x] select id, order_id, sku from order_items where order_id = ?
  [  1x] select * from orders where user_id = ?
```

A finding diagnostic helps investigate the access path. The final test result and suite outcome
determine whether the configured policy passed; a per-test console `[OK]` line can appear before
a later budget assertion fails.

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
`coverage` records which expected tests executed and produced audits. It is `null` when no
manifest was verified; this example does not establish whole-suite coverage. See
[Audit coverage](audit-coverage.md) to declare and enforce the tests a run must audit.
`comparisonInputs` identifies each test's effective analysis inputs. The empty object in this
example is valid for a standalone report but cannot support a verified comparison.

```json
{
  "schemaVersion": "1.6.0",
  "redaction": "REDACTED",
  "outcome": "FAIL",
  "incompleteReasons": [],
  "coverage": null,
  "comparisonInputs": {},
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
of silently misparsing it. The current version is **1.6.0**. QueryAudit 0.5.x wrote schema 1.0
without a run outcome; the comparator treats those reports as `INCONCLUSIVE` because it cannot
infer a trustworthy `PASS` from the per-test reports alone. Schema 1.1 added run outcomes, 1.2
added stable test identities, 1.3 added query-evidence retention counts, and 1.4 added the report
redaction mode. Schema 1.5 added expected-test coverage; 1.6 adds per-test comparison inputs.

Each version has a published JSON Schema. The deprecated Java method
`JsonReporter.toEnvelopeJson(List<QueryAuditReport>)` emits a legacy 1.0 envelope without run
outcomes or stable identity fields. A list of reports cannot establish whether the audit
completed or its policies passed. New callers should use
`JsonReporter.toRunEnvelopeJson(AuditRunResult)`.

### Run outcomes

The suite outcome uses one precedence rule everywhere: `INCONCLUSIVE > FAIL > PASS`.

| Outcome | Meaning |
|---|---|
| `PASS` | The reported audits completed and every configured policy and contract passed. A non-null `coverage` is also required to verify the expected-test manifest. |
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
| `POLICY_WRITE_FAILED` | Requested contract or query-count baseline recording failed. The launcher fails and the suite outcome is `INCONCLUSIVE`, even in report-only mode. |
| `UNSUPPORTED_SCHEMA` | Report comparison received a schema it cannot evaluate safely, including legacy 1.0 input with no outcome. |
| `EXPECTED_TEST_MISSING` | An expected test was not discovered, skipped, failed before completing its audit, or otherwise did not supply complete audit evidence. Report comparison also uses this for baseline-relative missing tests. |
| `COVERAGE_MANIFEST_UNREADABLE` | An explicitly configured expected-test manifest was missing, or the selected manifest was unreadable, empty, or malformed. |
| `COVERAGE_MANIFEST_MISMATCH` | Comparison inputs were verified against different expected-test manifests, or only one input had verified coverage. |
| `REPORT_REDACTION_MISMATCH` | Comparison inputs used different report redaction modes. |
| `CAPABILITY_INITIALIZATION_FAILED` | An enabled metadata, Hibernate, or other analysis capability could not initialize. |
| `CAPABILITY_EXECUTION_FAILED` | An available analysis capability failed during the audit, including unsupported parameterized SQL in the bundled EXPLAIN analyzers. |
| `COMPARISON_INPUTS_UNAVAILABLE` | A compared test lacks input metadata, or a custom detector or capability has inputs that cannot be fully identified. |
| `INCOMPATIBLE_AUDIT_INPUTS` | Compared tests used different versions, profiles, capabilities, or effective configuration and policy fingerprints. |
| `REPORT_WRITE_FAILED` | The selected JSON or HTML artifact could not be written. The extension fails an active audit; listener-only finalization cannot reliably fail the launcher. Always check the report artifact in CI. |

Field notes for machine consumers:

- `comparisonInputs` is keyed by stable `testId`. It records versions, the active profile and
  dialect, detector and capability identities, and fingerprints of effective settings and loaded
  policies. Missing entries are unverified, not implicit defaults. See
  [Comparison inputs](comparison-inputs.md) for compatibility rules and custom integration limits.
- `coverage: null` means the run did not verify an expected-test manifest. With coverage enabled,
  the JSON artifact includes counts and per-test execution/audit states even when HTML or console
  output is selected. Use the [coverage CI gate](audit-coverage.md#require-the-artifact-in-ci) to
  reject missing, skipped, or unaudited expected tests.
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
[`schema/report-1.3.schema.json`](https://haroya01.github.io/query-audit/schema/report-1.3.schema.json),
[`schema/report-1.4.schema.json`](https://haroya01.github.io/query-audit/schema/report-1.4.schema.json),
[`schema/report-1.5.schema.json`](https://haroya01.github.io/query-audit/schema/report-1.5.schema.json), and
[`schema/report-1.6.schema.json`](https://haroya01.github.io/query-audit/schema/report-1.6.schema.json).
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

### Parser identity

From 0.6.0, every standard installation includes the same structural parser dependency. Tools
can read its name and actual runtime version through `EnhancedSqlParser.parserName()` and
`EnhancedSqlParser.parserVersion()`. The version comes from the loaded JSqlParser dependency's
metadata, including when dependency management selects another version.

Unsupported SQL and statements above 10,000 characters still use the regex fallback. This is a
per-statement behavior, not a separate classpath-selected parser mode. Schema 1.6 records parser
identity in `comparisonInputs`; use the same dependency versions and audit settings for both runs.
See [input compatibility](comparison-inputs.md).

## Delta Verdict (compare two runs)

Run the comparator on reports from the baseline and candidate. It checks for new confirmed
findings, missing audit evidence, and compatible inputs before reporting resolution:

```bash
java -cp query-audit-core-<version>.jar \
    io.queryaudit.core.reporter.ReportComparator before.json after.json verdict.json
```

```
[QueryAudit] compare: PASS; 0 new, 1 resolved, 0 persisting; queries 11 -> 7
  RESOLVED n-plus-one (table: order_items) in OrderServiceTest.findOrders
```

This is an example with compatible, complete reports and one resolved finding. The displayed
`queries 11 -> 7` is context, not a count assertion: an increase alone does not fail this command.
Use [read/write budgets](annotations.md#expectqueries) or [snapshot contracts](contracts.md) for
count policy. Missing tests or changed audit settings make the comparison `INCONCLUSIVE`, even
when the candidate reports fewer findings. An existing confirmed finding may persist in a passing
comparison; inspect `persisting` when reviewing a particular fix.

- **Exit contract**: `0` for `PASS`, `1` for `FAIL`, and `2` for `INCONCLUSIVE` or a usage/parse
  error. A candidate run that already has outcome `FAIL` cannot become a successful comparison
  merely because it introduced no new finding.
- **`verdict.json`**: `{outcome, incompleteReasons, newFindings, resolved, persisting, complete,
  missingTests, unexpectedTests, inputDifferences, queryCountDelta, executionTimeMsDelta}`.
  Use the final `outcome` or process exit code as the gate.

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
- With [manifest coverage](audit-coverage.md), `missingTests` also includes expected tests that
  failed to supply complete audit evidence. `unexpectedTests` identifies candidate audits outside
  the baseline or declared manifest. A coverage gap prevents affected findings from being marked
  resolved. Different manifests, including coverage enabled on only one side, make the comparison
  `INCONCLUSIVE` with `COVERAGE_MANIFEST_MISMATCH` and no resolved findings.
- Both sides must identify compatible effective inputs for each compared test. Legacy reports,
  missing metadata, and unidentified custom inputs produce `COMPARISON_INPUTS_UNAVAILABLE`.
  Changed inputs produce `INCOMPATIBLE_AUDIT_INPUTS`. Both make the comparison `INCONCLUSIVE`
  and leave `resolved` empty; `inputDifferences` lists the test ID, field, and safe baseline/candidate
  values. See [Comparison inputs](comparison-inputs.md) before replacing a baseline.
- **Matching key**: `testId|type|normalized-pattern|sourceLocation`, so findings survive display
  name edits and unrelated refactors as long as the statement shape and call site are stable.
- The comparator accepts schema 1.0 and 1.1 reports and uses an exact `testClass|testName` fallback
  when one side lacks IDs. Those reports still lack verified comparison inputs and cannot produce
  a passing comparison. It rejects an ambiguous legacy match instead of assigning one old test
  to multiple stable IDs. Re-record archived baselines with QueryAudit 0.6 when a suite contains
  duplicate legacy identities. A display name changed before the first schema 1.2 run has no safe
  fallback and is reported as a missing old test plus a new test.
- Only **confirmed** findings participate. INFO and acknowledged findings are not part of the
  comparison's new-finding gate.
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

Issues in this section are eligible to fail the test when `failOnDetection` is `true`. They come
from structural SQL checks, database metadata, configured thresholds, or Hibernate events. Review
the evidence against the application semantics before changing a query or schema.

Confirmed issues have either **ERROR** or **WARNING** severity:

- **ERROR** -- rules assigned error severity, including N+1, missing WHERE/JOIN indexes, and
  functions on indexed columns.
- **WARNING** -- rules assigned warning severity, including excessive OR clauses, large OFFSET
  pagination, and missing ORDER BY/GROUP BY indexes.

The category and severity describe policy behavior; they do not guarantee that a finding is a
production performance problem.

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

    In QueryAudit 0.6, the setting applies to console, HTML, and JSON output, including aggregate
    summary counts. Keep it identical between comparison runs.
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

1. **Check the final test and audit outcomes.** `0 errors | 0 warnings` only describes findings;
   a budget, contract, or incomplete audit can still fail the run.

2. **Read the failed count policy.** Compare allowed or recorded counts with executed counts.
   Follow the listed SQL and captured call site to the extra access.

3. **Review an intended count change.** Update the annotation or re-record the contract, inspect
   its diff, and rerun. Keep the change beside the application code in the PR.

4. **Investigate relevant findings.** Review repeated access and index evidence with application
   context. INFO suggestions and execution plans may depend on fixtures and database statistics;
   check a proposed fetch or index change before applying it.

5. **Verify the comparison.** Use the final comparator outcome. Missing tests or changed audit
   settings leave resolution unverified even when there are fewer findings.

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
