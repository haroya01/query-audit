---
title: Troubleshooting
description: Find the symptom, verify capture, and make the smallest corrective change.
---

# Troubleshooting

**Start by proving capture:** a test that executes one `SELECT` must fail
`@ExpectQueries(select = 0)`. A passing upper bound and an empty finding list can both mean that
SQL bypassed capture. Use the complete [Spring check](../getting-started/spring-boot.md#run-a-controlled-first-audit)
or [plain JUnit quick start](../getting-started/quickstart.md).

| Symptom | First check | Next action |
| --- | --- | --- |
| SQL ran, but the report says zero queries | Does the one-SELECT/zero-budget check fail? | [Trace the datasource and audit activation](#queryaudit-not-detecting-any-queries) |
| A test passes after its Spring context is replaced | Does it use `@DirtiesContext` between methods? | Check the [known context-replacement gap](limitations.md) before trusting the result |
| No audit with a custom or inherited annotation | Which artifact/version is loaded, and does a direct annotation activate capture? | Check [shared-policy checks](limitations.md#shared-annotation-policies), then rerun the zero-budget proof |
| A budget fails unexpectedly | Which statements were captured during setup, the test, and teardown? | [Check the count boundary](#expectmaxquerycount-fails-unexpectedly) |
| A batched Hibernate fetch is reported as N+1 | How many SQL statements actually ran? | [Check the known batch false positive](#common-jpahibernate-issues) |
| An expected finding is missing | Was SQL captured, and is the rule enabled in this profile? | [Check rule inputs](#why-didnt-queryaudit-detect-my-issue) |
| No JSON/HTML file | Was that format selected, and did the test session finalize? | [Check report generation](#html-report-not-generated) |
| HTML is green but the run was incomplete | What do the test exit status and JSON outcome say? | [Use the canonical outcome](#html-report-not-generated) |
| CI differs from local execution | Same artifact, fixture, schema, profile, and policies? | [Compare effective inputs](#tests-fail-in-ci-but-pass-locally) |
| Memory or runtime grows sharply | Many lazy events, unique queries, or oversized tests? | [Measure and bound the audit](#outofmemoryerror-during-tests) |

The [known limitations](limitations.md) page links the reviewed defects and their issue status.
[Versions](../getting-started/versions.md) lists the published artifact’s scope and tested combinations.

## QueryAudit Not Detecting Any Queries

Run the capture proof **through the same datasource used by the code under test**. Confirm that
JUnit actually discovered and executed the test; a skipped test or cached/no-op build is not proof.

| Check | Correction |
| --- | --- |
| Audit activation | Put `@EnableQueryInspector` or `@QueryAudit` directly on the test class/method. Keep `query-audit.enabled` enabled. |
| Spring wiring | Include the starter and retain automatic wrapping unless another datasource-proxy bean already provides capture. |
| Plain JUnit wiring | Expose the static proxy and pass that same object into the repository; a separately retained raw datasource bypasses it. |
| Test timing | Execute the proof SQL in the test body. Captured per-test setup/teardown SQL remains in raw reports and budgets; detector analysis excludes it by default. Class-level initialization is outside the per-test audit. |
| Multiple datasources | Check the actual selected datasource; marking one `@Primary` does not combine all of them. |
| Async work | Keep the audited SQL on the test execution path; see [execution constraints](#parallel-capture-is-incomplete). |

A raw mutable `static DataSource` field can be wrapped automatically on 0.6.0+, but a repository
constructed earlier from the raw object will still bypass capture. The explicit proxy in
[plain JUnit installation](../getting-started/installation.md#plain-junit-5) avoids that ambiguity.

Context replacement and composed/inherited annotations have reported capture gaps
([#287](https://github.com/haroya01/query-audit/issues/287),
[#290](https://github.com/haroya01/query-audit/issues/290)). See their
[reproduction scope](limitations.md#reported-cases-to-check) and verify capture after either change.
A zero-query result alone does not prove a clean audit.

## Why Didn't QueryAudit Detect My Issue?

After capture is verified, check the rule's [requirements](../detections/overview.md), active
[profile](configuration.md), exclusions, suppressions, and threshold. Avoid changing several
settings at once: rerun the same small test after each change.

### The rule requires index metadata

Add the module for the **actual test database**: `query-audit-mysql` or
`query-audit-postgresql`. H2 can verify query budgets and database-independent checks, but it does
not provide these vendors' index/EXPLAIN evidence. See [index metadata](#index-metadata-not-collected).

### The query is suppressed

Inspect `suppress-patterns`, `suppress-queries`, and `.query-audit-baseline`. A broad pattern can
hide an entire rule or table. Narrow only the relevant entry and rerun. Suppression changes what
findings are reported; it does not mean the SQL was never captured or should be excluded from a
query budget.

### The issue type is INFO and `show-info` is disabled

Set `query-audit.report.show-info: true` to include informational findings in reports.
In `0.6.0`, hiding INFO also removes it from generated JSON. Keep this setting identical across
comparison runs; see [report behavior](reports.md) and [comparison inputs](comparison-inputs.md).

### The threshold is too high

Compare the observed count/value with the rule's effective threshold in
[configuration](configuration.md). For example, the default repeated-query N+1 threshold is three.
Do not lower thresholds globally just to make one expected finding appear.

### The query type is not analyzed

Detection focuses on SELECT, INSERT, UPDATE, and DELETE. Capturing a statement does not imply a
rule exists for it; DDL and session commands are not an equivalent source of detector coverage.

### The rule is disabled

A profile or `disabled-rules` can exclude it. Check `enabled-rules` and annotation overrides too.
For a comparison, changing the rule set intentionally changes the audit inputs; see
[comparison inputs](comparison-inputs.md).

### SQL is too complex for the parser

From 0.6.0, JSqlParser is required transitively. Missing/incompatible parser classes or missing
version metadata are installation errors: check dependency exclusions and overrides.

Unsupported statements and SQL longer than 10,000 characters use a limited built-in structural
fallback. Normalization and some pattern checks also use built-in parsing. A successfully executed
SQL statement is not proof that every detector understands its dialect or structure.

For a reproducible parser report, include sanitized SQL, database/version, QueryAudit version, and
`EnhancedSqlParser.parserName()` / `EnhancedSqlParser.parserVersion()`. Keep the minimal statement's
quotes, comments, whitespace, and nesting intact when those affect the result.

## INSERT/UPDATE/DELETE Not Counted

Use the zero-budget capture check for the write type in the same execution path. For JPA, check
when a flush actually sends SQL: a queued entity change is not yet a JDBC statement. Keep the flush
inside the audited test when that write is part of the intended contract. Check the installed
[version](../getting-started/versions.md) and [counting rules](contracts.md) before adjusting a budget.

## @ExpectMaxQueryCount Fails Unexpectedly

`@ExpectMaxQueryCount` limits the total captured query count, including reads and writes.
`@ExpectQueries` lets you limit each type separately. Both count all captured per-test setup,
test-body, and teardown statements. `includeSetupQueries` filters detector analysis inputs only;
it does not remove statements from the raw report or either query budget.

Read the failure's statement list first. Look for implicit ORM loads, explicit flushes, application
listeners, or fixture setup/teardown SQL. Keep the intended behavior under test and fix excess
work before raising the budget. Moving fixture work merely to make the assertion green can hide
what the test was meant to check.

## Double Proxy with gavlyukovskiy

If another datasource decorator already provides a query-aware Spring datasource, use
`query-audit.wrap-data-source.enabled: false`. Keep QueryAudit enabled and repeat the capture proof.
Do not disable wrapping around a raw datasource. See
[reuse an existing proxy](../getting-started/spring-boot.md#reuse-an-existing-datasource-proxy).

## Index Metadata Not Collected

Check these inputs in order:

1. The test uses a real MySQL/PostgreSQL database with its matching QueryAudit module.
2. Migrations and index creation finish before the audit initializes metadata.
3. The test connection points to the intended schema and can read the relevant catalogs/indexes.
4. The run has no `CAPABILITY_INITIALIZATION_FAILED` or `CAPABILITY_EXECUTION_FAILED` reason.

The MySQL provider uses `SHOW INDEX`; PostgreSQL uses `pg_catalog`. H2 compatibility mode does not
substitute for their real metadata. Use the [installation dependency tabs](../getting-started/installation.md)
and your normal database fixture. Do not add indexes only to satisfy a diagnostic: inspect the
existing index definitions and the actual query plan.

??? info "EXPLAIN fails on a statement containing ?"

    The bundled analyzers cannot reconstruct typed bind values from captured SQL. When their
    EXPLAIN rules are enabled, a statement containing `?` can make the audit incomplete; the
    conservative check also includes question marks in literals/comments/operators. Do not replace
    binds with guessed values to manufacture a passing plan. See [comparison inputs](comparison-inputs.md)
    for the rule/profile boundary and capability outcome.

## Common JPA/Hibernate Issues

### N+1 Not Detected on Lazy Collections

Verify that the association is actually accessed inside the audited work and inspect executed SQL.
A cache, fetch join, entity graph, or batch fetch can reduce SQL executions. Conversely, entity-load
events alone do not prove that one query ran per entity.

The reviewed source can flag a **two-query batched fetch** as N+1
([#289](https://github.com/haroya01/query-audit/issues/289)). Confirm the SQL count before changing a
working batch strategy, and consult [known limitations](limitations.md). A query budget can enforce
the observed statement count while the detector result is investigated.

### FetchType.EAGER Causes Extra Queries

Inspect the generated SQL and load timing. Choose the fetch strategy for the use case; `LAZY`, a
fetch join, or an entity graph may help, but changing the mapping alone does not prove fewer queries.
Rerun the same fixture and budget.

### Hibernate Envers / Audit Queries

Audit-table writes are still database writes. Include intended writes in the budget. A finding
suppression can reduce advice about those tables, but does not remove their SQL from count contracts.

### Hibernate Second-Level Cache

Use a controlled cache state and deterministic fixtures when comparing query counts. Decide
whether the test exercises a cold or warm path; do not increase the budget simply to absorb
unexplained changes between runs.

### Spring Data JPA Derived Queries

Generated SQL is subject to the same checks as handwritten SQL. Inspect the actual statement,
metadata, and plan before acting on advice; a detector finding is not a guarantee that an index or
query rewrite will help your production workload.

## Tests Fail in CI but Pass Locally

Compare the resolved QueryAudit/parser versions, database/version, schema, fixture data, cache
state, Spring profile, enabled rules, and loaded policy files. Keep the input conditions stable
before deciding whether a finding changed.

Review a changed `.query-audit-counts` or `.query-audit-contracts` file as a deliberate policy change.
Do not automatically regenerate it to make CI pass. See [contracts](contracts.md),
[comparison inputs](comparison-inputs.md), and the [CI gate](ci-cd.md).

## Parallel Capture Is Incomplete

Published `0.6.0` rejects concurrent audited methods. Keep audited tests on the same thread;
add this to `src/test/resources/junit-platform.properties`:

```properties
junit.jupiter.execution.parallel.enabled=false
```

Remove explicit `@Execution(CONCURRENT)` from audited classes and methods, or replace it with
`@Execution(SAME_THREAD)`. Keep audited SQL on the test execution path: this release does not
provide a supported asynchronous attribution API.

`@TestFactory` dynamic children also lack a per-child audit boundary. Use ordinary `@Test` or
`@ParameterizedTest` methods for audited cases, or exclude the factory. See
[limitations](limitations.md) before changing the audit scope.

## Report Not Printing

Check that the test ran, auditing is enabled, and the annotation is applied directly. Then inspect
stderr and the build tool's test output. Gradle users can temporarily show standard streams:

=== "Gradle · Kotlin"

    ```kotlin
    tasks.test {
        testLogging.showStandardStreams = true
    }
    ```

=== "Gradle · Groovy"

    ```groovy
    test {
        testLogging.showStandardStreams = true
    }
    ```

Logging visibility does not verify capture; repeat the zero-budget proof if needed.

## HTML Report Not Generated

On 0.6.0, the default is console-only. Select `query-audit.report.format: html` for HTML or `json`
for a machine report. Reports are written when the test session finalizes; an earlier engine,
initialization, or filesystem failure can prevent an artifact. Check the first error and the
configured output directory. See [report configuration](reports.md).

Remove a previous run's expected report before a diagnostic rerun so an old file cannot be mistaken
for new evidence. In CI, require both the test result and a freshly generated JSON outcome.

The reviewed HTML can show `all clean` while the canonical result is `INCONCLUSIVE`
([#296](https://github.com/haroya01/query-audit/issues/296)). Until that issue is fixed in your version,
use the test result and JSON for the verdict; a green HTML page is not a CI gate.

## "HikariDataSource has been closed" on Spring Boot 4.x

Check the QueryAudit version and context lifecycle first. Older proxy-close handling was tracked in
[#153](https://github.com/haroya01/query-audit/issues/153). Check the [version guidance](../getting-started/versions.md)
before applying an old workaround.

Disabling QueryAudit wrapping is appropriate only if another query-aware datasource is already
registered. It is not a general fix for a closed pool. Context replacement also has a separately
tracked capture gap in [#287](https://github.com/haroya01/query-audit/issues/287).

## OutOfMemoryError During Tests

Measure SQL count, unique statements, lazy-event volume, and heap use on one reproducible test.
`query-audit.max-queries` bounds SQL capture, but exceeding it makes the audit incomplete. It is not
a passing performance shortcut, and it does not bound every Hibernate event allocation.

Use smaller representative fixtures or narrower audited tests when those preserve the contract.
Increase heap only after understanding the source of growth. Suppressing findings does **not**
prevent SQL capture and should not be presented as a memory limit. See [known limitations](limitations.md)
for lazy-event allocation and capture boundaries.

## Performance Impact of QueryAudit

Measure the same test with auditing enabled and disabled on identical fixtures. Separate database
startup, SQL execution, capture, and analysis where possible. Cost depends on query/event volume,
SQL shape, metadata size, and enabled rules; there is no universal per-query overhead guarantee.

Start with the recommended profile and the specific paths you intend to protect. Changing limits
or disabling a rule changes the audit contract; record that decision and recheck the expected result.

## Diagnostic Checklist

For an [issue report](https://github.com/haroya01/query-audit/issues/new/choose), include:

- Installed QueryAudit, Java, JUnit, framework, parser, and database versions.
- The smallest test/fixture that reproduces the problem and the exact build command.
- Expected versus actual SQL counts or affected/returned rows, plus the capture-proof result.
- Active QueryAudit settings, relevant policies, and the first failure/incomplete reason.
- Sanitized SQL/report excerpts that preserve the syntax needed to reproduce the behavior.

Do not include credentials, private data, or raw production artifacts. JSON/Actions redaction does
not apply to console/HTML in the current contract; review what you share.

## See Also

- [Known limitations](limitations.md)
- [Configuration](configuration.md)
- [Annotations and budgets](annotations.md)
- [CI verification](ci-cd.md)
