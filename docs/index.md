---
title: QueryAudit for JUnit 5
description: Turn test SQL into findings, query budgets, and reviewable build artifacts.
hide:
  - navigation
  - toc
---

<div class="qa-hero">
  <div class="qa-hero__copy">
    <p class="qa-eyebrow">QUERYAUDIT FOR JUNIT 5</p>
    <h1>Make database behavior part of your test contract.</h1>
    <p class="qa-hero__lead">
      Inspect N+1 patterns, missing-index findings, unsafe DML, and query-count drift in the JUnit test that executes the SQL. Keep the evidence in HTML and versioned JSON for CI review.
    </p>
    <div class="qa-hero__actions">
      <a href="getting-started/quickstart/" class="md-button md-button--primary">Run the first audit</a>
      <a href="#choose-your-test-setup" class="md-button">Choose a setup</a>
    </div>
  </div>
  <div class="qa-terminal" aria-label="Abbreviated QueryAudit test failure">
    <div class="qa-terminal__bar">
      <span class="qa-terminal__dots" aria-hidden="true">● ● ●</span>
      <span>abbreviated test failure</span>
    </div>
    <pre><code>QueryAudit detected 1 issue(s)
in findOrdersWithItems:

<span class="qa-terminal__error">[ERROR] N+1 Query detected (table: items)</span>
  Detail: Lazy collection 'items' on Order
          initialized 5 times for 5 entities
  Suggestion: Use @EntityGraph, JOIN FETCH,
              or @BatchSize</code></pre>
  </div>
</div>

<ul class="qa-facts" aria-label="Compatibility and project facts">
  <li><strong>60+</strong><span>finding types</span></li>
  <li><strong>Java 17+</strong><span>CI on 17 and 21</span></li>
  <li><strong>JUnit 5</strong><span>per-test lifecycle</span></li>
  <li><strong>Boot 3 &amp; 4</strong><span>auto-configuration</span></li>
  <li><strong>MySQL + PostgreSQL</strong><span>index metadata</span></li>
  <li><strong>HTML + JSON</strong><span>review and automation</span></li>
</ul>

!!! note "Documentation versions"
    Dependency snippets use the current Maven Central release. Features labeled 0.6.0+ require
    QueryAudit 0.6.0 or later.

## Choose your test setup

Pick the database module that matches the database used by the test. PostgreSQL users can replace
`query-audit-mysql` with `query-audit-postgresql` in each example.

=== "Boot · Gradle"

    ```groovy
    dependencies {
        testImplementation 'io.github.haroya01:query-audit-spring-boot-starter:0.5.0' // x-release-please-version
        testImplementation 'io.github.haroya01:query-audit-mysql:0.5.0' // x-release-please-version
    }
    ```

=== "Boot · Maven"

    ```xml
    <dependencies>
        <dependency>
            <groupId>io.github.haroya01</groupId>
            <artifactId>query-audit-spring-boot-starter</artifactId>
            <version>0.5.0</version> <!-- x-release-please-version -->
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>io.github.haroya01</groupId>
            <artifactId>query-audit-mysql</artifactId>
            <version>0.5.0</version> <!-- x-release-please-version -->
            <scope>test</scope>
        </dependency>
    </dependencies>
    ```

=== "JUnit · Gradle"

    ```groovy
    dependencies {
        testImplementation 'io.github.haroya01:query-audit-mysql:0.5.0' // x-release-please-version
        testImplementation 'net.ttddyy:datasource-proxy:1.10'
    }
    ```

=== "JUnit · Maven"

    ```xml
    <dependencies>
        <dependency>
            <groupId>io.github.haroya01</groupId>
            <artifactId>query-audit-mysql</artifactId>
            <version>0.5.0</version> <!-- x-release-please-version -->
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>net.ttddyy</groupId>
            <artifactId>datasource-proxy</artifactId>
            <version>1.10</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
    ```

Spring Boot discovers and wraps the test `DataSource`. The plain JUnit setup explicitly exposes
the `ProxyDataSource` used by the repository as a static field; the
[installation guide](getting-started/installation.md#plain-junit-5) shows the complete wiring.

## Start with a controlled failure

Report findings first, and enforce one statement budget you understand:

```java
@SpringBootTest
@EnableQueryInspector
class OrderServiceQueryTest {

    @Autowired
    private OrderService orderService;

    @Test
    @ExpectQueries(select = 2, insert = 0, update = 0, delete = 0)
    void loadsOrdersWithItemsWithinBudget() {
        List<Order> orders = orderService.findRecentOrders();
        orders.forEach(order -> assertThat(order.getItems()).isNotEmpty());
    }
}
```

If loading the lazy association pushes the SELECT count above two, the budget failure lists those
statements and their captured application call sites. The console report above it groups confirmed
findings and informational advice. Fix the fetch shape, rerun the same test, and then replace
`@EnableQueryInspector` with `@QueryAudit` when confirmed findings should fail the test too.

[Follow the complete first run](getting-started/quickstart.md){ .md-button .md-button--primary }
[See the plain JUnit version](getting-started/quickstart.md#2-put-a-budget-on-one-path){ .md-button }

## Read a finding as evidence

QueryAudit combines several sources rather than treating every SQL smell as the same kind of
result. A finding can carry the normalized SQL shape, application call site, table or column,
threshold details, index state, and a suggested next change. The available fields depend on the
check and on what the test environment exposes.

Use the output in this order:

1. Start with the issue type, severity, or exceeded budget.
2. Use `Query`, `Source`, `Target`, and `Detail` to confirm why it applies to this test.
3. Review `Fix` or `Suggestion` against the application semantics and existing schema.
4. Run the same test again; do not accept a fix only because the original message disappeared.

<div class="qa-card-grid" markdown>
  <div class="qa-card" markdown>

<p class="qa-card__kicker">DETECT</p>

### [Review query and DML shapes](detections/overview.md)

Cover repeated access, unsafe writes, inefficient SQL forms, locking risks, ORM behavior, and connection use.

  </div>
  <div class="qa-card" markdown>

<p class="qa-card__kicker">INDEXES</p>

### [Use real index metadata](detections/missing-index.md)

Cross-reference MySQL `SHOW INDEX` or PostgreSQL `pg_catalog` metadata for checks that need schema context.

  </div>
  <div class="qa-card" markdown>

<p class="qa-card__kicker">BUDGETS</p>

### [Limit reads and writes](guide/annotations.md#expectqueries)

Set one total cap or independent SELECT, INSERT, UPDATE, and DELETE limits on a test method.

  </div>
  <div class="qa-card" markdown>

<p class="qa-card__kicker">CONTRACTS</p>

### [Record statement-count snapshots](guide/contracts.md)

Keep selected tests' statement counts in a compact file and review deliberate changes with the code.

  </div>
  <div class="qa-card" markdown>

<p class="qa-card__kicker">LIFECYCLE</p>

### [Choose audit coverage](guide/configuration.md)

Audit the test body by default, opt into setup and teardown SQL, or roll coverage across a full suite.

  </div>
  <div class="qa-card" markdown>

<p class="qa-card__kicker">REPORTS</p>

### [Keep reviewable artifacts](guide/reports.md)

Use console output while editing, HTML for review, and schema-versioned JSON for CI and tooling.

  </div>
</div>

<div class="qa-loop-section" markdown>
  <div class="qa-loop-section__copy" markdown>

## Verify changes with the same tests

A coding tool can use the fields present in `report.json` to propose a focused change. The test
suite remains the authority: rerun the same selection locally, then let CI repeat the audit from a
clean checkout.

  </div>
  <ol class="qa-loop" role="list">
    <li><strong>Capture</strong><span>Run the audited test and save its aggregate report.</span></li>
    <li><strong>Inspect</strong><span>Read the finding, query shape, call site, and available database context.</span></li>
    <li><strong>Change</strong><span>Update the query, mapping, or schema and review the application behavior.</span></li>
    <li><strong>Verify</strong><span>Rerun locally; let CI independently run and publish the evidence.</span></li>
  </ol>
</div>

QueryAudit 0.5.x writes both suite artifacts after a session with at least one completed audited
result:

```text
build/reports/query-audit/index.html
build/reports/query-audit/report.json
```

QueryAudit 0.6.0+ selects one suite report format. The default `console` mode writes no file;
choose `json` for machine consumers or `html` for browser review. Per-test console diagnostics
remain available.

The QueryAudit 0.6.0+ comparator classifies confirmed findings as new, resolved, or persisting and
writes an optional `verdict.json`:

<div class="qa-verdict" aria-label="QueryAudit report comparison result">
  <code>[QueryAudit] compare: PASS; 0 new, 1 resolved, 0 persisting; queries 11 -&gt; 7</code>
</div>

In QueryAudit 0.6.0+, the comparator returns `0` for `PASS`, `1` for `FAIL`, and `2` for
`INCONCLUSIVE`. A failed or incomplete candidate cannot appear successful merely because its
finding delta is empty. QueryAudit 0.5.x provides a basic finding delta without this completeness
guarantee. QueryAudit 0.6.0+ also includes structured remediation for supported high-precision
findings; other findings keep their human-readable suggestion.
Read the [report and comparator contract](guide/reports.md#delta-verdict-compare-two-runs) before
making it a required CI check. The [CI guide](guide/ci-cd.md) shows how to rerun tests with MySQL
or PostgreSQL, keep artifacts on failure, and emit GitHub Actions annotations.

## Compatibility

| Layer | Project baseline and coverage |
|---|---|
| Java | Requires 17; CI runs on 17 and 21 |
| JUnit | JUnit 5 extension; built and tested with 5.11.4 |
| Spring Boot | 3.x and 4.x; the current suite covers 3.4.1 and 4.0.6 |
| Database metadata | MySQL and PostgreSQL; integration tests run MySQL 8.0 and PostgreSQL 16 |

<div class="qa-cta" markdown>

## Continue with the task in front of you

| Goal | Guide |
|---|---|
| Install or verify capture | [Installation](getting-started/installation.md) |
| Complete the first fix loop | [Quick start](getting-started/quickstart.md) |
| Configure the starter | [Spring Boot integration](getting-started/spring-boot.md) |
| Choose annotations or budgets | [Annotations](guide/annotations.md) |
| Record query-count snapshots | [Query contracts](guide/contracts.md) |
| Parse or compare reports | [Reports](guide/reports.md) |
| Add the audit to CI | [CI/CD integration](guide/ci-cd.md) |
| Diagnose missing capture or metadata | [Troubleshooting](guide/troubleshooting.md) |

[Install QueryAudit](getting-started/installation.md){ .md-button .md-button--primary }
[Open troubleshooting](guide/troubleshooting.md){ .md-button }

</div>
