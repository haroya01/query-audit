---
title: QueryAudit for JUnit 5
description: Catch query problems, enforce query budgets, and review database behavior before merge.
hide:
  - navigation
  - toc
---

<div class="qa-hero">
  <div class="qa-hero__copy">
    <p class="qa-eyebrow">QUERYAUDIT FOR JUNIT 5</p>
    <h1>Make database behavior part of your test contract.</h1>
    <p class="qa-hero__lead">
      Catch N+1 queries, missing indexes, unsafe DML, and query-count drift before merge. Turn test SQL into findings, budgets, and reviewable contracts.
    </p>
    <div class="qa-hero__actions">
      <a href="getting-started/installation/" class="md-button md-button--primary">Install QueryAudit</a>
      <a href="#see-the-finding-where-it-started" class="md-button">See how it works</a>
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
  <li><strong>60+</strong><span>active finding types</span></li>
  <li><strong>Java 17+</strong><span>CI on 17 and 21</span></li>
  <li><strong>JUnit 5.9+</strong><span>test lifecycle integration</span></li>
  <li><strong>Boot 3 &amp; 4</strong><span>auto-configuration</span></li>
  <li><strong>MySQL + PostgreSQL</strong><span>index metadata</span></li>
  <li><strong>Apache 2.0</strong><span>available on Maven Central</span></li>
</ul>

## Two dependencies. One annotation.

Add the Spring Boot starter and the database module to your test configuration.

=== "Gradle"

    ```groovy
    dependencies {
        testImplementation 'io.github.haroya01:query-audit-spring-boot-starter:0.5.0' // x-release-please-version
        testImplementation 'io.github.haroya01:query-audit-mysql:0.5.0' // x-release-please-version
    }
    ```

=== "Maven"

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

For PostgreSQL, replace `query-audit-mysql` with `query-audit-postgresql`. The
[installation guide](getting-started/installation.md) has the full Gradle and Maven examples.

```java
@SpringBootTest
@QueryAudit
class OrderServiceTest {

    @Autowired
    private OrderService orderService;

    @Test
    void findOrdersWithItems() {
        List<Order> orders = orderService.findAllWithItems();

        assertThat(orders).hasSize(5);
    }
}
```

With the Spring Boot starter, QueryAudit discovers the test `DataSource`, captures its SQL, and
runs the checks selected by your profile. Configured findings fail the test that produced them.
The [quick start](getting-started/quickstart.md) walks through a complete first run.

## See the finding where it started

Small test datasets hide query problems. QueryAudit captures SQL while the test runs, then
evaluates query structure, repetition, Hibernate lazy loads, and available index metadata.
Reports keep the finding attached to its test and include the relevant context that could be
collected: SQL, application call site, table or column, index state, and a suggested next step.

The console excerpt above is intentionally shortened. Use the
[HTML and JSON reports](guide/reports.md) when you need the full test-suite view or a durable CI
artifact.

<div class="qa-card-grid" markdown>
  <div class="qa-card" markdown>

<p class="qa-card__kicker">DETECT</p>

### [Find query anti-patterns](detections/overview.md)

Cover N+1 access, unsafe DML, inefficient SQL shapes, locking risks, ORM behavior, and connection lifecycle problems.

  </div>
  <div class="qa-card" markdown>

<p class="qa-card__kicker">INDEXES</p>

### [Check real index metadata](detections/missing-index.md)

Cross-reference query columns with MySQL `SHOW INDEX` or PostgreSQL `pg_catalog` instead of guessing from SQL alone.

  </div>
  <div class="qa-card" markdown>

<p class="qa-card__kicker">BUDGETS</p>

### [Set per-test query limits](guide/annotations.md#expectqueries)

Cap total queries or set separate SELECT, INSERT, UPDATE, and DELETE budgets with JUnit annotations.

  </div>
  <div class="qa-card" markdown>

<p class="qa-card__kicker">CONTRACTS</p>

### [Record statement-count snapshots](guide/contracts.md)

Keep recorded tests' statement counts in a compact file and review intentional changes beside the code.

  </div>
  <div class="qa-card" markdown>

<p class="qa-card__kicker">ADOPTION</p>

### [Audit the full suite](guide/configuration.md#audit-coverage-mode)

Enable JUnit extension autodetection, choose a profile, set `mode: all`, and baseline known findings.

  </div>
  <div class="qa-card" markdown>

<p class="qa-card__kicker">REPORTS</p>

### [Keep useful build artifacts](guide/reports.md)

Produce console, HTML, versioned JSON, and GitHub Actions output for local work and CI review.

  </div>
</div>

<div class="qa-loop-section" markdown>
  <div class="qa-loop-section__copy" markdown>

## A feedback loop your tools can verify

A green unit test can still hide extra reads or writes. QueryAudit gives CI and automated
development tools an objective way to check database-facing changes.

  </div>
  <ol class="qa-loop" role="list">
    <li><strong>Capture</strong><span>Audited tests expose SQL statement templates and counts.</span></li>
    <li><strong>Inspect</strong><span><code>report.json</code> stores findings and available source, index, and remediation context.</span></li>
    <li><strong>Change</strong><span>Update the query, mapping, or schema with the evidence beside it.</span></li>
    <li><strong>Verify</strong><span>Run the same tests and compare the two reports.</span></li>
  </ol>
</div>

<div class="qa-verdict" aria-label="QueryAudit report comparison result">
  <code>[QueryAudit] compare: 0 new, 1 resolved, 0 persisting; queries 11 -&gt; 7</code>
</div>

The comparator returns `0` when there are no new confirmed findings, `1` when new findings
appear, and `2` when it cannot produce a verdict. Structured remediation is included for
supported high-precision findings; other findings keep their human-readable suggestion.
[Read the report and comparator contract](guide/reports.md#delta-verdict-compare-two-runs).

For recorded tests with SQL in both runs, query snapshot contracts check SELECT, INSERT, UPDATE,
and DELETE counts. Their file diff makes a recorded statement-count change visible beside the code.

<div class="qa-cta" markdown>

## Inspect findings before enforcing them

Use `@EnableQueryInspector` to report detected findings without failing on those findings. Query
budgets and recorded contracts continue to enforce their own limits. Review the first results,
select a rule profile, then switch to `@QueryAudit` when the suite is ready to enforce findings.

[Follow the quick start](getting-started/quickstart.md){ .md-button .md-button--primary }
[Record query contracts](guide/contracts.md){ .md-button }

</div>
