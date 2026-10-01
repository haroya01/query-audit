---
title: QueryAudit — catch N+1 queries before merge
description: Find N+1 queries at their call site in JUnit 5 tests, lock the fixed query counts in contracts, and gate pull requests in CI.
hide:
  - navigation
  - toc
---

<div class="qa-hero">
  <div class="qa-hero__copy">
    <p class="qa-eyebrow">N+1 AND QUERY REGRESSIONS FOR JUNIT 5</p>
    <h1>Catch N+1 queries<br>before merge.</h1>
    <p class="qa-hero__lead">
      QueryAudit reports an N+1 at the line that issues it, then locks the fixed
      query counts into contracts that every pull request has to keep.
    </p>
    <div class="qa-hero__actions">
      <a href="getting-started/installation/" class="md-button md-button--primary">Add to my tests →</a>
      <a href="getting-started/quickstart/" class="md-button">Run the example</a>
    </div>
    <p class="qa-hero__note">Java 17+ · JUnit 5 · Test dependencies only</p>
  </div>
  <div class="qa-terminal" aria-label="An N+1 fails a test at its call site, then the fixed test passes">
    <div class="qa-terminal__bar">
      <span>A loop loads one customer per order</span>
      <span class="qa-terminal__dots" aria-hidden="true">● ● ●</span>
    </div>
    <pre><code>@Test @QueryAudit
void listsOrderSummaries() { … }

<span class="qa-terminal__error">[ERROR] N+1 Query detected (table: customers)
  The same SELECT ran 5 times from one call site
  at OrderService.recentOrderSummaries:42</span>

<span class="qa-terminal__muted">Fetch the customers once. Run the test again.</span>

<span class="qa-terminal__success">"outcome": "PASS"</span></code></pre>
    <div class="qa-terminal__footer">N+1 at its call site → passing JSON outcome</div>
  </div>
</div>

<div class="qa-workflow-nav" markdown>

[**01** Find an N+1](#find-an-n1)
[**02** Lock the fix](#lock-the-fix)
[**03** Gate the pull request](#gate-the-pull-request)
[**04** Used on a production service](#used-on-a-production-service)

</div>

## Find an N+1

After [installing the starter](getting-started/installation.md), add `@QueryAudit` to a test that
reads related data:

```java
@SpringBootTest
@QueryAudit
class OrderServiceTest {
    @Autowired OrderService orderService;

    @Test
    void listsOrderSummaries() {
        assertEquals(5, orderService.recentOrderSummaries().size());
    }
}
```

With no configuration, one rule runs: the same SELECT with different values, three or more times,
from one full application call stack. If `recentOrderSummaries()` loads each order's customer inside its loop,
the test fails at that line:

```text
QueryAudit detected 1 issue(s) in listsOrderSummaries():

  [ERROR] N+1 Query detected (table: customers)
    Detail: The same SELECT ran 5 times from one call site
    Suggestion: Load the rows once before the loop: JOIN FETCH, @EntityGraph, or one query with an IN list.
    Call stack:
      at com.example.order.OrderService.recentOrderSummaries:42
      at com.example.order.OrderServiceTest.listsOrderSummaries:18
```

A batched `IN (?, ?, ...)` fetch is the fix, not the problem, so `@BatchSize` and batch fetching
stay quiet. Paging with `OFFSET` is not an N+1, and repeating a lookup with the same values is
reported as INFO without failing the test.
Hibernate lazy-load events add an INFO line that names the association to fetch.
Use `@EnableQueryInspector` instead of `@QueryAudit` to survey an existing suite without failing it.

[How N+1 detection works →](detections/n-plus-one.md)
· [Read SQL and call sites](guide/reports.md)

## Lock the fix

**A budget** is a limit you write on the test. At most two SELECTs and no writes:

```java
@Test
@ExpectQueries(select = 2, insert = 0, update = 0, delete = 0)
void listsOrderSummaries() {
    assertEquals(5, orderService.recentOrderSummaries().size());
}
```

If the loop comes back, the test fails even though the returned data is still correct:

```text
SELECT: executed 6, expected at most 2.
```

**A contract** is a count QueryAudit records for you. `QueryContractScope` counts only the request
or job you hand it, not fixture setup or assertions, and waits for the thread pools you name:

```yaml
query-audit:
  await-executors: [taskExecutor]
  contracts:
    path: src/test/resources/query-contracts
```

```java
@Autowired QueryContractScope contracts;

@Test
void listsLinks() throws Exception {
    contracts.verify("link-list", () -> mockMvc.perform(get("/api/v1/links")))
        .andExpect(status().isOk());
}
```

Record once with `-DqueryAudit.contracts.record=true`, commit the file, and review every later
change as one line in the pull request:

```diff
-@junit | link-list | 2 | 0 | 0 | 0 | 2
+@junit | link-list | 3 | 0 | 0 | 0 | 3
```

Without the re-record, the test fails with the delta and the SQL that grew. Whole test methods
can keep contracts in the same file.

**Try a budget failure** with the published library and in-memory H2:

```sh
git clone https://github.com/haroya01/query-audit.git
cd query-audit
./gradlew -p examples/first-audit test -PextraWrite=true --rerun-tasks
```

```text
UPDATE: executed 1, expected at most 0.
```

Rerun without `-PextraWrite=true` and the test passes.

[Lock a fixed path →](guide/choose-your-workflow.md)
· [Contracts](guide/contracts.md)
· [Quick start](getting-started/quickstart.md)

## Gate the pull request

Save the JSON report from the base branch and from the pull request, then compare them with the
matching core JAR:

```sh
java -cp "$QUERY_AUDIT_CORE_JAR" \
  io.queryaudit.core.reporter.ReportComparator before.json after.json verdict.json
```

| The pull request… | Result |
| --- | --- |
| adds no confirmed finding and keeps every budget and contract | `PASS` |
| breaks a budget or a contract | `FAIL` |
| skips or loses a test listed in the [expected-test manifest](guide/audit-coverage.md) | `INCONCLUSIVE` |
| changes rules, thresholds, or required analysis inputs | `INCONCLUSIVE` |

Add `--require-resolved <findingId>` to prove that one specific finding is gone.

[Set up the first CI check →](guide/first-ci-check.md)
· [Compare two runs](guide/ci-cd.md)
· [Comparison inputs](guide/comparison-inputs.md)

## Used on a production service

QueryAudit is dogfooded on [short-link](https://github.com/haroya01/short-link), a production
URL shortener built with Spring Boot and MySQL. Its test suite is the acceptance test for 0.7:

- On the same 45 audited tests, the default confirmed findings went from 142 under 0.6.0 to one
  N+1: bulk link creation looks up each new code with `findByShortCode`. The same loop repeats
  `countByUserId` for one user, which is reported as INFO. 0.6.0 had reported neither.
- All 584 HTTP query contracts kept the same counts after the move from a hand-written helper to
  `QueryContractScope`, which needs no internal QueryAudit class.
- One injected extra SELECT in link creation failed 13 contracts across 8 test classes, and each
  failure listed the repeated statement with its call site.

---

**Need more than N+1?** Index, `EXPLAIN`, and SQL style rules turn on with `profile: strict` or
`enabled-rules`. [Optional rules](detections/overview.md)

**Check your setup:** [Spring Boot](getting-started/spring-boot.md)
· [Plain JUnit](getting-started/installation.md#plain-junit-5)
· [QuickPerf comparison](guide/coming-from-quickperf.md)
· [Versions](getting-started/versions.md)
· [Known limitations](guide/limitations.md)
