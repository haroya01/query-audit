---
title: QueryAudit — query policies for JUnit 5
description: Review query-count contracts across JUnit tests and compare CI runs with checks for missing audits and changed analysis settings.
hide:
  - navigation
  - toc
---

<div class="qa-hero">
  <div class="qa-hero__copy">
    <p class="qa-eyebrow">QUERY POLICIES FOR JUNIT 5</p>
    <h1>Keep query changes<br>under test.</h1>
    <p class="qa-hero__lead">
      Review query-count contracts across your tests. Compare CI runs with checks
      for missing audits and changed analysis settings.
    </p>
    <div class="qa-hero__actions">
      <a href="getting-started/quickstart/" class="md-button md-button--primary">Run the example →</a>
      <a href="getting-started/installation/" class="md-button">Add to my tests</a>
    </div>
    <p class="qa-hero__note">Java 17+ · JUnit 5 · Test dependencies only</p>
  </div>
  <div class="qa-terminal" aria-label="An unexpected UPDATE fails a read-only query budget">
    <div class="qa-terminal__bar">
      <span>A read path adds a write</span>
      <span class="qa-terminal__dots" aria-hidden="true">● ● ●</span>
    </div>
    <pre><code>@ExpectQueries(
    select = 1,
    insert = 0, update = 0, delete = 0)

<span class="qa-terminal__error">UPDATE: executed 1, expected at most 0.</span>

<span class="qa-terminal__muted">Remove the write. Run the test again.</span>

<span class="qa-terminal__success">"outcome": "PASS"</span></code></pre>
    <div class="qa-terminal__footer">Budget failure → passing JSON outcome</div>
  </div>
</div>

<div class="qa-workflow-nav" markdown>

[**01** Keep reads free of writes](#keep-reads-free-of-writes)
[**02** Review count changes](#review-count-changes)
[**03** Find the SQL and call site](#find-the-sql-and-call-site)
[**04** Compare complete CI runs](#compare-complete-ci-runs)

</div>

## Keep reads free of writes

After [enabling capture](getting-started/installation.md), add a budget to your test:

```java
@Test
@ExpectQueries(select = 2, insert = 0, update = 0, delete = 0)
void loadsOrders() {
    var orders = orderService.findRecentOrders();
    assertEquals(3, orders.size());
}
```

At most two SELECTs and no INSERT, UPDATE, or DELETE. Omitted fields are unchecked.

**Try the unexpected-write failure** with the published library and in-memory H2:

```sh
git clone https://github.com/haroya01/query-audit.git
cd query-audit
./gradlew -p examples/first-audit test -PextraWrite=true --rerun-tasks
```

```text
UPDATE: executed 1, expected at most 0.
```

Remove the write by rerunning without `-PextraWrite=true`: the test passes and the JSON
outcome becomes `PASS`. Use `-PextraQuery=true` to try the extra-SELECT failure.

[Copy the complete test and commands →](getting-started/quickstart.md)

## Review count changes

In your own Maven project with capture enabled, record SELECT/INSERT/UPDATE/DELETE counts
for tests without an inline budget:

```sh
mvn test -DqueryAudit.contracts.record=true
```

Commit `.query-audit-contracts`. On the next test run, a changed count fails:

```text
QueryAudit: placeOrder() deviates from its recorded query contract (.query-audit-contracts).
  INSERT: contract 1, executed 3 (+2)
```

For an intended change, re-record and review the contract diff with the code.
The contract checks counts in both directions; keep ordinary assertions for results and affected rows.

[Record contracts with Maven or Gradle →](guide/contracts.md)

## Find the SQL and call site

Select JSON in your Spring test configuration, then rerun the failing test:

```yaml
query-audit:
  auto-open-report: false
  report:
    format: json
```

Inspect each captured statement and its application frames:

```sh
jq '.reports[].queries[] | {sql, stackTrace}' \
  build/reports/query-audit/report.json
```

The example's unexpected write produces this evidence (excerpt; line numbers can change):

```json
{
  "sql": "UPDATE orders SET status = ? WHERE id = ?",
  "stackTrace": "example.audit.FirstAuditTest.writeOnReadPath:43\nexample.audit.FirstAuditTest.readsOnce:26"
}
```

For the example, use `examples/first-audit/build/reports/query-audit/report.json`.

[Read the example's SQL evidence →](getting-started/quickstart.md)
· [Report fields](guide/reports.md)

## Compare complete CI runs

Save baseline and candidate reports, then compare with the matching core JAR:

```sh
java -cp "$QUERY_AUDIT_CORE_JAR" \
  io.queryaudit.core.reporter.ReportComparator before.json after.json verdict.json
```

Set `QUERY_AUDIT_CORE_JAR` to your downloaded `query-audit-core` JAR path.
With an [expected-test manifest](guide/audit-coverage.md), CI can distinguish a fix from a missing test:

| What changed? | Comparison result |
| --- | --- |
| A finding was fixed; the same tests ran with compatible inputs | `PASS`, with the finding in `resolved` if no other check fails |
| A query budget or recorded contract failed | `FAIL` |
| An expected test was skipped or lost its audit | `INCONCLUSIVE` |
| A rule, threshold, or required analysis input changed | `INCONCLUSIVE` |

Require both JUnit and the audit verdict to pass. Enforce counts with budgets or contracts;
a count delta in the comparison is only a summary.

[Set up the first CI check →](guide/first-ci-check.md)
· [Compare two runs](guide/ci-cd.md)
· [Input compatibility](guide/comparison-inputs.md)

---

**Need query advice?** N+1, SQL, and index findings provide supporting evidence.
[Look up a finding](detections/overview.md).

**Check your setup:** [Spring Boot](getting-started/spring-boot.md)
· [Plain JUnit](getting-started/installation.md#plain-junit-5)
· [QuickPerf comparison](guide/coming-from-quickperf.md)
· [Versions](getting-started/versions.md)
· [Known limitations](guide/limitations.md)
