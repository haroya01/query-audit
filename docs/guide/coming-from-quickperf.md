---
title: Coming from QuickPerf
description: Preserve existing count assertions and add reviewed contracts, SQL evidence, and complete CI comparisons.
---

# Coming from QuickPerf

Keep the meaning of your existing query assertions, then choose whether the suite needs
reviewed count contracts and CI comparison. QuickPerf already supports SQL count checks.
Use these QueryAudit workflows as needed:

| Workflow | Use | Result |
|---|---|---|
| Forbid writes in a read test | `@ExpectQueries(insert = 0, update = 0, delete = 0)` | A captured INSERT, UPDATE, or DELETE fails the test |
| Review exact per-test count changes | [Snapshot contracts](contracts.md) | A count change requires a reviewed contract update |
| Investigate a failure | [Budget and contract diagnostics](reports.md#read-a-policy-failure) | Count delta, captured SQL, and an available call site |
| Verify CI comparisons | [Expected-test manifest](audit-coverage.md) and [comparison inputs](comparison-inputs.md) | Missing audits and incompatible settings produce `INCONCLUSIVE` |

Try one existing SQL test before applying the setup across the suite.

## Choose by testing goal

| Your goal | QuickPerf approach | QueryAudit approach |
|---|---|---|
| Allow at most two SELECTs | `@ExpectMaxSelect(2)` | `@ExpectQueries(select = 2)` |
| Require exactly two SELECTs | `@ExpectSelect(2)` | Keep the exact assertion, or deliberately adopt a [snapshot contract](contracts.md) for all recorded SELECT/INSERT/UPDATE/DELETE counts. `@ExpectQueries(select = 2)` permits fewer SELECTs |
| Forbid INSERT, UPDATE, and DELETE | Zero expectations for each write type | `@ExpectQueries(insert = 0, update = 0, delete = 0)` |
| Investigate repeated SELECTs or N+1 | SQL counts and repeated-SELECT checks | Inspect findings first with `@EnableQueryInspector`; verify the query shape before enabling a finding gate |
| Measure heap allocation or profile the JVM | Heap/JVM annotations and JFR support | Keep a JVM measurement tool; QueryAudit does not provide these measurements |

QuickPerf documents exact and maximum counts separately in its
[SQL annotation guide](https://github.com/quick-perf/doc/wiki/SQL-annotations), and its
[JVM guide](https://github.com/quick-perf/doc/wiki/JVM-annotations) covers allocation and profiling.
The table maps testing goals; it does not promise equivalent capture boundaries or detection
behavior across the two libraries.

## Preserve the budget's meaning

This QueryAudit declaration allows zero, one, or two SELECTs, forbids the three listed write types,
and does not require any SQL to run:

```java
@Test
@ExpectQueries(select = 2, insert = 0, update = 0, delete = 0)
void readsOrdersWithinBudget() {
    var orders = orderService.findRecentOrders();
    assertFalse(orders.isEmpty());
}
```

Each field is an independent **upper bound**. An omitted field defaults to `-1` and is not checked;
negative values mean unspecified. For example, `@ExpectQueries(select = 2)` alone permits writes.
Use explicit zeros to forbid them, and keep ordinary assertions that verify the returned result.

QueryAudit's inline budgets count captured setup and teardown SQL too. Its general finding analysis
uses test-body SQL by default. Move fixture creation outside the counted window only when that is
the contract you intend; otherwise budget for it. A recorded snapshot checks SELECT/INSERT/UPDATE/DELETE
counts in both directions. It does not snapshot SQL text, DDL, or result rows. A test with no recorded
entry has no snapshot contract to enforce, and an inline `@ExpectQueries` takes precedence over that method's snapshot.
See [annotations](annotations.md) and [snapshot contracts](contracts.md).

## Try one test

1. Follow the [quick start](../getting-started/quickstart.md) to add the test dependency and verify
   one captured query. Check the [supported version](../getting-started/versions.md) before adopting
   the setup across the suite.
2. Use QueryAudit's capture setup for the selected test. If you already have a datasource-proxy
   bean, follow the [existing-proxy setup](../getting-started/spring-boot.md#reuse-an-existing-datasource-proxy).
   Running both libraries on the same test is not a verified migration path.
3. Begin with `@EnableQueryInspector`, check the reported query count against the actual work, then
   add the chosen budget. Temporarily lower the budget to confirm that it fails and restore it.
4. Compare one extra-query change and one fewer-query change. Confirm that the outcomes match
   your chosen upper-bound or exact-profile contract before applying it to more tests.

Report-only inspection leaves findings non-fatal. Explicit budgets and setup/reporting failures can
still fail the test. Check [limitations](limitations.md) before enabling Hibernate batch-fetch,
context-refresh, or execution-plan findings as gates.

## Add a reviewable CI comparison

Once the contract works locally, QueryAudit can produce a JSON artifact and compare a baseline run
with a candidate run. This shows new, resolved, and persisting findings together
with audit completeness and compatible inputs. A failed or incomplete candidate cannot produce a
successful comparison solely because it has no new findings. Total query-count deltas are
informational in this comparison; keep budgets or contracts to enforce counts.

Use the [CI setup](ci-cd.md), [report contract](reports.md), and
[expected-test coverage](audit-coverage.md) guides for this workflow. An HTML page or a green test
body alone is not proof that all intended tests were audited. Keep both runs on the same declared
audit scope, configuration, and compatible report version.
