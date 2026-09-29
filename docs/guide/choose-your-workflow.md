---
title: Choose Your Workflow
description: Keep reads free of writes, review query-count changes, locate failing SQL, and verify CI comparisons.
---

# Choose Your Workflow

Add a read/write budget to one existing database test, then keep its policy reviewable as the
suite grows.

| Use it to… | Add or run | Result |
|---|---|---|
| Keep reads free of writes | `@ExpectQueries(select = 2, insert = 0, update = 0, delete = 0)` | More than two SELECTs or any listed write fails the test. [Example below](#protect-a-read-path) |
| Review per-test count changes in a PR | Record `.query-audit-contracts`, rerun the suite, and review intentional updates | Changed SELECT/INSERT/UPDATE/DELETE counts fail until the contract is updated. [Contracts](contracts.md) |
| Locate the SQL behind a failure | Read the budget or contract diagnostic | Counts, SQL, and an available captured call site identify the access to investigate. [Reports](reports.md#read-a-policy-failure) |
| Keep missing tests or changed settings from looking like a fix | Commit an expected-test manifest and compare two JSON reports | Missing audit evidence or incompatible settings produce `INCONCLUSIVE`. [Coverage](audit-coverage.md) · [Comparison](reports.md#delta-verdict-compare-two-runs) |

The [quick start](../getting-started/quickstart.md) sets up capture. Check
[versions](../getting-started/versions.md) before using report or comparison features.

## Protect a read path

On an existing Spring Boot database test, use a class-level inspector and put the budget on the test
method. The example below assumes the test already has an injected `orderService` and fixtures:

```java
@SpringBootTest
@EnableQueryInspector
class OrderServiceQueryTest {
    // Existing orderService injection and test fixtures.

    @Test
    @ExpectQueries(select = 2, insert = 0, update = 0, delete = 0)
    void recentOrdersStayWithinBudget() {
        var orders = orderService.findRecentOrders();
        assertFalse(orders.isEmpty());
        orders.forEach(order -> assertFalse(order.getItems().isEmpty()));
    }
}
```

Use `io.queryaudit.junit5.EnableQueryInspector` and `io.queryaudit.junit5.ExpectQueries`; the
assertions are from `org.junit.jupiter.api.Assertions`. Keep association access inside the
transaction when the mapping requires it. Calling the repository without exercising the data the
application later reads may leave lazy queries unobserved. Flush pending JPA changes inside the
test transaction so writes execute in the audited window.

If the read path starts issuing an UPDATE, the budget diagnostic includes the statement.
Example excerpt:

```text
QueryAudit: recentOrdersStayWithinBudget() exceeded its query budget.
UPDATE: executed 1, expected at most 0.
  update orders set last_viewed_at = ? where id = ?
```

Inspect the report's JSON `stackTrace` for the application caller. A console diagnostic can show a
JDBC proxy as its first frame; the [runnable example's SQL evidence](reports.md#read-a-policy-failure)
shows where to find the read path that issued the write.

The limits are upper bounds. `select = 2` permits zero, one, or two SELECTs; it does not require
exactly two. `0` forbids that statement type, and an omitted attribute does not restrict it. The
budget counts captured statements, not returned rows or elapsed time.

Detected findings remain advisory under `@EnableQueryInspector`, while the budget is an assertion.
Once you have reviewed a finding and want to enforce it, choose the appropriate
[auditing annotation](annotations.md). If you need a complete test without application-specific
types, use the [capture smoke test](first-ci-check.md#1-verify-capture-with-one-small-test).

## Move from local feedback to CI

Use the [first CI check](first-ci-check.md) for one operation. Expand to
[expected-test coverage](audit-coverage.md) when the job must prove that a specific set of tests was
audited. Add [report comparison](reports.md#delta-verdict-compare-two-runs) when reviewers need to
distinguish new, resolved, and persisting findings across runs.

A finding baseline, a query-count baseline, and a snapshot contract serve different purposes. The
[CI adoption table](ci-cd.md#gradual-adoption) helps choose one without silently accepting a new
query budget.

## Inspect findings when a policy changes

| Investigation | Start here | What to check |
|---|---|---|
| Repeated reads or a possible N+1 | Use `@EnableQueryInspector` and exercise the relevant associations. [N+1 guide](../detections/n-plus-one.md) | Captured SQL, Hibernate evidence, batching, and fetch strategy |
| A possible missing index | Install the module for the test database. [Missing indexes](../detections/missing-index.md) | Existing composite indexes, representative queries, and the execution plan |
| Existing findings during adoption | Review and acknowledge specific findings. [Suppressing findings](suppressing.md) | A finding baseline does not change inline budgets or snapshot contracts |

Run the smallest test that reproduces the access pattern and read its SQL, call site, and available
database evidence. Hibernate events and database metadata have different requirements; the
[detection overview](../detections/overview.md) explains which inputs each check uses.

For N+1 investigation, include several related entities and exercise the association access that
the application performs. Compare the SQL before and after a fetch or batching change. For an
index finding, use the same database family and relevant schema as the application, then inspect
the existing composite indexes and query plan before adding an index. A finding is a reason to
investigate the access path, not proof that a suggested schema change improves production latency.
