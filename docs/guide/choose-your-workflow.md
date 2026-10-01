---
title: Lock a Fixed Path
description: Find an N+1 in an existing suite, lock the fixed path with a budget or a contract, and gate it in CI.
---

# Lock a Fixed Path

QueryAudit works in three steps: find an N+1, lock the fix, and gate the pull request. This page
applies them to an existing Spring Boot suite.

| To keep… | Add | The test fails when |
|---|---|---|
| A fixed N+1 from returning | `@QueryAudit` on the class | the same SELECT runs three or more times from one call site with different values |
| A read path bounded and free of writes | `@ExpectQueries(select = 2, insert = 0, update = 0, delete = 0)` | it runs more SELECTs than the limit or any listed write |
| A request's or job's exact counts | [`QueryContractScope`](contracts.md#contract-a-request-or-job) and a recorded contract | any count changes, in either direction, until it is re-recorded |
| Every audited test's exact counts | [Record mode](contracts.md#recording) across the suite | any recorded test's counts change |

## Find the N+1

Survey before you enforce. `@EnableQueryInspector` reports findings without failing tests, while
explicit budgets and contracts still fail:

```java
@SpringBootTest
@EnableQueryInspector
class OrderServiceQueryTest {
    // Existing orderService injection and test fixtures.

    @Test
    void recentOrders() {
        var orders = orderService.findRecentOrders();
        orders.forEach(order -> assertFalse(order.getItems().isEmpty()));
    }
}
```

A confirmed `n-plus-one` finding names the repeated SELECT and the call stack that issued it.
Include several related entities and exercise the association access the application performs;
with one row, a loop cannot repeat. Keep association access inside the transaction when the
mapping requires it, and flush pending JPA changes so writes execute in the audited window.
[How N+1 detection works](../detections/n-plus-one.md) · [Read SQL and call sites](reports.md)

After the fix, switch the class to `@QueryAudit` so the finding fails the test if it returns.
With optional rules enabled, `@QueryAudit(failOn = IssueType.N_PLUS_ONE)` fails on N+1 alone.

## Budget a read path

A budget is a limit you write on the test method:

```java
@Test
@ExpectQueries(select = 2, insert = 0, update = 0, delete = 0)
void recentOrdersStayWithinBudget() {
    var orders = orderService.findRecentOrders();
    assertFalse(orders.isEmpty());
    orders.forEach(order -> assertFalse(order.getItems().isEmpty()));
}
```

Use `io.queryaudit.junit5.EnableQueryInspector` and `io.queryaudit.junit5.ExpectQueries`; the
assertions are from `org.junit.jupiter.api.Assertions`. If the read path starts issuing an UPDATE,
the diagnostic includes the statement. Example excerpt:

```text
QueryAudit: recentOrdersStayWithinBudget() exceeded its query budget.
UPDATE: executed 1, expected at most 0.
  update orders set last_viewed_at = ? where id = ?
```

The limits are upper bounds. `select = 2` permits zero, one, or two SELECTs; it does not require
exactly two. `0` forbids that statement type, and an omitted attribute does not restrict it.
`total` limits all statements together. The budget counts captured statements, not returned rows
or elapsed time.

## Contract a request or job

A contract is a count QueryAudit records for you. Compared with a budget:

| | Budget | Contract |
|---|---|---|
| Who writes the number | You, in the annotation | Record mode, in a committed file |
| What fails | Going over the limit | Any change, up or down |
| Scope | One test method | A test method, a request, a job, or a journey |
| Fits | A few rules such as "no writes on this path" | Tracking hundreds of requests and reviewing every change |

A test method often mixes fixture setup, the request under test, and assertions.
`QueryContractScope` counts only the work you hand it:

```java
@Autowired QueryContractScope contracts;

@Test
void listsLinks() throws Exception {
    contracts.verify("link-list", () -> mockMvc.perform(get("/api/v1/links")))
        .andExpect(status().isOk());
}
```

Record with `-DqueryAudit.contracts.record=true` (Maven) or `-PqueryAudit.contracts.record=true`
(Gradle with the [property bridge](ci-cd.md#plain-junit-build-tool-setup)), commit the file, and
review later changes in its diff. A scope without a recorded contract fails with the line to add.
[Contracts](contracts.md)

## Move to CI

Use the [first CI check](first-ci-check.md) for one operation. Expand to
[expected-test coverage](audit-coverage.md) when the job must prove that a specific set of tests was
audited. Add [report comparison](reports.md#delta-verdict-compare-two-runs) when reviewers need to
distinguish new, resolved, and persisting findings across runs.

A finding baseline and a contract serve different purposes. The
[CI adoption table](ci-cd.md#gradual-adoption) helps choose one without silently accepting a new
query count.

## Investigate optional findings

The default profile runs only the N+1 rule. If you enable [optional rules](../detections/overview.md),
check each finding against real evidence before you enforce it:

| Investigation | Start here | What to check |
|---|---|---|
| A possible missing index | Install the module for the test database. [Missing indexes](../detections/missing-index.md) | Existing composite indexes, representative queries, and the execution plan |
| Existing findings during adoption | Review and acknowledge specific findings. [Suppressing findings](suppressing.md) | A finding baseline does not change budgets or contracts |

For an index finding, use the same database family and relevant schema as the application, then
inspect the existing composite indexes and query plan before adding an index. A finding is a reason
to investigate the access path, not proof that a suggested schema change improves production
latency.
