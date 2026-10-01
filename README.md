# QueryAudit

**Catch N+1 queries and query regressions in JUnit 5 tests before merge.**

[![Build](https://github.com/haroya01/query-audit/actions/workflows/ci.yml/badge.svg)](https://github.com/haroya01/query-audit/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.haroya01/query-audit-core)](https://central.sonatype.com/artifact/io.github.haroya01/query-audit-core)
[![Java 17+](https://img.shields.io/badge/Java-17%2B-blue)](https://openjdk.org/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

QueryAudit watches the SQL your existing database tests run. It reports an N+1 at the line that
issues it. Once you fix a path, it records that path's query counts as a contract, so a later
change that adds a query fails the pull request instead of reaching production.

1. **Find** an N+1. With no configuration, one rule runs: the same SELECT, three or more times,
   from one call site.
2. **Lock** the fix. Put a budget on a test, or record the query counts of a test, an HTTP
   request, or a job in a contract file.
3. **Gate** the pull request. CI compares two runs and reports `PASS`, `FAIL`, or `INCONCLUSIVE`.

## Install

Add the Spring Boot starter to your test dependencies. It wraps the `DataSource` your tests
already use.

```kotlin
dependencies {
    testImplementation("io.github.haroya01:query-audit-spring-boot-starter:0.6.1") // x-release-please-version
}
```

Maven, plain JUnit 5, and a capture check are in the
[installation guide](docs/getting-started/installation.md).

## 1. Find an N+1

Add `@QueryAudit` to a test that reads related data:

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

If `recentOrderSummaries()` loads each order's customer inside its loop, the test fails:

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
stay quiet. When Hibernate is present, its lazy-load events add an INFO line that names the
association to fetch. To survey an existing suite without failing it, use `@EnableQueryInspector`
instead of `@QueryAudit`.

Index, `EXPLAIN`, and SQL style rules are still available. Turn them on with `profile: strict` or
`enabled-rules`; see [rule profiles](docs/guide/configuration.md#rule-profiles).

## 2. Lock the fix

**A budget** is a limit you write on the test:

```java
@Test
@ExpectQueries(select = 2, insert = 0, update = 0, delete = 0)
void listsOrderSummaries() {
    assertEquals(5, orderService.recentOrderSummaries().size());
}
```

At most two SELECTs and no writes. If the loop comes back, the test fails even though the
returned data is still correct:

```text
SELECT: executed 6, expected at most 2.
```

**A contract** is a count QueryAudit records for you. A test method often mixes fixture setup,
the request under test, and assertions, so `QueryContractScope` counts only the work you hand it.
It waits for the thread pools you name, so background work the request triggers is counted too.
The Spring Boot starter provides it:

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

Record once with `-DqueryAudit.contracts.record=true` (Maven) or
`-PqueryAudit.contracts.record=true` (Gradle with the
[property bridge](docs/guide/ci-cd.md#plain-junit-build-tool-setup)), then commit the file. A pull
request that changes a count must re-record it, and the reviewer sees the change as one line:

```diff
-@junit | link-list | 2 | 0 | 0 | 0 | 2
+@junit | link-list | 3 | 0 | 0 | 0 | 3
```

Without the re-record, the test fails with the delta and the SQL that grew. Contracts compare
counts in both directions, and the same file also holds contracts for whole test methods.
Use `contracts.open("signup-journey")` in a `try` block to cover several requests.
[Contracts guide](docs/guide/contracts.md)

**Try a budget failure in a minute** with the published library and in-memory H2:

```sh
git clone https://github.com/haroya01/query-audit.git
cd query-audit
./gradlew -p examples/first-audit test -PextraWrite=true --rerun-tasks
```

The read path writes one row, so the budget fails with `UPDATE: executed 1, expected at most 0.`
Run it again without `-PextraWrite=true` and it passes. [Quick start](docs/getting-started/quickstart.md)

## 3. Gate the pull request

Save the JSON report from the base branch and from the pull request, then compare them with the
matching core JAR:

```sh
java -cp "$QUERY_AUDIT_CORE_JAR" \
  io.queryaudit.core.reporter.ReportComparator before.json after.json verdict.json
```

```text
[QueryAudit] compare: PASS; 0 new, 1 resolved, 0 persisting; queries 11 -> 7
```

| The pull request… | Result |
| --- | --- |
| adds no confirmed finding and keeps every budget and contract | `PASS` |
| breaks a budget or a contract | `FAIL` |
| skips or loses an expected test | `INCONCLUSIVE` |
| changes rules, thresholds, or required analysis inputs | `INCONCLUSIVE` |

A changed setting never looks like a fix, and neither does a missing test once you list the tests
you expect. Add `--require-resolved <findingId>` to prove that one specific finding is gone.
[First CI check](docs/guide/first-ci-check.md)
· [Expected tests](docs/guide/audit-coverage.md)
· [Comparison inputs](docs/guide/comparison-inputs.md)

## Used on a production service

QueryAudit is dogfooded on [short-link](https://github.com/haroya01/short-link), a production
URL shortener built with Spring Boot and MySQL. Its test suite was the acceptance test for 0.7.0:

- On the same 45 audited tests, the default findings went from 142 under 0.6.0 to 2 under 0.7.0.
  Both are real per-link lookups repeated inside bulk link creation, and 0.6.0 had reported
  neither as a confirmed finding.
- All 584 HTTP query contracts kept the same counts after the move from a hand-written helper to
  `QueryContractScope`, which needs no internal QueryAudit class.
- One injected extra SELECT in link creation failed 13 contracts across 8 test classes, and each
  failure listed the repeated statement with its call site.

## Supported scope

Java 17+, JUnit 5, and database-backed tests. QueryAudit checks the paths your tests exercise;
keep representative fixtures and ordinary assertions.

| CI check | Tested combination |
| --- | --- |
| Build and regular tests | Java 17 / 21; Spring Boot 3.4.1 |
| Boot 4 lifecycle suite | Java 17 / 21; Spring Boot 4.0.6 |
| MySQL integration | Java 21; MySQL 8.0 |
| PostgreSQL integration | Java 21; PostgreSQL 16 |

MySQL and PostgreSQL modules add index metadata for the optional index rules.
See [versions](docs/getting-started/versions.md) and [known limitations](docs/guide/limitations.md).

[Documentation](https://haroya01.github.io/query-audit/)
· [Coming from QuickPerf](docs/guide/coming-from-quickperf.md)
· [Contributing](CONTRIBUTING.md)
· [Apache 2.0 license](LICENSE)
