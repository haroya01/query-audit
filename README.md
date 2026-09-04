# QueryAudit

**Make database behavior part of your test contract.**

[![Build](https://github.com/haroya01/query-audit/actions/workflows/ci.yml/badge.svg)](https://github.com/haroya01/query-audit/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.haroya01/query-audit-core)](https://central.sonatype.com/artifact/io.github.haroya01/query-audit-core)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Documentation](https://img.shields.io/badge/docs-haroya01.github.io-blue)](https://haroya01.github.io/query-audit)
[![Java 17+](https://img.shields.io/badge/Java-17%2B-blue)](https://openjdk.org/)

QueryAudit observes SQL executed by database-backed JUnit 5 tests. It can report or fail a test on
N+1 patterns, missing-index findings when database metadata is available, and unsafe DML. Explicit
query budgets enforce upper bounds on reads and writes. With test-scoped dependencies, its
instrumentation stays on the test runtime path.

**Java 17+ · JUnit 5 · Spring Boot · MySQL · PostgreSQL**

See [CI-verified compatibility](#compatibility-tested-by-this-project) for the exact tested versions.

[**Run the 5-minute quick start**](https://haroya01.github.io/query-audit/getting-started/quickstart/)
· [Choose an installation](https://haroya01.github.io/query-audit/getting-started/installation/)
· [Browse detection rules](https://haroya01.github.io/query-audit/detections/overview/)
· [Add QueryAudit to CI](https://haroya01.github.io/query-audit/guide/ci-cd/)

The dependency coordinates below use the current Maven Central release. Features labeled 0.6.0+
require QueryAudit 0.6.0 or later.

## See the evidence in the test output

A finding stays attached to the test and carries the context available to that check. An
abbreviated Hibernate N+1 failure looks like this:

```text
QueryAudit detected 1 issue(s) in findOrdersWithItems:

  [ERROR] N+1 Query detected (table: items)
    Detail: Lazy collection 'items' on Order initialized 5 times for 5 different entities
    Suggestion: Use @EntityGraph, JOIN FETCH, or @BatchSize
```

Reports may also include the normalized SQL, application call site, table, column, index context,
and a suggested fix. The available evidence depends on the check and the test environment.

## Try it on one Spring Boot test

Add the starter and the module for the database used by the test. PostgreSQL users can replace
`query-audit-mysql` with `query-audit-postgresql`.

<details open>
<summary>Gradle</summary>

```groovy
dependencies {
    testImplementation 'io.github.haroya01:query-audit-spring-boot-starter:0.6.0' // x-release-please-version
    testImplementation 'io.github.haroya01:query-audit-mysql:0.6.0' // x-release-please-version
}
```

</details>

<details>
<summary>Maven</summary>

```xml
<dependencies>
    <dependency>
        <groupId>io.github.haroya01</groupId>
        <artifactId>query-audit-spring-boot-starter</artifactId>
        <version>0.6.0</version> <!-- x-release-please-version -->
        <scope>test</scope>
    </dependency>
    <dependency>
        <groupId>io.github.haroya01</groupId>
        <artifactId>query-audit-mysql</artifactId>
        <version>0.6.0</version> <!-- x-release-please-version -->
        <scope>test</scope>
    </dependency>
</dependencies>
```

</details>

Start in report-only mode. QueryAudit captures SQL and prints findings without making those
findings fail the test:

```java
@SpringBootTest
@EnableQueryInspector
class OrderServiceQueryTest {

    @Autowired
    private OrderService orderService;

    @Test
    void loadsOrdersWithItems() {
        List<Order> orders = orderService.findRecentOrders();
        orders.forEach(order -> assertThat(order.getItems()).isNotEmpty());
    }
}
```

Run only that test and read the console report:

```bash
./gradlew test --tests OrderServiceQueryTest
# or: mvn -Dtest=OrderServiceQueryTest test
```

Once the result makes sense, replace `@EnableQueryInspector` with `@QueryAudit` to fail on
configured findings. Add an explicit budget when upper bounds on reads and writes are part of the
contract:

```java
@Test
@ExpectQueries(select = 2, insert = 0, update = 0, delete = 0)
void loadsOrdersWithItemsWithinBudget() {
    orderService.findRecentOrdersWithItems();
}
```

Using plain JUnit 5? The
[installation guide](https://haroya01.github.io/query-audit/getting-started/installation/#plain-junit-5)
shows the portable `DataSource` setup and a capture verification test.

## What you can put under test

| Workflow | What QueryAudit provides |
|---|---|
| Query review | More than 60 finding types for repeated access, missing or ineffective indexes, SQL and DML anti-patterns, locking, ORM behavior, and connection use |
| Database context | MySQL `SHOW INDEX` and PostgreSQL `pg_catalog` metadata for checks that need the real index state |
| Per-test budgets | A total cap with `@ExpectMaxQueryCount`, or separate SELECT, INSERT, UPDATE, and DELETE limits with `@ExpectQueries` |
| Snapshot contracts | A reviewable statement-count file for selected tests; 0.6.0+ also records zero-query expectations, and an inline `@ExpectQueries` budget takes precedence for that method |
| Test lifecycle | Test-body SQL by default, with an option to include setup and teardown SQL |
| Gradual adoption | Report-only mode, rule profiles, suppressions, and baselines for known findings |
| Review and automation | Console diagnostics, an HTML report, or schema-versioned JSON selected for the job at hand |

Availability depends on the SQL shape and whether Hibernate events, database metadata, or EXPLAIN
output are available. The [detection overview](https://haroya01.github.io/query-audit/detections/overview/)
explains those requirements.

## Use reports in a verification loop

**0.5.x:** QueryAudit writes both `index.html` and `report.json` under
`build/reports/query-audit/` after a session with at least one completed audited result. Its
comparator reports a finding delta; an empty delta alone does not prove that the two runs contain
the same tests.

**0.6.0+:** `query-audit.report.format` selects one suite report format. The default `console` mode
writes no file; choose `json` or `html`. JSON reports and comparisons carry `PASS`, `FAIL`, or
`INCONCLUSIVE`, and a failed or incomplete candidate cannot produce exit code `0`.

For a Spring Boot test on 0.6.0+, select JSON when CI or another tool needs a stable artifact:

```yaml
# src/test/resources/application.yml
query-audit:
  report:
    format: json
```

Run the same test command as before. QueryAudit writes one aggregate
`build/reports/query-audit/report.json`. Use its findings, query shapes, call sites, and available
database context to make a focused change, then rerun the same tests from a clean checkout. The
report comparator classifies confirmed findings as new, resolved, or persisting and refuses to
report success when the candidate audit is failed or incomplete.

```text
[QueryAudit] compare: PASS; 0 new, 1 resolved, 0 persisting; queries 11 -> 7
```

Set `format: html` when a browser artifact is more useful. A 0.6.0+ run selects one suite report
format; console diagnostics remain available in either case. Plain JUnit users can pass the
setting into the test JVM with the build-tool setup in the CI guide. Read the
[report contract](https://haroya01.github.io/query-audit/guide/reports/) for schema versions,
stable test identities, exit codes, and comparison rules, or the
[CI guide](https://haroya01.github.io/query-audit/guide/ci-cd/) for copy-ready pipelines.

## Compatibility tested by this project

The current `main` branch verifies these combinations in the
[pull-request CI workflow](https://github.com/haroya01/query-audit/blob/main/.github/workflows/ci.yml):

| CI coverage | Java | Verified version | Scope |
|---|---|---|---|
| Build and regular tests | 17 and 21 | Spring Boot 3.4.1 | Compilation and regular starter tests |
| Dedicated `boot4Test` suite | 17 and 21 | Spring Boot 4.0.6 | Multi-context lifecycle regression, included in `check` and therefore `build` |
| MySQL integration tests | 21 | MySQL 8.0 (`mysql:8.0`) | Database metadata and EXPLAIN |
| PostgreSQL integration tests | 21 | PostgreSQL 16 (`postgres:16-alpine`) | Database metadata and EXPLAIN |

The [starter build](https://github.com/haroya01/query-audit/blob/main/query-audit-spring-boot-starter/build.gradle)
pins the Spring Boot versions and wires `boot4Test` into `check`.
The [MySQL](https://github.com/haroya01/query-audit/blob/main/query-audit-mysql/src/test/java/io/queryaudit/mysql/MySqlIntegrationTest.java)
and [PostgreSQL](https://github.com/haroya01/query-audit/blob/main/query-audit-postgresql/src/test/java/io/queryaudit/postgresql/PostgreSqlIntegrationTest.java)
test fixtures select the database images.

Java 17 is the source/target baseline; the regular JUnit 5 suite uses 5.11.4.
Artifacts are published to Maven Central for Gradle and Maven consumers. These baselines
and Spring Boot 3.x/4.x integration are not a guarantee that every version in those lines,
or every Java/Boot/database combination, has been verified. In particular, the dedicated
Boot 4 suite is not the full Boot 3 suite rerun against Boot 4.
The broader scheduled matrix and support policy are tracked in
[#208](https://github.com/haroya01/query-audit/issues/208).

## Modules

| Module | Purpose |
|---|---|
| `query-audit-core` | Analysis engine, models, report schema, and comparator |
| `query-audit-junit5` | JUnit 5 lifecycle integration and annotations |
| `query-audit-mysql` | MySQL index metadata support |
| `query-audit-postgresql` | PostgreSQL index metadata support |
| `query-audit-spring-boot-starter` | Spring Boot auto-configuration and properties |

Ready to evaluate it? [Start with one test in report-only mode](https://haroya01.github.io/query-audit/getting-started/quickstart/),
then promote the behavior you trust into a budget, finding gate, or snapshot contract.

Configuration options, suppression formats, report settings, and troubleshooting steps are in the
[documentation](https://haroya01.github.io/query-audit/).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development setup and contribution process.

## License

QueryAudit is licensed under the [Apache License 2.0](LICENSE).
