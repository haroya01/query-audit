# QueryAudit

**Make database behavior part of your test contract.**

[![Build](https://github.com/haroya01/query-audit/actions/workflows/ci.yml/badge.svg)](https://github.com/haroya01/query-audit/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.haroya01/query-audit-core)](https://central.sonatype.com/artifact/io.github.haroya01/query-audit-core)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Documentation](https://img.shields.io/badge/docs-haroya01.github.io-blue)](https://haroya01.github.io/query-audit)
[![Java 17+](https://img.shields.io/badge/Java-17%2B-blue)](https://openjdk.org/)

QueryAudit watches the SQL executed by JUnit 5 tests and catches N+1 queries, missing
indexes, unsafe DML, and query-count drift before merge. Findings can fail the test that
introduced them, while reports and snapshot contracts make database behavior reviewable in
CI.

**Java 17+ · JUnit 5.9+ · Spring Boot 3.x and 4.x · MySQL 5.7+ · PostgreSQL 12+**

[Get started](https://haroya01.github.io/query-audit/getting-started/quickstart/) ·
[Detection rules](https://haroya01.github.io/query-audit/detections/overview/) ·
[Query contracts](https://haroya01.github.io/query-audit/guide/contracts/) ·
[Reports](https://haroya01.github.io/query-audit/guide/reports/) ·
[CI/CD](https://haroya01.github.io/query-audit/guide/ci-cd/)

## See the problem in the failing test

QueryAudit connects a finding to the test that produced it and includes the context available
for that check. A Hibernate N+1 finding looks like this in the test failure:

```text
QueryAudit detected 1 issue(s) in findOrdersWithItems:

  [ERROR] N+1 Query detected (table: items)
    Detail: Lazy collection 'items' on Order initialized 5 times for 5 different entities
    Suggestion: Use @EntityGraph, JOIN FETCH, or @BatchSize
```

_Abbreviated test failure. Reports may also include the SQL, application call site, table,
column, index context, and a concrete fix, depending on the check._

## Add two test dependencies

### Gradle

```groovy
dependencies {
    testImplementation 'io.github.haroya01:query-audit-spring-boot-starter:0.5.0' // x-release-please-version
    testImplementation 'io.github.haroya01:query-audit-mysql:0.5.0' // x-release-please-version
}
```

### Maven

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

Using PostgreSQL? Replace `query-audit-mysql` with `query-audit-postgresql`. The
[installation guide](https://haroya01.github.io/query-audit/getting-started/installation/) has
the full Gradle and Maven examples.

Then annotate the test:

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

The test now captures its SQL, runs the applicable checks, and fails on configured findings.
Start with `@EnableQueryInspector` to report detected findings without failing on those findings.

## What QueryAudit covers

- **Detect query problems.** More than 60 finding types cover N+1 access, missing and ineffective
  indexes, SQL anti-patterns, unsafe DML, locking risks, ORM behavior, and connection lifecycle
  problems. Database-aware checks use MySQL `SHOW INDEX` or PostgreSQL `pg_catalog` metadata.
- **Enforce query budgets.** Set a total cap with `@ExpectMaxQueryCount` or separate
  SELECT/INSERT/UPDATE/DELETE limits with `@ExpectQueries`.
- **Record snapshot contracts.** Store statement counts for recorded tests that execute SQL in a
  small, reviewable file. Changes between SQL-executing runs fail until deliberately recorded.
- **Audit a full suite.** Enable JUnit extension autodetection and set `mode: all` for opt-out
  coverage, choose a `strict`, `recommended`, or `minimal` rule profile, and acknowledge existing
  findings with a baseline.
- **Choose the right output.** Console logs keep findings close to the failing test, while HTML,
  versioned JSON, and GitHub Actions output support review and automation. The JSON suite envelope
  states `PASS`, `FAIL`, or `INCONCLUSIVE`, so automation does not mistake partial collection for
  success.

The [detection overview](https://haroya01.github.io/query-audit/detections/overview/) groups
checks by severity and explains which checks depend on Hibernate, index metadata, or EXPLAIN
output.

## A verifiable change loop

Since 0.5.0, QueryAudit can turn test output into a repeatable review loop:

1. Select the JSON report format, run the audited tests, and keep `report.json` as a CI artifact.
2. Use the context available on the finding, such as SQL, a normalized pattern, or a call site,
   to change the query, mapping, or schema. Reports may also include structured remediation and
   captured index context for supported high-precision findings.
3. Run the same tests again.
4. Compare the two reports. The comparator classifies new, resolved, and persisting findings
   and returns a CI-friendly exit code.

```text
[QueryAudit] compare: PASS; 0 new, 1 resolved, 0 persisting; queries 11 -> 7
```

The report comparator returns `0`, `1`, or `2` for `PASS`, `FAIL`, or `INCONCLUSIVE`. Known schema
1.x inputs keep their available finding delta when the comparison is incomplete, but can never
produce exit code `0`. For recorded tests with SQL in both runs, query snapshot contracts add a
separate guard for statement counts, so a change that turns one query into many or adds writes to a
read path must update the contract. Enable JUnit extension autodetection and set `mode: all` when
you want opt-out coverage across the suite.

Read [Reports](https://haroya01.github.io/query-audit/guide/reports/) for the report format and
comparator, or [Query Contracts](https://haroya01.github.io/query-audit/guide/contracts/) for the
record-and-review workflow.

## Modules

| Module | Purpose |
|---|---|
| `query-audit-core` | Analysis engine, checks, models, and reporters |
| `query-audit-junit5` | JUnit 5 extension and annotations |
| `query-audit-mysql` | MySQL index metadata support |
| `query-audit-postgresql` | PostgreSQL index metadata support |
| `query-audit-spring-boot-starter` | Spring Boot auto-configuration and properties |

Configuration options, suppression formats, adoption profiles, and report settings are covered
in the [configuration reference](https://haroya01.github.io/query-audit/guide/configuration/).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the development setup and contribution process.

## License

QueryAudit is licensed under the [Apache License 2.0](LICENSE).
