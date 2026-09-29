---
title: Versions and compatibility
description: Check the published library's capabilities, report schema, and tested combinations.
---

# Versions and compatibility

The examples use **published QueryAudit `0.6.0`**. Keep every QueryAudit module on the same version.
The release code is tagged [`v0.6.0`](https://github.com/haroya01/query-audit/tree/v0.6.0)
(commit `8c16bf9`).

## What you can use in 0.6.0

| Capability | Start here |
| --- | --- |
| Read-path policies with explicit SELECT/INSERT/UPDATE/DELETE budgets | [Fail an unexpected write, then pass](quickstart.md) |
| Per-test count contracts, including zero-query tests | [Record and review count changes](../guide/contracts.md) |
| Captured SQL and application call sites | [Inspect audit results](../guide/reports.md) |
| JSON `PASS` / `FAIL` / `INCONCLUSIVE` with expected-test coverage | [Require the intended tests in CI](../guide/audit-coverage.md) |
| Comparison-input checks before classifying finding changes | [Compare compatible runs](../guide/comparison-inputs.md) |
| Spring DataSource wrapping | [Spring Boot setup](spring-boot.md) |
| Mutable static `DataSource` capture in plain JUnit | [Plain JUnit setup](installation.md#plain-junit-5) |

The published [JSON reporter](https://github.com/haroya01/query-audit/blob/v0.6.0/query-audit-core/src/main/java/io/queryaudit/core/reporter/JsonReporter.java)
uses schema **`1.6.0`**. Keep the report reader and library versions aligned.

Audited methods must run on the same thread: `0.6.0` rejects concurrent audited methods and
`@TestFactory` audit boundaries. Use direct audit annotations for the installation proof, and
verify capture when changing Spring contexts or sharing annotation policies.
See [limitations](../guide/limitations.md) for the supported scope and reported cases.

## Tested combinations

The release's [CI workflow](https://github.com/haroya01/query-audit/blob/v0.6.0/.github/workflows/ci.yml)
and [starter test configuration](https://github.com/haroya01/query-audit/blob/v0.6.0/query-audit-spring-boot-starter/build.gradle)
define this matrix. Entries identify the configured checks; they do not establish compatibility
with every intermediate framework or database version.

| Check | Java | Framework or database | Scope |
| --- | --- | --- | --- |
| Regular build and tests | 17, 21 | Spring Boot `3.4.1`, JUnit Jupiter `5.11.4` | Core, JUnit, and starter tests |
| Dedicated `boot4Test` | 17, 21 | Spring Boot `4.0.6` | Selected multi-context lifecycle cases |
| MySQL integration tests | 21 | MySQL 8.0 | Database metadata and EXPLAIN |
| PostgreSQL integration tests | 21 | PostgreSQL 16 | Database metadata and EXPLAIN |

Java 17 is the library's source/target baseline. Supply your JDBC driver, database, migrations,
and fixtures. H2 verifies capture and portable SQL checks; MySQL/PostgreSQL plan checks require
the matching engine. Broader compatibility work is tracked in
[#208](https://github.com/haroya01/query-audit/issues/208).

## Verify the loaded dependency

Run from the module containing your audited test:

=== "Gradle"

    ```bash
    ./gradlew dependencyInsight --dependency query-audit-core --configuration testRuntimeClasspath
    ```

=== "Maven"

    ```bash
    mvn dependency:tree -Dincludes='io.github.haroya01:*'
    ```

Check dependency-management overrides or local builds if the resolved version differs from your
declaration. Then [run an intentional budget violation](quickstart.md) to verify capture.
The runnable example downloads the published artifact independently of the repository build.
