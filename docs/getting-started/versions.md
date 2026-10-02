---
title: Versions and compatibility
description: Check the published library's capabilities, upgrade notes, report schema, and tested combinations.
---

# Versions and compatibility

These guides describe **published QueryAudit `0.7.1`**. Keep every QueryAudit module on the same
version. The release code is tagged [`v0.7.1`](https://github.com/haroya01/query-audit/tree/v0.7.1).
`0.7.0` is also on Maven Central, but use `0.7.1`: in 0.7.0 SQL that a server thread runs, as with
`RANDOM_PORT` tests, does not count toward the test, so an N+1 behind such a request does not fail
the test and the run is only `INCONCLUSIVE`.

## What you can use in 0.7.1

| Capability | Start here |
| --- | --- |
| N+1 detection at the call site, the only default built-in rule | [N+1 detection](../detections/n-plus-one.md) |
| Read-path budgets with SELECT/INSERT/UPDATE/DELETE/total limits | [Annotations](../guide/annotations.md#expectqueries) |
| Recorded count contracts for test methods, requests, jobs, and journeys | [Contracts](../guide/contracts.md) |
| Captured SQL and application call sites, printed with each failure | [Read SQL and call sites](../guide/reports.md) |
| JSON `PASS` / `FAIL` / `INCONCLUSIVE` with expected-test coverage | [Require expected tests](../guide/audit-coverage.md) |
| Comparison-input checks and `--require-resolved` finding targets | [Comparison inputs](../guide/comparison-inputs.md) |
| Spring DataSource wrapping that follows a replaced test context | [Spring Boot setup](spring-boot.md) |
| Mutable static `DataSource` capture in plain JUnit | [Plain JUnit setup](installation.md#plain-junit-5) |
| Optional index, `EXPLAIN`, and SQL style rules | [Optional rules](../detections/overview.md) |

The [JSON reporter](https://github.com/haroya01/query-audit/blob/v0.7.1/query-audit-core/src/main/java/io/queryaudit/core/reporter/JsonReporter.java)
writes schema **`1.7.0`**, the same schema as `0.6.1`. Keep the report reader and library versions
aligned.

## Upgrading from 0.6

| Change in 0.7 | What to do |
| --- | --- |
| The default `recommended` profile runs only call-site N+1 among built-in rules. In 0.6 it ran most rules | To keep the 0.6 coverage, set `profile: strict` or list rule codes in `enabled-rules`. See [rule profiles](../guide/configuration.md#rule-profiles) |
| Hibernate lazy-load N+1 evidence is INFO; one batch fetch no longer fails | Rely on the call-site finding to fail a test. See [N+1 detection](../detections/n-plus-one.md) |
| Each audited method captures into its own session, so concurrent tests no longer share SQL. SQL from another thread, such as an `@Async` flush or a `RANDOM_PORT` request, counts toward the test while it is the only audit running; during parallel audits it makes the run `INCONCLUSIVE` | Name the pools in [`await-executors`](../guide/configuration.md#background-work) so the test waits for their work. With parallel audits, wrap the tasks with `QueryCaptureSession.wrap`. See [parallel capture](../guide/troubleshooting.md#parallel-capture-is-incomplete) |
| The N+1 rule fails a test only when a repeated SELECT binds different values; the same values are INFO, and `OFFSET` pages are skipped | Nothing to change. Suppress `n-plus-one` on a test whose keyset page loop is intended |
| Capture rebinds to the new `DataSource` after a Spring test context is replaced ([#287](https://github.com/haroya01/query-audit/issues/287)) | None |
| One setting name rule for `application.yml`, `-D`, and `-P`; earlier names stay accepted | Prefer the new names. See [setting names](../guide/configuration.md#setting-names) |
| Test-method and scoped contracts share one file or directory, set by `query-audit.contracts.path` | Existing `.query-audit-contracts` files keep working. See [contracts](../guide/contracts.md#configuration) |
| `@ExpectQueries(total = n)` replaces `@ExpectMaxQueryCount`; `@DetectNPlusOne` and count baselines are deprecated | Move to `@ExpectQueries`, `@QueryAudit(failOn = N_PLUS_ONE)`, and contracts. See [annotations](../guide/annotations.md) |
| `ReportComparator.Finding` and `ReportComparator.Verdict` add record components | Earlier constructors remain. See [Java API compatibility](../guide/reports.md#delta-verdict-compare-two-runs) |

The active rules changed, so a 0.6 report is not a valid comparison baseline. Record a new
baseline report with 0.7.1 before comparing runs in CI.

## Tested combinations

The release's [CI workflow](https://github.com/haroya01/query-audit/blob/v0.7.1/.github/workflows/ci.yml)
and [starter test configuration](https://github.com/haroya01/query-audit/blob/v0.7.1/query-audit-spring-boot-starter/build.gradle)
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
The runnable example pins its own published release, independently of the repository build.
