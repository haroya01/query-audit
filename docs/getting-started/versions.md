---
title: Versions and compatibility
description: Check the published library's capabilities, upgrade notes, report schema, and tested combinations.
---

# Versions and compatibility

These guides describe **published QueryAudit `0.7.2`**. Keep every QueryAudit module on the same
version. The release code is tagged [`v0.7.2`](https://github.com/haroya01/query-audit/tree/v0.7.2).
`0.7.0` and `0.7.1` are also on Maven Central, but use `0.7.2`. In 0.7.0, SQL that a server thread
runs, as with `RANDOM_PORT` tests, does not count toward the test. In 0.7.1, a failed scoped
contract leaves the run `PASS`, and a lazy-proxy N+1 changes its finding ID on every run. See
[changes in 0.7.2](#changes-in-072).

## What you can use in 0.7.2

| Capability | Start here |
| --- | --- |
| N+1 detection at the call site, the only default built-in rule | [N+1 detection](../detections/n-plus-one.md) |
| Read-path budgets with SELECT/INSERT/UPDATE/DELETE/total limits, or exact counts | [Annotations](../guide/annotations.md#expectqueries) |
| Recorded count contracts for test methods, requests, jobs, and journeys | [Contracts](../guide/contracts.md) |
| Captured SQL and application call sites, printed with each failure | [Read SQL and call sites](../guide/reports.md) |
| JSON `PASS` / `FAIL` / `INCONCLUSIVE` with expected-test coverage | [Require expected tests](../guide/audit-coverage.md) |
| Comparison-input checks and `--require-resolved` finding targets | [Comparison inputs](../guide/comparison-inputs.md) |
| Spring DataSource wrapping that follows a replaced test context | [Spring Boot setup](spring-boot.md) |
| Mutable static `DataSource` capture in plain JUnit | [Plain JUnit setup](installation.md#plain-junit-5) |
| Optional index, `EXPLAIN`, and SQL style rules | [Optional rules](../detections/overview.md) |

The [JSON reporter](https://github.com/haroya01/query-audit/blob/v0.7.2/query-audit-core/src/main/java/io/queryaudit/core/reporter/JsonReporter.java)
writes schema **`1.7.0`**, the same schema as `0.6.1`. Keep the report reader and library versions
aligned.

Not every public type is supported. See [Supported public API](../architecture/supported-api.md) for
the surface you may depend on, [API compatibility policy](../architecture/api-compatibility.md) for
how it is enforced, and [compatibility changelog](../architecture/api-changelog.md) for what changed
since `0.5`.

## Changes in 0.7.2

| Change | What to do |
| --- | --- |
| `@ExpectQueries(exact = true)` makes every declared count exact, so fewer queries fail too | Optional. See [exact counts](../guide/annotations.md#exact-counts) |
| A failed `QueryContractScope` makes the run `FAIL`, also in a test class without an audit annotation. In 0.7.1 the JSON outcome, the HTML banner, and the comparison stayed `PASS` | None |
| An N+1 reached through a Hibernate lazy proxy keeps its finding ID across runs. In 0.7.1 the ID changed on every run, so a comparison reported the finding as new and resolved, and `--require-resolved` could call it resolved while it remained | Record new comparison baselines with 0.7.2 |
| Contract counts and budget limits are no longer comparison inputs, so a pull request that re-records a contract compares as `PASS` instead of `INCONCLUSIVE` | None. Review the contract change in the diff |
| The console prints only application frames under `Source:`, and a contract that ran fewer queries shows a negative delta | None |

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
baseline report with 0.7.2 before comparing runs in CI.

[Upgrading to 0.7.0](migrating-0.7.md) collects the same changes as a checklist, adds the `0.7.1`
and `0.7.2` corrections, and lists the Java API additions that need no code change.

## Tested combinations

The release's [CI workflow](https://github.com/haroya01/query-audit/blob/v0.7.2/.github/workflows/ci.yml)
and [starter test configuration](https://github.com/haroya01/query-audit/blob/v0.7.2/query-audit-spring-boot-starter/build.gradle)
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
