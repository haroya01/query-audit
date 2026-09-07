# QueryAudit

**Keep query changes under test.**

[![Build](https://github.com/haroya01/query-audit/actions/workflows/ci.yml/badge.svg)](https://github.com/haroya01/query-audit/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.haroya01/query-audit-core)](https://central.sonatype.com/artifact/io.github.haroya01/query-audit-core)
[![Java 17+](https://img.shields.io/badge/Java-17%2B-blue)](https://openjdk.org/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

Review query-count contracts across your JUnit 5 tests. Compare CI runs with checks for
missing audits and changed analysis settings. Start with read/write limits and inspect
the captured SQL when a policy fails.

## Keep a read path free of writes

After [enabling SQL capture](docs/getting-started/installation.md), put the policy beside your test:

```java
@Test
@ExpectQueries(select = 2, insert = 0, update = 0, delete = 0)
void loadsOrders() {
    var orders = orderService.findRecentOrders();
    assertEquals(3, orders.size());
}
```

At most two SELECTs. No INSERT, UPDATE, or DELETE. If the path adds an UPDATE,
the budget fails even when the returned orders are correct:

```text
UPDATE: executed 1, expected at most 0.
```

Omitted fields are unchecked. Keep functional assertions for returned data and affected rows.

### Run a failure, then pass

Requires **Java 17+** and Git. This example uses the published library and in-memory H2.

```sh
git clone https://github.com/haroya01/query-audit.git
cd query-audit
./gradlew -p examples/first-audit test -PextraWrite=true --rerun-tasks
```

Expected: a query-budget failure for one unexpected UPDATE. Remove the write and rerun:

```sh
./gradlew -p examples/first-audit test --rerun-tasks
```

Expected: the test passes and `report.json` contains `"outcome": "PASS"`.
Use `-PextraQuery=true` to try the extra-SELECT failure too.

[Complete code and output](docs/getting-started/quickstart.md)
· [Install in your project](docs/getting-started/installation.md)
· [Troubleshoot a setup failure](docs/guide/troubleshooting.md)

## Review count changes with your code

In your own Maven project with capture enabled, record SELECT/INSERT/UPDATE/DELETE counts
for tests without an inline budget:

```sh
mvn test -DqueryAudit.contracts.record=true
```

Commit `.query-audit-contracts`. Subsequent `mvn test` runs fail when a recorded count changes:

```text
QueryAudit: placeOrder() deviates from its recorded query contract (.query-audit-contracts).
  INSERT: contract 1, executed 3 (+2)
```

If the change is intended, re-record and review the contract file's diff in the same PR.
Contracts compare counts in both directions; they do not snapshot SQL text or result rows.

[Recording, Gradle setup, and contract updates](docs/guide/contracts.md)

## Find the SQL and call site

Select JSON in your Spring test configuration:

```yaml
query-audit:
  auto-open-report: false
  report:
    format: json
```

After running the test, inspect the captured statements and application frames:

```sh
jq '.reports[].queries[] | {sql, stackTrace}' \
  build/reports/query-audit/report.json
```

For the runnable example, use `examples/first-audit/build/reports/query-audit/report.json`.
The unexpected write has this JSON evidence (excerpt; source line numbers can change):

```json
{
  "sql": "UPDATE orders SET status = ? WHERE id = ?",
  "stackTrace": "example.audit.FirstAuditTest.writeOnReadPath:43\nexample.audit.FirstAuditTest.readsOnce:26"
}
```

[Read a failure](docs/guide/reports.md#read-a-policy-failure)
· [Investigate an N+1, SQL, or index finding](docs/detections/overview.md)

## Compare the same tests in CI

Save a baseline and candidate JSON report, then compare them with the matching core JAR:

```sh
java -cp "$QUERY_AUDIT_CORE_JAR" \
  io.queryaudit.core.reporter.ReportComparator before.json after.json verdict.json
```

Set `QUERY_AUDIT_CORE_JAR` to the downloaded `query-audit-core` JAR path.
The comparator prints a result such as:

```text
[QueryAudit] compare: PASS; 0 new, 1 resolved, 0 persisting; queries 11 -> 7
```

| Candidate result | CI comparison |
| --- | --- |
| Compatible, complete audit; no new confirmed finding or policy failure | `PASS` |
| Query budget or contract fails | `FAIL` |
| An expected test is missing or skipped | `INCONCLUSIVE` |
| Analysis settings changed or required analysis evidence is missing | `INCONCLUSIVE` |

Declare expected tests with an [audit coverage manifest](docs/guide/audit-coverage.md).
Require the JUnit run and audit verdict to pass. Budgets and contracts enforce query counts;
the comparator's count delta is a summary, not its own limit.

[First CI check](docs/guide/first-ci-check.md)
· [Baseline and comparison setup](docs/guide/ci-cd.md)
· [Comparison inputs](docs/guide/comparison-inputs.md)

## Add to an existing project

For Spring Boot, add the starter to the test classpath:

```kotlin
dependencies {
    testImplementation("io.github.haroya01:query-audit-spring-boot-starter:0.6.0") // x-release-please-version
}
```

Enable capture with `@EnableQueryInspector` on the test class, then add `@ExpectQueries`
to the method. Findings remain advisory; explicit budgets still fail.
The [installation guide](docs/getting-started/installation.md) includes Maven, Groovy,
plain JUnit, and a capture check. MySQL and PostgreSQL modules add database index metadata.

## Supported scope

Java 17+, JUnit 5, and database-backed tests. The checks cover the paths exercised by
those tests; representative fixtures and ordinary assertions remain necessary.

| Source CI check | Tested combination |
| --- | --- |
| Build and regular tests | Java 17 / 21; Spring Boot 3.4.1 |
| Dedicated Boot 4 lifecycle suite | Java 17 / 21; Spring Boot 4.0.6 |
| MySQL integration | Java 21; MySQL 8.0 |
| PostgreSQL integration | Java 21; PostgreSQL 16 |

The Boot 4 lane is a focused lifecycle suite. See [versions](docs/getting-started/versions.md)
and [known limitations](docs/guide/limitations.md) for the published release's supported scope.

[Documentation](https://haroya01.github.io/query-audit/)
· [Coming from QuickPerf](docs/guide/coming-from-quickperf.md)
· [Contributing](CONTRIBUTING.md)
· [Apache 2.0 license](LICENSE)
