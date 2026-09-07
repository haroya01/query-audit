---
title: Your first read-path policy
description: Reject an unexpected write, inspect its SQL and call site, then pass the same JUnit test.
---

# Your first read-path policy

Run a test that returns the expected order, yet fails because the read path writes to the database.
The example uses published QueryAudit `0.6.0` and in-memory H2. You need **Java 17+, Git, and network access**.

## 1. Run the unexpected-write failure

```sh
git clone https://github.com/haroya01/query-audit.git
cd query-audit
./gradlew -p examples/first-audit test -PextraWrite=true --rerun-tasks
```

The test permits one SELECT and zero INSERTs, UPDATEs, or DELETEs:

```java
@ExpectQueries(select = 1, insert = 0, update = 0, delete = 0)
```

The order still has the expected status. The added UPDATE fails the policy:

```text
QueryAudit: readsOnce() exceeded its query budget.
UPDATE: executed 1, expected at most 0.
  UPDATE orders SET status = 'NEW' WHERE id = 1
```

This is the expected failure. A download, compilation, or startup error does not verify capture.
Use [troubleshooting](../guide/troubleshooting.md) if you do not see the budget message.

## 2. Inspect the SQL and call site

Open the generated audit:

```text
examples/first-audit/build/reports/query-audit/report.json
```

| Field | Result of the failing run |
| --- | --- |
| `outcome` | `FAIL` |
| `reports[0].summary.totalQueries` | `2`: one SELECT and one UPDATE |
| UPDATE entry in `reports[0].queries`: `sql` | `UPDATE orders SET status = ? WHERE id = ?` |
| Its `stackTrace` | Starts with `example.audit.FirstAuditTest.writeOnReadPath:43` |

JSON redacts literal values and retains application call sites. In this release the budget
message's first call site can be a JDBC proxy; use the JSON evidence to locate the application code.

## 3. Remove the write and pass

Run the same test without the flag:

```sh
./gradlew -p examples/first-audit test --rerun-tasks
```

```text
[QueryAudit] 1 tests, 1 queries — all clean
[QueryAudit] outcome: PASS
```

The new JSON contains one SELECT and no writes. The query policy and functional assertions both pass.

To try a query-count regression too:

```sh
./gradlew -p examples/first-audit test -PextraQuery=true --rerun-tasks
```

```text
QueryAudit: readsOnce() exceeded its query budget.
SELECT: executed 2, expected at most 1.
```

Each run replaces `report.json`. To verify all three outcomes and retain their reports, run:

```sh
python3 .github/scripts/verify_first_audit.py
```

The verifier checks Gradle exits, JUnit results, captured SQL, application call sites, and the
published artifact version. Results are saved under
`examples/first-audit/build/reports/first-audit-verification/`.

## 4. Apply the budget to your code

| Your setup | Next step |
| --- | --- |
| Existing Spring Boot database test | [Install the starter](installation.md#spring-boot), then [verify capture](spring-boot.md#run-a-controlled-first-audit) |
| Plain JUnit 5 / JDBC | [Expose the instrumented DataSource](installation.md#plain-junit-5) |

Keep the test's functional assertions. Add `@EnableQueryInspector` and `@ExpectQueries` directly
to the test, and make the service or repository use the instrumented DataSource. Choose the SELECT
limit for that operation; zero INSERT/UPDATE/DELETE limits protect a read path from those writes.

**Budgets are upper bounds.** `select = 1` also permits zero captured SELECTs; omitted attributes
are unchecked. Verify a deliberate violation through your real operation before trusting a pass.
`@EnableQueryInspector` keeps findings informational while explicit query budgets still fail.

??? example "Complete runnable source"

    The fixture is prepared before the audit starts. Both flags change the operation under test;
    the assertions and policy stay the same.

    ```java
    --8<-- "examples/first-audit/src/test/java/example/audit/FirstAuditTest.java"
    ```

    [Java source](https://github.com/haroya01/query-audit/blob/main/examples/first-audit/src/test/java/example/audit/FirstAuditTest.java)
    · [Gradle build](https://github.com/haroya01/query-audit/blob/main/examples/first-audit/build.gradle)

??? info "Database and version scope"

    H2 proves capture and budgets. Index-dependent checks may print a missing-metadata notice;
    add the MySQL or PostgreSQL module when checking that database's indexes.
    The example downloads the [published release](versions.md) independently of the repository build.
    Review [known limitations](../guide/limitations.md) before requiring a whole suite in CI.

## Keep the policy across changes

- [Record test counts in a contract file](../guide/contracts.md) and review intentional changes in the diff.
- [Inspect a failed audit](../guide/reports.md) using its SQL and call sites.
- [Add the first CI check](../guide/first-ci-check.md), then [require audited tests](../guide/audit-coverage.md)
  and [compare compatible runs](../guide/comparison-inputs.md).
