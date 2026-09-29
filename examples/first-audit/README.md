# Keep a read path free of writes

From the **repository root**, run this Java 17+ example against in-memory H2:

```sh
./gradlew -p examples/first-audit test -PextraWrite=true --rerun-tasks
```

The order's status assertion passes, but the added UPDATE violates the test's zero-write policy:

```text
QueryAudit: readsOnce() exceeded its query budget.
UPDATE: executed 1, expected at most 0.
  UPDATE orders SET status = 'NEW' WHERE id = 1
```

**This failure is expected.** A dependency, compilation, or startup error does not verify capture.

Open `examples/first-audit/build/reports/query-audit/report.json`. Its `outcome` is `FAIL`;
the UPDATE's redacted SQL and application call site appear in `reports[0].queries`:

```text
sql:        UPDATE orders SET status = ? WHERE id = ?
stackTrace: example.audit.FirstAuditTest.writeOnReadPath:43
```

The stack trace continues with the test caller. Use the JSON for the application location;
the first call site in the budget exception can be a JDBC proxy in this release.

## Pass the same policy

Remove the flag:

```sh
./gradlew -p examples/first-audit test --rerun-tasks
```

```text
[QueryAudit] 1 tests, 1 queries — all clean
[QueryAudit] outcome: PASS
```

One SELECT, no INSERT/UPDATE/DELETE, and the same functional assertions pass.
The policy is `@ExpectQueries(select = 1, insert = 0, update = 0, delete = 0)`.

## Catch an added query too

```sh
./gradlew -p examples/first-audit test -PextraQuery=true --rerun-tasks
```

```text
QueryAudit: readsOnce() exceeded its query budget.
SELECT: executed 2, expected at most 1.
```

Verify all three cases and retain the JUnit XML, JSON, and logs:

```sh
python3 .github/scripts/verify_first_audit.py
```

Artifacts are under `examples/first-audit/build/reports/first-audit-verification/`.

## Apply it to your tests

1. Start with the [complete test](src/test/java/example/audit/FirstAuditTest.java) and [build file](build.gradle).
2. Make the service or repository use the instrumented DataSource and keep the functional assertions.
3. Replace `readOne()` with the operation under test, choose a budget, and verify an intentional violation.

Budgets are upper bounds: `select = 1` permits zero or one captured SELECT. Verifying a failure
first proves the SQL is captured. The fixture is prepared before the audit starts.

Next: [installation](../../docs/getting-started/installation.md),
[count contracts](../../docs/guide/contracts.md), and the [first CI check](../../docs/guide/first-ci-check.md).

This standalone project downloads published QueryAudit `0.6.0` and test dependencies.
H2 verifies capture and budgets; MySQL/PostgreSQL index metadata requires the matching database module.
