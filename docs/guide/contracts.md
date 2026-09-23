# Query Snapshot Contracts

Record SELECT/INSERT/UPDATE/DELETE counts for audited tests in `.query-audit-contracts`.
A later increase or decrease fails the test. Review an intentional update beside the code change.

```diff
-@junit | [engine:junit-jupiter]/[class:com.example.OrderServiceTest]/[method:placeOrder()] | 2 | 1 | 1 | 0 | 4
+@junit | [engine:junit-jupiter]/[class:com.example.OrderServiceTest]/[method:placeOrder()] | 2 | 3 | 1 | 0 | 6
```

This example adds two INSERTs to `placeOrder()`. Contracts compare **counts**, not SQL text,
WHERE conditions, returned rows, or every possible database behavior. Keep ordinary result
assertions, and use [inline budgets](annotations.md#expectqueries) when fewer queries should pass
without updating a snapshot.

!!! note "Version scope"
    Query snapshot contracts were introduced in 0.5. QueryAudit 0.6 records stable JUnit IDs,
    escapes IDs that contain policy-file delimiters, and includes audited tests that execute zero
    queries. QueryAudit 0.5 uses class and display-name identities and skips zero-query tests.

## Recording

Run the suite once in record mode. The Gradle command assumes the
[`Test.systemProperty` bridge](ci-cd.md#plain-junit-build-tool-setup) from the CI guide.

=== "Gradle"

    ```bash
    ./gradlew test -PqueryAuditContractsRecord=true
    ```

=== "Maven"

    ```bash
    mvn test -DqueryAudit.contracts.record=true
    ```

Every completed audited test's SELECT/INSERT/UPDATE/DELETE counts are written to
`.query-audit-contracts` in the working directory (pipe-separated, sorted, human-reviewable):

```
# QueryAudit Query Contracts
# Format: identityType | identityValue | selectCount | insertCount | updateCount | deleteCount | totalCount
# @junit identityValue escapes: \| (pipe), \\ (backslash), \r (CR), \n (LF)
@junit | [engine:junit-jupiter]/[class:com.example.OrderServiceTest]/[method:findOrders()] | 3 | 0 | 0 | 0 | 3
@junit | [engine:junit-jupiter]/[class:com.example.OrderServiceTest]/[method:placeOrder()] | 2 | 1 | 1 | 0 | 4
```

Stable JUnit IDs remain one human-readable row when an identity contains policy-file delimiters or
line breaks. In an `@junit` identity value, QueryAudit writes `\|` for a pipe, `\\` for a backslash,
`\r` for a carriage return, and `\n` for a line feed. These escapes apply only to the `@junit`
identity value; the five count fields and legacy rows remain unescaped. Unknown or incomplete
stable-ID escapes make the policy file invalid and the diagnostic identifies the affected line.

Commit the file. Pair with [`mode: all`](configuration.md#audit-coverage-mode) to record counts
across the audited suite. Use an [expected-test manifest](audit-coverage.md) to require the tests
you rely on; a contract entry alone does not fail when its test is skipped.

## Enforcement

On every subsequent run, each test with a recorded entry is compared against its contract.
A count deviation fails with the delta. Increases also list captured statements of that type and
their first captured stack frame when available. Illustrative diagnostic (SQL list excerpt):

```
QueryAudit: placeOrder() deviates from its recorded query contract (.query-audit-contracts).
  INSERT: contract 1, executed 3 (+2)
    insert into order_items (order_id, sku) values (?, ?)
      at com.example.OrderService.placeOrder:87
If the change is intended, re-record the contracts with -DqueryAudit.contracts.record=true and review the file diff.
```

The frame can identify a JDBC proxy; inspect `reports[].queries[].stackTrace` in the
[JSON report](reports.md#read-a-policy-failure) for available application callers.

The final line names the underlying test-JVM property. Gradle projects using the bridge rerun with
`-PqueryAuditContractsRecord=true`; Maven projects use the `-D` form shown in the diagnostic.

Failures from `@ExpectMaxQueryCount`, `@ExpectQueries`, and snapshot contracts are test assertions
rather than findings. Rule profiles, `disabled-rules`, `suppress-patterns`, severity overrides, and
the issue baseline do not change their result. Update the declared budget or re-record the contract
when the count change is intentional.

Rules of enforcement:

- **Both directions fail.** Fewer queries than the contract also fails — snapshot semantics.
  A reduction belongs in the contract diff too.
- **Tests without an entry are not enforced.** New tests never fail retroactively;
  re-recording picks them up.
- **`@ExpectQueries` wins.** A method carrying an inline budget is exempt from the file
  contract — the annotation is the more specific declaration.
- **Record mode skips count comparison**, so a suite with contract deviations can re-record.
  The existing file must still be readable and valid because recording keeps entries for tests that
  did not run.
- **Invalid files fail the run.** An existing contract file that is malformed or unreadable stops
  audit initialization. The error identifies the file and, for malformed entries, the line number.
  A missing file remains valid and means that no contracts have been recorded yet.
- **Recording failures fail the run.** If the requested contract or count-baseline file cannot be
  saved, the launcher reports a failure and the suite is `INCONCLUSIVE` with `POLICY_WRITE_FAILED`.
  Report-only mode does not suppress this failure. Fix the destination and rerun recording before
  committing the policy file.

## Updating

Re-record and review the diff:

=== "Gradle"

    ```bash
    ./gradlew test -PqueryAuditContractsRecord=true
    git diff .query-audit-contracts
    ```

=== "Maven"

    ```bash
    mvn test -DqueryAudit.contracts.record=true
    git diff .query-audit-contracts
    ```

Recording merges: tests that ran are updated, entries for tests that didn't run are kept.

### Migrating files recorded by 0.5

QueryAudit 0.6 continues to read the 0.5 `testClass | displayName | ...` rows. An exact legacy row
can be enforced while a test has no stable row, and QueryAudit prints a migration warning because
that identity cannot distinguish packages or duplicate display names. Recording with 0.6 adds an
`@junit | <uniqueId>` row; subsequent runs prefer it. Old rows stay in the file so partial recording
does not discard tests that did not run. Do not mix 0.5 and 0.6 runners after recording stable rows.
A 0.5 reader may parse an unescaped `@junit` row as seven ordinary fields, but it treats `@junit` as
a class name and cannot match that row to the test. It also cannot parse the 0.6 escaping for a pipe
or line break. Upgrade every runner before relying on stable rows. Backslash sequences in preserved
legacy rows retain their original literal meaning, and stable-ID escaping is not applied
retroactively.

If a test still needs a legacy row that also matches another stable JUnit ID, the run fails with an
ambiguity diagnostic. Once every matching test has its own stable row, the preserved legacy row is
ignored. Run the complete audited suite in record mode and review the new stable rows instead of
letting one old contract apply to two tests. A display name changed before the first 0.6 recording
cannot be linked to its old row safely, so re-record the complete suite once when upgrading.

## Configuration

| Test-JVM system property | Gradle project property | Description |
|---|---|---|
| `queryAudit.contracts.record` | `queryAuditContractsRecord` | Set to `true` to record or refresh contracts instead of enforcing them |
| `queryAudit.contractsPath` | `queryAuditContractsPath` | Override the contracts file location |

The Gradle names use the [shared property bridge](ci-cd.md#plain-junit-build-tool-setup). Maven users pass the
test-JVM property with `-D`, for example `-DqueryAudit.contractsPath=config/query-contracts`.

## Contract a request or job

A test method often mixes fixture setup, the request under test, and database assertions.
`QueryContractScope` (0.7.0) counts only the work you pass to it. Use the `QueryInterceptor`
bean that the Spring Boot starter registers, or the interceptor you hooked into a plain
DataSource:

```java
private final QueryContractScope contracts =
    QueryContractScope.forDirectory(queryInterceptor, Path.of("src/test/resources/query-contracts"))
        .awaitingCompletion(asyncWork::awaitIdle);

@Test
void createsALink() throws Exception {
    MvcResult result =
        contracts.verify("link-create", () -> mockMvc.perform(post("/api/v1/links").content(body)).andReturn());
    assertThat(result.getResponse().getStatus()).isEqualTo(201);
}
```

`verify` returns the work's result. Use `capture` and `verify(captured)` separately when the test
needs the captured `queries()` or `counts()` before the check. `awaitingCompletion` runs after
the work and before capture stops, so SQL from background work the request triggered is counted
once the callback waits for it.

Every `*.contracts` file and `.query-audit-contracts` in the directory is read. Entries use the
same format as test contracts, with the scope ID as the `@junit` identity:

```text
@junit | link-create | 1 | 1 | 0 | 0 | 2
```

| Situation | Result |
|---|---|
| Counts match | passes and returns the work's result |
| Counts differ in either direction | `AssertionError` with the delta and the SQL of the grown types |
| No entry for the scope ID | `AssertionError`; new scopes are never accepted silently |
| The same ID in two files | `IllegalStateException` |
| The capture reached `max-queries` | `AssertionError`; a truncated capture cannot verify a contract |
| Another capture is running | `IllegalStateException` |

With `-DqueryAudit.contracts.record=true`, `verify` writes the counts instead: an existing ID is
updated in the file that owns it and a new ID goes to `.query-audit-contracts` in the directory.
Recording rewrites that file's header comments. Review the diff before committing.

A scope counts SQL from every thread that uses the audited DataSource while it is open. Run
scoped tests sequentially against an isolated database, keep schedulers off, and do not combine
a scope with `@QueryAudit` capture in the same test.

## Contracts vs. related features

| | Scope | Fails on | Update flow |
|---|---|---|---|
| **Contracts** | every recorded test | any count deviation, both directions | re-record, review file diff |
| [`@ExpectQueries`](annotations.md#expectqueries) | one method | budget exceeded | edit the annotation |
| [`QueryContractScope`](#contract-a-request-or-job) | one request, job, or journey | any count deviation, missing contract | re-record, review file diff |
| Count baseline (`queryAudit.updateBaseline`) | tests with a recorded baseline | threshold-based regression finding, subject to finding policy | update baseline |
| [Issue baseline](suppressing.md) | findings | new findings | acknowledge |
