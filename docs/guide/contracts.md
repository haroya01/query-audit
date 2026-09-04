# Query Snapshot Contracts

Snapshot testing for database behavior: record every test's query profile once, then fail any
change — in either direction — until the contract is explicitly re-recorded.

!!! note "Version scope"
    Query snapshot contracts were introduced in 0.5. QueryAudit 0.6 records stable JUnit IDs,
    escapes IDs that contain policy-file delimiters, and includes audited tests that execute zero
    queries. QueryAudit 0.5 uses class and display-name identities and skips zero-query tests.

What the [baseline](suppressing.md) does for *findings*, contracts do for *behavior*. A
regression detector only catches increases; a contract catches **every deviation**, which is
what makes it reviewable: after a legitimate change you re-record, and the contracts file's
diff *is* the behavior change, sitting in the PR next to the code that caused it.

This matters most when code is written or modified by automation: a generated change that
turns one UPDATE into N, or quietly adds writes to a read path, is invisible in a code diff
and green in ordinary tests. With contracts, any DB-behavior change must surface as an
explicit contract update.

---

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

Commit the file. Pair with [`mode: all`](configuration.md#audit-coverage-mode) to freeze the
whole suite's behavior in one run.

## Enforcement

On every subsequent run, each test with a recorded entry is compared against its contract.
Any deviation fails with the full delta, the offending SQL, and its call site:

```
QueryAudit: placeOrder() deviates from its recorded query contract (.query-audit-contracts).
  INSERT: contract 1, executed 3 (+2)
    insert into order_items (order_id, sku) values (?, ?)
      at com.example.OrderService.placeOrder(OrderService.java:87)
If the change is intended, re-record the contracts with -DqueryAudit.contracts.record=true and review the file diff.
```

The final line names the underlying test-JVM property. Gradle projects using the bridge rerun with
`-PqueryAuditContractsRecord=true`; Maven projects use the `-D` form shown in the diagnostic.

Failures from `@ExpectMaxQueryCount`, `@ExpectQueries`, and snapshot contracts are test assertions
rather than findings. Rule profiles, `disabled-rules`, `suppress-patterns`, severity overrides, and
the issue baseline do not change their result. Update the declared budget or re-record the contract
when the database behavior change is intentional.

Rules of enforcement:

- **Both directions fail.** Fewer queries than the contract also fails — snapshot semantics.
  An improvement is still a behavior change that belongs in the contract diff.
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

## Contracts vs. related features

| | Scope | Fails on | Update flow |
|---|---|---|---|
| **Contracts** | every recorded test | any count deviation, both directions | re-record, review file diff |
| [`@ExpectQueries`](annotations.md#expectqueries) | one method | budget exceeded | edit the annotation |
| Count baseline (`queryAudit.updateBaseline`) | every test | count **regression** (increase) | update baseline |
| [Issue baseline](suppressing.md) | findings | new findings | acknowledge |
