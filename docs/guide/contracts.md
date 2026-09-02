# Query Snapshot Contracts

Snapshot testing for database behavior: record every test's query profile once, then fail any
change — in either direction — until the contract is explicitly re-recorded.

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

Run the suite once with the record flag:

```bash
./gradlew test -DqueryAudit.contracts.record=true
```

Every audited test's SELECT/INSERT/UPDATE/DELETE counts are written to
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

!!! note "Gradle daemon and `-D` flags"
    Gradle only forwards `-D` system properties to the test JVM if your `test` task is
    configured to do so. If the flag doesn't seem to take effect, pass it explicitly:

    ```groovy
    tasks.named('test') {
        systemProperty 'queryAudit.contracts.record',
            providers.gradleProperty('recordContracts').getOrElse('false')
    }
    ```

    and run `./gradlew test -PrecordContracts=true`.

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

## Updating

Re-record and review the diff:

```bash
./gradlew test -DqueryAudit.contracts.record=true
git diff .query-audit-contracts
```

Recording merges: tests that ran are updated, entries for tests that didn't run are kept.

### Migrating files recorded by 0.5

QueryAudit 0.6 continues to read the 0.5 `testClass | displayName | ...` rows. An exact legacy row
can be enforced while a test has no stable row, and QueryAudit prints a migration warning because
that identity cannot distinguish packages or duplicate display names. Recording with 0.6 adds an
`@junit | <uniqueId>` row; subsequent runs prefer it. Old rows stay in the file so partial recording
does not discard tests that did not run. QueryAudit 0.5 can read the resulting seven-column file as
long as its stable IDs do not require 0.6 escaping; it does not understand the new syntax when an ID
contains a pipe or line break. Backslash sequences in legacy rows retain their original literal
meaning, and stable-ID escaping is not applied retroactively.

If a test still needs a legacy row that also matches another stable JUnit ID, the run fails with an
ambiguity diagnostic. Once every matching test has its own stable row, the preserved legacy row is
ignored. Run the complete audited suite in record mode and review the new stable rows instead of
letting one old contract apply to two tests. A display name changed before the first 0.6 recording
cannot be linked to its old row safely, so re-record the complete suite once when upgrading.

## Configuration

| System property | Description |
|---|---|
| `-DqueryAudit.contracts.record=true` | Record/refresh contracts instead of enforcing them |
| `-DqueryAudit.contractsPath=path` | Override the contracts file location |

## Contracts vs. related features

| | Scope | Fails on | Update flow |
|---|---|---|---|
| **Contracts** | every recorded test | any count deviation, both directions | re-record, review file diff |
| [`@ExpectQueries`](annotations.md#expectqueries) | one method | budget exceeded | edit the annotation |
| Count baseline (`-DqueryAudit.updateBaseline`) | every test | count **regression** (increase) | update baseline |
| [Issue baseline](suppressing.md) | findings | new findings | acknowledge |
