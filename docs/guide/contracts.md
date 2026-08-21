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
# Format: testClass | testMethod | selectCount | insertCount | updateCount | deleteCount | totalCount
OrderServiceTest | findOrders() | 3 | 0 | 0 | 0 | 3
OrderServiceTest | placeOrder() | 2 | 1 | 1 | 0 | 4
```

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

Rules of enforcement:

- **Both directions fail.** Fewer queries than the contract also fails — snapshot semantics.
  An improvement is still a behavior change that belongs in the contract diff.
- **Tests without an entry are not enforced.** New tests never fail retroactively;
  re-recording picks them up.
- **`@ExpectQueries` wins.** A method carrying an inline budget is exempt from the file
  contract — the annotation is the more specific declaration.
- **Record mode skips enforcement**, so a red suite can always re-record.

## Updating

Re-record and review the diff:

```bash
./gradlew test -DqueryAudit.contracts.record=true
git diff .query-audit-contracts
```

Recording merges: tests that ran are updated, entries for tests that didn't run are kept.

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
