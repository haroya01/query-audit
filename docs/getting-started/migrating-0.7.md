---
title: Upgrading to 0.7.0
description: What changed in QueryAudit 0.7.0 and 0.7.2, and the exact edit each change needs.
---

# Upgrading to 0.7.0

`0.7.0` is the current release line. Use the latest patch, `0.7.2`. This page lists every change that
needs an edit in your project, with the edit. Nothing here needs action if you only read reports and
never configured a rule.

`0.7.1` and `0.7.2` are corrections within the same line; their behavior changes are listed at the
end and need a re-recorded baseline rather than a code change.

Keep every QueryAudit module on the same version. Check the resolved version with
[`dependencyInsight`](versions.md#verify-the-loaded-dependency).

## Java and configuration APIs

No supported Java type was removed or renamed in `0.7.0`, so nothing fails to compile. Two additions
changed how existing code behaves:

| Change | What to do |
| --- | --- |
| `ReportComparator.Finding` gained `testId` (`0.6.0`) and `findingId` and `column` (`0.6.1`). Earlier constructors are retained | None to compile. If you compare or cache `Finding` values, key on `findingId()`: records derive `equals` and `hashCode` from every component, so a `Finding` built through the old constructor is not equal to one with an explicit `testId` |
| `ReportComparator.Verdict` grew from 6 to 11 components. Every earlier constructor arity is retained | None to compile. Read `Verdict.outcome()` or the process exit code rather than any single boolean |
| `JsonReporter.toEnvelopeJson(List)` is deprecated and emits a legacy `1.0` envelope with no run outcome | Switch to `JsonReporter.toRunEnvelopeJson(AuditRunResult)` if you build a report yourself |

The full per-type history is in the [compatibility changelog](../architecture/api-changelog.md).

## Behavior changes needing action

| Change in 0.7 | What to do |
| --- | --- |
| The default `recommended` profile runs only call-site N+1 among built-in rules. In 0.6 the default ran most rules | To keep 0.6 coverage, set `profile: strict`, or list the rule codes you enforce in `enabled-rules`. See [rule profiles](../guide/configuration.md#rule-profiles) |
| Hibernate lazy-load N+1 evidence is now INFO, and the failing finding is a SELECT repeated with different values at one call site. One batch fetch no longer fails a test | Rely on the call-site finding to fail a test. See [N+1 detection](../detections/n-plus-one.md) |
| Each audited method captures into its own session, so concurrent tests no longer share SQL. SQL from another thread, such as an `@Async` flush or a `RANDOM_PORT` request, counts toward the test when it is the only audit running; during parallel audits it makes the run `INCONCLUSIVE` | Name the pools tests hand work to in [`await-executors`](../guide/configuration.md#background-work). With parallel audits, wrap the tasks with `QueryCaptureSession.wrap`. See [parallel capture](../guide/troubleshooting.md#parallel-capture-is-incomplete) |
| The N+1 rule fails a test only when a repeated SELECT binds different values; the same values are INFO, and `OFFSET` paging is skipped | Nothing to change. Suppress `n-plus-one` on a test whose keyset paging loop is intended |
| Capture rebinds to the new `DataSource` after a Spring test context is replaced ([#287](https://github.com/haroya01/query-audit/issues/287)) | None |
| One setting name per key for `application.yml`, `-D`, and `-P`. Earlier names are still accepted | Prefer the new names. The full table with accepted aliases is in [setting names](../guide/configuration.md#setting-names) |
| Test-method and scoped contracts share one file or directory, set by `queryAudit.contracts.path` | Existing `.query-audit-contracts` files keep working. See [contracts](../guide/contracts.md#configuration) |
| `@ExpectQueries(total = n)` replaces `@ExpectMaxQueryCount`. `@DetectNPlusOne` and query-count baselines are deprecated, not removed | Move to `@ExpectQueries`, `@QueryAudit(failOn = N_PLUS_ONE)`, and [query contracts](../guide/contracts.md). The old annotations keep working through `0.7.x`; see the [deprecation policy](../architecture/api-compatibility.md#deprecation-policy) |
| `QueryContractScope` verifies a contract for one request, job, or journey | None. It is additive |
| A new `1.0` report is not a valid comparison baseline, because the active rules changed | Re-record a comparison baseline with the new version. A 0.6 report and a 0.7 report with different active rules are not equivalent, and the comparator will not treat them as such |

## Changes in 0.7.1 and 0.7.2

These are corrections, not new configuration. Each one changes what an unchanged project reports, so
re-record comparison baselines after taking them.

| Release | Change | What to do |
| --- | --- | --- |
| `0.7.1` | SQL that a server thread runs, as with `RANDOM_PORT` tests, no longer counts toward the test | None, unless you relied on it. If a test's SQL no longer appears, the work now runs outside the audited scope |
| `0.7.1` | A failed scoped contract left the run `PASS` | None. `0.7.2` fails the run, also in a test class with no audit annotation |
| `0.7.1` | A lazy-proxy N+1 changed its finding ID on every run, so a comparison reported it as new and resolved, and `--require-resolved` could call it resolved while it remained | Record new comparison baselines with `0.7.2` |
| `0.7.2` | `@ExpectQueries(exact = true)` makes every declared count exact, so fewer queries fail too | Optional. See [exact counts](../guide/annotations.md#exact-counts) |
| `0.7.2` | Contract counts and budget limits are no longer comparison inputs, so a pull request that re-records a contract compares as `PASS` instead of `INCONCLUSIVE` | None. Review the contract change in the diff |
| `0.7.2` | The console prints only application frames under `Source:`, and a contract that ran fewer queries shows a negative delta | None |

## Checklist

1. Move every QueryAudit module to `0.7.2` together.
2. If you relied on the 0.6 default rule selection, set `profile: strict` or list rule codes.
3. Name background executors in `queryAudit.awaitExecutors`.
4. Replace `@DetectNPlusOne` and `@ExpectMaxQueryCount` with `@ExpectQueries` and contracts. Optional
   now, required before `1.0`.
5. Re-record your comparison baseline report.
6. Run `./gradlew test`. Read the run outcome, not a single finding.

## See also

* [Versions and compatibility](versions.md) — what each release contains.
* [Compatibility changelog](../architecture/api-changelog.md) — per-type API history since `0.5`.
* [Supported public API](../architecture/supported-api.md) — what you may depend on.
