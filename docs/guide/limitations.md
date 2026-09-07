---
title: Limitations
description: Choose a supported audit scope and handle the cases that need extra verification.
---

# Limitations

QueryAudit is most useful when a repeatable database test exercises the behavior you want to keep:
a bounded number of reads, no unexpected writes, or a query shape you have reviewed. Start with
one known query, verify capture, and promote the checks you trust into contracts.

## Choose the right audit scope

| If your test uses… | What to expect | Practical approach |
|---|---|---|
| MySQL or PostgreSQL features | Metadata and plans come from the test database | Use the same database engine and schema for those checks. H2 is useful for capture and portable SQL checks |
| Small or uniform fixtures | Plans and repeated-access patterns can differ from larger or skewed data | Add representative sizes and associations. A passing test does not establish production latency, throughput, or lock behavior |
| SELECT bind parameters | The built-in EXPLAIN adapters cannot replay captured bind values/types; parameterized SELECTs make enabled EXPLAIN analysis incomplete | Use query budgets and supported SQL/metadata checks for this scope. Evaluate plans separately with real parameter values; do not replace placeholders with invented values |
| Several Spring `DataSource` beans | The extension resolves one source by type | Make the audited source unambiguous, normally with `@Primary`, and ensure the code under test uses it. Wrapping every bean does not mean every source is audited |
| An existing datasource-proxy | The starter can use a query-aware source without adding another wrapper | Set `query-audit.wrap-data-source.enabled=false` only when that proxy is already present; see [Spring setup](../getting-started/spring-boot.md) |
| Dynamic tests from `@TestFactory` | There is no supported per-child audit boundary | Use ordinary or parameterized tests for audited cases, or exclude the factory |
| Concurrent or asynchronous work | `0.6.0` rejects concurrent audited methods; asynchronous attribution is not a supported contract | Run audited methods on the same thread and keep the audited SQL on that execution path; see [execution settings](troubleshooting.md#parallel-capture-is-incomplete) |

The default `recommended` profile excludes the three built-in EXPLAIN rules (`full-scan`,
`filesort`, and `temporary-table`). If you enable a broader profile but intentionally exclude
EXPLAIN, keep these rules disabled in both comparison runs. The resulting contract covers the
remaining enabled checks, not execution-plan verification. An unexpected `INCONCLUSIVE` needs
investigation; treating it as `PASS` removes that distinction.

Query budgets count captured statements, not returned or affected rows. A one-statement query
can still read or modify many rows. Keep functional assertions and representative fixtures beside
the budget.

## Reported cases to check

The following issues were reproduced on the integration checkout `34d6e27`, whose Gradle version
is `0.6.0`. That checkout contains changes after the published release. These reports do **not**
establish that the same behavior was reproduced against the Maven Central artifact. Use each
issue's version and reproduction details when checking your setup; these links do not imply that a
fix is included in `0.6.0`.

| Workflow | Reported behavior | Check for your setup |
|---|---|---|
| Spring context replacement | Capture can remain attached to an old context ([#287](https://github.com/haroya01/query-audit/issues/287)) | Use a stable context for the initial audit. If context replacement is required, verify capture after every replacement before trusting a zero-query result |
| Hibernate batch loading or large lazy-load fixtures | A batch fetch can be reported as N+1, and lazy-event recording has excessive allocation ([#289](https://github.com/haroya01/query-audit/issues/289), [#295](https://github.com/haroya01/query-audit/issues/295)) | Start with bounded fixtures and inspect actual SQL counts. Validate batch-fetch findings before making them fatal |
| SQL safety and result-size findings | Some literal/comment keywords, clause boundaries, and inferred row bounds can cause misses or false positives ([#288](https://github.com/haroya01/query-audit/issues/288), [#291](https://github.com/haroya01/query-audit/issues/291), [#292](https://github.com/haroya01/query-audit/issues/292)) | Add a known violating control for the SQL shape you gate. Use explicit write budgets where the contract forbids writes, and assert affected/result rows separately |
| Joined-query EXPLAIN analysis | MySQL may omit later plan rows; PostgreSQL may attribute a nested node to the wrong table ([#293](https://github.com/haroya01/query-audit/issues/293), [#294](https://github.com/haroya01/query-audit/issues/294)) | Inspect the complete native plan before acting on an index or scan finding |
| CI comparison and HTML review | HTML can omit an incomplete verdict, INFO visibility can invalidate comparisons, and method links may not target the right section ([#296](https://github.com/haroya01/query-audit/issues/296), [#297](https://github.com/haroya01/query-audit/issues/297), [#298](https://github.com/haroya01/query-audit/issues/298)) | Gate on the canonical JSON verdict and verify expected coverage; keep comparison settings identical. Open the class page directly for human review |
| Saving a finding baseline through the core API | A filename-only relative save path can fail ([#299](https://github.com/haroya01/query-audit/issues/299)) | Supply an absolute path or an explicit parent directory |

## Shared annotation policies

Use direct `@EnableQueryInspector` / `@QueryAudit` and budget annotations for the first audit.
A composed/inherited activation gap was reported in
[#290](https://github.com/haroya01/query-audit/issues/290) on the integration checkout described
above. It has not been reproduced here against the published artifact, and these docs do not
claim a shipped fix. Before sharing policies through custom annotations or inheritance, require
an intentional budget failure through the exact declaration your tests will use.

## Reports and shared CI logs

The default redaction policy applies to machine-readable JSON and GitHub Actions annotations.
It does **not** redact the console or HTML report. Console diagnostics are still printed when JSON
is selected. Use synthetic test values and review log/artifact access before sharing them.
Extending the policy to human-readable output is tracked in
[#300](https://github.com/haroya01/query-audit/issues/300); see the
[current redaction contract](reports.md#machine-report-redaction).

For reliable CI evidence, select JSON, check its outcome, and verify
[expected-test coverage](audit-coverage.md). Keep separate output and policy files for separate
test JVMs unless your build coordinates their writes. The
[CI guide](ci-cd.md) describes how to pass settings into the forked test process.
