# Compatibility changelog since 0.5

What changed in the supported public surface from `0.5.0` to the current release line, and whether it
breaks you. Organised by API rather than by release, for consumers who need to know when a specific
type changed.

Every entry was produced by diffing the compiled `query-audit-core` and `query-audit-junit5` classes
of each release tag with the project's own snapshot tool,
`.github/scripts/snapshot_public_api_from_git.py`, using one scope definition so the comparison is
like-for-like. Releases: `0.5.0` (`0588528`), `0.6.0` (`4b06c85`), `0.6.1` (`6af0e96`), `0.7.0`
(`d47f441`), `0.7.1` (`9dabf13`), `0.7.2` (`3155277`).

## Summary

| Release | Types added | Types removed | Members removed | Source compatible? |
| --- | --- | --- | --- | --- |
| `0.6.0` | 13 | 0 | 2, both on `LazyLoadTracker` | Yes, with one binary break (below) |
| `0.6.1` | 1 | 0 | 0 | Yes |
| `0.7.0` | 28 | 0 | 0 | Yes |
| `0.7.1` | 0 | 0 | 0 | Yes |
| `0.7.2` | 1 | 0 | 0 | Yes |

No supported type was ever removed or renamed. The only removals are the two Hibernate SPI methods
on `LazyLoadTracker` in `0.6.0`.

## `ReportComparator.Finding` and the `testId` component

`ReportComparator.Finding` is a record. Adding a component changes its canonical constructor and its
accessors, which is a binary change; QueryAudit kept the earlier constructors so source and binary
compatibility hold.

| Release | Change | Constructors available after |
| --- | --- | --- |
| `0.5.0` | 6 components: `testClass`, `testName`, `type`, `table`, `detail`, `key` | 6-arg |
| `0.6.0` | `String testId` added as the first component; `testId()` accessor added | 7-arg and the retained 6-arg |
| `0.6.1` | `String findingId` and `String column` added; `findingId()` and `column()` accessors added | 9-arg, 7-arg, and the retained 6-arg |

The 6-argument constructor sets `testId`, `findingId`, and `column` to `null`. The retained
constructors are annotated in the source as keeping the `0.5` signature.

`ReportComparator.TestRef` is different from how it is often described: it **did not exist in
`0.5.0`**. It was added in `0.6.0` with three components, `testId`, `testClass`, `testName`, plus a
two-argument compatibility constructor. `testId` was not added to an existing `TestRef`; the whole
type arrived with it.

| Release | `TestRef` |
| --- | --- |
| `0.5.0` | Did not exist. `Verdict` had no test references. |
| `0.6.0` | Added: `testId`, `testClass`, `testName`, plus a 2-arg constructor. |

Both changes arrived with report schema **1.2**, which added stable test identities.

### Behavioral effect to know about

Records derive `equals`, `hashCode`, and `toString` from their components, so a `Finding` built
through the retained 6-argument constructor is **not** equal to one built with an explicit `testId`,
and its `toString()` differs. Code that compares or caches `Finding` values across versions should
compare the fields it cares about, or key on `findingId()` from `0.6.1` onward. This is a behavior
change, not a signature change, so the floor cannot catch it.

## The report outcome envelope

`0.5.x` wrote a schema `1.0` report with per-test results and no run-level verdict. A reader could
not tell a clean run from a run that never collected anything.

`0.6.0` introduced the outcome envelope, report schema `1.1`:

| Type | Purpose |
| --- | --- |
| `AuditOutcome` | `PASS`, `FAIL`, `INCONCLUSIVE`, with the precedence `INCONCLUSIVE > FAIL > PASS`. |
| `AuditIncompleteReason` | One reason a run is incomplete, with a stable `code` and a nullable `detail`. |
| `IncompleteReasonCode` | The closed set of reason codes. |
| `AuditRunResult` | The run-level record: reports, outcome, reasons, coverage, comparison inputs. |
| `AuditCoverage`, `AuditCoverage$Test`, `AuditCoverage$Gap` | Expected-test coverage, added for `0.6.1` coverage checking. |
| `QueryEvidenceStatus`, `TestSelector` | Supporting types for retained evidence and test selection. |

On `ReportComparator.Verdict`, `0.6.0` added `outcome()`, `incompleteReasons()`, `missingTests()`,
`unexpectedTests()`, `inputDifferences()`, and `complete()`, and grew the canonical constructor from 6
to 10 components. **Every earlier constructor arity was retained**, including the original 6-argument
one, so existing construction sites compile and link unchanged.

Later envelope growth on `Verdict`:

| Release | Added |
| --- | --- |
| `0.6.1` | `findingIdentity()`, `ReportComparator.FindingIdentity` |
| `0.7.0` | `targetResolutions()`, `allTargetsResolved()`, `comparisonComplete()`, `noNewRegressions()` |

On `JsonReporter`, `0.6.0` added `toRunEnvelopeJson(AuditRunResult)`, a redaction-aware
`toJson(QueryAuditReport, ReportRedaction)`, `toEnvelopeJson(List, ReportRedaction)`, and redaction
constructor overloads. `JsonReporter.toEnvelopeJson(List<QueryAuditReport>)` is deprecated: it emits
a legacy `1.0` envelope that cannot express a run outcome. Use
`toRunEnvelopeJson(AuditRunResult)`.

`0.5.x` reports stay readable. The comparator treats a `1.0` report as `INCONCLUSIVE` rather than
inferring a pass, and exit code `2` is the observable result.

## Other additions by release

### `0.6.0`

| Type | Note |
| --- | --- |
| `ExplainAnalysisException`, `ExplainAnalysisException$Reason` | Plan-analysis failure contract. |
| `QueryCaptureSnapshot` | Capture snapshot. Later internal; see below. |
| `AuditRunResult` and the outcome types above | The envelope. |
| `ReportComparator$TestRef` | Test references, with `testId`. |

### `0.6.1`

| Type | Note |
| --- | --- |
| `ReportComparator$FindingIdentity` | Declares whether findings were matched by recorded identity, by legacy shape, or not at all. |

### `0.7.0`

The largest release for the surface, all additive. It added the custom rule SPI
(`AuditExtensions`, `AuditExtensions$Builder`, `AuditRule`, `AuditRuleException`,
`AuditRuleException$Reason`, `AuditRuleTestKit`, `FindingKindId`, `LegacyDetectionRuleAdapter`,
`RuleContext`, `RuleDescriptor`, `RuleId`), scoped contracts (`QueryContractScope`,
`QueryContractScope$ContractCapture`, `CapturedQueries`), report delivery
(`AuditReportPublisher`, `AuditReportSink`, `PublicationResult`, `PublicationResult$Delivery`,
`PublicationResult$Status`, `PublishedAuditRun`, `ReportSinkRegistration`), the unified finding model
(`Finding`, `AuditFindings`), `QueryCaptureSession`, `QueryCaptureSession$Scope`,
`ReportComparator.FindingId`, and `ReportComparator.TargetResolution` with its `Status`.

It also added `ReportComparator.compare(String, String, Collection)` for explicit
`--require-resolved` targets, and `@ExpectQueries.total()`.

### `0.7.1`

No supported type changed. The only addition anywhere in scope was
`LazyLoadTracker.getDroppedEventCount()`, on a type that is internal as of the 1.0 floor.

### `0.7.2`

| Type | Note |
| --- | --- |
| `QueryContractViolation` | A failed query contract in the run envelope. |
| `@ExpectQueries.exact()` | Makes every declared count exact. Added with a default, so compatible. |

## The one removal: `LazyLoadTracker`, `0.6.0`

`LazyLoadTracker` implemented two Hibernate interfaces and exposed their methods:

```text
-  implements org.hibernate.event.spi.InitializeCollectionEventListener
-  implements org.hibernate.event.spi.PostLoadEventListener
-  public void onInitializeCollection(InitializeCollectionEvent)
-  public void onPostLoad(PostLoadEvent)
```

`0.6.0` removed all four so the extension no longer forces Hibernate classes onto the classpath.
Callers of those two methods, and any implementer of those interfaces, must move to the recording
methods added in the same release: `recordProxyResolved`, `recordCollectionInitialized`,
`recordExplicitLoad`, `captureApplicationStack`, `isProxyResolution`, and `hasFindByIdInStack`.

This break predates the compatibility floor, so no check existed to catch it. `LazyLoadTracker` is
**not** on the 1.0 supported surface and its remaining members have no guarantee; see
[Supported public API](supported-api.md#3-custom-rule-and-metadata-provider-spis).

## Unreleased on this branch

Not part of any release, recorded here because it is the kind of change the floor exists to catch.

| Change | Effect |
| --- | --- |
| `AuditRunResult` gained a `contractViolations` component | Removed the 3-argument constructor and the 5-argument constructor ending in `Map<String, ComparisonInputs>`. **Binary and source breaking** for anyone constructing `AuditRunResult` directly. |
| `JsonReporter.SCHEMA_VERSION` `1.7.0` to `1.8.0` | Additive. Schema `1.8` adds `contractViolations` to the run envelope. |
| `SqlParser` gained `hasOuterJoinClause` and `hasOuterUsingClause` | Additive, and unconstrained: `SqlParser` is internal. |

The `AuditRunResult` break is a judgement call for the maintainer: restore the removed constructors,
or accept it and re-record the floor with the reason. The floor is currently recorded after this
change, so it does not fail on it.

## What is not covered

| Area | Why |
| --- | --- |
| Internal types | Not in the floor by design. A change to `io.queryaudit.core.parser.SqlParser` between releases is invisible here and unconstrained. |
| Behavior | The floor compares JVM descriptors only. Any supported method may change what it does in a bug fix. |
| Report schema contents | Versioned by `schemaVersion`, not by the Java floor. See [Supported public API](supported-api.md#5-report-and-contract-schemas). |
| Query contract file format | Human-reviewable text that gains fields; not a Java contract. |
| Configuration defaults | Covered by documentation review, not by a signature check. |

## See also

* [Supported public API](supported-api.md) — what the surface is.
* [API compatibility and deprecation policy](api-compatibility.md) — how it is enforced.
* [Upgrading to 0.7.0](../getting-started/migrating-0.7.md) — behavior changes needing action.
