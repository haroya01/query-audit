# Supported public API

This page defines which QueryAudit APIs are supported for 1.0 and which are internal. It is the
answer to "what may I depend on without a patch or minor release breaking me?".

Everything outside the surface below is internal. Internal types stay `public` in Java because the
JVM, the JUnit Platform service loader, or cross-package collaborators need them to be accessible.
That accessibility is not a promise. Each internal package says so in its `package-info.java`, and
the [API compatibility check](api-compatibility.md#how-the-floor-is-enforced) does not protect it.

```text
supported   May be used by application and build code. A patch or minor release will not
            remove or change these signatures.
internal    Implementation detail. No compatibility guarantee at all, in any release.
```

## How to tell the two apart

| Signal | Meaning |
| --- | --- |
| Type appears in this page | Supported |
| Package has a `package-info.java` naming the supported types it contains | Everything else in that package is internal |
| Package or class name contains `internal` | Internal |
| Rule code in the [rule reference](../detections/overview.md) | The behavior is documented; the implementing class is still internal |
| Type is in the compatibility floor, `config/public-api/088f64d.json` | Supported |

The floor is the authoritative machine-readable list. This page is the human-readable contract; if
they ever disagree, the floor is what CI enforces and this page is the bug.

## 1. JUnit 5 annotations and extension behavior

Supported types, all in `io.queryaudit.junit5`:

| Type | Target | Role |
| --- | --- | --- |
| `@QueryAudit` | type, method | Declares the audit and its per-test policy. The only supported way to start an audit. |
| `@ExpectQueries` | method | Read-path budgets per statement type, plus `total` and `exact`. |
| `@ExpectMaxQueryCount` | method | **Deprecated since 0.7.0.** Use `@ExpectQueries(total = ...)`. |
| `@DetectNPlusOne` | type, method | **Deprecated since 0.7.0.** Use `@QueryAudit(failOn = IssueType.N_PLUS_ONE)`. |
| `@QueryAuditExclude` | type, method | Opts a test out of auditing. |
| `@EnableQueryInspector` | type | Enables the inspector UI for a class. |
| `BooleanOverride` | — | `INHERIT` / `TRUE` / `FALSE` tri-state used by tri-state annotation attributes. |
| `QueryAuditExtension` | — | The `Extension` that runs an audit. Referenced by `@ExtendWith`, not normally called. |
| `QueryAuditDataSourceStore` | — | The `ExtensionContext.Store` key for a replaced test `DataSource`. |

Supported annotation attributes, with their defaults:

| Type | Attribute | Default |
| --- | --- | --- |
| `@QueryAudit` | `suppress` | `{}` |
| `@QueryAudit` | `failOn` | `{}` |
| `@QueryAudit` | `failOnKinds` | `{}` |
| `@QueryAudit` | `nPlusOneThreshold` | `-1` (inherit) |
| `@QueryAudit` | `failOnDetection` | `BooleanOverride.INHERIT` |
| `@QueryAudit` | `baselinePath` | `""` |
| `@QueryAudit` | `autoOpenReport` | `BooleanOverride.INHERIT` |
| `@QueryAudit` | `includeSetupQueries` | `false` |
| `@ExpectQueries` | `select`, `insert`, `update`, `delete`, `total` | `-1` (no limit) |
| `@ExpectQueries` | `exact` | `false` (added in 0.7.2) |
| `@DetectNPlusOne` | `threshold` | `3` |

Adding an attribute with a default is a compatible change. Removing an attribute, renaming it, or
removing its default is not, and needs a `BREAKING CHANGE:` footer.

Documented behavior is in [Annotations](../guide/annotations.md) and
[Configuration](../guide/configuration.md#queryaudit-annotation-attributes).

## 2. Spring Boot configuration properties

`io.queryaudit.spring.QueryAuditProperties` and its nested types are supported. The contract is the
set of bindable `query-audit.*` keys, not the JavaBean accessors, which exist for binding.

Supported keys: `enabled`, `failOn-detection`, `mode`, `profile`, `enabled-rules`,
`disabled-rules`, `severity-overrides`, `suppress-patterns`, `suppress-queries`, `baseline-path`,
`auto-open-report`, `max-queries`, `await-executors`, `report.*`, `contracts.*`, and one nested
block per optional rule (`n-plus-one`, `offset-pagination`, `or-clause`, `large-in-list`,
`too-many-joins`, `excessive-column`, `repeated-insert`, `repeated-update`, `write-amplification`,
`slow-query`, `count-instead-of-exists`, `connection-held-idle`, `wrap-datasource`).

Rules:

* A new key or a new nested block may be added in a minor release. Absent means the documented
  default, so existing configuration keeps working.
* Removing or renaming a key, or changing a documented default, is a breaking change.
* Spring Boot relaxed binding means `query-audit.report.output-dir` and
  `queryAudit.report.outputDir` are the same setting. One setting name per key is the rule, and the
  full table with earlier accepted aliases is in
  [Setting names](../guide/configuration.md#setting-names).

`QueryAuditAutoConfiguration` is internal. Do not import it or depend on its bean methods.

## 3. Custom rule and metadata-provider SPIs

Custom rules, in `io.queryaudit.core.extension`:

| Type | Role |
| --- | --- |
| `AuditRule` | The rule contract: `descriptor()` and `evaluate(RuleContext)`. |
| `RuleDescriptor` | Rule identity, version, finding kinds, effective settings. |
| `RuleId`, `FindingKindId` | Validated identifiers. Custom IDs must be namespaced; built-in finding-kind codes are also accepted. |
| `RuleContext` | The evidence handed to a rule, including captured SQL. |
| `AuditExtensions` | Registration, with `AuditExtensions.Builder`. |
| `AuditRuleException` | Rule failure, with a `Reason`. |
| `AuditRuleTestKit` | Test support for rule authors. |
| `LegacyDetectionRuleAdapter` | Adapter for the older `DetectionRule` interface. |

`io.queryaudit.core.extension.internal` is internal and excluded from the floor.

Database and plan analysis SPIs:

| Type | Role |
| --- | --- |
| `io.queryaudit.core.detector.DetectionRule` | The original rule interface. `evaluate(List<QueryRecord>, IndexMetadata)`. |
| `io.queryaudit.core.detector.QueryAuditAnalyzer` | Runs a rule set. |
| `io.queryaudit.core.analyzer.IndexMetadataProvider` | Contributes index metadata for a database. Discovered by `ServiceLoader`. |
| `io.queryaudit.core.analyzer.ExplainAnalyzer` | Contributes plan analysis for a database. Discovered by `ServiceLoader`. |
| `io.queryaudit.core.analyzer.ExplainAnalysisException` | Plan-analysis failure, with a `Reason`. |

Supported: implement these, return the documented types, and register through
`META-INF/services`. Not supported: subclassing the built-in rules, or importing
`io.queryaudit.core.parser`, `io.queryaudit.core.detector` rule classes, `JpaIndexScanner`,
`IssueFingerprintDeduplicator`, or anything in `io.queryaudit.core.provenance`.

The model types a rule reads and returns are supported: everything in
`io.queryaudit.core.model`, including `Finding`, `Issue`, `IssueType`, `Severity`, `QueryRecord`,
`IndexInfo`, `IndexMetadata`, `AuditRunResult`, and `AuditOutcome`.

## 4. CLI commands and exit codes

One command, on `query-audit-core`:

```bash
java -cp query-audit-core-<version>.jar \
    io.queryaudit.core.reporter.ReportComparator before.json after.json [verdict.json] \
    [--require-resolved <findingId>]...
```

| Exit code | Meaning |
| --- | --- |
| `0` | The comparison outcome is `PASS`. |
| `1` | The comparison outcome is `FAIL`: at least one new confirmed finding. |
| `2` | The comparison outcome is `INCONCLUSIVE`, **or** the command could not run: bad arguments, unreadable input, or an unwritable verdict file. |

Exit code `2` deliberately covers both "the comparison could not be trusted" and "the tool could
not run". Treat `2` as "do not gate on this run": it is never a pass.

Arguments, the `--` positional terminator, and `--require-resolved=<findingId>` are supported. The
one-screen summary format and the verdict JSON are **not** a stable interface; parse `verdict.json`,
not stdout.

Programmatic equivalents in `ReportComparator` are supported: `compare`, `toJson`, `toSummary`, and
`main`. Use the final `Verdict.outcome()` or the process exit code, never a single boolean.

## 5. Report and contract schemas

These are machine-readable contracts, versioned independently of the library version.

**`report.json`.** The envelope is validated against
[`report.schema.json`](../schema/report.schema.json). Every schema version has its own file in
`docs/schema/`, and `JsonReporter.SCHEMA_VERSION` is the source of truth for the version the current
library writes.

| Schema | Added |
| --- | --- |
| 1.0 | Per-test reports, no run outcome. |
| 1.1 | Run outcomes (`outcome`, `incompleteReasons`). |
| 1.2 | Stable test identities (`testId`). |
| 1.3 | Query-evidence retention counts. |
| 1.4 | Report redaction mode. |
| 1.5 | Expected-test coverage (`coverage`). |
| 1.6 | Per-test comparison inputs (`comparisonInputs`). |
| 1.7 | Stable finding IDs (`findingId`) and `occurrences`. |
| 1.8 | `contractViolations` on the run envelope. |

Compatibility rules for readers:

* Within 1.x, a field may be **added** and the schema version bumped. Readers must ignore unknown
  fields and must not require a field they have seen to still be there.
* A field may be **removed or retyped** only in a 2.x schema, which would be a major release.
* `schemaVersion` is semver, so a consumer can detect incompatible input instead of misparsing it.
* A 1.0 report has no run outcome. The comparator treats it as `INCONCLUSIVE` rather than inferring
  a pass.

**Query contracts.** `QueryContracts`, `QueryCountBaseline`, and `QueryCounts` are supported. The
recorded file format, `.query-audit-contracts`, is **not** covered by the Java floor: it is
human-reviewable text, it gains fields as rules need them, and an older library may reject a newer
file. Keep the library version that recorded a contract, and review contract changes in the diff.

## What is deliberately not supported

| Area | Why |
| --- | --- |
| 64 built-in rule classes in `io.queryaudit.core.detector` | Their behavior is documented by rule code; the classes are implementation and change as rules evolve. |
| `io.queryaudit.core.parser` | Shared parse support. Use the SQL given to your rule. |
| JDBC proxy internals in `io.queryaudit.core.interceptor` | Only `QueryCaptureSession` is user-facing. |
| `io.queryaudit.core.provenance` | Tracks report-schema internals; read the report instead. |
| `io.queryaudit.core.baseline`, `dedup`, `identity`, `ranking` | Internal report and identity machinery. |
| `QueryAuditAutoConfiguration` | Internal Spring wiring. |
| `AuditCoverageListener` | Public only because the JUnit Platform loads it by service. |
| Report HTML and console layout | Presentation, not contract. |

## See also

* [API compatibility policy](api-compatibility.md) — how the floor is enforced, and how deprecations work.
* [Compatibility changelog since 0.5](api-changelog.md) — what changed, and whether it breaks you.
* [Upgrading to 0.7.0](../getting-started/migrating-0.7.md) — behavior changes in the current release line.
* [Custom Extensions](../guide/extensions.md) — writing a rule.
