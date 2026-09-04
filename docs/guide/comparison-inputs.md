# Comparison inputs

A finding disappearing from a report does not always mean the query was fixed. The candidate
could have disabled a rule, raised a threshold, added a suppression, or lost access to index
metadata. QueryAudit checks the effective inputs of both runs before it reports a resolved finding.

Schema 1.6 adds `comparisonInputs` to the suite envelope. It is an object keyed by stable test ID,
because different test classes can use different audit settings. The JUnit extension captures the
inputs used by each audit automatically. This metadata is retained even when raw query evidence
is compacted from a large suite report.

## What is recorded

| Field | Meaning |
|---|---|
| `queryAuditVersion` | Version of the loaded QueryAudit artifact. This is separate from the JSON schema version. |
| `profile` | Effective rule profile for this test. |
| `databaseDialect` | Database product identified through JDBC metadata. |
| `parser.name`, `parser.version` | Structural parser and its loaded dependency version. |
| `detectorCapabilities` | Active detector implementation identities, sorted for deterministic output. |
| `detectorInputsComplete` | Whether the inputs of every active detector can be fully identified. |
| `capabilities` | Availability, source identity, and input completeness for index metadata, Hibernate events, EXPLAIN, and repository return-type resolution. |
| `fingerprints` | SHA-256 digests of the effective rule settings, thresholds, suppressions, loaded query contracts and query-count policies, and finding baseline. |

Fingerprint inputs are canonicalized before hashing, including collection order and null values.
The report contains the digest, not policy file contents, raw environment values, or absolute
policy paths. Moving an unchanged policy file does not itself change its fingerprint. Changing
its effective contents does.

Rule-setting fingerprints include explicit rule selection, severity overrides, audit mode,
capture limits, setup-query handling, and policy behavior such as `failOnDetection`. Threshold
fingerprints include limits that can change which findings appear. Query-policy fingerprints
include inline expectations and recording modes as well as loaded file contents. A report's
format or output directory is not an analysis input; redaction compatibility is checked separately.

The capability entries have this shape:

```json
{
  "state": "ABSENT",
  "source": "none",
  "inputsComplete": true
}
```

`ABSENT` means that capability was not provided for this audit, for example Hibernate tracking
in a plain JDBC test. `AVAILABLE` identifies a capability that can contribute evidence. `FAILED`
means an enabled capability failed; it must not be treated as an intentional absence.

An initialization or execution failure makes the standalone run `INCONCLUSIVE`, with
`CAPABILITY_INITIALIZATION_FAILED` or `CAPABILITY_EXECUTION_FAILED`. Partial findings can remain
in the report. The bundled MySQL and PostgreSQL EXPLAIN analyzers reject SQL containing `?`
because captured SQL does not include typed bind values. This conservative check also rejects a
question mark inside a literal, comment, or database-specific operator. The analyzers do not
substitute guessed values and claim a usable plan. For SQL without `?`, each distinct captured
statement is explained as written; different literal values are not collapsed into one plan.
Such an enabled analysis failure is incomplete and must be investigated.

When `full-scan`, `filesort`, and `temporary-table` are all disabled, the extension skips the
bundled EXPLAIN analyzers. Custom analyzers still run because they may emit other finding types.
The recommended and minimal profiles disable all three bundled rules by default. Selecting strict or explicitly enabling
one of these rules enables the analysis, so its failures affect the outcome.

## Which comparisons are accepted

QueryAudit takes a conservative approach: all recorded inputs affecting findings must agree for
the compared tests. This includes library and parser versions, profiles, dialects, detector lists,
capability sources and states, and every fingerprint. A version change requires a new baseline
even when it is a patch release. Enabling a stronger profile or adding a capability also requires
review; the comparator does not guess whether a configuration change is harmless.

These changes do not by themselves invalidate a comparison:

- Moving unchanged policy files or changing an output directory.
- Reordering inputs whose canonical form is the same, such as a set of suppression patterns.
- Changing display names while stable test IDs remain the same.
- Compacting retained query records without changing findings or their effective inputs.

Different known inputs produce `INCONCLUSIVE` with `INCOMPATIBLE_AUDIT_INPUTS` and an empty
`resolved` list. The verdict's `inputDifferences` identifies the affected test and fields:

```json
{
  "testId": "[engine:junit-jupiter]/[class:com.example.OrderServiceTest]/[method:findRecentOrders()]",
  "field": "profile",
  "baseline": "recommended",
  "candidate": "minimal"
}
```

Values in this list are recognized versions and controlled names, booleans represented as
strings, or fingerprints. Capability sources and other custom values are fingerprinted in the
verdict; SQL, file paths, policy contents, and exception messages are not copied into the
difference details. `testId` can be `null` for a run with no audited test identity.

After an intentional input change, run the baseline and candidate under the same reviewed
configuration. Do not accept a new baseline merely to make the gate green: review the changed
profile, thresholds, suppressions, and policies first. The command and its exit-code contract are
documented in [Delta verdict](reports.md#delta-verdict-compare-two-runs).

## Missing or custom inputs

An empty `comparisonInputs` object means the run did not identify its comparison inputs. A
missing entry for a compared test has the same meaning. Neither implies default settings. A
standalone report can still be `PASS`, but comparing it produces `COMPARISON_INPUTS_UNAVAILABLE`
and `INCONCLUSIVE`, even if both reports are missing the same metadata.

Reports from schemas before 1.6 also lack verified inputs. QueryAudit can read their findings for
inspection, but they cannot establish a successful comparison. Re-run an archived baseline with
the current library and the intended configuration instead of adding guessed metadata to old
JSON. Malformed metadata or unsupported fields are rejected; the comparison CLI exits with code
`2`.

Custom detectors and analysis providers may depend on private settings that QueryAudit cannot
inspect. Their code identity alone is insufficient. If `detectorInputsComplete` or a capability's
`inputsComplete` is `false`, the comparison remains unverified even when both flags and source
identities match. The custom analysis can still run and contribute findings to a standalone report.
A failed capability is never comparable, even when both runs show the same failure.

Core-only integrations can attach known inputs through
`AuditRunResult.withComparisonInputs(Map<String, ComparisonInputs>)`. Use the actual effective
settings and loaded policies; do not set completeness flags to `true` for unobserved custom
configuration. The original run-result constructors remain available and default to an empty
input map.

Metadata identifiers can expose test, package, class, and implementation names. Keep secrets out
of these names. Custom identities containing path or SQL syntax are emitted as deterministic
hash tokens, but hashing is not encryption and does not make a public artifact anonymous.

## Keep coverage and test controls

Input compatibility does not replace [audit coverage](audit-coverage.md). The coverage manifest
checks that expected tests actually supplied audit evidence; comparison inputs check whether
the reported tests used compatible analysis conditions. Keep both the standalone JSON gate and
the comparison gate in CI.

This metadata does not fingerprint the entire database, fixture data, application source tree,
or execution environment. Use controlled test fixtures and review schema/data changes when
interpreting a delta. Matching inputs establish the audit configuration contract, not proof that
every external condition was identical.
