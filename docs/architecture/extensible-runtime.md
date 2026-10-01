# Extensible runtime: implementation and ownership

The four agreed implementation slices are present in the development source. They are additive
APIs, not a release announcement. See [Custom Extensions](../guide/extensions.md) for examples,
deliberate limits and the distinction between legacy and unified result views.

## Design rule

A company-specific rule or run output should not require editing QueryAudit or subclassing its
JUnit extension. Keep extension interfaces small, host policy explicit, and adapters responsible
for framework lifecycle. Do not introduce a public interface for every internal helper.

## Responsibilities

| Owner | Responsibility | Does not own |
| --- | --- | --- |
| `AuditExtensions` | Immutable registrations, assembly identity | Execution, capture or bean destruction |
| `AuditExtensionResolver` | One catalog per JUnit scope | Policy or database selection |
| `DatabaseProviders` | Explicit matching provider, lazy legacy fallback | Connections or findings |
| `AuditSettingsResolver` | Annotation discovery, effective configuration and overrides | Resource cleanup or verdict |
| `AuditResources` | Scope-owned capture hooks/tracker/holder cleanup and partial rollback | Application DataSources or Spring beans |
| `QueryCaptureSession` / `InvocationCapture` | Per-invocation evidence and lexical context / JUnit Store ownership | Shared application state or scheduling |
| `DataSourceHooks` / `HibernateListenerLeases` | Shared hook registration and last-owner release | Per-test evidence or user resource destruction |
| `FindingFailurePolicy` | Built-in/custom kind failure selection and its fingerprint | Rule execution or presentation |
| `AuditScope` / `AuditScopeInitializer` | Framework store inheritance / owned initialization | Rule policy or output formatting |
| `AuditTestAnalysis` | Per-test capture-to-report orchestration | Scope ownership or transport |
| `AuditAssertions` | Query budgets/contracts and test failure messages | Detection, publication or suite state |
| `QueryAuditAnalyzer` + `FindingPolicy` | Run rules, apply host selection/suppression/severity/baseline | Framework stores or output |
| `AuditRuleRuntime` | Snapshot/validate descriptors, discovery, conflicts and result contract | Trust certification or arbitrary verdict hooks |
| `AuditReportPublisher` | Restricted projection and independent delivery results | Changing analysis facts |
| `SuiteAuditState` | Canonical run evidence and INFO retention | Human output visibility or query retention overrides |
| `SuiteReportFinalizer` / `AuditReportArtifacts` | Once-only finalization / local artifact writing | Closing user implementations |
| `SqlTableReferences` | Shared lightweight alias lookup | A detector's index policy |
| `SqlParserRouting` | One AST/fallback/null/length policy | SQL grammar or dependency-error masking |
| `SqlAstEqualities` / `UniqueIndexEquality` | Required scalar equality evidence / complete unique-key proof | Query execution or speculative single-row assumptions |
| `ComparisonEnvelopeReader` | Schema validation into immutable typed values | Comparison decisions |
| `ReportComparisonEngine` / `ReportVerdictWriter` | Typed comparison / verdict output | JSON input traversal |
| `AuditFindings` / `ReportFindings` | Complete public finding view / legacy compatibility | Classification policy |

The JUnit entry point retains public constructors and delegates lifecycle work to named owners.
A few old internal test seams remain short delegations; they are not public extension points.
New tests target the named owners or real framework execution. Internal collaborators are
concrete and package-private where possible. SQL grammar branches are kept when they describe
real grammar, rather than replaced with indirect dispatch solely to lower branch counts.

## 1. Registration and defaults

Spring assembles beans in bean-name order. A user catalog replaces automatic assembly; custom
config/interceptor beans make defaults back off. Plain Java, static `@RegisterExtension`, and
Spring/JUnit consume the same catalog. Built-ins remain enabled according to profile and policy.
New `AuditRule` providers can also use ServiceLoader with deterministic class-name ordering.

Explicit matching metadata/EXPLAIN providers precede legacy discovery. Multiple matching explicit
providers, duplicate active open rule IDs/kind ownership, and active explicit/discovered duplicate
classes are configuration errors. Proxy equivalence is not guessed. Replacing behavior is explicit:
disable its policy kind and register the desired implementation/kind.

## 2. Lifecycle and settings

Settings discovery and merging live in one resolver while preserving existing method/class,
Spring, system-property and default precedence. Existing annotation replacement semantics remain.
Direct count/contract assertions remain separate from finding suppressions and baselines.

One resource holder owns installed hooks, tracker state and thread-local cleanup. Initialization
failure and normal closure use the same idempotent cleanup path; later cleanup failures are
suppressed on the primary failure. Application-owned resources are not closed by the catalog.

`AuditScope` stores and reads the single typed `AuditResources` holder; it does not duplicate its
members under string keys. `LegacyAuditResourceStore` confines read-only compatibility with older
internal stores. Per-root `SuiteAuditState` owns retained reports, so consecutive Launcher runs
do not require a global report reset. Concurrent classes/methods use isolated capture sessions.
Source listeners route callbacks to the bound invocation, pin SQL ownership at execution start,
and retain ordinary standalone `start`/`stop` behavior outside scoped capture. Shared DataSource
registrations use copy-on-write listener lists; shared Hibernate registries have one leased listener
and lazy-event router. Neither cleanup can detach another active owner's hooks.
Explicit `QueryCaptureSession.wrap` propagates executor work. Unowned work is adopted by the only
active capture; with several active captures, and for unfinished work, it fails completeness. See the [parallel contract](../guide/extensions.md#parallel-capture).

## 3. Open findings and host policy

| Identity | Meaning |
| --- | --- |
| Catalog registration ID | How the host assembled an extension |
| `RuleId` | Versioned implementation contract |
| `FindingKindId` | Namespaced category for selection, severity, suppression and baseline |
| `FindingId` | Host-generated logical finding identity in one test |

`AuditRule` receives a readonly `RuleContext` and returns `Finding` values. Its immutable
descriptor declares supported kinds, behavior version and effective non-secret settings. The
runtime rejects descriptor mutation, null results, undeclared kinds and reserved namespaces.
`LegacyDetectionRuleAdapter` bridges existing rules; `Issue`, `IssueType` and existing
constructors are retained. `FindingPolicy` is the single classification path for both models.

Report collection inputs are snapshotted and copies preserve both legacy and custom findings.
`report.getFindings()` is the recommended complete view; old enum-based getters remain legacy-only.
`ReportFindings` confines legacy conversion and nullable-list compatibility. Conversion of a
custom kind to an unrelated enum is never used. JSON groups observations under stable IDs; HTML,
console and GitHub Actions output include custom findings. The 1.7 schema already permits custom
type strings, so no incompatible shape change is needed.

Console, HTML and GitHub Actions render the unified findings buckets through one row path. Stable
finding IDs are visible for built-in and custom rows, including acknowledged and compact rows.
`LegacyFindingPresentation` isolates built-in descriptions, impact scoring and old HTML checkbox
identities. The existing HTML `data-key` and report hash stay compatible with saved checkbox state.

Baseline, suppression and comparison use namespaced kind codes. The integrated #210 comparator
accepts repeatable `--require-resolved`, distinguishes resolved/new/persisting facts, and treats
INFO/acknowledged targets as still persisting. Missing tests, weak/unknown inputs, incomplete runs
and incompatible reports cannot manufacture a resolved target. CLI parsing and target selection
are separated from comparison. The reader validates schema evolution once into typed snapshots;
the comparison engine performs no untyped JSON lookup, and the writer owns output formatting.

Canonical run evidence is independent of human-facing INFO visibility. Hiding informational rows
does not delete targets from machine JSON or sink summaries, and does not bypass query-evidence
retention limits. Open rules also evaluate empty captures; legacy empty-capture behavior stays
compatible. Repeated logical findings are assigned one conservative effective-severity bucket.

Rule implementation bytes, version, declared kinds and settings contribute a host-generated
fingerprint; raw settings are not serialized. Delivery transport does not change analysis identity.
Hidden custom inputs remain unverified: a descriptor is not a proof of dependency closure.
Compatibility differences expose a typed changed/unavailable classification shared by consumers.

## 4. Publication and author tests

`AuditReportSink` receives `PublishedAuditRun`, a separate immutable, strongly minimized JSON
summary. It exposes IDs, enums and counts, not SQL, names/selectors, source locations, arbitrary
diagnostics, config or exceptions. FULL local report settings do not weaken this boundary.

`PublicationResult` keeps analysis outcome and required/optional delivery statuses separate.
Ordinary sink exceptions are isolated; later sinks are attempted. Interruptions are restored,
remaining deliveries become NOT_ATTEMPTED, and VM errors propagate. JUnit required-delivery
failure fails suite publication without rewriting analysis PASS/FAIL; optional diagnostics are
sanitized. Analysis PASS is not an assertion that every required delivery has already succeeded.
Legacy `Reporter` remains compatible and is not advertised as this safe boundary.

Metadata and EXPLAIN declaration, selection and execution failures expose a host-owned reason
code and safe registration IDs; invalid or oversized IDs receive an ordinal placeholder. Discovery
failures before an implementation is selected cannot be attributed to an invented registration.
Sink diagnostics expose registration ID, required policy and delivery status, never exception text.

`AuditRuleTestKit` checks descriptors, returned kinds, immutability and repeatability without a
JUnit dependency. It does not claim to prove semantic correctness, concurrency safety or hidden
input completeness. Tests cover actual Launcher execution, Spring assembly, nested ownership,
policies, baseline, stable IDs, target comparison, redaction and failure paths.
The separate published-POM consumer also executes a custom rule and sinks without internal,
JUnit or Spring imports. It verifies policy changes, immutable results, stable IDs and minimized
publication using only the artifact's declared dependencies.

## Maintainability cleanup

Unused HTML renderers have been removed after checking the live call graph. CSS and JavaScript
are classpath resources embedded in generated standalone HTML, keeping offline behavior.
Shared alias parsing no longer belongs to `MissingIndexDetector` merely because it was its
first caller. Read-only query captures skip the derived-delete suffix scan. Redundant narration
comments were removed where responsibilities now have names; comments explaining compatibility,
SQL ambiguity, redaction and ownership remain.

`MissingIndexDetector` reads in WHERE → JOIN → ORDER → GROUP order. Immutable query/table facts
separate evidence collection from recommendations; the WHERE result explicitly carries the
already-suggested columns into ORDER analysis. A unique-index shortcut requires equality for every
key column in the same proven table scope. OR, NOT, joins and partial composite keys do not create
that proof. AST traversal stays in the cached parser path, not inside the detector.

AST parsing/cache, column/table extraction and subquery rewrite have separate owners. Lightweight
extraction is split by syntax responsibility, with shared source scanning and clause boundaries.
`SqlParserRouting` owns fallback selection in one place; public parser signatures and legitimate
fallback behavior remain stable. See the [maintainer task map](maintaining.md) for change locations
and the tests that protect them.

## Deliberate boundaries

No hot reload, dependency-injection container inside core, arbitrary verdict replacement, automatic
network retry, async delivery engine or untrusted-plugin sandbox is introduced. Spring owns its
beans; sinks must acquire delivery resources inside publish if their Spring context may already
be closed at root finalization. Public source/binary signatures, behavior, and report schemas are
verified separately. This improves the agreed architecture without pretending all future code
quality decisions can be settled once.
