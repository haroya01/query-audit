# Maintaining QueryAudit

Start from the behavior you need to change, not from the largest class. Internal collaborators
are concrete and package-private; they are not additional extension points. Application authors
should start with [Custom Extensions](../guide/extensions.md) instead.

## Task map

| Change | Start here | Protect with tests |
| --- | --- | --- |
| Add an application-specific rule | `AuditRule`, `AuditExtensions` | `example.audit.ExtensionQuickstartTest`, `OpenAuditRuleAnalyzerTest` |
| Select, suppress, override severity, or baseline a finding | `FindingPolicy`, `OpenFindingClassifier` | `OpenAuditRuleAnalyzerTest`, `QueryAuditAnalyzerTest` |
| Read results, including custom kinds | `QueryAuditReport.getFindings()` / `AuditFindings` | `AuditFindingsTest`, `QueryAuditReportSnapshotTest` |
| Change annotation/configuration precedence | `AuditSettingsResolver` | `AuditSettingsResolverTest` |
| Change scope inheritance or ownership | `AuditScope`, `AuditResources`, read-only `LegacyAuditResourceStore` | `AuditScopeResourcesTest`, `AuditResourcesTest`, `ProgrammaticAuditExtensionsTest` |
| Change parallel capture / shared source ownership | `QueryCaptureSession`, `InvocationCapture`, `DataSourceHooks`, `HibernateListenerLeases` | `QueryCaptureSessionTest`, `ParallelAuditCaptureLauncherTest`, `ParallelSpringHibernateCaptureTest`, `DataSourceHooksConcurrencyTest` |
| Change run isolation | `SuiteAuditState`, `SuiteReportFinalizer` | `AuditRunIsolationRegressionTest`, `CanonicalAuditRunTest` |
| Change selected custom finding failure | `FindingFailurePolicy` | `FindingFailurePolicyTest`, `CustomFindingFailurePolicyIntegrationTest` |
| Change provider selection / failure attribution | `DatabaseProviders` | `DatabaseProvidersTest`, `IndexMetadataCapabilityTest`, `ExplainExtensionsSelectionTest` |
| Change capture or metadata initialization | `AuditScopeInitializer`, `IndexMetadataCollector` | JUnit integration and no-Hibernate suites |
| Change per-test analysis | `AuditTestAnalysis`, `ExplainAnalysis` | `OpenAuditRuleIntegrationTest`, `ExplainExtensionsSelectionTest` |
| Change per-test presentation or assertions | `AuditTestReporting`, `AuditTestAssertions` | visibility, query-budget, and failure-outcome tests |
| Change count contracts/baseline files | `QueryCountPolicies`, `AuditPolicyFiles`, `AuditAssertions` | count-contract, assertion, and baseline tests |
| Change run evidence / INFO visibility | `SuiteAuditState` | `CanonicalAuditRunTest`, `ReportInfoVisibilityTest` |
| Change artifact writing / delivery | `AuditReportArtifacts`, `SuiteReportFinalizer`, `AuditReportPublisher` | `ReportSinkFinalizerTest`, publisher tests |
| Change AST/fallback selection | `SqlParserRouting` | parser routing and resilience tests |
| Change SQL syntax extraction | parser map below | parser corpus/fuzz plus affected detector tests |
| Accept a report schema | `ComparisonEnvelopeReader` | `ComparisonEnvelopeReaderTest`, schema contract tests |
| Change comparison or resolution decisions | `ReportComparisonEngine`, `ComparisonTargets` | `ReportComparatorTest`, `TargetResolutionComparisonTest` |
| Change verdict JSON/text | `ReportVerdictWriter` | comparator/CLI output tests |
| Change HTML styling or behavior | `io/queryaudit/core/reporter/html/report.css`, `report.js` resources | `HtmlReportAssetsTest`, HTML reporter tests |
| Change human finding rows / stable IDs | Console, HTML, GitHub Actions reporters; `LegacyFindingPresentation` | reporter tests, legacy checkbox identity regressions |
| Change unique-key index reasoning | `MissingIndexDetector`, `UniqueIndexEquality`, `SqlAstEqualities` | `MissingIndexUniqueConstraintRegressionTest`, `SqlRequiredScalarEqualitiesTest` |

Names in this table are under `query-audit-core/src/main/java/io/queryaudit/core` or
`query-audit-junit5/src/main/java/io/queryaudit/junit5`; tests use matching module packages.
The HTML resources are under `query-audit-core/src/main/resources`.

## Read a test execution in order

`QueryAuditExtension` is the JUnit adapter. Its callbacks activate a scope, start capture, run
per-test analysis, and close the scope. It does not implement SQL extraction, report schema
validation, or delivery transports.

The class scope owns source hooks; each method invocation owns an `InvocationCapture` and a
`QueryCaptureSession` with its own query, connection and lazy-load collectors. Nested methods
borrow hooks, never mutable capture state. `DataSourceHooks` and `HibernateListenerLeases` reference
count shared installations. `AuditResources` cleans
up hooks and holders, not the application's DataSource or Spring beans. `AuditScopeInitializer`
uses that same cleanup path when initialization is only partly complete. Before changing a
store key or adding a resource, decide who owns it and which callback closes it.

`AuditTestAnalysis` coordinates captured evidence, rules, EXPLAIN, regression detection, and
canonical evidence recording. `AuditTestReporting` presents diagnostics before `AuditTestAssertions`
can throw a policy failure. `SuiteAuditState` retains canonical informational findings separately from the
human-facing projection. `SuiteReportFinalizer` closes the run once, writes local artifacts,
and attempts registered sinks. Analysis outcome and delivery outcome stay separate.

## Parser map

Keep the two existing public entry points: `SqlParser` provides lightweight extraction,
`EnhancedSqlParser` offers structural extraction with fallback. Both are facades over private
implementation details; a public parser plugin interface is not needed for an internal refactor.

| Concern | Lightweight path | AST path |
| --- | --- | --- |
| Routing, null/size/failure policy | `SqlParserRouting` chooses the path once per operation | same owner |
| Parse/cache lifecycle | not applicable | `SqlAstParser` |
| Strings, identifiers, nesting, statement scope | `SqlText`, `SqlIdentifiers`, `SqlSourceScanner`, `SqlStatementScope` | shared source scanning where needed |
| Statement/pagination recognition | `SqlStatementPatterns` | public enhanced operations use the same recognizers |
| Clause boundaries | `SqlClauseBodies` | `SqlAstClauses` |
| Column references | `SqlColumnReferences` | `SqlAstColumns` |
| Required scalar equalities | no speculative fallback proof | `SqlAstEqualities` |
| Table names and aliases | `SqlTableNames`, `SqlTableReferences` | `SqlAstTableNames` |
| Functions / OR predicates | `SqlFunctionExpressions`, `SqlOrPredicates` | preserved lightweight semantics |
| Subquery rewrite | `SqlStatementScope` | `SqlAstSubqueries` |

An unsupported statement or oversized SQL can legitimately use fallback. Dependency linkage and
initialization errors must not silently look like an unsupported query. Preserve the routing
tests' distinction. Add the failing SQL to a focused regression test before changing a grammar
branch; do not replace real grammar choices with an extra class per keyword.

## One result API, one policy owner

New consumers use `report.getFindings()`: confirmed, informational, and acknowledged categories,
each with built-in and application kinds. Do not merge `get*Issues()` with `getCustom*Findings()`
in new integration code. `ReportFindings` is the internal compatibility boundary preserving the
old enum-only and nullable-list contracts. Do not remove it until an explicitly versioned API
migration permits those contracts to change.

Rule output is evidence, not a verdict. The host controls selection, suppression, severity,
baseline, and completeness. New transports implement `AuditReportSink`; they do not modify the
analysis result or receive arbitrary local report data. New application rules declare namespaced
kinds instead of editing `IssueType` or adding a switch branch to a central dispatcher.

## A small verification loop

1. Reproduce the behavior with a focused test next to its owner.
2. Change that owner, keeping public entry points and data contracts stable.
3. Run the focused suite, then all affected module tests.
4. For lifecycle/schema/parser changes, run the full build and real database integrations.

```bash
./gradlew :query-audit-core:test --tests example.audit.ExtensionQuickstartTest
./gradlew :query-audit-core:test :query-audit-junit5:test :query-audit-junit5:noHibernateTest
./gradlew build :query-audit-mysql:integrationTest :query-audit-postgresql:integrationTest
./gradlew :query-audit-core:publishMavenPublicationToConsumerTestRepository
./gradlew -p query-audit-core/src/consumerTest run
./gradlew verifyExtensionDocumentation
./gradlew checkPublicApiCompatibility
```

The MySQL/PostgreSQL `integrationTest` tasks require Docker. `build` automatically compiles every
complete Java example in the extension guide and runs its core-only quickstart; use
`verifyExtensionDocumentation` to run that check directly. A release also needs API compatibility
checks, the strict documentation site build, and review of intentional behavior changes. A green test suite is not a
claim that a human unfamiliar with the code has completed an adoption study.

`checkPublicApiCompatibility` is also part of `build`. Its versioned floor under
`config/public-api` comes from the exact recorded Git source revision, not the working tree.
It compares declared public/protected JVM and generic signatures and incompatible access/shape
changes; safe additions are allowed. Real compiled mutation fixtures prove the checker catches
removals and breaks to existing implementers. This is a bounded contract gate, not proof of
behavioral compatibility, serialization formats, constant/annotation values, all overload
resolution, or inherited-member equivalence. Review deliberate API migrations explicitly; never
refresh the floor just to make a failing build green. The published-POM consumer and semantic
tests protect separate runtime and behavioral contracts.

## Review questions

- Can a maintainer name the owner of the changed behavior in one sentence?
- Does a new abstraction have an independent responsibility or a real consumer, rather than
  merely making a file shorter?
- Does control flow read in execution order, with policy choices explicit?
- Are comments explaining a constraint (grammar, privacy, ownership, compatibility), rather
  than narrating the next line?
- Can a new application feature use the public interfaces without importing `internal`, JUnit
  context, Spring container, or a third-party AST into the rule?
- Are missing evidence and failed delivery still distinct from a clean audit?

Prefer named methods and cohesive values to deep inheritance or a generic service registry.
The extension catalog is an explicit assembly boundary, not a new dependency-injection framework.
