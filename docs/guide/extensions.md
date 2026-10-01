# Custom Extensions

Use configuration for thresholds, profiles, severity, and suppression. When the behavior itself
needs to change, implement a small extension interface and register it with
`AuditExtensions`. Spring Boot can assemble this catalog from beans; plain JUnit and core-only
applications can assemble the same catalog in Java.

!!! note "Version scope"
    This guide describes the explicit-extension API in the current development source. Do not
    assume an older released artifact contains `AuditExtensions` or the catalog-accepting
    `QueryAuditExtension` constructor. See [Installation](../getting-started/installation.md)
    for the normal module and database setup.

## Supported Extension Points

| Interface | Responsibility | Registration method |
|---|---|---|
| `AuditRule` (default for new rules) | Declare independently named finding kinds and evaluate readonly evidence | `auditRule(id, rule)` |
| `DetectionRule` (compatibility) | Evaluate captured queries and return existing `Issue` values | `rule(id, rule)` |
| `IndexMetadataProvider` | Read index metadata for a supported database | `indexMetadataProvider(id, provider)` |
| `ExplainAnalyzer` | Analyze query plans for a supported database | `explainAnalyzer(id, analyzer)` |
| `AuditReportSink` | Publish a restricted immutable run summary | `reportSink(id, required, sink)` |

Legacy interfaces remain in `io.queryaudit.core.detector` and `io.queryaudit.core.analyzer`.
Open rule interfaces and the catalog live in `io.queryaudit.core.extension`; safe output interfaces
live in `io.queryaudit.core.reporter.delivery`. No Spring dependency is needed
to implement or assemble them.

Adding a rule does **not** disable the built-in rules. The analyzer runs enabled built-ins,
legacy `ServiceLoader` rules, and then explicitly registered rules. Normal profile selection,
disabled rules, severity overrides, suppression, and baseline classification still apply.

## Start Here: New Integrations

For new code, use **`AuditRule` → `AuditExtensions` → `QueryAuditAnalyzer.withExtensions` →
`report.getFindings()`**. This path works without Spring, JUnit, internal classes, or an enum change.
Use `AuditReportSink` for run-level output. The old `DetectionRule` / `Issue` path below is for
existing integrations, not a second API new users need to learn.

First copy [ApplicationBudgetRule](#open-finding-kinds), then run this core-only example:

Use the core module on your normal Gradle/Maven runtime classpath, including its transitive
dependencies (not just the standalone core JAR). In particular, the SQL parser dependency is
required at runtime. Java 17 or newer is required.

```java
package example.audit;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.extension.AuditExtensions;
import io.queryaudit.core.model.AuditFindings;
import io.queryaudit.core.model.LifecyclePhase;
import io.queryaudit.core.model.QueryRecord;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class BudgetQuickstart {
    public static void main(String[] args) {
        var extensions = AuditExtensions.builder()
                .auditRule("application-budget", new ApplicationBudgetRule(75))
                .build();
        var analyzer = QueryAuditAnalyzer.withExtensions(QueryAuditConfig.defaults(), null, extensions);
        var query = new QueryRecord("select id from orders", "select id from orders",
                TimeUnit.MILLISECONDS.toNanos(80), 1, null, 0, LifecyclePhase.TEST);
        var report = analyzer.analyze("OrderTest", "listOrders", List.of(query), null);
        AuditFindings findings = report.getFindings();
        boolean budgetExceeded = findings.confirmed().stream()
                .anyMatch(finding -> finding.kindId().equals(ApplicationBudgetRule.KIND));
        if (!budgetExceeded) throw new AssertionError("Expected a budget finding");
    }
}
```

In Spring, expose `new ApplicationBudgetRule(75)` as an `AuditRule` bean and keep `@QueryAudit`
on your test. In plain JUnit, pass the catalog to a **static** `@RegisterExtension
QueryAuditExtension` field. Do not additionally list that implementation in `ServiceLoader`.

`report.getFindings()` returns immutable `confirmed()`, `informational()`, and `acknowledged()`
lists, each including built-in and custom kinds. `errors()` and `warnings()` filter confirmed
findings; `all()` includes all three categories. Policy decisions remain the host's responsibility.
For a copied projection use `report.withFindings(new AuditFindings(...))`; do not mutate lists.

The executable public-API exercise is
`query-audit-core/src/test/java/example/audit/ExtensionQuickstartTest.java`:

```bash
./gradlew :query-audit-core:test --tests example.audit.ExtensionQuickstartTest
```

For an external consumer check, the following publishes only to a local test repository and runs
`ExtensionConsumer` through `ParserConsumer` using the published POM dependencies. It implements a
custom budget rule, changes its severity/disabling policy, correlates finding IDs, and exercises
optional/required sinks and publication privacy. No internal, JUnit or Spring API is needed.

```bash
./gradlew :query-audit-core:publishMavenPublicationToConsumerTestRepository
./gradlew -p query-audit-core/src/consumerTest run
```

Catalogs expose immutable ordered `rulesById()`, `auditRulesById()`,
`indexMetadataProvidersById()` and `explainAnalyzersById()` views alongside the existing list getters. Registration IDs identify
assembly; they are distinct from rule IDs, finding kinds and host-generated finding IDs. Metadata
and EXPLAIN failures report safe IDs plus host-owned reason codes. Use short non-secret IDs with
letters, digits, `.`, `_`, `:`, `/` or `-`; invalid diagnostic IDs are replaced, not echoed.

JUnit supports concurrent audited classes and methods, including nested, repeated and parameterized
invocations. Every invocation owns its query, connection and Hibernate event capture; shared hooks
are released only when their last owner finishes. See [Parallel capture](#parallel-capture) for
configuration and the explicit asynchronous-work contract. Consecutive Launcher runs own separate
report collections.

For codebase changes rather than app customization, start with the
[maintainer task map](../architecture/maintaining.md).

## Compatibility: Existing DetectionRule Integrations

The following helper is used in the examples below. Save it as
`src/test/java/example/audit/ApplicationSlowQueryRule.java`.

```java
package example.audit;

import io.queryaudit.core.detector.DetectionRule;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class ApplicationSlowQueryRule implements DetectionRule {
    private final long thresholdMillis;
    private final long thresholdNanos;

    public ApplicationSlowQueryRule(long thresholdMillis) {
        if (thresholdMillis <= 0) {
            throw new IllegalArgumentException("thresholdMillis must be positive");
        }
        this.thresholdMillis = thresholdMillis;
        this.thresholdNanos = TimeUnit.MILLISECONDS.toNanos(thresholdMillis);
    }

    @Override
    public String getRuleCode() {
        return IssueType.SLOW_QUERY.getCode();
    }

    @Override
    public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata metadata) {
        return queries.stream()
                .filter(query -> query.executionTimeNanos() > thresholdNanos)
                .map(query -> new Issue(
                        IssueType.SLOW_QUERY,
                        Severity.WARNING,
                        query.sql(),
                        null,
                        null,
                        "Query exceeded the application budget of " + thresholdMillis + " ms",
                        "Investigate this application's query budget"))
                .toList();
    }
}
```

This intentionally small example demonstrates the SPI. For a different slow-query threshold
alone, prefer `query-audit.slow-query.warning-ms` instead of a custom implementation.

`getRuleCode()` declares the rule's policy code, here `slow-query`. The emitted
`IssueType.SLOW_QUERY` uses the same code for reporting and suppression. Disabling or
suppressing `slow-query` therefore affects both this rule and the built-in slow-query rule;
if both run, they may produce overlapping findings. The current SPI does not replace a built-in
implementation just because the codes match.

!!! note "Legacy rule compatibility"
    `DetectionRule` returns `Issue` values backed by the existing `IssueType` enum. A registration ID
    such as `application:latency-budget` does not create a new issue type or a new suppression
    code. Use the additive `AuditRule` API below for independently named finding types.

## Spring Boot: Register Beans

Include this configuration in your application's test context, for example with
`@Import(AuditCustomization.class)` on an existing `@SpringBootTest`. Continue opting tests
in with `@QueryAudit`; no custom extension instance is required.

```java
package example.audit;

import io.queryaudit.core.detector.DetectionRule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class AuditCustomization {
    @Bean
    DetectionRule applicationSlowQueryRule() {
        return new ApplicationSlowQueryRule(75);
    }
}
```

The starter also collects `IndexMetadataProvider` and `ExplainAnalyzer` beans. For example,
these beans expose the existing MySQL implementations through Spring. This configuration
requires the `query-audit-mysql` module; replace the implementations with your own when you
need different database behavior.

```java
package example.audit;

import io.queryaudit.core.analyzer.ExplainAnalyzer;
import io.queryaudit.core.analyzer.IndexMetadataProvider;
import io.queryaudit.mysql.MySqlExplainAnalyzer;
import io.queryaudit.mysql.MySqlIndexMetadataProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class AuditDatabaseCustomization {
    @Bean
    IndexMetadataProvider applicationIndexMetadata() {
        return new MySqlIndexMetadataProvider();
    }

    @Bean
    ExplainAnalyzer applicationExplain() {
        return new MySqlExplainAnalyzer();
    }
}
```

Automatic collection has a small, explicit contract:

- Beans are ordered alphabetically by bean name within each SPI, not by `@Order` or `@Primary`.
- IDs are `rule:<beanName>`, `audit-rule:<beanName>`, `index-metadata:<beanName>`,
  `explain:<beanName>`, and `report-sink:<beanName>` (or an explicit sink registration ID).
- A bean alias does not register the bean again. Distinct bean definitions with different names
  remain separate registrations, even if they return the same object.
- A bean implementing multiple supported SPIs is registered once in each role, with different
  ID prefixes.
- The catalog is assembled once; it is not a live view of later bean registrations.

### Replace Automatic Collection

Expose **one** `AuditExtensions` bean when you want to select or order registrations yourself.
This replaces automatic SPI bean collection, not built-in rules or legacy discovery.
For example, use the following configuration **instead of** the two bean configurations above:

```java
package example.audit;

import io.queryaudit.core.extension.AuditExtensions;
import io.queryaudit.mysql.MySqlExplainAnalyzer;
import io.queryaudit.mysql.MySqlIndexMetadataProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class ExplicitAuditCatalog {
    @Bean
    AuditExtensions applicationAuditExtensions() {
        return AuditExtensions.builder()
                .rule("application:latency-budget", new ApplicationSlowQueryRule(75))
                .indexMetadataProvider("application:mysql-indexes", new MySqlIndexMetadataProvider())
                .explainAnalyzer("application:mysql-explain", new MySqlExplainAnalyzer())
                .build();
    }
}
```

Other SPI beans are not automatically merged into this catalog. Multiple catalog beans are a
configuration error in the JUnit integration; `@Primary` does not resolve that ambiguity.
`AuditExtensions.empty()` disables explicit registrations only.

The starter also backs off its default `QueryAuditConfig` and `QueryInterceptor` beans when
you provide the corresponding type. A custom config bean replaces property-based construction;
it is not automatically merged with `application.yml`. A custom interceptor is used as supplied,
so its capture limits and other initialization are your responsibility.

The catalog can be shared across tests without freezing each test's configuration. The JUnit
integration still resolves annotation settings for each test. In the annotation-based Spring
path, a method-level `@QueryAudit` replaces the class-level annotation; see
[Annotations](annotations.md) for the existing precedence rules.

## Plain JUnit: Register a Catalog

Use a **static** `@RegisterExtension` field. This registration itself opts the class into
auditing. The following H2 example needs the H2 JDBC driver on the test classpath in addition
to `query-audit-junit5` and the helper rule above.

```java
package example.audit;

import io.queryaudit.core.extension.AuditExtensions;
import io.queryaudit.junit5.QueryAuditExtension;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class ApplicationQueryTest {
    @RegisterExtension
    static final QueryAuditExtension audit = new QueryAuditExtension(
            AuditExtensions.builder()
                    .rule("application:latency-budget", new ApplicationSlowQueryRule(75))
                    .build());

    // Mutable so the extension can replace it with a capturing proxy.
    static DataSource dataSource = createDataSource();

    private static DataSource createDataSource() {
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:application-audit;DB_CLOSE_DELAY=-1");
        return source;
    }

    @Test
    void readsThroughTheAuditedDataSource() throws Exception {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery("SELECT 1")) {
            org.junit.jupiter.api.Assertions.assertTrue(result.next());
        }
    }
}
```

Do not combine this registration with `@QueryAudit` or
`@ExtendWith(QueryAuditExtension.class)` on the same test. Those can start capture before the
explicit catalog is registered; the extension rejects the conflict instead of silently ignoring
your catalog. An instance (non-static) field is also rejected. JUnit extension autodetection is
supported alongside this static registration in the default annotated mode; it does not run
the audit twice. Use the normal configuration and system-property settings for this path.

The H2 example demonstrates query capture and rule registration, not MySQL or PostgreSQL plan
analysis. Register providers appropriate to the database actually used by your tests.

## Core-only Legacy Compatibility

Core-only code can consume the catalog's rules through the existing analyzer constructor.
This example uses a synthetic query record and an empty, caller-supplied metadata snapshot;
it does not connect to a database.

```java
package example.audit;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.extension.AuditExtensions;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.reporter.ConsoleReporter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class CoreAuditExample {
    public static void main(String[] args) {
        AuditExtensions extensions = AuditExtensions.builder()
                .rule("application:latency-budget", new ApplicationSlowQueryRule(75))
                .build();
        QueryAuditConfig config = QueryAuditConfig.defaults();
        QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, List.of(), extensions.rules());
        List<QueryRecord> queries = List.of(new QueryRecord(
                "SELECT 1", TimeUnit.MILLISECONDS.toNanos(100), System.currentTimeMillis(), null));

        QueryAuditReport report = analyzer.analyze(
                "applicationBudget", queries, new IndexMetadata(Map.of()));
        new ConsoleReporter().report(report);
    }
}
```

`List.of()` in the constructor means an empty preloaded finding baseline. The catalog itself
does not capture queries, collect metadata, execute EXPLAIN, publish reports, or enforce test
failure. Passing `extensions.rules()` only connects rules. If a core-only host also registers
providers, it must select and invoke them through `indexMetadataProviders()` and
`explainAnalyzers()` and handle their results and failures itself. The JUnit integration supplies
that database orchestration; it is not hidden inside `QueryAuditAnalyzer`.

## Registration, Selection, and Lifecycle

### IDs and Duplicates

Registration IDs must be nonblank and unique across the **entire** catalog, including different
SPI roles. Duplicate IDs fail during builder registration with `IllegalArgumentException`;
null implementations are rejected. IDs are exact strings, not normalized rule codes.

The getters return immutable snapshots in registration order. `registrationIds()` exposes IDs
for assembly diagnostics; IDs do not define detection policy or make results trusted. Building
a second catalog after changing a builder does not change the first one. Implementations are
not copied, and registering the same instance under two different IDs preserves both entries.

Legacy `ServiceLoader` discovery remains available. Choose one registration channel for each
rule implementation: registering the same active concrete rule class both explicitly (including a
Spring bean) and through `ServiceLoader` is rejected rather than silently deduplicated. This
check does not identify equivalent implementations behind different classes or Spring proxies.
Two explicit registrations of the same class under different IDs are still allowed and both run.

### Database Provider Selection

For each database SPI independently, the JUnit integration matches `supportedDatabase()`
against the JDBC database product name using a case-insensitive substring match.
Providers must declare a nonblank database identifier, such as `mysql` or `postgresql`.

1. Exactly one matching explicit provider takes precedence over legacy discovered providers.
2. More than one matching explicit provider is an error, not an ordering-based choice. This
   includes aliases in the database identifiers that both match the same product.
3. If no explicit provider matches, the existing `ServiceLoader` fallback remains in use.
   A provider for a different database does not suppress that fallback.

Bean name order does not resolve provider ambiguity. Metadata failures and EXPLAIN failures are
reported as incomplete audit capability, not a clean result. An `ExplainAnalyzer` must throw
`ExplainAnalysisException` when a selected plan cannot be read; do not return an empty list to
hide that failure. See [Audit Coverage](audit-coverage.md) and
[Adding Database Support](../architecture/new-database.md) for the capability and database contracts.

### State and Ownership

Keep extensions stateless where possible; otherwise make them safe for the host's concurrent
execution. A catalog is immutable, but the objects it contains may be reused across test classes
and methods. Do not keep a test's query list, active JDBC connection, or annotation configuration
in a shared rule or provider field.

The integration keeps capture and effective test configuration outside the shared catalog.
Spring owns the lifecycle of extension beans, including their destruction. QueryAudit does not
close registered implementations. Objects constructed inside an explicit catalog are not
automatically individual Spring beans; their creator remains responsible for any resources.
The same ownership rule applies to plain JUnit and core-only registrations.

Standard `IndexMetadata` and `QueryAuditReport` instances now snapshot their constructor
collections and return unmodifiable collections. Mutating a provider's original map/list no longer
changes data already handed to the audit. Code that previously modified a getter's collection
must instead construct a new value. Public constructor signatures and existing null handling
remain unchanged; this is an intentional collection-mutability behavior change.

## Current Limits

Custom extension implementations and their hidden settings remain **unverified comparison
inputs**. A stable registration ID or a reused implementation class cannot prove that its
constructor arguments, collaborators, or external inputs are unchanged. Do not treat catalog
registration as a guarantee that two reports are comparable; see
[Comparison inputs](comparison-inputs.md).

`Reporter` beans are not automatically collected. They retain their per-test compatibility
contract; they are not the safe output boundary. Built-in report configuration remains available.
Use `AuditReportSink` for new run-level integrations.

## Open Finding Kinds

Keep three identities separate: a catalog registration ID identifies assembly, `RuleId` identifies
the rule implementation contract, and `FindingKindId` identifies the category used by policy and
baseline. `FindingId.of(testId, finding)` is the host-generated occurrence identity used by reports
and `--require-resolved`. A rule does not supply its own occurrence ID or final verdict.

Save this as `example/audit/ApplicationBudgetRule.java`:

```java
package example.audit;

import io.queryaudit.core.extension.AuditRule;
import io.queryaudit.core.extension.FindingKindId;
import io.queryaudit.core.extension.RuleContext;
import io.queryaudit.core.extension.RuleDescriptor;
import io.queryaudit.core.extension.RuleId;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.Severity;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public final class ApplicationBudgetRule implements AuditRule {
    public static final FindingKindId KIND = FindingKindId.of("shop:latency-budget");
    private final long thresholdNanos;
    private final RuleDescriptor descriptor;

    public ApplicationBudgetRule(long milliseconds) {
        if (milliseconds <= 0) throw new IllegalArgumentException("Budget must be positive");
        thresholdNanos = TimeUnit.MILLISECONDS.toNanos(milliseconds);
        descriptor = new RuleDescriptor(new RuleId("shop:budget-rule"), "1",
                Set.of(KIND), Map.of("milliseconds", Long.toString(milliseconds)));
    }

    @Override
    public RuleDescriptor descriptor() { return descriptor; }

    @Override
    public List<Finding> evaluate(RuleContext context) {
        return context.queries().stream()
                .filter(query -> query.executionTimeNanos() > thresholdNanos)
                .map(query -> new Finding(KIND, Severity.WARNING, query.sql(), null, null,
                        "Application query budget exceeded", "Review this use case's query budget"))
                .toList();
    }
}
```

Register with `builder.auditRule("application-budget", new ApplicationBudgetRule(75))`, or expose
it as an `AuditRule` Spring bean. Core-only hosts use
`QueryAuditAnalyzer.withExtensions(config, baselinePath, extensions)` to connect **both** rule APIs.
The old constructor with `extensions.rules()` connects only legacy rules, intentionally.
For discovery use `META-INF/services/io.queryaudit.core.extension.AuditRule` and a public no-arg
implementation. Discovery order is implementation class name, followed by explicit registration order.

Kinds are lowercase namespaced IDs, such as `shop:latency-budget`. `core:` and `query-audit:` are
reserved. Different active rules cannot claim the same kind; duplicate active rule IDs, undeclared
returned kinds, null results, descriptor mutation, and explicit/discovered duplicates fail fast.
Use one rule to emit multiple declared kinds when they share an implementation responsibility.

Configuration uses the finding kind, not the rule ID:

```yaml
query-audit:
  severity-overrides:
    "shop:latency-budget": ERROR
  # disabled-rules: ["shop:latency-budget"]
```

Method suppression uses `@QueryAudit(suppress = "shop:latency-budget")`. Baseline entries use the
same kind in the first column and the same SQL-pattern matching as built-ins. The host applies
selection, suppression, severity, and baseline classification once. Direct `@ExpectQueries` and
query-budget contracts remain independent assertions; a finding suppression does not disable them.

Use `@QueryAudit(failOnKinds = "shop:latency-budget")` to fail only for selected custom finding
kinds. `failOnKinds` also accepts built-in kind codes. It is combined with the legacy `failOn`
enum selection; when both arrays are empty, all confirmed findings retain the existing failure
policy. Informational and acknowledged findings do not fail this policy. Invalid kind strings
are rejected during configuration, even when the capture is empty or finding failure is disabled.
Method annotations still replace class annotations. Selection changes affect comparison fingerprints.

Explicit catalog rule declaration, execution and output-contract failures throw `AuditRuleException`,
an `IllegalArgumentException` subtype exposing `registrationId()`, nullable `ruleId()` and a fixed
`reason()`. The original message, cause, SQL and settings are not attached. This intentionally
replaces arbitrary implementation exception types on the catalog/`withExtensions` path; catch the
typed diagnostic there. The legacy List-based analyzer constructor and `AuditRuleTestKit` retain
their original exception behavior. A registration ID is an assembly diagnostic, not a finding ID.

## Parallel Capture

Enable normal JUnit parallel scheduling in `src/test/resources/junit-platform.properties`:

```properties
junit.jupiter.execution.parallel.enabled=true
junit.jupiter.execution.parallel.mode.default=concurrent
junit.jupiter.execution.parallel.mode.classes.default=concurrent
```

JUnit's own lifecycle rules still apply: for example, a `PER_CLASS` test instance needs explicit
`@Execution(CONCURRENT)` to run its methods concurrently. QueryAudit does not make mutable test
fields, database rows, custom rules or application beans thread-safe. Use separate test data and
thread-safe extension implementations. Spring tests may share a proxied DataSource and
SessionFactory; each invocation must use its own EntityManager/transaction. Do not close a shared
Spring context while a sibling test still uses it.

Capture starts in QueryAudit's `beforeEach`, transitions through SETUP → TEST → TEARDOWN, and stops
for analysis in its `afterEach`. SQL is assigned to the invocation and phase where execution
**started**. Query limits are per invocation. QueryAudit-owned metadata and EXPLAIN work is excluded.
User `@BeforeAll`/`@AfterAll` method SQL is outside per-test capture and is suppressed so it cannot
contaminate concurrently running classes.
The class owns source hooks; the method Store owns the capture handle and closes it after callbacks.
Closing one test or class does not stop another. Repeated/parameterized invocation IDs remain distinct
even when display names are equal; run publication keeps its privacy-preserving hashed IDs.

For executor work, wrap the task **on the audited test thread before submitting it**, and wait for
completion before the test returns. Wrapping reserves work immediately, including time in the
executor queue; a rejected, cancelled or never-executed wrapper is not evidence of completed work.
Each returned wrapper is single-use; create a fresh wrapper for each submission. Context is explicit,
not an inheritable thread-local:

```java
package example.audit;

import io.queryaudit.core.interceptor.QueryCaptureSession;
import java.util.concurrent.ExecutorService;
import javax.sql.DataSource;

public final class AuditedAsyncWork {
    public static int query(DataSource source, ExecutorService executor) throws Exception {
        return executor.submit(QueryCaptureSession.wrap(() -> {
            try (var connection = source.getConnection();
                    var statement = connection.createStatement();
                    var rows = statement.executeQuery("SELECT 1")) {
                if (!rows.next()) throw new IllegalStateException("Expected one row");
                return rows.getInt(1);
            }
        })).get();
    }
}
```

Unwrapped worker SQL observed while capture is active makes affected audits INCONCLUSIVE instead
of silently passing. Still-running SQL/tasks or late SQL observed before finalization also produce
safe reason codes tied to the invocation ID. Ordinary transaction rollback/connection cleanup after
analysis is allowed. Finish all work within the test lifecycle: no finalized report can be revised
by an unobserved future job after its hooks and root have closed. Preemptive timeout threads require
the same explicit propagation and completion discipline. `@TestFactory` dynamic children still
have no per-child audit lifecycle and are rejected; this is separate from ordinary parallel tests.
Known excluded/disabled invocations that receive the extension callbacks are suppressed for their
whole method lifecycle. Uninstrumented application/background threads cannot be automatically
distinguished from forgotten propagation; do not share their active JDBC work with an audited
window without arranging explicit ownership. Concurrent baseline/contract recording merges are
serialized within a JVM; separate JVMs must use separate files or external coordination.

`INCONCLUSIVE` is the canonical audit outcome, distinct from whether the JUnit test body succeeded.
CI should consume the audit verdict, not infer complete evidence solely from a green JUnit body.

New consumers use `QueryAuditReport.getFindings()` for the complete result.
`getConfirmedFindings()`, `getInfoFindings()`, and `getAcknowledgedFindings()` remain convenience
aliases. Existing `get*Issues()`, `getErrors()`, `getWarnings()`, and `getAcknowledgedCount()`
are legacy-only views; `getCustom*Findings()` is a compatibility view containing only custom kinds.
These signatures remain available, but new code should not combine their lists manually.
`Finding.toIssue()` is empty for a custom kind, never an unrelated enum.
JSON, HTML, console and GitHub Actions output include custom results. JSON keeps its 1.7 envelope:
the already-open `type` string carries the namespaced kind. INFO/baselined custom findings still
count as persisting when explicitly required by #210.

Descriptors and effective settings are snapshotted. Only a fingerprint, not the settings map, is
included in JUnit comparison capabilities. Changed declared inputs are detected; hidden inputs
remain unverified. Neither a plugin boolean nor identical descriptor text can certify complete
comparison inputs. A core host with independently verified provenance can supply `ComparisonInputs`.

## Safe Run-level Publication

`AuditReportSink.publish(PublishedAuditRun)` receives a restricted immutable summary, with its own
schema version: analysis outcome, reason codes, counts, validated kind IDs, severities and hashed
test/finding identities. It contains **no SQL, database identifiers, test names/selectors, source
locations, free-form diagnostics, configuration or exception objects**, even if local JSON uses
FULL detail. Hashes permit correlation; this is data minimization, not an anonymity guarantee.

Register an optional notification with `builder.reportSink("team:notification", false, sink)`;
use `true` for a required delivery. In Spring, bare `AuditReportSink` beans are optional. To mark
one required, expose `new ReportSinkRegistration("ci:archive", true, sink)` as a bean. The same
sink instance is not automatically added a second time as a bare bean.

JUnit publishes once at run finalization, in registration order. All contexts in that run must
agree on sink registrations. Implementations are owned by the application; do not depend on a
class-scoped resource that Spring has already closed before root finalization. Acquire any
delivery connection inside `publish`, close it there, and configure bounded I/O timeouts.

`showInfo=false` hides informational rows in human-facing output, not canonical run evidence.
Machine JSON and sinks retain informational findings so a hidden target cannot appear resolved.

Ordinary sink failures do not stop subsequent sinks. Required failure fails suite publication;
optional failure produces a sanitized delivery diagnostic. Analysis outcome is unchanged, so a
sink seeing analysis PASS is **not** being told that all required publication has succeeded.
Interruption is preserved and remaining deliveries are marked NOT_ATTEMPTED. VM errors propagate.
There are no automatic retries, asynchronous delivery or network sandbox guarantees.

Core hosts publish explicitly and inspect the separate result. The host chooses a run outcome
after checking its own policy and completeness. For a completed error-only policy, use
`report.getFindings().errors().isEmpty()` to choose `AuditRunResult.pass(List.of(report))` or
`AuditRunResult.fail(List.of(report))`. If evidence collection or analysis failed, use
`AuditRunResult.inconclusive(reports, reason)` instead; an empty finding list is not proof of
complete coverage. These factories do not perform capture, policy evaluation, or provenance
verification for you. JUnit already assembles this run result automatically.

```java
package example.audit;

import io.queryaudit.core.extension.AuditExtensions;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.reporter.delivery.AuditReportPublisher;
import io.queryaudit.core.reporter.delivery.PublicationResult;

public final class PublicationExample {
    public static PublicationResult publish(AuditRunResult run) {
        AuditExtensions extensions = AuditExtensions.builder()
                .reportSink("team:console-summary", false, summary -> System.out.println(summary.json()))
                .build();
        return new AuditReportPublisher(extensions.reportSinks()).publish(run);
    }
}
```

`PublicationResult.analysisOutcome()`, `hasRequiredFailures()`, `hasOptionalFailures()` and
`deliveries()` keep analysis and delivery facts separate. Exceptions/messages are never exported
through this result. Legacy local reporters retain their existing detail behavior.

## Extension Contract Tests

`AuditRuleTestKit.verify(rule, new RuleContext(queries, metadata))` validates declarations and
returned kinds, checks descriptor stability, and returns a snapshot of the findings.
`verifyDeterministic(rule, context)` additionally evaluates twice and rejects differing results.
The kit has no JUnit dependency and does not claim to prove thread safety, discover hidden inputs,
or sandbox untrusted code. Test your threshold boundaries and expected results in your normal
test framework. `RuleContext` snapshots query lists and metadata and exposes no JUnit context,
Spring container, mutable collector, connection, or third-party parser AST.

Open rules also receive an empty context for tests that captured no queries; handle that case
explicitly. The legacy detector empty-capture behavior is preserved. If repeated observations
share one logical finding ID, host classification uses the highest effective severity and retains
the observations together; one identity cannot simultaneously be informational and confirmed.

See [Extensible Runtime](../architecture/extensible-runtime.md) for ownership and compatibility.
