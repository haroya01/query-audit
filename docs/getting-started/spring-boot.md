---
title: Spring Boot Integration
description: Install the starter, verify DataSource capture, and configure an existing proxy.
---

# Spring Boot Integration

The Spring Boot starter wraps test `DataSource` beans and makes QueryAudit configuration available
through `application.yml`. SQL is captured when it passes through that wrapped `DataSource` during
an active JUnit audit.

!!! note "Version scope"
    Dependency snippets use the current Maven Central release. The report-selection properties and
    run outcomes on this page require QueryAudit 0.6.0 or later. On 0.5.x, omit the format
    selection; a session with at least one completed audited result writes both HTML and schema 1.0
    JSON.

## Add the starter

Add the starter and the module for the database used by the test. PostgreSQL users can replace
`query-audit-mysql` with `query-audit-postgresql`.

=== "Gradle · Kotlin"

    ```kotlin
    dependencies {
        testImplementation("io.github.haroya01:query-audit-spring-boot-starter:0.6.0") // x-release-please-version
        testImplementation("io.github.haroya01:query-audit-mysql:0.6.0") // x-release-please-version
    }
    ```

=== "Gradle · Groovy"

    ```groovy
    dependencies {
        testImplementation 'io.github.haroya01:query-audit-spring-boot-starter:0.6.0' // x-release-please-version
        testImplementation 'io.github.haroya01:query-audit-mysql:0.6.0' // x-release-please-version
    }
    ```

=== "Maven"

    ```xml
    <dependencies>
        <dependency>
            <groupId>io.github.haroya01</groupId>
            <artifactId>query-audit-spring-boot-starter</artifactId>
            <version>0.6.0</version> <!-- x-release-please-version -->
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>io.github.haroya01</groupId>
            <artifactId>query-audit-mysql</artifactId>
            <version>0.6.0</version> <!-- x-release-please-version -->
            <scope>test</scope>
        </dependency>
    </dependencies>
    ```

Keep both dependencies in the test scope so the proxy and analyzer are absent from the production
runtime classpath.

## Run a controlled first audit

Start with report-only behavior on one database-facing test:

```java
@SpringBootTest
@EnableQueryInspector
class OrderServiceQueryTest {

    @Autowired
    private OrderService orderService;

    @Test
    void loadsRecentOrders() {
        orderService.findRecentOrders();
    }
}
```

Run `./gradlew test --tests OrderServiceQueryTest` or
`mvn -Dtest=OrderServiceQueryTest test`. The console report should name the test and show the SQL
captured through the application `DataSource`. Replace `@EnableQueryInspector` with `@QueryAudit`
when configured findings should fail the test.

Use the [quick start](quickstart.md) to add a read/write budget and work through the first failure.

## Configure the test profile

All starter properties use the `query-audit` prefix. Keep them in `src/test/resources` or a profile
that only tests activate:

```yaml
query-audit:
  enabled: true
  profile: recommended
  fail-on-detection: false
  report:
    format: console
    show-info: true
```

A common adoption sequence is:

1. Use `@EnableQueryInspector` or `fail-on-detection: false` to review existing findings.
2. Add explicit query budgets to the paths whose behavior is understood.
3. Use `@QueryAudit` or restore `fail-on-detection: true` where confirmed findings should fail.
4. Select `json` for CI automation or `html` for a browser artifact.

```yaml
query-audit:
  fail-on-detection: true
  auto-open-report: false
  report:
    format: json
    output-dir: build/reports/query-audit
```

See the [configuration reference](../guide/configuration.md) for every property, its default, rule
profiles, suppression precedence, and full-suite coverage.

## How DataSource capture works

At test application startup, the starter:

1. registers `QueryAuditAutoConfiguration`;
2. creates the shared configuration and interceptor beans; and
3. applies a `BeanPostProcessor` that wraps each Spring `DataSource` bean with
   [datasource-proxy](https://github.com/ttddyy/datasource-proxy).

When an audited test starts, the JUnit extension resolves the Spring `DataSource` and attaches its
per-test listener to the query-aware proxy. The listener is detached when the test class finishes.
Only statements routed through that object during the active audit window can be attributed to the
test.

If the context contains several `DataSource` beans, make the one used by the audited repository
resolvable by type, normally with `@Primary`. A missing or ambiguous active `DataSource` fails with
setup guidance instead of producing a trustworthy empty audit.

## Reuse an existing datasource-proxy

If another library already exposes a datasource-proxy `DataSource`, disable only QueryAudit's
automatic wrapper:

```yaml
query-audit:
  wrap-data-source:
    enabled: false
```

The JUnit extension finds the existing proxy and attaches its listener for the audit. No custom
`BeanPostProcessor` or second `QueryInterceptor` bean is required.

Do not set `query-audit.enabled: false` for this integration. That disables QueryAudit's
configuration as well as its wrapper. Also do not disable wrapping when the context exposes only a
raw `DataSource`; an active audit requires a query-aware Spring object and will fail during setup if
capture cannot be installed reliably.

Leaving automatic wrapping enabled around an existing proxy can create a nested proxy. It may still
capture SQL, but disabling the extra wrapper keeps the data path easier to reason about.

## Audit a full suite

The default `annotated` mode only audits tests marked with a QueryAudit annotation. To use opt-out
coverage, enable JUnit extension autodetection in `src/test/resources/junit-platform.properties`:

```properties
junit.jupiter.extensions.autodetection.enabled=true
```

Then select full-suite mode:

```yaml
query-audit:
  mode: all
  profile: recommended
```

Use `@QueryAuditExclude` on tests that should remain outside the audit. Enabling autodetection by
itself does not widen coverage while the mode remains `annotated`.

## Disable QueryAudit

Disable QueryAudit behavior when the test profile should perform no audit work:

```yaml
query-audit:
  enabled: false
```

For a temporary report-only run, keep the starter enabled and use `@EnableQueryInspector` or
`fail-on-detection: false` instead.

## Next steps

- [Complete the first fix loop](quickstart.md)
- [Choose annotations and budgets](../guide/annotations.md)
- [Configure profiles and coverage](../guide/configuration.md)
- [Keep JSON or HTML in CI](../guide/ci-cd.md)
- [Troubleshoot missing capture](../guide/troubleshooting.md#queryaudit-not-detecting-any-queries)
