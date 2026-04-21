---
title: Spring Boot Integration
description: How QueryAudit auto-configures itself in Spring Boot test environments.
---

# Spring Boot Integration

!!! abstract "What you'll learn"
    How the Spring Boot starter works under the hood, how to configure it via `application.yml`, and how to integrate with existing datasource-proxy setups.

QueryAudit provides a Spring Boot starter that automatically intercepts every SQL query
executed during your `@SpringBootTest` tests and analyzes them for performance anti-patterns.

---

## How Auto-Configuration Works

When you add `query-audit-spring-boot-starter` to your test dependencies, three things happen
at application startup:

1. **`QueryAuditAutoConfiguration`** is registered via
   `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`
   (Spring Boot 3.x style).
2. A shared **`QueryInterceptor`** bean is created. This interceptor records every SQL
   statement that passes through the proxied DataSource.
3. A **`BeanPostProcessor`** wraps every `DataSource` bean in a
   [datasource-proxy](https://github.com/ttddyy/datasource-proxy) proxy that feeds
   query events to the interceptor.

The auto-configuration activates only when:

- A `javax.sql.DataSource` class is on the classpath (`@ConditionalOnClass`).
- The property `query-audit.enabled` is `true` (which is the default).

Because the proxy is applied through a `BeanPostProcessor`, it works transparently with
any connection pool (HikariCP, Tomcat, etc.) and any JPA provider (Hibernate, EclipseLink).

---

## Dependency Setup

=== "Gradle (Kotlin DSL)"

    ```kotlin
    dependencies {
        testImplementation("io.github.haroya01:query-audit-spring-boot-starter:0.3.0")
        testImplementation("io.github.haroya01:query-audit-mysql:0.3.0")
    }
    ```

=== "Gradle (Groovy DSL)"

    ```groovy
    dependencies {
        testImplementation 'io.github.haroya01:query-audit-spring-boot-starter:0.3.0'
        testImplementation 'io.github.haroya01:query-audit-mysql:0.3.0'
    }
    ```

=== "Maven"

    ```xml
    <dependency>
        <groupId>io.github.haroya01</groupId>
        <artifactId>query-audit-spring-boot-starter</artifactId>
        <version>0.3.0</version>
        <scope>test</scope>
    </dependency>
    <dependency>
        <groupId>io.github.haroya01</groupId>
        <artifactId>query-audit-mysql</artifactId>
        <version>0.3.0</version>
        <scope>test</scope>
    </dependency>
    ```

!!! tip "Using PostgreSQL?"
    Replace `query-audit-mysql` with `query-audit-postgresql` in all examples on this page.

---

## application.yml Configuration

All properties live under the `query-audit` prefix. Below is a complete example with
every available option and its default value:

```yaml
query-audit:
  enabled: true                       # Master switch (default: true)
  fail-on-detection: true             # Fail tests on confirmed issues (default: true)
  n-plus-one:
    threshold: 3                      # Repeated query count before flagging (default: 3)
  offset-pagination:
    threshold: 1000                   # OFFSET value that triggers a warning (default: 1000)
  or-clause:
    threshold: 3                      # OR conditions before flagging (default: 3)
  suppress-patterns:                  # Issue codes or qualified patterns to suppress
    - "select-all"
    - "missing-where-index:users.email"
  report:
    format: console                   # Report format: console (default: console)
    output-dir: build/reports/query-audit
    show-info: true                   # Include INFO-level issues in the report (default: true)
```

### Common Configurations

=== "Strict (CI pipeline)"

    ```yaml
    # src/test/resources/application-ci.yml
    query-audit:
      enabled: true
      fail-on-detection: true
      report:
        show-info: true
    ```

=== "Relaxed (local development)"

    ```yaml
    # src/test/resources/application-local.yml
    query-audit:
      enabled: true
      fail-on-detection: false    # Report issues but don't fail
      report:
        show-info: true
    ```

=== "Minimal (only critical issues)"

    ```yaml
    # src/test/resources/application-test.yml
    query-audit:
      enabled: true
      fail-on-detection: true
      report:
        show-info: false          # Hide INFO-level items
      suppress-patterns:
        - "select-all"            # Ignore SELECT * warnings
    ```

!!! tip "Test-only profile"
    Put this configuration in `src/test/resources/application-test.yml` and activate it
    with `@ActiveProfiles("test")` so it never leaks into production.

---

## Basic Usage with @QueryAudit

Annotate your test class (or individual methods) with `@QueryAudit`. The extension
intercepts SQL, runs analysis after each test, and prints a report to the console.

```java
@SpringBootTest
@QueryAudit
class OrderServiceTest {

    @Autowired
    private OrderService orderService;

    @Test
    void findRecentOrders_shouldUseIndex() {
        orderService.findRecentOrders(userId);
        // All queries executed by findRecentOrders are captured and analyzed.
        // The test fails if a confirmed issue (ERROR or WARNING) is detected.
    }

    @QueryAudit(failOnDetection = BooleanOverride.FALSE)  // Override: report only, never fail
    @Test
    void batchExport_reportOnly() {
        orderService.exportAll();
    }
}
```

---

## Using with an Existing datasource-proxy (gavlyukovskiy)

If your project already uses
[spring-boot-data-source-decorator](https://github.com/gavlyukovskiy/spring-boot-data-source-decorator)
(gavlyukovskiy's library), the `DataSource` is already a datasource-proxy instance.
Adding QueryAudit's `BeanPostProcessor` on top would create a **double proxy**, which
works but adds unnecessary overhead.

There are two approaches to avoid this:

### Approach 1: Hook into the existing proxy via a listener (recommended)

Disable QueryAudit's own `BeanPostProcessor` and instead register the `QueryInterceptor`
as an additional listener on the existing proxy.

```java
@TestConfiguration
class QueryAuditTestConfig {

    @Bean
    public QueryInterceptor queryGuardInterceptor() {
        return new QueryInterceptor();
    }

    @Bean
    public BeanPostProcessor attachQueryAuditListener(QueryInterceptor interceptor) {
        return new BeanPostProcessor() {
            @Override
            public Object postProcessAfterInitialization(Object bean, String beanName)
                    throws BeansException {
                if (bean instanceof ProxyDataSource proxyDs) {
                    // Add QueryInterceptor to the existing proxy's listener chain
                    proxyDs.getProxyConfig()
                           .getMethodListener()
                           .addListener(interceptor);
                }
                return bean;
            }
        };
    }
}
```

Then disable QueryAudit's built-in proxy in your test configuration:

```yaml
# application-test.yml
query-audit:
  enabled: false          # Disable the auto BeanPostProcessor
  fail-on-detection: true # Still used by @QueryAudit annotation
```

!!! note
    With this approach, QueryAudit's auto-configuration `BeanPostProcessor` is disabled.
    The `@QueryAudit` annotation and JUnit extension still work normally because they
    read the `QueryInterceptor` bean from the application context.

### Approach 2: Accept the double proxy

If simplicity is more important than a few microseconds of overhead per query,
just leave both libraries enabled. The proxies stack transparently and both will
receive query events. This is perfectly fine for test environments.

---

## DataSource Proxy Wrapping Internals

Under the hood, `DataSourceProxyFactory.wrap()` uses `ProxyDataSourceBuilder` from
datasource-proxy:

```java
ProxyDataSourceBuilder.create(original)
    .name("query-audit")
    .listener(interceptor)
    .build();
```

The `QueryInterceptor` implements datasource-proxy's listener interface. When a query
executes, the interceptor records the SQL text, parameter bindings, execution time, and
a normalized form of the query (for N+1 pattern detection).

---

## Disabling QueryAudit

To temporarily disable QueryAudit without removing the dependency:

```yaml
query-audit:
  enabled: false
```

Or per-test:

```java
@QueryAudit(failOnDetection = BooleanOverride.FALSE)  // Still reports, but doesn't fail
// Or use the dedicated alias:
@EnableQueryInspector
```

---

## Next Steps

- :material-arrow-right: [Configuration Reference](../guide/configuration.md) -- Full list of `application.yml` properties
- :material-arrow-right: [Annotations Guide](../guide/annotations.md) -- All 4 annotations explained
- :material-arrow-right: [Detection Rules](../detections/overview.md) -- All 57 detection rules
