# Annotations Guide

Auditing annotations register `QueryAuditExtension` automatically. `@QueryAuditExclude` is the
opt-out marker; it does not activate auditing on its own. Start with
[Lock a Fixed Path](choose-your-workflow.md) to choose between a budget and a contract. This
reference documents the published `0.7.1` annotation API.

---

## Quick Reference

| Annotation | Target | Purpose | Test Failure |
|---|---|---|---|
| `@QueryAudit` | Class / Method | Run the configured detection profile | Yes (configurable) |
| `@EnableQueryInspector` | Class | Report findings without enforcing them | No finding failure; separate budget annotations still assert |
| `@ExpectQueries` | Method | Assert query budgets per type and in total | Yes (on budget exceeded) |
| `@DetectNPlusOne` | Class / Method | Deprecated since 0.7.0; use `@QueryAudit(failOn = N_PLUS_ONE)` | Yes (on N+1 only) |
| `@ExpectMaxQueryCount` | Method | Deprecated since 0.7.0; use `@ExpectQueries(total = n)` | Yes (on count exceeded) |
| `@QueryAuditExclude` | Class / Method | Opt a test out of auditing (the `mode: all` escape hatch) | No |

### Annotation Attributes at a Glance

| Attribute | Annotation | Type | Default | Description |
|---|---|---|---|---|
| `failOnDetection` | `@QueryAudit` | `BooleanOverride` | `INHERIT` (yml default: `true`) | Fail the test on confirmed issues |
| `nPlusOneThreshold` | `@QueryAudit` | `int` | `-1` (use yml/default: 3) | Override N+1 threshold |
| `suppress` | `@QueryAudit` | `String[]` | `{}` | Issue codes to suppress |
| `failOn` | `@QueryAudit` | `IssueType[]` | `{}` (all confirmed) | Only fail on specific issue types |
| `failOnKinds` | `@QueryAudit` | `String[]` | `{}` | Select built-in/custom kind codes; union with `failOn` |
| `baselinePath` | `@QueryAudit` | `String` | `""` (default path) | Path to baseline file |
| `autoOpenReport` | `@QueryAudit` | `BooleanOverride` | `INHERIT` (yml default: `true`) | Open HTML report in browser after tests |
| `includeSetupQueries` | `@QueryAudit` | `boolean` | `false` | Include `@BeforeEach`/`@AfterEach` queries in analysis |
| `threshold` | `@DetectNPlusOne` | `int` | `3` | Repeated query count to consider N+1 |
| `value` | `@ExpectMaxQueryCount` | `int` | *(required)* | Maximum number of queries allowed |
| `select` | `@ExpectQueries` | `int` | `-1` (not verified) | Maximum SELECT queries allowed |
| `insert` | `@ExpectQueries` | `int` | `-1` (not verified) | Maximum INSERT queries allowed |
| `update` | `@ExpectQueries` | `int` | `-1` (not verified) | Maximum UPDATE queries allowed |
| `delete` | `@ExpectQueries` | `int` | `-1` (not verified) | Maximum DELETE queries allowed |
| `total` | `@ExpectQueries` | `int` | `-1` (not verified) | Maximum captured statements of any type |

Choose how findings are treated with one class-level annotation: `@QueryAudit` fails the test on
confirmed findings, and `@EnableQueryInspector` reports them without failing. Put query budgets on
the method with `@ExpectQueries`. Under the default profile, the only built-in finding is a
call-site N+1.

!!! info "Why `BooleanOverride` instead of `boolean`?"
    Java annotation attributes cannot distinguish between "explicitly set to default" and
    "not specified" with a `boolean` type. `BooleanOverride` is a tri-state enum
    (`INHERIT`/`TRUE`/`FALSE`). An unspecified value on the selected method-level or class-level
    annotation falls back to `application.yml` or built-in defaults. A method-level annotation
    replaces the class declaration, as described below. Use `BooleanOverride.TRUE` or
    `BooleanOverride.FALSE` to explicitly override.

---

## @QueryAudit

The primary annotation. Enables query analysis using the configured profile across
SELECT, INSERT, UPDATE, and DELETE statements.

```java
@SpringBootTest
@QueryAudit
class OrderServiceTest {

    @Test
    void findOrders() {
        // Analyze SELECT/INSERT/UPDATE/DELETE captured through the audited DataSource.
        // Test fails if any confirmed issue (ERROR or WARNING) is detected.
    }
}
```

### Attributes

| Attribute | Type | Default | Description |
|---|---|---|---|
| `failOnDetection` | `BooleanOverride` | `INHERIT` (yml default: `true`) | Fail the test on confirmed issues. Use `BooleanOverride.TRUE`/`FALSE` to override. |
| `nPlusOneThreshold` | `int` | `-1` (use yml/default: 3) | Override N+1 threshold |
| `suppress` | `String[]` | `{}` | Issue codes to suppress |
| `failOn` | `IssueType[]` | `{}` (all confirmed) | Only fail on specific issue types |
| `failOnKinds` | `String[]` | `{}` | Select namespaced custom or built-in kinds; both selection arrays empty means all confirmed findings |
| `baselinePath` | `String` | `""` (default path) | Path to baseline file |
| `autoOpenReport` | `BooleanOverride` | `INHERIT` (yml default: `true`) | Open HTML report in browser after tests |
| `includeSetupQueries` | `boolean` | `false` | Include queries from `@BeforeEach`/`@AfterEach` lifecycle methods in analysis. Default analyzes only `@Test` body queries. |

### Usage Patterns

=== "Full analysis (default)"

    ```java
    @QueryAudit
    @SpringBootTest
    class OrderServiceTest {
        // Fails on any confirmed issue
    }
    ```

=== "Report only"

    ```java
    @QueryAudit(failOnDetection = BooleanOverride.FALSE)
    @SpringBootTest
    class OrderServiceTest {
        // Findings are advisory; explicit budgets and contracts still apply.
    }
    ```

=== "Custom threshold"

    ```java
    @QueryAudit(nPlusOneThreshold = 5)
    @SpringBootTest
    class BatchJobTest {
        // Higher threshold for batch operations
    }
    ```

=== "Selective failure"

    ```java
    @QueryAudit(failOn = {IssueType.N_PLUS_ONE, IssueType.UPDATE_WITHOUT_WHERE})
    @SpringBootTest
    class OrderServiceTest {
        // Only fails on N+1 and WHERE-less UPDATE/DELETE
    }
    ```

=== "Suppress specific issues"

    ```java
    @QueryAudit(suppress = {"select-all", "offset-pagination"})
    @SpringBootTest
    class LegacyServiceTest {
        // Known issues suppressed while migrating
    }
    ```

### Class-Level vs Method-Level

When both class-level and method-level annotations are present, the method-level
annotation takes precedence and **replaces** (not merges) the class-level settings.

```java
@QueryAudit(nPlusOneThreshold = 5)  // Class-level: applies to all tests
@SpringBootTest
class OrderServiceTest {

    @Test
    void findOrders() {
        // Uses class-level config: threshold=5, failOnDetection=true
    }

    @QueryAudit(failOnDetection = BooleanOverride.FALSE)  // Method-level: overrides entire class config
    @Test
    void exportAll() {
        // Report only for this specific test.
        // nPlusOneThreshold reverts to default (3), NOT the class-level 5,
        // because method-level replaces (not merges) the class-level settings.
    }

    @QueryAudit(nPlusOneThreshold = 2, suppress = {"select-all"})
    @Test
    void findTopOrders() {
        // Method-level: strict threshold, select-all suppressed.
        // Class-level threshold of 5 is ignored.
    }
}
```

!!! info "Configuration priority"
    **method-level > class-level > application.yml > built-in defaults**

    When a method-level `@QueryAudit` is present, it completely replaces the
    class-level annotation. Attributes not set on the method-level annotation
    fall back to `application.yml` or built-in defaults -- not to the class-level values.

---

## @EnableQueryInspector

Lightweight report-only mode. Equivalent to `@QueryAudit(failOnDetection = BooleanOverride.FALSE)`.

```java
@SpringBootTest
@EnableQueryInspector
class OrderServiceTest {

    @Test
    void findOrders() {
        // Reports detected findings without making those findings fatal.
        // Explicit budgets and contracts still apply.
    }
}
```

!!! tip "Use this for gradual adoption"
    Start with `@EnableQueryInspector` to review findings before making them fatal.
    Explicit budgets, contracts, and capture or reporting failures can still fail the run.
    Switch to `@QueryAudit` when reviewed findings should fail the test too.

---

## @QueryAuditExclude

Opts a test class or method out of auditing.

Built for `mode: all` suites (see [Audit Coverage Mode](configuration.md#audit-coverage-mode)),
where every test is audited by default and this is the escape hatch for tests that
intentionally violate a rule — load-shape fixtures, migration replays, bulk seed helpers.

```java
@QueryAuditExclude   // intentionally replays thousands of single-row inserts
class SeedDataReplayTest { ... }
```

Also honored in the default `annotated` mode: a method carrying `@QueryAuditExclude` is
skipped even when its class is annotated with `@QueryAudit`.

```java
@QueryAudit
class OrderServiceTest {

    @Test
    void findOrders() { ... }          // audited

    @QueryAuditExclude
    @Test
    void bulkFixtureReplay() { ... }   // skipped
}
```

---

## @DetectNPlusOne

!!! warning "Deprecated since 0.7.0"
    N+1 is the default rule, so `@QueryAudit(failOn = IssueType.N_PLUS_ONE, nPlusOneThreshold = 2)`
    does the same. `@DetectNPlusOne` keeps working.

Focused annotation that **only** fails on N+1 patterns. All other detection rules still
run and report, but won't cause a test failure.

### Class-Level Example

```java
@SpringBootTest
@DetectNPlusOne
class OrderServiceTest {

    @Test
    void findOrdersWithItems() {
        List<Order> orders = orderService.findAll();
        for (Order order : orders) {
            order.getItems().size();  // N+1! Test will fail.
        }
    }

    @Test
    void findSingleOrder() {
        orderService.findById(1L);
        // No N+1 here, test passes.
        // Other issues (e.g., SELECT *) are reported but don't fail.
    }
}
```

### Method-Level Example

```java
@SpringBootTest
@QueryAudit  // Full analysis for most tests
class OrderServiceTest {

    @DetectNPlusOne(threshold = 2)  // Only fail on N+1 for this specific test
    @Test
    void findOrdersWithItems() {
        List<Order> orders = orderService.findAll();
        for (Order order : orders) {
            order.getItems().size();
        }
    }
}
```

### Attributes

| Attribute | Type | Default | Description |
|---|---|---|---|
| `threshold` | `int` | `3` | Repeated query count to consider N+1 |

### How N+1 Detection Works

QueryAudit detects N+1 at **two levels**:

1. **SQL-level** (all environments): Normalizes each query (`SELECT * FROM items WHERE id = ?`),
   groups by pattern, and counts executions. If the same pattern appears >= threshold times
   from the same call site, it's flagged.

2. **Hibernate-level** (when Hibernate is on classpath): Registers as a Hibernate event
   listener for `INIT_COLLECTION` and `POST_LOAD` events. This catches:
    - `@OneToMany` / `@ManyToMany` lazy collection loading
    - `@ManyToOne` / `@OneToOne` proxy resolution

   Hibernate event findings use ERROR severity. Review them with actual SQL counts and fetch
   settings; they do not establish the cost of the operation or exclude false positives.

```
Test Code
    |
    v
orderService.findAll()  --- SQL-level: "SELECT * FROM orders" (1 execution)
    |
    v
for (order : orders)
    order.getItems()    --- SQL-level: "SELECT * FROM items WHERE order_id = ?" (N executions)
                        --- Hibernate-level: INIT_COLLECTION event for Order.items (N loads)
                        --- Both detect N+1: pattern repeated N times
```

---

## @ExpectMaxQueryCount

!!! warning "Deprecated since 0.7.0"
    Use `@ExpectQueries(total = 5)`, which reports the total with the other budgets.
    `@ExpectMaxQueryCount` keeps working.

Asserts that a test method does not exceed a specific number of total queries.
All query types (SELECT, INSERT, UPDATE, DELETE) are counted.

```java
@SpringBootTest
@QueryAudit
class OrderServiceTest {

    @Test
    @ExpectMaxQueryCount(5)
    void createOrder() {
        orderService.createOrder(request);
        // Fails if more than 5 total queries are executed
    }

    @Test
    @ExpectMaxQueryCount(3)
    void findOrderById() {
        orderService.findById(1L);
        // Ensures a simple lookup stays efficient
    }
}
```

### Attributes

| Attribute | Type | Default | Description |
|---|---|---|---|
| `value` | `int` | *(required)* | Maximum number of queries allowed |

### Failure Message

When exceeded, you get:

```
QueryAudit: createOrder executed 8 queries, expected at most 5.
Tip: Check the Query Patterns section in the report above to identify which queries to optimize.
```

!!! warning "Counts ALL queries"
    `@ExpectMaxQueryCount` counts all query types, including INSERTs from test data setup.
    If you use `@BeforeEach` to seed data, those INSERTs are included in the count.
    Consider using a higher limit or moving setup to `@BeforeAll`.

---

## @ExpectQueries

Asserts per-type query budgets for a test method. Each attribute limits one query type
(SELECT / INSERT / UPDATE / DELETE) independently; attributes left at `-1` are not verified.

```java
@SpringBootTest
@QueryAudit
class OrderServiceTest {

    @Test
    @ExpectQueries(select = 2, insert = 1)
    void createOrder() {
        orderService.createOrder(request);
        // Fails if more than 2 SELECTs or more than 1 INSERT are executed
    }

    @Test
    @ExpectQueries(insert = 0, update = 0, delete = 0)
    void findOrderById() {
        orderService.findById(1L);
        // A captured INSERT, UPDATE, or DELETE fails this budget.
    }
}
```

### Attributes

| Attribute | Type | Default | Description |
|---|---|---|---|
| `select` | `int` | `-1` (not verified) | Maximum SELECT queries allowed |
| `insert` | `int` | `-1` (not verified) | Maximum INSERT queries allowed |
| `update` | `int` | `-1` (not verified) | Maximum UPDATE queries allowed |
| `delete` | `int` | `-1` (not verified) | Maximum DELETE queries allowed |
| `total` | `int` | `-1` (not verified) | Maximum captured statements of any type |

### Failure Message

When a budget is exceeded, the diagnostic lists captured statements of the violated type and
available source information. The console's first frame can be a JDBC proxy; inspect the JSON
`stackTrace` for application frames. Example excerpt:

```
QueryAudit: createOrder() exceeded its query budget.
SELECT: executed 3, expected at most 2.
  select * from orders where customer_id = ?
    at com.example.OrderService.createOrder:42
  select * from members where id = ?
    at com.example.OrderService.loadCustomer:57
  ...
```

!!! tip "Use `0` to forbid a query type"
    `@ExpectQueries(insert = 0, update = 0, delete = 0)` turns a test into a read-only
    contract -- useful for guarding query-only endpoints against accidental writes.

!!! warning "Counts ALL queries"
    Budgets count queries from the whole test lifecycle, including INSERTs from `@BeforeEach`
    data setup. To count one request or job only, use a
    [scoped contract](contracts.md#contract-a-request-or-job).

`total` caps every statement while the type attributes constrain individual types; both can be
set on one annotation.

---

## Combining Annotations

Annotations can be combined for fine-grained control:

```java
@SpringBootTest
@QueryAudit(nPlusOneThreshold = 2)  // Fail on N+1 from the second repetition
class OrderServiceTest {

    @Test
    @ExpectQueries(total = 10, update = 0)  // Also enforce budgets
    void createOrder() {
        orderService.createOrder(request);
    }

    @Test
    void findOrders() {
        // Only the class-level @QueryAudit applies here
        orderService.findAll();
    }
}
```

## Composed and Inherited Annotations

QueryAudit finds its annotations where JUnit finds the extension, so a shared test annotation or
an audited base class turns auditing on for every test that uses it:

```java
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@SpringBootTest
@QueryAudit
public @interface AuditedIntegrationTest {}

@AuditedIntegrationTest
class OrderServiceTest { ... }

@QueryAudit
abstract class AuditedTestBase { ... }

class PaymentServiceTest extends AuditedTestBase { ... }
```

| Declared on | Applies |
|---|---|
| The test method, directly or through a composed annotation | First |
| The test class, directly, through a composed annotation, or through an implemented interface | Next |
| A superclass, nearest first | Next |
| An enclosing class, then its superclasses | Last |

The nearest declaration wins, so `@QueryAudit(nPlusOneThreshold = 4)` on a subclass replaces the
settings of an audited base class. `@QueryAuditExclude` is the exception: on the method, the
class, a superclass, or an enclosing class, it excludes the test even when a nearer declaration
enables auditing. `@ExpectQueries` and `@ExpectMaxQueryCount` target methods only, so put them on
each test method.

---

## Without Spring Boot

All annotations work without Spring Boot. Expose the `ProxyDataSource` used by the repository as a
static field so QueryAudit can discover it and attach its listener:

```java
@QueryAudit
class OrderRepositoryTest {

    static DataSource dataSource =
            ProxyDataSourceBuilder.create(new HikariDataSource(hikariConfig()))
                    .name("query-audit")
                    .build();

    @Test
    void findByStatus() {
        try (Connection conn = dataSource.getConnection()) {
            PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM orders WHERE status = ?");
            ps.setString(1, "PENDING");
            ResultSet rs = ps.executeQuery();
            // This connection comes from the discovered proxy.
        }
    }

    private static HikariConfig hikariConfig() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://localhost:3306/test");
        config.setUsername("root");
        config.setPassword("password");
        return config;
    }
}
```

Add `net.ttddyy:datasource-proxy:1.10` to the test compile classpath. The database module already
provides the QueryAudit JUnit integration. See [Plain JUnit installation](../getting-started/installation.md#plain-junit-5)
for Gradle and Maven examples.

In QueryAudit 0.6, the extension can also replace a mutable static field declared as
`javax.sql.DataSource` with its recording proxy for the duration of the test class. The manual
proxy shown above remains compatible with both 0.5 and 0.6.

---

## See Also

- [Configuration Reference](configuration.md) -- All configuration options and defaults
- [Suppressing Issues](suppressing.md) -- How to suppress specific detections
- [Reports](reports.md) -- Understanding console, JSON, and HTML report output
