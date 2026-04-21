# Annotations Guide

QueryAudit provides four annotations for different use cases. All annotations trigger the
`QueryAuditExtension` JUnit 5 extension automatically.

---

## Quick Reference

| Annotation | Target | Purpose | Test Failure |
|---|---|---|---|
| `@QueryAudit` | Class / Method | Full analysis with all 57 detection rules | Yes (configurable) |
| `@EnableQueryInspector` | Class | Report-only mode, never fails | No |
| `@DetectNPlusOne` | Class / Method | N+1 detection only | Yes (on N+1 only) |
| `@ExpectMaxQueryCount` | Method | Assert max query count | Yes (on count exceeded) |

### Annotation Attributes at a Glance

| Attribute | Annotation | Type | Default | Description |
|---|---|---|---|---|
| `failOnDetection` | `@QueryAudit` | `boolean` | `true` | Fail the test on confirmed issues |
| `nPlusOneThreshold` | `@QueryAudit` | `int` | `-1` (use default: 3) | Override N+1 threshold |
| `suppress` | `@QueryAudit` | `String[]` | `{}` | Issue codes to suppress |
| `failOn` | `@QueryAudit` | `IssueType[]` | `{}` (all confirmed) | Only fail on specific issue types |
| `baselinePath` | `@QueryAudit` | `String` | `""` (default path) | Path to baseline file |
| `autoOpenReport` | `@QueryAudit` | `boolean` | `false` | Open HTML report in browser after tests |
| `threshold` | `@DetectNPlusOne` | `int` | `3` | Repeated query count to consider N+1 |
| `value` | `@ExpectMaxQueryCount` | `int` | *(required)* | Maximum number of queries allowed |

---

## @QueryAudit

The primary annotation. Enables full query analysis with all 57 detection rules across
SELECT, INSERT, UPDATE, and DELETE statements.

```java
@SpringBootTest
@QueryAudit
class OrderServiceTest {

    @Test
    void findOrders() {
        // All queries (SELECT, INSERT, UPDATE, DELETE) are captured and analyzed.
        // Test fails if any confirmed issue (ERROR or WARNING) is detected.
    }
}
```

### Attributes

| Attribute | Type | Default | Description |
|---|---|---|---|
| `failOnDetection` | `boolean` | `true` | Fail the test on confirmed issues |
| `nPlusOneThreshold` | `int` | `-1` (use default: 3) | Override N+1 threshold |
| `suppress` | `String[]` | `{}` | Issue codes to suppress |
| `failOn` | `IssueType[]` | `{}` (all confirmed) | Only fail on specific issue types |
| `baselinePath` | `String` | `""` (default path) | Path to baseline file |
| `autoOpenReport` | `boolean` | `false` | Open HTML report in browser after tests |

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
        // Reports issues but never fails
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
        // Reports all detected issues to console
        // Never fails the test
    }
}
```

!!! tip "Use this for gradual adoption"
    Start with `@EnableQueryInspector` to see what QueryAudit finds without breaking
    your builds. Once you've reviewed the issues, switch to `@QueryAudit` to enforce them.

---

## @DetectNPlusOne

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

   Hibernate-level detection is **authoritative** (ERROR severity, zero false positives).

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

## Combining Annotations

Annotations can be combined for fine-grained control:

```java
@SpringBootTest
@QueryAudit                    // Full analysis on all tests
@DetectNPlusOne(threshold = 2) // Strict N+1 threshold
class OrderServiceTest {

    @Test
    @ExpectMaxQueryCount(10)   // Also enforce query count limit
    void createOrder() {
        orderService.createOrder(request);
    }

    @Test
    void findOrders() {
        // Only @QueryAudit + @DetectNPlusOne apply here
        orderService.findAll();
    }
}
```

---

## Without Spring Boot

All annotations work without Spring Boot. QueryAudit resolves the `DataSource`
by looking for a `static DataSource` field in the test class:

```java
@QueryAudit
class OrderRepositoryTest {

    // QueryAudit auto-discovers this field
    static DataSource dataSource = new HikariDataSource(hikariConfig());

    @Test
    void findByStatus() {
        try (Connection conn = dataSource.getConnection()) {
            PreparedStatement ps = conn.prepareStatement(
                "SELECT * FROM orders WHERE status = ?");
            ps.setString(1, "PENDING");
            ResultSet rs = ps.executeQuery();
            // QueryAudit captures and analyzes this query
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

---

## See Also

- [Configuration Reference](configuration.md) -- All configuration options and defaults
- [Suppressing Issues](suppressing.md) -- How to suppress specific detections
- [Reports](reports.md) -- Understanding console, JSON, and HTML report output
