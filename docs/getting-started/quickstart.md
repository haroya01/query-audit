---
title: Quick Start
description: Get your first QueryAudit report in under five minutes.
---

# Quick Start

!!! abstract "What you'll learn"
    How to add `@QueryAudit` to a test, trigger real detections (N+1, missing index, batch insert), read the report, and fix the issues.

This guide walks you through adding QueryAudit to a Spring Boot test, triggering real detections, and reading the report.

---

## The Simplest Example

If you just want to see QueryAudit in action, this is all you need:

```java
@SpringBootTest
@QueryAudit
class MyServiceTest {

    @Autowired
    private MyService myService;

    @Test
    void myMethod_shouldNotHaveQueryIssues() {
        myService.doSomething();
        // That's it. QueryAudit analyzes every SQL query executed above
        // and fails the test if it finds performance anti-patterns.
    }
}
```

No configuration. No extra beans. No proxy setup. Just the annotation.

---

## Full Walkthrough

### Step 1: Add Dependencies

=== "Gradle"

    ```gradle
    dependencies {
        testImplementation 'io.github.haroya01:query-audit-spring-boot-starter:0.1.0'
        testImplementation 'io.github.haroya01:query-audit-mysql:0.1.0'
    }
    ```

=== "Maven"

    ```xml
    <dependency>
        <groupId>io.github.haroya01</groupId>
        <artifactId>query-audit-spring-boot-starter</artifactId>
        <version>0.1.0</version>
        <scope>test</scope>
    </dependency>
    <dependency>
        <groupId>io.github.haroya01</groupId>
        <artifactId>query-audit-mysql</artifactId>
        <version>0.1.0</version>
        <scope>test</scope>
    </dependency>
    ```

!!! tip "Using PostgreSQL?"
    Replace `query-audit-mysql` with `query-audit-postgresql`. Everything else stays the same.

---

### Step 2: Annotate Your Test

```java
@SpringBootTest
@QueryAudit
class OrderServiceTest {

    @Autowired
    private OrderService orderService;

    @Test
    void findRecentOrders() {
        List<Order> orders = orderService.findRecentOrders(10);

        assertThat(orders).isNotEmpty();
        orders.forEach(order -> {
            // This triggers lazy loading -- each order fires a separate
            // SELECT to fetch its items. Classic N+1 problem.
            assertThat(order.getItems()).isNotNull();
        });
    }
}
```

!!! note "No other changes needed"
    You don't need to change your `DataSource`, add a proxy bean, or modify `application.yml`. The `@QueryAudit` annotation handles everything.

---

### Step 3: Run Your Tests

```bash
./gradlew test --tests OrderServiceTest
```

QueryAudit automatically:

1. **Intercepts** every SQL query (SELECT, INSERT, UPDATE, DELETE) executed during the test
2. **Collects index metadata** via `SHOW INDEX` (MySQL) or `pg_catalog` (PostgreSQL) from your test database
3. **Applies 57 detection rules** to the captured queries
4. **Prints a report** to the console output
5. **Fails the test** if confirmed issues are found (configurable)

---

### Step 4: Read the Report

```
================================================================================
                          QUERY AUDIT REPORT
                   OrderServiceTest (12 queries analyzed)
================================================================================

CONFIRMED ISSUES (action required)
────────────────────────────────────────────────────────────────────────────────

[ERROR] N+1 Query Detected
  Repeated query: select * from order_items where order_id = ?
  Executions:     10 times (threshold: 3)
  Source:         com.example.OrderService.findRecentOrders:42
  Suggestion:     Use JOIN FETCH or @EntityGraph to load order_items
                  with the parent query.

[ERROR] Missing Index on WHERE column
  Query:   select * from order_items where order_id = ?
  Table:   order_items
  Column:  order_id
  Suggestion: CREATE INDEX idx_order_items_order_id
              ON order_items (order_id);

[WARNING] Repeated single-row INSERT should use batch insert
  Query:   insert into audit_log (action, entity_id, ...) values (?, ?, ...)
  Table:   audit_log
  Detail:  Single-row INSERT executed 10 times on table 'audit_log'.
  Suggestion: Use batch INSERT (saveAll() in JPA with hibernate.jdbc.batch_size).

────────────────────────────────────────────────────────────────────────────────
INFO (for review)
────────────────────────────────────────────────────────────────────────────────

[WARNING] SELECT * Usage
  Query:   select * from orders where created_at > ?
  Table:   orders
  Suggestion: List only the columns you need

================================================================================
  3 confirmed issues | 1 info | 12 queries
================================================================================
```

### Understanding the Sections

#### CONFIRMED Issues (ERROR / WARNING)

These are structural problems that **will cause performance degradation at scale**. They are verified by SQL pattern analysis and index metadata cross-referencing.

| Severity | Meaning | Action |
|----------|---------|--------|
| **ERROR** | Critical -- logic bugs, full table locks, N+1 | Must fix before merge |
| **WARNING** | Important -- missing indexes, inefficient patterns | Should fix |

#### INFO Items

Best-practice suggestions. Won't fail your build by default.

!!! tip "You can promote INFO to CONFIRMED"
    Change severity in the configuration. See [Configuration](../guide/configuration.md).

---

### Step 5: Fix the Issues

#### Fix N+1 Query

```java
// Before: lazy loading causes N+1
@Query("SELECT o FROM Order o WHERE o.createdAt > :date")
List<Order> findRecentOrders(@Param("date") LocalDateTime date);

// After: JOIN FETCH loads items in a single query
@Query("SELECT o FROM Order o JOIN FETCH o.items WHERE o.createdAt > :date")
List<Order> findRecentOrders(@Param("date") LocalDateTime date);
```

#### Fix Missing Index

```sql
CREATE INDEX idx_order_items_order_id ON order_items (order_id);
```

#### Fix Repeated Single INSERT

```yaml
# application.yml
spring:
  jpa:
    properties:
      hibernate:
        jdbc:
          batch_size: 50
        order_inserts: true
```

```java
// Use saveAll() instead of individual save() calls
auditLogRepository.saveAll(auditLogs);
```

---

## More Annotations

Once you're comfortable with `@QueryAudit`, explore the 4 annotations:

| Annotation | Use when... |
|---|---|
| `@QueryAudit` | You want full analysis with test failure on issues |
| `@EnableQueryInspector` | You want report-only mode (same as `@QueryAudit(failOnDetection = false)`) |
| `@DetectNPlusOne` | You only care about N+1 patterns |
| `@ExpectMaxQueryCount(5)` | You want to enforce a query budget |

---

## Next Steps

- :material-arrow-right: [Spring Boot Integration](spring-boot.md) -- Auto-configuration details and `application.yml` options
- :material-arrow-right: [Annotations Guide](../guide/annotations.md) -- All 4 annotations and when to use each
- :material-arrow-right: [Configuration](../guide/configuration.md) -- Tune thresholds, suppress issues
- :material-arrow-right: [Detection Rules](../detections/overview.md) -- All 57 detection rules explained
