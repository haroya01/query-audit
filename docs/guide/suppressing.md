# Suppressing Issues

Not every detected issue is a problem. Sometimes a full table scan is intentional,
a `SELECT *` is used in a test helper, or a health-check query should be ignored.
QueryAudit provides multiple ways to suppress issues at different levels.

---

## When to Suppress vs When to Fix

Before suppressing an issue, consider whether it should be fixed instead. Use the
following decision process:

```
Is the issue a real performance problem in production?
  |
  +-- YES --> Fix it.
  |           Examples: N+1 queries, missing indexes on high-traffic tables
  |
  +-- NO, it's intentional
  |     |
  |     +-- Is it temporary? (e.g., migrating legacy code)
  |     |     |
  |     |     +-- YES --> Suppress with a comment, track in issue tracker
  |     |     +-- NO  --> Suppress permanently
  |     |
  |     +-- Is it a test artifact? (e.g., setup queries, health checks)
  |           |
  |           +-- YES --> Use suppress-queries for SQL substrings
  |           +-- NO  --> Use suppress-patterns with qualified codes
  |
  +-- UNSURE --> Start with @EnableQueryInspector (report only),
                 review findings, then decide
```

### Common Reasons to Suppress

| Scenario | Recommended Action | Mechanism |
|---|---|---|
| Small lookup table (e.g., `config`, `feature_flags`) | Suppress | `suppress-patterns: ["missing-where-index:config.key"]` |
| Batch job with intentional full scans | Suppress per test | `@QueryAudit(suppress = {"select-all", "unbounded-result-set"})` |
| Health-check query (`SELECT 1`) | Suppress globally | `suppress-queries: ["SELECT 1"]` |
| Test helper utility queries | Suppress per class | `@QueryAudit(suppress = {"select-all"})` |
| Known trade-off accepted by the team | Suppress globally | `suppress-patterns` in application.yml |
| Legacy code being actively migrated | Suppress temporarily | `suppress-patterns` + track in issue tracker |
| Hibernate Envers / audit table queries | Suppress globally | `suppress-queries: ["_aud", "revinfo"]` |
| Connection pool validation queries | Suppress globally | `suppress-queries: ["SELECT 1", "SET autocommit"]` |

### When NOT to Suppress

| Scenario | Why Not | Better Alternative |
|---|---|---|
| N+1 query on a high-traffic endpoint | Causes production latency | Use JOIN FETCH or `@EntityGraph` |
| Missing index on a frequently queried column | Full table scans at scale | Add the index |
| UPDATE without WHERE | Data corruption risk | Add a WHERE clause |
| Suppressing all of a rule type globally | Hides real problems | Use qualified suppression instead |
| Cartesian JOIN | Explosive row count | Add proper JOIN conditions |
| Non-sargable expression (function on column) | Index unusable, full scan at scale | Rewrite the WHERE clause |

### Real-World Examples: Suppress vs Fix

=== "Fix: N+1 in REST endpoint"

    ```java
    // BEFORE: N+1 -- each order triggers a lazy load of items
    @GetMapping("/orders")
    public List<OrderDto> getOrders() {
        return orderRepository.findAll().stream()  // 1 query
            .map(order -> new OrderDto(
                order.getId(),
                order.getItems().size()            // N queries!
            ))
            .toList();
    }

    // AFTER: Fixed with JOIN FETCH
    @Query("SELECT o FROM Order o JOIN FETCH o.items")
    List<Order> findAllWithItems();                // 1 query total
    ```

=== "Suppress: Lookup table without index"

    ```java
    // The 'config' table has 10 rows and is never expected to grow.
    // A missing index warning is noise here.
    @QueryAudit(suppress = {"missing-where-index:config.key"})
    @Test
    void shouldLoadAppConfig() {
        configService.getValue("app.version");
    }
    ```

=== "Suppress: Test data setup"

    ```java
    // @BeforeEach inserts test data that triggers repeated-single-insert.
    // These are test artifacts, not production patterns.
    @QueryAudit(suppress = {"repeated-single-insert"})
    @SpringBootTest
    class OrderServiceTest {
        @BeforeEach
        void setUp() {
            orderRepository.save(new Order(...));
            orderRepository.save(new Order(...));
            orderRepository.save(new Order(...));
        }
    }
    ```

=== "Suppress: Admin dashboard with SELECT *"

    ```java
    // The admin dashboard intentionally shows all columns.
    // SELECT * is the correct choice here.
    @QueryAudit(suppress = {"select-all"})
    @Test
    void shouldDisplayAllUserFields() {
        adminService.getUserDetails(userId);
    }
    ```

---

## Annotation-Level Suppression

Use the `suppress` attribute on `@QueryAudit` to suppress specific issue types for a
test class or method.

### Suppress by Issue Code

```java
@QueryAudit(suppress = {"select-all", "n-plus-one"})
@SpringBootTest
class BatchExportTest {
    // All select-all and n-plus-one detections are suppressed
    // for every test in this class.
}
```

### Qualified Suppression

To suppress an issue only for a specific table and column, use the
`issue-code:table.column` format:

```java
@QueryAudit(suppress = {
    "missing-where-index:orders.status",   // Only suppress this specific column
    "select-all"                           // Suppress all select-all findings
})
@Test
void findPendingOrders() {
    orderService.findByStatus("PENDING");
}
```

The qualified format is `<issue-code>:<table>.<column>`:

| Pattern | What it suppresses |
|---|---|
| `"n-plus-one"` | All N+1 detections |
| `"missing-where-index"` | All missing WHERE index detections |
| `"missing-where-index:orders.status"` | Only the missing index on `orders.status` |
| `"missing-join-index:order_items.order_id"` | Only the missing join index on `order_items.order_id` |

!!! tip "Be specific"
    Prefer qualified suppression over blanket suppression. Suppressing
    `"missing-where-index:orders.status"` is safer than suppressing all
    `"missing-where-index"` findings, because the latter hides real problems
    on other columns.

### Method-Level vs Class-Level

Method-level `suppress` takes precedence over class-level. Note that method-level
**replaces** (does not merge with) the class-level list:

```java
@QueryAudit(suppress = {"select-all"})          // Class-level
@SpringBootTest
class OrderServiceTest {

    @Test
    void findOrders() {
        // select-all is suppressed (inherited from class)
    }

    @QueryAudit(suppress = {"n-plus-one"})       // Method-level overrides
    @Test
    void findOrdersWithItems() {
        // Only n-plus-one is suppressed; select-all is NOT suppressed
        // because the method-level annotation replaces the class-level one.
    }
}
```

---

## Config-Level Suppression (application.yml)

Use `suppress-patterns` in `application.yml` to suppress issues globally across all
tests. This is useful for project-wide decisions.

```yaml
query-audit:
  suppress-patterns:
    - "select-all"                              # Suppress all SELECT * warnings
    - "missing-where-index:config.key"          # Known small lookup table
    - "offset-pagination"                       # Team accepts offset pagination for now
```

The same qualified format (`issue-code:table.column`) works here.

---

## Query-Level Suppression

Use `suppress-queries` to suppress specific SQL strings. This is useful for queries
you do not control, like connection validation or ORM-generated metadata queries.

### In application.yml

```yaml
query-audit:
  suppress-queries:
    - "SELECT 1"                     # Health-check
    - "SHOW WARNINGS"               # MySQL driver internals
    - "SET autocommit"              # Connection pool setup
    - "_aud"                        # Hibernate Envers audit tables
    - "revinfo"                     # Hibernate Envers revision info
```

### Programmatic

```java
QueryAuditConfig config = QueryAuditConfig.builder()
    .addSuppressQuery("SELECT 1")                 // Health-check
    .addSuppressQuery("SHOW WARNINGS")            // MySQL driver internals
    .addSuppressQuery("SET autocommit")           // Connection pool setup
    .build();
```

### How Matching Works

Query suppression uses **case-insensitive substring matching**. If the recorded SQL
(after whitespace normalization and lowercasing) contains the suppress string, the
entire query is excluded from analysis.

For example, `addSuppressQuery("SELECT 1")` suppresses:

- `SELECT 1`
- `select 1`
- `SELECT 1 FROM dual`
- `/* ping */ SELECT 1`

!!! warning "Be careful with broad patterns"
    A pattern like `"SELECT"` would suppress every query. Keep suppress patterns as
    specific as possible.

---

## Disabling Rules Entirely

To completely disable a rule so it never runs (not just suppress its output), use
`disabled-rules`:

```yaml
query-audit:
  disabled-rules:
    - "select-all"          # Never run SELECT * detection
    - "unbounded-result-set" # Never run unbounded result set detection
```

!!! info "Disabled vs suppressed"
    **Disabled rules** are never executed -- they have zero performance cost.
    **Suppressed issues** are still detected but filtered from the report and
    do not cause test failures. Use `disabled-rules` when you are certain a
    rule is never relevant for your project.

---

## Baseline-Based Suppression

The query count baseline tracks known issue counts per test. Issues that match
the baseline are classified as **ACKNOWLEDGED** rather than CONFIRMED, and do
not cause test failures.

```bash
# Create or update the baseline from current test results
./gradlew test -DqueryAudit.updateBaseline=true
```

This is useful for gradual adoption: establish a baseline of existing issues, then
only fail on new regressions.

---

## Severity Overrides

You can change the severity of specific rules without disabling them. This lets you
promote warnings to errors (stricter) or demote errors to warnings (more lenient):

```yaml
query-audit:
  severity-overrides:
    unbounded-result-set: ERROR    # Promote to error -- always require LIMIT
    select-all: WARNING            # Promote from INFO to WARNING
    offset-pagination: INFO        # Demote to INFO -- just informational
```

---

## Suppression Summary

| Level | Mechanism | Scope | Granularity |
|---|---|---|---|
| Method annotation | `@QueryAudit(suppress = {...})` | Single test method | Issue code or qualified |
| Class annotation | `@QueryAudit(suppress = {...})` | All tests in the class | Issue code or qualified |
| application.yml | `suppress-patterns` | All tests in the project | Issue code or qualified |
| application.yml | `suppress-queries` | All tests in the project | SQL substring |
| application.yml | `disabled-rules` | All tests in the project | Rule code (never runs) |
| application.yml | `severity-overrides` | All tests in the project | Change severity per rule |
| Programmatic | `addSuppressQuery(...)` | All tests using that config | SQL substring |
| Baseline | `.query-audit-baseline` file | All tests in the project | Per-test issue matching |

---

## Available Issue Codes

For a complete list of issue codes that can be used in suppression, see the
[Issue Types Reference](configuration.md#issue-types-reference) in the Configuration
guide.

---

## See Also

- [Configuration Reference](configuration.md) -- All suppression-related configuration options
- [Annotations Guide](annotations.md) -- Using `suppress` attribute on annotations
- [CI/CD Integration](ci-cd.md) -- Using baseline files in CI pipelines
