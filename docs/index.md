---
title: Query Guard -- Automatic Query Performance Analysis for JUnit Tests
description: Catch N+1 queries, missing indexes, and SQL anti-patterns before they hit production.
---

<style>
.qg-hero {
  text-align: center;
  padding: 2rem 0 3rem;
}
.qg-hero h1 {
  font-size: 3rem;
  font-weight: 800;
  margin-bottom: 0.5rem;
}
.qg-hero .qg-tagline {
  font-size: 1.25rem;
  color: var(--md-default-fg-color--light);
  max-width: 640px;
  margin: 0 auto 2rem;
}
.qg-features {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 1.5rem;
  margin: 2rem 0;
}
.qg-feature {
  padding: 1.5rem;
  border-radius: 8px;
  border: 1px solid var(--md-default-fg-color--lightest);
}
.qg-feature h3 {
  margin-top: 0;
}
.qg-flow {
  display: flex;
  flex-wrap: wrap;
  justify-content: center;
  align-items: center;
  gap: 0.5rem;
  margin: 2rem 0;
  font-size: 1.05rem;
}
.qg-flow-step {
  padding: 0.75rem 1.25rem;
  border-radius: 8px;
  border: 1px solid var(--md-default-fg-color--lightest);
  background: var(--md-code-bg-color);
  text-align: center;
  font-weight: 500;
}
.qg-flow-arrow {
  font-size: 1.5rem;
  color: var(--md-default-fg-color--light);
}
</style>

<div class="qg-hero" markdown>

# Query Guard

<p class="qg-tagline">
Stop shipping slow queries. Catch N+1, missing indexes, and 57 other SQL anti-patterns automatically during your JUnit tests.
</p>

[Get Started](getting-started/installation.md){ .md-button .md-button--primary }
[View on GitHub](https://github.com/query-audit/query-audit){ .md-button }

</div>

---

<div class="qg-features" markdown>

<div class="qg-feature" markdown>

### :material-magnify-scan: 57 Detection Rules

Catches N+1 queries, missing indexes, `SELECT *`, DML anti-patterns, batch insert optimization, functions in `WHERE` clauses, implicit type conversions, locking risks, ORM inefficiencies, and more.

</div>

<div class="qg-feature" markdown>

### :material-check-decagram: 100% Reliable

Confirmed detections are **structural** -- they inspect SQL text and index metadata, not runtime data. No flaky heuristics. No false positives. If Query Guard flags it, it is a real problem.

</div>

<div class="qg-feature" markdown>

### :material-cog-off: Zero Config

Add one annotation to your test class. That's it. Query Guard auto-discovers your `DataSource`, intercepts queries, analyzes them, and reports issues. Works with JUnit 5 and Spring Boot. Supports **MySQL** and **PostgreSQL**.

</div>

<div class="qg-feature" markdown>

### :material-file-document-check: Actionable Reports

Every issue includes the exact SQL statement, affected table, column, the detection rule that fired, and a concrete fix suggestion you can apply immediately.

</div>

</div>

---

## How It Works

<div class="qg-flow">
  <div class="qg-flow-step">:material-test-tube: Run JUnit test</div>
  <div class="qg-flow-arrow">:material-arrow-right:</div>
  <div class="qg-flow-step">:material-database-search: Intercept SQL queries</div>
  <div class="qg-flow-arrow">:material-arrow-right:</div>
  <div class="qg-flow-step">:material-table-key: Fetch index metadata</div>
  <div class="qg-flow-arrow">:material-arrow-right:</div>
  <div class="qg-flow-step">:material-magnify: Apply 57 rules</div>
  <div class="qg-flow-arrow">:material-arrow-right:</div>
  <div class="qg-flow-step">:material-alert-circle: Report & fail</div>
</div>

Query Guard hooks into your test's `DataSource` via a lightweight proxy. During test execution, it captures every SQL statement. After the test completes, it:

1. **Parses** each query to identify tables, columns, joins, and clauses
2. **Fetches index metadata** from your database (MySQL `SHOW INDEX` or PostgreSQL `pg_catalog`)
3. **Cross-references** the query structure against actual indexes
4. **Applies 57 detection rules** covering N+1, missing indexes, DML safety, locking risks, and more
5. **Produces a structured report** with severity, root cause, and fix suggestions

No runtime agents. No production overhead. Just add an annotation.

---

## See It in Action

Add a single annotation to any test class:

```java
@SpringBootTest
@QueryAudit // (1)!
class OrderServiceTest {

    @Autowired
    private OrderService orderService;

    @Test
    void findOrdersWithItems() {
        List<Order> orders = orderService.findAllWithItems();
        assertThat(orders).hasSize(5);
    }
}
```

1.  That's it. No configuration, no extra beans, no proxy wiring.

Run your tests, and Query Guard produces a report like this:

```
================================================================================
                          QUERY GUARD REPORT
                    OrderServiceTest (8 queries analyzed)
================================================================================

CONFIRMED ISSUES (action required)
────────────────────────────────────────────────────────────────────────────────

[ERROR] N+1 Query Detected
  Repeated query: select * from order_items where order_id = ?
  Executions:     5 times (threshold: 3)
  Suggestion:     Use JOIN FETCH or @EntityGraph to load order_items
                  with the parent query.

[ERROR] Missing Index
  Query:   select * from order_items where order_id = ?
  Table:   order_items
  Column:  order_id
  Suggestion: CREATE INDEX idx_order_items_order_id
              ON order_items (order_id);

[WARNING] Repeated single-row INSERT should use batch insert
  Query:   insert into orders (...) values (?, ?, ?)
  Table:   orders
  Detail:  Single-row INSERT executed 10 times. Each INSERT causes a
           separate network round-trip and log flush.
  Suggestion: Use batch INSERT (saveAll() in JPA with hibernate.jdbc.batch_size).

────────────────────────────────────────────────────────────────────────────────
INFO (for review)
────────────────────────────────────────────────────────────────────────────────

[WARNING] SELECT * Usage
  Query:   select * from orders where user_id = ?
  Table:   orders
  Suggestion: List only the columns you need

================================================================================
  3 confirmed issues | 1 info | 8 queries
================================================================================
```

!!! success "CONFIRMED vs INFO"
    **CONFIRMED** issues are structural problems -- missing indexes, N+1 patterns, unsafe DML. These will cause performance issues at scale and should be fixed.

    **INFO** items are best-practice suggestions like avoiding `SELECT *`. They won't fail your build by default, but are worth reviewing.

---

## Why Query Guard?

Most teams discover query performance problems in one of two painful ways: a production incident, or a slow code review where someone manually checks SQL logs.

Existing tools help with **observability** -- they let you *see* your queries:

| Tool | What it does | What it doesn't do |
|---|---|---|
| **datasource-proxy** | Logs queries and execution time | No analysis, no detection |
| **p6spy** | Logs queries with bind parameters | Same -- logging only |
| **Spring Hibernate statistics** | Counts queries per session | Counts, but doesn't analyze |

**Query Guard closes the gap.** It doesn't just log queries -- it applies 57 detection rules to every captured query, cross-references index metadata from your database (MySQL `SHOW INDEX` or PostgreSQL `pg_catalog`), and produces a structured report with concrete fix suggestions.

| Capability | datasource-proxy | p6spy | Query Guard |
|---|:---:|:---:|:---:|
| Query logging | :material-check: | :material-check: | :material-check: |
| Bind parameter capture | :material-close: | :material-check: | :material-check: |
| N+1 detection | :material-close: | :material-close: | :material-check: |
| Missing index detection | :material-close: | :material-close: | :material-check: |
| DML anti-pattern detection | :material-close: | :material-close: | :material-check: |
| SQL anti-pattern detection | :material-close: | :material-close: | :material-check: |
| Query count regression | :material-close: | :material-close: | :material-check: |
| Fix suggestions | :material-close: | :material-close: | :material-check: |
| CI/CD fail-on-issue | :material-close: | :material-close: | :material-check: |

---

## Getting Started

Ready to add Query Guard to your project? It takes about two minutes.

[Installation Guide :material-arrow-right:](getting-started/installation.md){ .md-button .md-button--primary }
