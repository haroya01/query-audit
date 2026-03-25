# QueryAudit

**Catch SQL performance problems in your tests -- before they hit production.**

[![Build](https://github.com/haroya01/query-audit/actions/workflows/ci.yml/badge.svg)](https://github.com/haroya01/query-audit/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.haroya01/query-audit-core)](https://search.maven.org/search?q=g:io.github.haroya01)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Documentation](https://img.shields.io/badge/docs-query--guard.github.io-blue)](https://haroya01.github.io/query-audit)
[![Java 17+](https://img.shields.io/badge/Java-17%2B-blue)](https://openjdk.org/)

---

## Why QueryAudit?

Most SQL performance problems -- N+1 queries, missing indexes, unsafe DML -- are invisible during development because test datasets are small. They only surface in production under real load, causing outages and firefighting.

**QueryAudit shifts that discovery left into your test suite.**

| | Without QueryAudit | With QueryAudit |
|---|---|---|
| **N+1 queries** | Discovered in production via slow dashboards | Test fails immediately with the exact query and fix suggestion |
| **Missing indexes** | Noticed after a table grows to millions of rows | Detected by cross-referencing `SHOW INDEX` / `pg_catalog` during test |
| **Unsafe DML** | `UPDATE` without `WHERE` runs fine on 3 test rows | Flagged as a confirmed issue before merge |
| **SELECT \*** | Works fine until the table has 50 columns | Reported with a suggestion to list only needed columns |
| **Feedback loop** | Days to weeks (production monitoring) | Seconds (test execution) |

---

## What It Does

QueryAudit intercepts every SQL query executed during your JUnit tests, analyzes each one against **57 detection rules**, cross-references index metadata from your database, and fails your build when it finds performance anti-patterns.

- **57 detection rules** covering N+1 queries, missing indexes, DML safety, locking risks, ORM anti-patterns, and more
- **Zero configuration** -- add one annotation and go
- **Actionable reports** -- every issue includes the SQL, table, column, and a concrete fix suggestion
- **No production overhead** -- runs only in your test suite

### Supported Databases

| Database | Index Metadata Source | Module |
|---|---|---|
| **MySQL** 5.7+ / 8.0+ | `SHOW INDEX` | `query-audit-mysql` |
| **PostgreSQL** 12+ | `pg_catalog` | `query-audit-postgresql` |

---

## Quick Start

### 1. Add Dependencies

#### Gradle (MySQL)

```groovy
dependencies {
    testImplementation 'io.github.haroya01:query-audit-spring-boot-starter:0.1.0'
    testImplementation 'io.github.haroya01:query-audit-mysql:0.1.0'
}
```

#### Gradle (PostgreSQL)

```groovy
dependencies {
    testImplementation 'io.github.haroya01:query-audit-spring-boot-starter:0.1.0'
    testImplementation 'io.github.haroya01:query-audit-postgresql:0.1.0'
}
```

<details>
<summary><strong>Maven</strong></summary>

**MySQL:**

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

**PostgreSQL:**

```xml
<dependency>
    <groupId>io.github.haroya01</groupId>
    <artifactId>query-audit-spring-boot-starter</artifactId>
    <version>0.1.0</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>io.github.haroya01</groupId>
    <artifactId>query-audit-postgresql</artifactId>
    <version>0.1.0</version>
    <scope>test</scope>
</dependency>
```

</details>

#### Without Spring Boot

```groovy
dependencies {
    testImplementation 'io.github.haroya01:query-audit-junit5:0.1.0'
    testRuntimeOnly 'io.github.haroya01:query-audit-mysql:0.1.0'  // or query-audit-postgresql
}
```

### 2. Annotate Your Test

```java
@SpringBootTest
@QueryAudit
class OrderServiceTest {

    @Autowired
    private OrderService orderService;

    @Test
    void findRecentOrders_shouldUseIndex() {
        orderService.findRecentOrders(userId);
        // QueryAudit automatically analyzes all SQL executed during this test.
        // If an N+1 pattern or missing index is detected, the test fails.
    }
}
```

### 3. Read the Report

```
================================================================================
                          QUERY AUDIT REPORT
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

---

## 57 Detection Rules

QueryAudit ships with 57 detection rules organized into two confidence tiers:

**Confirmed (ERROR / WARNING)** -- structural and schema-based checks that are reliable regardless of test data size. These inspect SQL text, repetition patterns, and cross-reference actual index metadata from your database.

**Info** -- EXPLAIN-based and heuristic checks. Useful as early warnings but may vary with data volume.

| Category | Examples | Rules |
|---|---|---|
| **N+1 & Repetition** | N+1 queries, repeated single INSERT, mergeable queries | 3 |
| **Missing Index** | WHERE, JOIN, ORDER BY, GROUP BY, DML columns without index | 5 |
| **Index Misuse** | Composite leading column, redundant index, covering index opportunity, write amplification | 4 |
| **SQL Anti-Patterns** | SELECT *, function in WHERE, OR abuse, OFFSET pagination, LIKE wildcard, implicit type conversion | 6 |
| **DML Safety** | UPDATE without WHERE, DML without index, INSERT with SELECT *, INSERT ON DUPLICATE KEY | 6 |
| **Join Issues** | Cartesian join, too many joins, implicit join, unused join, correlated subquery | 5 |
| **Locking** | FOR UPDATE without index, FOR UPDATE on non-unique index, range lock risk, INSERT...SELECT locks source | 4 |
| **Query Structure** | DISTINCT misuse, HAVING misuse, UNION without ALL, large IN list, NOT IN subquery, ORDER BY RAND | 8 |
| **Hibernate / ORM** | Collection delete-reinsert, derived delete loads entities, excessive column fetch | 3 |
| **MySQL-Specific** | FIND_IN_SET, REGEXP usage, string concat in WHERE, implicit columns INSERT | 4 |
| **EXPLAIN-Based** | Full table scan, filesort, temporary table | 3 |
| **Miscellaneous** | Slow query, unbounded result set, query count regression, non-deterministic pagination, and more | 6 |

See the [Detection Rules Overview](https://haroya01.github.io/query-audit/detections/overview/) for the complete reference.

---

## Annotations

| Annotation | Description |
|---|---|
| `@QueryAudit` | Full analysis -- intercepts queries, runs all 57 detection rules, fails on confirmed issues |
| `@EnableQueryInspector` | Report-only mode -- runs all detections but never fails the test |
| `@DetectNPlusOne` | Focused check -- fails only if N+1 query patterns are detected |
| `@ExpectMaxQueryCount(n)` | Query budget -- fails if more than `n` queries are executed |

---

## Modules

| Module | Description |
|---|---|
| `query-audit-core` | Core analysis engine, detection rules, and SPI interfaces |
| `query-audit-junit5` | JUnit 5 extension and annotations |
| `query-audit-mysql` | MySQL `SHOW INDEX` metadata provider |
| `query-audit-postgresql` | PostgreSQL `IndexMetadataProvider` via `pg_catalog` |
| `query-audit-spring-boot-starter` | Spring Boot auto-configuration |

---

## Configuration

Configure via `application.yml` (Spring Boot) or programmatically:

```yaml
query-audit:
  enabled: true
  fail-on-detection: true
  n-plus-one:
    threshold: 3
  offset-pagination:
    threshold: 1000
  or-clause:
    threshold: 3
  suppress-patterns:
    - "select-all"
    - "missing-where-index:users.email"
  report:
    format: console
    show-info: true
```

See the [Configuration Reference](https://haroya01.github.io/query-audit/guide/configuration/) for the full list of options.

---

## Documentation

Full documentation is available at **[query-audit.github.io/query-audit](https://haroya01.github.io/query-audit)**.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for details on building, testing, and submitting pull requests.

---

## License

This project is licensed under the Apache License 2.0 -- see the [LICENSE](LICENSE) file for details.
