# QueryAudit

**Catch SQL performance problems in your tests -- before they hit production.**

[![Build](https://github.com/haroya01/query-audit/actions/workflows/ci.yml/badge.svg)](https://github.com/haroya01/query-audit/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.github.haroya01/query-audit-core)](https://search.maven.org/search?q=g:io.github.haroya01)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Documentation](https://img.shields.io/badge/docs-haroya01.github.io-blue)](https://haroya01.github.io/query-audit)
[![Java 17+](https://img.shields.io/badge/Java-17%2B-blue)](https://openjdk.org/)

---

## Why QueryAudit?

Most SQL performance problems -- N+1 queries, missing indexes, unsafe DML -- are invisible during development because test datasets are small. They only surface in production under real load, causing outages and firefighting.

**QueryAudit shifts that discovery left into your test suite** — and since 0.5.0 it goes a
step further: it turns your suite's database behavior into an explicit, enforceable
**contract**. Detect the anti-patterns, freeze the query profile as
[snapshot contracts](https://haroya01.github.io/query-audit/guide/contracts/), and gate every
change — human or automated — on an explicit, reviewable behavior diff.

| | Without QueryAudit | With QueryAudit |
|---|---|---|
| **N+1 queries** | Discovered in production via slow dashboards | Test fails immediately with the exact query and fix suggestion |
| **Missing indexes** | Noticed after a table grows to millions of rows | Detected by cross-referencing `SHOW INDEX` / `pg_catalog` during test |
| **Unsafe DML** | `UPDATE` without `WHERE` runs fine on 3 test rows | Flagged as a confirmed issue before merge |
| **SELECT \*** | Works fine until the table has 50 columns | Reported with a suggestion to list only needed columns |
| **Feedback loop** | Days to weeks (production monitoring) | Seconds (test execution) |

---

## What It Does

QueryAudit intercepts every SQL query executed during your JUnit tests, analyzes each one against **66 detection rules**, cross-references index metadata from your database, and fails your build when it finds performance anti-patterns.

- **66 detection rules** covering N+1 queries, missing indexes, DML safety, locking and race risks, connection-lifecycle misuse, ORM anti-patterns, and more
- **Zero configuration** -- add one annotation and go; or flip [`mode: all`](https://haroya01.github.io/query-audit/guide/configuration/#audit-coverage-mode) to audit every test with per-test opt-out
- **Rule profiles** -- `strict` / `recommended` / `minimal` tiers for a quiet, trustworthy first run
- **Per-type query budgets** -- `@ExpectQueries(select = 2, insert = 1)` fails the test when a SELECT / INSERT / UPDATE / DELETE budget is exceeded, and [snapshot contracts](https://haroya01.github.io/query-audit/guide/contracts/) record those budgets for the whole suite in one run
- **Actionable reports** -- every issue includes the SQL, table, column, call site, a concrete fix suggestion, and (in `report.json`) a machine-readable remediation hint
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
    testImplementation 'io.github.haroya01:query-audit-spring-boot-starter:0.4.0' // x-release-please-version
    testImplementation 'io.github.haroya01:query-audit-mysql:0.4.0' // x-release-please-version
}
```

#### Gradle (PostgreSQL)

```groovy
dependencies {
    testImplementation 'io.github.haroya01:query-audit-spring-boot-starter:0.4.0' // x-release-please-version
    testImplementation 'io.github.haroya01:query-audit-postgresql:0.4.0' // x-release-please-version
}
```

<details>
<summary><strong>Maven</strong></summary>

**MySQL:**

```xml
<dependency>
    <groupId>io.github.haroya01</groupId>
    <artifactId>query-audit-spring-boot-starter</artifactId>
    <version>0.4.0</version> <!-- x-release-please-version -->
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>io.github.haroya01</groupId>
    <artifactId>query-audit-mysql</artifactId>
    <version>0.4.0</version> <!-- x-release-please-version -->
    <scope>test</scope>
</dependency>
```

**PostgreSQL:**

```xml
<dependency>
    <groupId>io.github.haroya01</groupId>
    <artifactId>query-audit-spring-boot-starter</artifactId>
    <version>0.4.0</version> <!-- x-release-please-version -->
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>io.github.haroya01</groupId>
    <artifactId>query-audit-postgresql</artifactId>
    <version>0.4.0</version> <!-- x-release-please-version -->
    <scope>test</scope>
</dependency>
```

</details>

#### Without Spring Boot

```groovy
dependencies {
    testImplementation 'io.github.haroya01:query-audit-junit5:0.4.0' // x-release-please-version
    testRuntimeOnly 'io.github.haroya01:query-audit-mysql:0.4.0'  // or query-audit-postgresql // x-release-please-version
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

## 66 Detection Rules

QueryAudit ships with 66 active detection rules (68 issue types in the catalog; one detector
is disabled by default and one EXPLAIN-based type is reserved), organized into two confidence
tiers:

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
| **Locking & Races** | FOR UPDATE without index, FOR UPDATE on non-unique index, range lock risk, INSERT...SELECT locks source, read-modify-write without lock | 5 |
| **Query Structure** | DISTINCT misuse, HAVING misuse, UNION without ALL, large IN list, NOT IN subquery, ORDER BY RAND | 8 |
| **Hibernate / ORM** | Collection delete-reinsert, derived delete loads entities, excessive column fetch | 3 |
| **MySQL-Specific** | FIND_IN_SET, REGEXP usage, string concat in WHERE, implicit columns INSERT | 4 |
| **Connection Lifecycle** | Connection held while non-database work runs (the pool-exhaustion shape) | 1 |
| **EXPLAIN-Based** | Full table scan, filesort, temporary table | 3 |
| **Miscellaneous** | Slow query, unbounded result set, query count regression, non-deterministic pagination, and more | 6 |

See the [Detection Rules Overview](https://haroya01.github.io/query-audit/detections/overview/) for the complete reference.

---

## Annotations

| Annotation | Description |
|---|---|
| `@QueryAudit` | Full analysis -- intercepts queries, runs the full rule set, fails on confirmed issues |
| `@EnableQueryInspector` | Report-only mode -- runs all detections but never fails the test |
| `@DetectNPlusOne` | Focused check -- fails only if N+1 query patterns are detected |
| `@ExpectMaxQueryCount(n)` | Query budget -- fails if more than `n` queries are executed |
| `@ExpectQueries(select=n, ...)` | Per-type query budget -- fails when a SELECT/INSERT/UPDATE/DELETE budget is exceeded |
| `@QueryAuditExclude` | Opts a test class or method out of auditing -- the `mode: all` escape hatch |

Query budgets make per-test contracts explicit -- and `@ExpectQueries(insert = 0, update = 0, delete = 0)` turns a test into a read-only contract:

```java
@Test
@ExpectQueries(select = 2, insert = 1)  // per-type budgets
@ExpectMaxQueryCount(5)                 // total cap
void createOrder() {
    orderService.createOrder(request);
}
```

---

## Beyond Detection

Since 0.5.0, detection is the first rung of a ladder -- the rest turns findings into enforced, machine-consumable contracts:

- **[Audit coverage `mode: all`](https://haroya01.github.io/query-audit/guide/configuration/#audit-coverage-mode)** -- audit every test in the suite; tests opt *out* with `@QueryAuditExclude`. Pairs with the baseline so brownfield adoption fails only on *new* violations.
- **[Rule profiles](https://haroya01.github.io/query-audit/guide/configuration/#rule-profiles)** -- `strict` (everything), `recommended` (opinionated rules off), `minimal` (safety-critical gate). Explicit `disabled-rules` / `enabled-rules` always win.
- **[Query snapshot contracts](https://haroya01.github.io/query-audit/guide/contracts/)** -- record every test's query profile once; any deviation, in either direction, fails until the contract is re-recorded. The contracts file diff *is* the behavior change, reviewable in the PR.
- **[Versioned `report.json` + delta verdict](https://haroya01.github.io/query-audit/guide/reports/)** -- a `schemaVersion`ed envelope embedding call sites, structured remediation hints, and the index state behind each finding, plus a compare command whose exit code answers the fix-loop question: *did my change resolve the finding without introducing new ones?* Built for CI gates and automated dev loops alike -- a tool (or an agent) can act on the report without database access and verify its fix from the verdict alone.

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
  mode: annotated        # or "all" -- audit every test, opt out with @QueryAuditExclude
  profile: recommended   # strict | recommended | minimal
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
