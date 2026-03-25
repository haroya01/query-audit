# Architecture Overview

This page describes Query Guard's internal architecture, module structure, key interfaces,
and the full lifecycle of a query from execution to report.

---

## High-Level Flow

```
  JUnit Test Lifecycle
  ====================

  @BeforeAll
      |
      +- 1. Resolve DataSource (Spring context or static field)
      +- 2. Wrap DataSource with datasource-proxy
      +- 3. Collect index metadata (via database-specific provider)
      +- 4. Register Hibernate LazyLoadTracker (if Hibernate present)
      +- 5. Load query count baseline

  @BeforeEach
      |
      +- 6. interceptor.start() -- begin capturing queries

  Test Method Executes
      |
      +- Application code (JPA, JDBC, MyBatis, etc.)
      |       |
      |       v
      +- DataSource Proxy (datasource-proxy)
      |       |
      |       v
      +- QueryInterceptor.afterQuery()
      |       +- Records: SQL text, execution time, stack trace
      |       +- Normalizes: literals -> ?, whitespace collapsed, lowercased
      |
      +- Hibernate events (if applicable)
              +- INIT_COLLECTION -> LazyLoadTracker (collection lazy load)
              +- POST_LOAD -> LazyLoadTracker (proxy resolution)

  @AfterEach
      |
      +-  7. interceptor.stop()
      +-  8. QueryAuditAnalyzer.analyze()
      |       +- Run 57 DetectionRules against recorded queries
      |       +- Cross-reference index metadata
      |       +- Filter suppressed issues
      |       +- Split into CONFIRMED / INFO / ACKNOWLEDGED
      +-  9. Merge Hibernate N+1 issues (if any)
      +- 10. Detect query count regression (vs baseline)
      +- 11. ConsoleReporter.report() -> print to stdout
      +- 12. HtmlReportAggregator.addReport() -> accumulate for HTML
      +- 13. Check @ExpectMaxQueryCount
      +- 14. Check @DetectNPlusOne
      +- 15. Check failOnDetection -> throw AssertionError if needed

  @AfterAll
      |
      +- 16. Write HTML report to build/reports/query-audit/
      +- 17. Write JSON report (if configured)
      +- 18. Update query count baseline (if -DqueryGuard.updateBaseline=true)
      +- 19. Auto-open report in browser (if configured)
```

---

## Module Structure

```
query-audit/
+-- query-audit-core                   Core engine (no framework dependencies)
|   +-- detector/                      57 detection rules
|   +-- interceptor/                   QueryInterceptor, DataSourceProxyFactory
|   +-- parser/                        SQL parser (regex-based)
|   +-- model/                         Issue, QueryRecord, IssueType, Severity
|   +-- regression/                    Query count baseline & regression detection
|   +-- baseline/                      Issue baseline (acknowledged issues)
|   +-- config/                        QueryAuditConfig builder
|   +-- reporter/                      ConsoleReporter, JsonReporter, HtmlReporter
|
+-- query-audit-mysql                  MySQL-specific implementation
|   +-- MySqlIndexMetadataProvider     SHOW INDEX + INFORMATION_SCHEMA
|
+-- query-audit-postgresql             PostgreSQL-specific implementation
|   +-- PostgreSqlIndexMetadataProvider  pg_catalog system tables
|
+-- query-audit-junit5                 JUnit 5 integration
|   +-- QueryAuditExtension            Lifecycle callbacks (BeforeAll/Each, AfterAll/Each)
|   +-- @QueryAudit                    Full analysis annotation
|   +-- @EnableQueryInspector          Report-only annotation
|   +-- @DetectNPlusOne               N+1 focused annotation
|   +-- @ExpectMaxQueryCount           Query count assertion annotation
|
+-- query-audit-spring-boot-starter    Spring Boot auto-configuration
    +-- QueryAuditAutoConfiguration    BeanPostProcessor for DataSource wrapping
    +-- QueryAuditProperties           application.yml binding
```

### Module Dependency Graph

```
query-audit-spring-boot-starter
       |
       +-->  query-audit-junit5
       |         |
       |         +-->  query-audit-core
       |
       +-->  query-audit-core

query-audit-mysql
       |
       +-->  query-audit-core
       +-->  query-audit-junit5

query-audit-postgresql
       |
       +-->  query-audit-core
       +-->  query-audit-junit5
```

### Design Principle: Separation of Concerns

| Module | Knows About | Does NOT Know About |
|---|---|---|
| **core** | SQL parsing, detection rules, config, reporting | JUnit, Spring, any specific database |
| **mysql** | MySQL `SHOW INDEX`, `INFORMATION_SCHEMA` | JUnit, Spring |
| **postgresql** | PostgreSQL `pg_catalog` system tables | JUnit, Spring |
| **junit5** | JUnit 5 lifecycle, DataSource resolution | Spring, any specific database |
| **spring-boot-starter** | Spring Boot auto-configuration | Any specific database |

---

## Core Components

### QueryInterceptor

The entry point for query capture. Implements datasource-proxy's `QueryExecutionListener`.

```
  Application Code
       |
       v
  DataSource (wrapped by datasource-proxy)
       |
       |  SQL execution
       v
  QueryInterceptor.afterQuery()
       |
       +- Filter: skip if not active (outside test method)
       +- Capture: SQL text, execution time (nanos), timestamp
       +- Stack trace: up to 10 non-framework frames
       +- Store: Collections.synchronizedList(ArrayList<QueryRecord>)
```

**Stack trace capture** filters out framework classes to show only application code:

```
Filtered out: java.lang.Thread, sun.*, jdk.internal.*, org.springframework.*,
              org.hibernate.*, org.junit.*, net.ttddyy.*, com.zaxxer.*, ...

Kept: com.example.OrderService.findOrders:42
      com.example.OrderController.getOrders:28
```

This stack trace is used for:

- N+1 call-site grouping (same query from same location = N+1)
- Source location in the report

### QueryRecord

Immutable record storing a captured query:

```java
record QueryRecord(
    String sql,              // Raw SQL: "SELECT * FROM orders WHERE id = 123"
    String normalizedSql,    // Normalized: "select * from orders where id = ?"
    long executionTimeNanos, // Execution time
    long timestamp,          // Capture time
    String stackTrace,       // Application stack frames
    int fullStackHash        // Hash for call-site grouping
)
```

**Normalization rules:**

| Input | Output | Purpose |
|---|---|---|
| `WHERE id = 123` | `WHERE id = ?` | Group identical queries with different params |
| `WHERE name = 'John'` | `WHERE name = ?` | Handle string literals |
| `IN (1, 2, 3)` | `IN (?)` | Collapse IN lists |
| `SELECT  *   FROM` | `select * from` | Collapse whitespace + lowercase |

### QueryAuditAnalyzer

Central coordinator that runs all detection rules:

```
QueryAuditAnalyzer.analyze(testName, queries, indexMetadata)
       |
       +- 1. Filter suppressed queries
       |
       +- 2. Run 57 DetectionRules
       |      +- NPlusOneDetector
       |      +- SelectAllDetector
       |      +- MissingIndexDetector
       |      +- UpdateWithoutWhereDetector
       |      +- DmlWithoutIndexDetector
       |      +- RepeatedSingleInsertDetector
       |      +- ... (51 additional rules)
       |      +- Each returns List<Issue>
       |
       +- 3. Filter suppressed patterns
       |
       +- 4. Check against baseline (acknowledged issues)
       |
       +- 5. Split results
       |      +- CONFIRMED: ERROR + WARNING severity
       |      +- INFO: INFO severity
       |      +- ACKNOWLEDGED: baseline-matched issues
       |
       +- 6. Return QueryAuditReport
```

### DetectionRule Interface

Every detection rule implements this single interface:

```java
public interface DetectionRule {
    List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata);
}
```

Rules receive **all** recorded queries (SELECT + INSERT + UPDATE + DELETE) and the
collected index metadata. Each rule returns zero or more `Issue` objects.

### Issue Model

```java
record Issue(
    IssueType type,       // Enum: N_PLUS_ONE, MISSING_WHERE_INDEX, ...
    Severity severity,    // ERROR, WARNING, INFO
    String query,         // The normalized SQL that triggered this issue
    String table,         // Affected table
    String column,        // Affected column (nullable)
    String detail,        // Human-readable description of the problem
    String suggestion,    // How to fix it
    String sourceLocation // Stack trace (nullable)
)
```

---

## Extension Points

Query Guard is designed for extensibility via Java ServiceLoader. No changes to core
modules are required when adding support for new databases or custom rules.

### Adding a Custom Detection Rule

Implement `DetectionRule` and register via ServiceLoader:

```java
// 1. Implement the interface
public class MyCustomDetector implements DetectionRule {
    @Override
    public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
        // Your detection logic
    }
}

// 2. Register in META-INF/services/io.queryaudit.core.detector.DetectionRule
//    com.example.MyCustomDetector
```

The rule is automatically discovered and runs alongside all built-in detectors.

### Adding a New Database

Implement `IndexMetadataProvider` and register via ServiceLoader:

```java
// 1. Implement the interface
public class MariaDbIndexMetadataProvider implements IndexMetadataProvider {
    @Override
    public String supportedDatabase() { return "mariadb"; }

    @Override
    public IndexMetadata getIndexMetadata(Connection conn) throws SQLException {
        // Query system catalogs
    }
}

// 2. Register in META-INF/services/io.queryaudit.core.analyzer.IndexMetadataProvider
//    com.example.MariaDbIndexMetadataProvider
```

### Adding a New Reporter

Implement the `Reporter` interface:

```java
public class SlackReporter implements Reporter {
    @Override
    public void report(QueryAuditReport report) {
        // Send report summary to Slack
    }
}
```

### Extension Points Summary

| Extension Point | Interface | ServiceLoader File | Purpose |
|---|---|---|---|
| Detection rule | `DetectionRule` | `io.queryaudit.core.detector.DetectionRule` | Add custom query analysis rules |
| Database support | `IndexMetadataProvider` | `io.queryaudit.core.analyzer.IndexMetadataProvider` | Add index metadata for new databases |
| EXPLAIN analysis | `ExplainAnalyzer` | `io.queryaudit.core.analyzer.ExplainAnalyzer` | Parse database-specific EXPLAIN output |
| Reporter | `Reporter` | *(programmatic registration)* | Add custom report output formats |

---

## Detection Rules (57 Active Rules)

### SELECT-Focused Rules

| Rule | Issue Type | Severity | What it detects |
|---|---|---|---|
| NPlusOneDetector | `N_PLUS_ONE` | ERROR | Repeated query patterns from same call site |
| SelectAllDetector | `SELECT_ALL` | INFO | `SELECT *` usage |
| CountInsteadOfExistsDetector | `COUNT_INSTEAD_OF_EXISTS` | INFO | `COUNT(*)` where `EXISTS` is better |
| UnboundedResultSetDetector | `UNBOUNDED_RESULT_SET` | WARNING | SELECT without LIMIT |
| SlowQueryDetector | `SLOW_QUERY` | WARNING/ERROR | Queries exceeding time thresholds |
| ~~DuplicateQueryDetector~~ | `DUPLICATE_QUERY` | WARNING | Exact duplicate SQL *(disabled -- awaiting parameter tracking)* |
| CoveringIndexDetector | `COVERING_INDEX_OPPORTUNITY` | INFO | Queries that could use covering indexes |
| DistinctMisuseDetector | `DISTINCT_MISUSE` | WARNING | Unnecessary DISTINCT |
| HavingMisuseDetector | `HAVING_MISUSE` | WARNING | HAVING on non-aggregate columns |
| UnionWithoutAllDetector | `UNION_WITHOUT_ALL` | INFO | UNION without ALL |
| SelectCountStarWithoutWhereDetector | `COUNT_STAR_WITHOUT_WHERE` | INFO | `COUNT(*)` without WHERE scans entire table |
| ExcessiveColumnFetchDetector | `EXCESSIVE_COLUMN_FETCH` | INFO | Query fetches too many columns |

### WHERE Clause Rules

| Rule | Issue Type | Severity | What it detects |
|---|---|---|---|
| WhereFunctionDetector | `WHERE_FUNCTION` | ERROR | `DATE()`, `LOWER()`, etc. wrapping columns |
| OrAbuseDetector | `OR_ABUSE` | WARNING | Excessive OR conditions (>= threshold) |
| LikeWildcardDetector | `LIKE_LEADING_WILDCARD` | WARNING | Leading wildcard `LIKE '%...'` |
| NullComparisonDetector | `NULL_COMPARISON` | ERROR | `= NULL` instead of `IS NULL` |
| RedundantFilterDetector | `REDUNDANT_FILTER` | INFO | Duplicate WHERE conditions |
| StringConcatInWhereDetector | `STRING_CONCAT_IN_WHERE` | WARNING | String concatenation in WHERE prevents index usage |
| CaseInWhereDetector | `CASE_IN_WHERE` | WARNING | CASE expression in WHERE prevents index usage |

### Index-Related Rules

| Rule | Issue Type | Severity | What it detects |
|---|---|---|---|
| MissingIndexDetector | `MISSING_WHERE_INDEX` | ERROR | Unindexed WHERE columns |
| MissingIndexDetector | `MISSING_JOIN_INDEX` | ERROR | Unindexed JOIN columns |
| MissingIndexDetector | `MISSING_ORDER_BY_INDEX` | WARNING | Unindexed ORDER BY columns |
| MissingIndexDetector | `MISSING_GROUP_BY_INDEX` | WARNING | Unindexed GROUP BY columns |
| CompositeIndexDetector | `COMPOSITE_INDEX_LEADING_COLUMN` | WARNING | Composite index leading column unused |
| IndexRedundancyDetector | `REDUNDANT_INDEX` | WARNING | One index is prefix of another |
| ForUpdateWithoutIndexDetector | `FOR_UPDATE_WITHOUT_INDEX` | ERROR | FOR UPDATE without index |
| RangeLockDetector | `RANGE_LOCK_RISK` | WARNING | Range + FOR UPDATE on unindexed column |

### DML Rules

| Rule | Issue Type | Severity | What it detects |
|---|---|---|---|
| UpdateWithoutWhereDetector | `UPDATE_WITHOUT_WHERE` | ERROR | UPDATE/DELETE without WHERE |
| DmlWithoutIndexDetector | `DML_WITHOUT_INDEX` | WARNING | UPDATE/DELETE WHERE without index |
| RepeatedSingleInsertDetector | `REPEATED_SINGLE_INSERT` | WARNING | Same INSERT pattern repeated >= 3 times |
| InsertSelectAllDetector | `INSERT_SELECT_ALL` | WARNING | `INSERT ... SELECT *` |
| InsertOnDuplicateKeyDetector | `INSERT_ON_DUPLICATE_KEY` | WARNING | INSERT ON DUPLICATE KEY UPDATE may cause deadlocks |
| InsertSelectLocksSourceDetector | `INSERT_SELECT_LOCKS_SOURCE` | INFO | INSERT...SELECT locks source table rows |
| ImplicitColumnsInsertDetector | `IMPLICIT_COLUMNS_INSERT` | WARNING | INSERT without explicit column list |
| SubqueryInDmlDetector | `SUBQUERY_IN_DML` | WARNING | Subquery in UPDATE/DELETE cannot use semijoin optimization |

### JOIN Rules

| Rule | Issue Type | Severity | What it detects |
|---|---|---|---|
| CartesianJoinDetector | `CARTESIAN_JOIN` | ERROR | JOIN without ON condition |
| TooManyJoinsDetector | `TOO_MANY_JOINS` | WARNING | Excessive number of JOINs |
| ImplicitJoinDetector | `IMPLICIT_JOIN` | WARNING | Implicit comma-separated join syntax |
| UnusedJoinDetector | `UNUSED_JOIN` | WARNING | LEFT JOIN table never referenced in query |
| CorrelatedSubqueryDetector | `CORRELATED_SUBQUERY` | WARNING | Correlated subquery in SELECT |

### Locking Rules

| Rule | Issue Type | Severity | What it detects |
|---|---|---|---|
| ForUpdateNonUniqueIndexDetector | `FOR_UPDATE_NON_UNIQUE` | WARNING | FOR UPDATE on non-unique index causes gap locks |
| ForUpdateWithoutTimeoutDetector | `FOR_UPDATE_WITHOUT_TIMEOUT` | WARNING | FOR UPDATE without NOWAIT or SKIP LOCKED |

### Pagination and Ordering Rules

| Rule | Issue Type | Severity | What it detects |
|---|---|---|---|
| OffsetPaginationDetector | `OFFSET_PAGINATION` | WARNING | Large OFFSET value |
| OrderByLimitWithoutIndexDetector | `ORDER_BY_LIMIT_WITHOUT_INDEX` | WARNING | ORDER BY + LIMIT without index |
| OrderByRandDetector | `ORDER_BY_RAND` | ERROR | ORDER BY RAND() causes full table scan and sort |
| NonDeterministicPaginationDetector | `NON_DETERMINISTIC_PAGINATION` | INFO | ORDER BY + LIMIT on non-unique column |
| LimitWithoutOrderByDetector | `LIMIT_WITHOUT_ORDER_BY` | WARNING | LIMIT without ORDER BY returns non-deterministic rows |

### ORM / Hibernate Rules

| Rule | Issue Type | Severity | What it detects |
|---|---|---|---|
| CollectionManagementDetector | `COLLECTION_DELETE_REINSERT` | WARNING | DELETE-all + re-INSERT pattern indicates inefficient collection management |
| DerivedDeleteDetector | `DERIVED_DELETE_LOADS_ENTITIES` | WARNING | Derived delete loads entities before individual deletes |
| MergeableQueriesDetector | `MERGEABLE_QUERIES` | INFO | Multiple queries to same table could be merged |

### Other Rules

| Rule | Issue Type | Severity | What it detects |
|---|---|---|---|
| SargabilityDetector | `NON_SARGABLE_EXPRESSION` | ERROR | Arithmetic on columns (`col + 1 = ?`) |
| ImplicitTypeConversionDetector | `IMPLICIT_TYPE_CONVERSION` | WARNING | String column compared to number |
| LargeInListDetector | `LARGE_IN_LIST` | WARNING | IN clause with too many values |
| WriteAmplificationDetector | `WRITE_AMPLIFICATION` | WARNING | Table with > 6 indexes |
| NotInSubqueryDetector | `NOT_IN_SUBQUERY` | ERROR | NOT IN subquery returns empty when subquery contains NULL |
| GroupByFunctionDetector | `GROUP_BY_FUNCTION` | WARNING | Function in GROUP BY prevents index usage |
| RegexpInsteadOfLikeDetector | `REGEXP_INSTEAD_OF_LIKE` | WARNING | REGEXP/RLIKE prevents index usage |
| FindInSetDetector | `FIND_IN_SET_USAGE` | WARNING | FIND_IN_SET indicates comma-separated values violating 1NF |
| WindowFunctionWithoutPartitionDetector | `WINDOW_FUNCTION_WITHOUT_PARTITION` | WARNING | Window function without PARTITION BY |
| ForceIndexHintDetector | `FORCE_INDEX_HINT` | INFO | FORCE/USE/IGNORE INDEX hint overrides optimizer |
| QueryCountRegressionDetector | `QUERY_COUNT_REGRESSION` | WARNING | Query count regression vs baseline |

---

## Index Metadata Collection

Query Guard collects index information from **two sources** and merges them:

```
                    +-------------------------+
                    | IndexMetadata (merged)   |
                    +-----------+-------------+
                                |
                  +-------------+-------------+
                  |                           |
   +--------------+----------+   +------------+-----------+
   | Database metadata       |   | JPA @Table metadata    |
   | (SHOW INDEX / pg_catalog)|   | (@Index annotations)   |
   |                         |   |                        |
   | Authoritative:          |   | Supplementary:         |
   | real indexes from       |   | indexes declared in    |
   | the test database       |   | JPA entity classes     |
   +-------------------------+   +------------------------+
```

**Database metadata** (via `IndexMetadataProvider` SPI) is authoritative -- it reflects
the actual indexes that exist in the test database. **JPA metadata** supplements this
with indexes declared in `@Table(indexes = ...)` that might not yet exist in the
test schema (e.g., when using `ddl-auto=create-drop`).

### IndexMetadataProvider SPI

```java
public interface IndexMetadataProvider {
    String supportedDatabase();   // e.g., "mysql"
    IndexMetadata getIndexMetadata(Connection connection) throws SQLException;
}
```

Implementations are discovered via **Java ServiceLoader**. The provider's
`supportedDatabase()` return value is matched against `Connection.getMetaData().getDatabaseProductName()`.

---

## Query Count Regression Detection

Query Guard tracks query counts per test method across runs using a baseline file
(`.query-audit-counts`).

```
  Run 1:  OrderServiceTest.findOrders -> 5 SELECT, 0 INSERT -> saved to baseline
  Run 2:  OrderServiceTest.findOrders -> 15 SELECT, 0 INSERT -> regression detected!
          (3x increase, +10 queries)
```

**Regression thresholds** (both must be met):

| Condition | Threshold | Rationale |
|---|---|---|
| Increase ratio | >= 1.5x (50%) | Catches significant increases |
| Absolute increase | >= 5 queries | Avoids false positives on small tests (2 -> 4 = 2x but only +2) |

**Severity mapping:**

| Ratio | Severity |
|---|---|
| >= 3.0x | ERROR |
| >= 2.0x | WARNING |
| >= 1.5x | WARNING |

### Updating the Baseline

```bash
./gradlew test -DqueryGuard.updateBaseline=true
```

---

## Report Generation

Query Guard includes three reporter implementations, all in `query-audit-core`:

### Console Report (`ConsoleReporter`)

ANSI-colored output printed to stdout after each test method. The default reporter.

### JSON Report (`JsonReporter`)

Machine-readable JSON output. Uses `StringBuilder` exclusively to avoid external
JSON library dependencies.

### HTML Report (`HtmlReporter` / `HtmlReportAggregator`)

Aggregated report generated by `HtmlReportAggregator` and written to
`build/reports/query-audit/index.html` after all tests complete. Provides filtering,
search, and drill-down by test class. Self-contained single HTML file with embedded
CSS and JavaScript.

---

## Design Principles

### Zero False Positives on CONFIRMED Issues

CONFIRMED issues are **structurally certain** based on SQL parsing and index metadata.
Query Guard never guesses. Issues that depend on data volume or query planner behavior
are classified as INFO, not CONFIRMED.

### Test-Time Only

Query Guard is a **test dependency**. It never runs in production. The datasource
proxy wrapping only happens in the test classpath.

### Transparent to Application Code

Application code does not need to know about Query Guard. The DataSource proxy is
injected transparently via `BeanPostProcessor` (Spring Boot) or reflection-based
DataSource resolution (plain JUnit 5).

### Extensibility via SPI

- **New database**: Implement `IndexMetadataProvider` in a new module, register via ServiceLoader
- **New detection rule**: Implement `DetectionRule`, register via ServiceLoader or programmatically
- **New reporter**: Implement `Reporter` interface

---

## See Also

- [Adding Database Support](new-database.md) -- Step-by-step guide for new database modules
- [Contributing Guide](contributing.md) -- How to add rules, reporters, and database support
- [Configuration Reference](../guide/configuration.md) -- All configuration options
