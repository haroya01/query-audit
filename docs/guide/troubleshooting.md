# Troubleshooting

Common issues and solutions when using QueryAudit.

---

## QueryAudit Not Detecting Any Queries

**Symptom:** Report shows `0 queries analyzed` even though your test executes SQL.

**Causes and fixes:**

=== "Spring Boot: DataSource not wrapped"

    The `QueryAuditAutoConfiguration` BeanPostProcessor may not have been applied.

    **Check:** Add `@QueryAudit` or `@EnableQueryInspector` to your test class.
    Without these annotations, the extension is not registered.

    ```java
    @SpringBootTest
    @QueryAudit           // <-- Required
    class OrderServiceTest { ... }
    ```

=== "Non-Spring: DataSource not found"

    QueryAudit needs a `static DataSource` field in the test class.

    ```java
    @QueryAudit
    class OrderRepositoryTest {
        // Must be static for QueryAuditExtension to find it
        static DataSource dataSource = createDataSource();
    }
    ```

=== "Queries executed outside test method"

    QueryAudit only captures queries between `@BeforeEach` and `@AfterEach`.
    Queries in `@BeforeAll` or static initializers are not captured.

=== "QueryAudit disabled"

    Check that you have not set `query-audit.enabled: false` in your test configuration:

    ```yaml
    # Verify this is not set
    query-audit:
      enabled: false   # <-- This disables all query interception
    ```

---

## Why Didn't QueryAudit Detect My Issue?

**Symptom:** You know a query has a problem, but QueryAudit does not flag it.

**Possible causes:**

### The rule requires index metadata

Many detection rules (e.g., `missing-where-index`, `missing-join-index`,
`composite-index-leading`) require index metadata from the database. If metadata
is not available, these rules silently skip.

**Fix:** Ensure the correct database module is on your test classpath:

```groovy
// Gradle
testImplementation 'io.github.haroya01:query-audit-mysql:${version}'
// or
testImplementation 'io.github.haroya01:query-audit-postgresql:${version}'
```

### The query is suppressed

Check your `suppress-patterns`, `suppress-queries`, and `disabled-rules` configuration.
A broad suppress pattern may be hiding the issue.

```yaml
# Check for broad suppressions
query-audit:
  suppress-patterns:
    - "missing-where-index"    # This suppresses ALL missing index findings!
```

!!! tip "Debugging suppressions"
    Temporarily remove all suppression settings and re-run the test to see if
    the issue appears. Then narrow down which suppression pattern was hiding it.

### The issue type is INFO and `show-info` is disabled

INFO-level issues are hidden when `report.show-info` is `false`.

```yaml
query-audit:
  report:
    show-info: true    # Ensure INFO issues are visible
```

### The threshold is too high

Some rules use thresholds. If your query does not exceed the threshold, it won't
be flagged:

| Rule | Threshold Setting | Default |
|---|---|---|
| N+1 | `n-plus-one.threshold` | 3 |
| Large IN list | `large-in-list.threshold` | 100 |
| Too many JOINs | `too-many-joins.threshold` | 5 |
| OR abuse | `or-clause.threshold` | 3 |
| OFFSET pagination | `offset-pagination.threshold` | 1000 |
| Excessive columns | `excessive-column.threshold` | 15 |
| Repeated INSERT | `repeated-insert.threshold` | 3 |
| Slow query (warning) | `slow-query.warning-ms` | 500 |
| Slow query (error) | `slow-query.error-ms` | 3000 |

### The query type is not analyzed

QueryAudit analyzes SELECT, INSERT, UPDATE, and DELETE statements. DDL statements
(`CREATE TABLE`, `ALTER TABLE`) and session-level commands (`SET`, `SHOW`) are not
analyzed.

### The rule is disabled

Check if the rule has been explicitly disabled:

```yaml
query-audit:
  disabled-rules:
    - "missing-where-index"    # This rule never runs!
```

### SQL is too complex for the parser

QueryAudit uses JSQLParser for structural SQL analysis (extracting WHERE columns,
JOIN columns, table names, etc.) with a regex-based fallback for simpler pattern
checks. Extremely complex or non-standard SQL (CTEs with multiple levels of nesting,
database-specific syntax extensions) may not be fully parsed. If you suspect a
parsing issue, check the console report to see if the query's normalized form
looks correct.

---

## INSERT/UPDATE/DELETE Not Counted

**Symptom:** Query count only shows SELECTs, INSERT/UPDATE/DELETE are always 0.

**Cause:** You may be using an older version. DML capture was added in 0.2.0.

**Fix:** Update to the latest version of QueryAudit.

---

## @ExpectMaxQueryCount Fails Unexpectedly

**Symptom:** Test fails with "executed N queries, expected at most M" where N is higher
than expected.

**Cause:** `@ExpectMaxQueryCount` counts **all** query types, including INSERTs
from `@BeforeEach` test data setup.

**Fix options:**

1. Increase the limit to account for setup queries
2. Move data setup to `@BeforeAll` (executed before capturing starts)
3. Use `@Sql` annotations for test data (executed before the extension lifecycle)

**Debugging tip:** Check the console report's query list to see exactly which queries
were counted. Look for unexpected queries from:

- `@BeforeEach` setup methods
- Hibernate schema validation queries
- Spring Security filter chain queries
- Connection pool validation queries

---

## Double Proxy with gavlyukovskiy

**Symptom:** You see duplicate query logs or performance degradation in tests.

**Cause:** Both QueryAudit and spring-boot-data-source-decorator are wrapping
the DataSource.

**Fix:** See [Spring Boot Integration - Using with existing datasource-proxy](../getting-started/spring-boot.md#using-with-an-existing-datasource-proxy-gavlyukovskiy).

---

## Index Metadata Not Collected

**Symptom:** Missing index detections don't fire even though indexes are missing.

**Causes:**

### No database module on classpath

Ensure the correct module is in your test dependencies:

=== "MySQL"

    ```groovy
    testImplementation 'io.github.haroya01:query-audit-mysql:${version}'
    ```

=== "PostgreSQL"

    ```groovy
    testImplementation 'io.github.haroya01:query-audit-postgresql:${version}'
    ```

### Using H2 or embedded database

QueryAudit's MySQL module uses `SHOW INDEX` and the PostgreSQL module uses
`pg_catalog` system tables. H2 and other embedded databases are not supported.
To get full index-based detection, use Testcontainers with a real database in
your test environment.

!!! tip "Migrating from H2 to Testcontainers"
    If you currently use H2 for tests, consider switching to Testcontainers
    for more realistic testing. This enables QueryAudit's full detection
    capabilities and catches issues that H2's compatibility mode may hide.

    ```groovy
    testImplementation 'org.testcontainers:mysql:1.20.4'
    // or
    testImplementation 'org.testcontainers:postgresql:1.20.4'
    ```

### Tables created after metadata collection

If tables are created after QueryAudit collects metadata, the indexes won't be
visible. This usually works fine because QueryAudit collects metadata in
`@BeforeAll`, after Spring context initialization.

**Fix:** Ensure your schema is created before tests start. If using `ddl-auto=create-drop`,
this is handled automatically by Spring.

### JDBC connection permissions

The database user must have read access to the system catalogs:

- **MySQL:** Access to `INFORMATION_SCHEMA` and ability to run `SHOW INDEX`
- **PostgreSQL:** Access to `pg_class`, `pg_index`, `pg_attribute`, `pg_stats`

---

## Common JPA/Hibernate Issues

### N+1 Not Detected on Lazy Collections

**Symptom:** Lazy-loaded collections cause N+1 queries, but QueryAudit does not flag them.

**Possible causes:**

1. **Collection accessed outside test method:** If the collection is loaded in a
   `@Transactional` service method that completes before the query capture window,
   the queries may not be captured.

2. **Threshold too high:** The default N+1 threshold is 3. If fewer than 3 entities
   are loaded, the repeated query count stays below the threshold.

    ```java
    // Only 2 orders -> only 2 lazy loads -> below threshold of 3
    @QueryAudit(nPlusOneThreshold = 2)  // Lower the threshold
    ```

3. **Batch fetching enabled:** If Hibernate batch fetching is configured
   (`@BatchSize` or `hibernate.default_batch_fetch_size`), the query pattern
   changes and may not trigger N+1 detection.

4. **Using `@EntityGraph` or `JOIN FETCH`:** If the relationship is already
   eagerly fetched in the query, there is no N+1 -- QueryAudit is correctly
   not flagging it.

### FetchType.EAGER Causes Extra Queries

**Symptom:** Queries you did not write appear in the report.

**Cause:** `FetchType.EAGER` on `@ManyToOne` or `@OneToOne` triggers additional
SELECT queries automatically.

**Fix:** Change to `FetchType.LAZY` and use `JOIN FETCH` or `@EntityGraph` where needed.

```java
// BEFORE: Eager fetching causes extra queries
@ManyToOne(fetch = FetchType.EAGER)  // Default for @ManyToOne
private User user;

// AFTER: Lazy fetching, load only when needed
@ManyToOne(fetch = FetchType.LAZY)
private User user;
```

### Hibernate Envers / Audit Queries

**Symptom:** Extra INSERT queries appear for audit tables.

**Fix:** Suppress the audit table queries:

```yaml
query-audit:
  suppress-queries:
    - "_aud"              # Suppress Envers audit table queries
    - "revinfo"           # Suppress revision info queries
```

### Hibernate Second-Level Cache

**Symptom:** Query counts vary between test runs.

**Cause:** Hibernate's second-level cache may serve some queries from cache,
changing the SQL query count between runs.

**Fix:** Either disable the second-level cache in tests or use a higher tolerance
in `@ExpectMaxQueryCount`.

```yaml
# application-test.yml
spring:
  jpa:
    properties:
      hibernate:
        cache:
          use_second_level_cache: false
```

### Spring Data JPA Derived Queries

**Symptom:** QueryAudit flags issues on queries you did not write explicitly.

**Cause:** Spring Data JPA generates SQL from method names (e.g.,
`findByStatusAndCreatedAtAfter`). The generated SQL may trigger detections
like `missing-where-index`.

**Fix:** This is working as intended. The generated SQL runs in production and
should be optimized. Add the missing index or use `@Query` with optimized SQL.

---

## Tests Fail in CI but Pass Locally

**Symptom:** QueryAudit detects issues in CI that don't appear locally.

**Possible causes:**

1. **Different database:** CI uses a real MySQL/PostgreSQL instance while local
   uses H2. QueryAudit detects more issues with real databases because index
   metadata is available.

2. **Test ordering:** Tests run in a different order in CI, causing different
   query patterns.

3. **Baseline drift:** The `.query-audit-counts` baseline file is out of date.
   Update it: `./gradlew test -DqueryGuard.updateBaseline=true`

4. **Schema differences:** The CI database may have different indexes or table
   definitions than your local environment.

5. **Different Spring profiles:** CI may activate a different Spring profile
   with different QueryAudit settings.

---

## Report Not Printing

**Symptom:** No QueryAudit report appears in test output.

**Check:**

1. Ensure the annotation is present (`@QueryAudit` or `@EnableQueryInspector`)
2. Check that `query-audit.enabled` is not set to `false` in your test config
3. Look for `[QueryAudit]` error messages in stderr
4. Verify that no test framework filter is suppressing stdout output
5. If using Gradle, ensure test output is not being suppressed:

    ```groovy
    test {
        testLogging {
            showStandardStreams = true
        }
    }
    ```

---

## HTML Report Not Generated

**Symptom:** No `build/reports/query-audit/index.html` file after tests.

**Cause:** HTML reports are generated in `@AfterAll`. If tests fail before
that point (e.g., Spring context fails to start), no report is written.

**Fix:** Check your test logs for startup failures. The HTML report is only
generated when at least one test method completes.

---

## OutOfMemoryError During Tests

**Symptom:** Tests crash with `java.lang.OutOfMemoryError` when QueryAudit is enabled.

**Cause:** QueryAudit records every SQL statement for analysis. Tests that generate
a very large number of queries (e.g., batch processing tests) can exhaust heap memory.

**Fix options:**

1. Lower the max queries per test:
    ```yaml
    query-audit:
      max-queries: 5000
    ```

2. Increase JVM heap:
    ```groovy
    test {
        jvmArgs '-Xmx1g'
    }
    ```

3. Suppress high-volume queries:
    ```yaml
    query-audit:
      suppress-queries:
        - "INSERT INTO batch_table"
    ```

4. Use `@EnableQueryInspector` only on specific test classes rather than globally.

---

## Performance Impact of QueryAudit

**Symptom:** Tests run noticeably slower with QueryAudit enabled.

**Expected impact:** QueryAudit adds a small overhead per query (microseconds) for
interception and recording. The analysis phase runs after each test method and is
proportional to the number of unique query patterns.

**If impact is significant:**

1. Reduce `max-queries` to limit recording overhead
2. Disable rules you don't need with `disabled-rules`
3. Use `@EnableQueryInspector` selectively rather than on every test class
4. Check if the slow-down is from QueryAudit or from running against a real
   database (vs H2). Testcontainers startup adds time to the first test.

---

## Diagnostic Checklist

When reporting an issue or debugging unexpected behavior, check these items:

- [ ] QueryAudit annotation is present on the test class or method
- [ ] Correct database module is on the test classpath (`query-audit-mysql` or `query-audit-postgresql`)
- [ ] `query-audit.enabled` is not set to `false`
- [ ] No overly broad `suppress-patterns` or `suppress-queries`
- [ ] No rules are disabled via `disabled-rules` that you expect to run
- [ ] `report.show-info` is `true` if checking for INFO-level issues
- [ ] The test database has the expected schema and indexes
- [ ] The database user has permissions to read system catalogs
- [ ] JVM heap is sufficient for the test suite size

---

## See Also

- [Configuration Reference](configuration.md) -- All configuration options and defaults
- [Annotations Guide](annotations.md) -- Correct annotation usage
- [CI/CD Integration](ci-cd.md) -- CI-specific configuration
- [Architecture Overview](../architecture/overview.md) -- Understanding the analysis pipeline
