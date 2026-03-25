# Reports

After each test method, QueryAudit generates a report summarizing all detected issues.
The report helps you quickly identify which queries need attention and why.

QueryAudit includes three reporter implementations, all fully implemented:

| Reporter | Class | Output | Use Case |
|---|---|---|---|
| **Console** | `ConsoleReporter` | ANSI-colored stdout | Development and local testing |
| **JSON** | `JsonReporter` | Structured JSON file | CI artifacts, dashboards, trend tracking |
| **HTML** | `HtmlReporter` / `HtmlReportAggregator` | Self-contained HTML file | Build artifacts, PR reviews, team sharing |

All three reporters implement the `Reporter` interface from `query-audit-core`.

---

## Console Report

The default reporter prints a human-readable, ANSI-colored report to standard output
after each test method.

### Example Output

```
------------------------------------------------------------------------
  QUERY AUDIT REPORT
  Test: findRecentOrders_shouldUseIndex
------------------------------------------------------------------------

--- CONFIRMED (100% reliable) ---

  [ERROR] N+1 Query detected
    Query:  SELECT * FROM order_items WHERE order_id = ?
    Target: order_items
    Detail: Query repeated 12 times (threshold: 3)
    Fix:    Use JOIN FETCH, @EntityGraph, or batch loading (IN clause)

  [ERROR] Missing index on WHERE column
    Query:  SELECT * FROM orders WHERE user_id = ? ORDER BY created_at DESC
    Target: orders.user_id
    Detail: Column 'user_id' is used in WHERE clause but has no index
    Fix:    CREATE INDEX idx_orders_user_id ON orders (user_id);

  [WARNING] SELECT * usage
    Query:  SELECT * FROM orders WHERE user_id = ? ORDER BY created_at DESC
    Target: orders
    Detail: SELECT * returns all columns; consider selecting only needed columns

--- INFO (may vary with data volume) ---

  [INFO] Full table scan detected
    Query:  SELECT * FROM config WHERE key = 'app.version'
    Target: config
    Detail: Full table scan on 'config' (small table, may be acceptable)

[OK] 5 queries passed

------------------------------------------------------------------------
  4 unique patterns | 18 total queries | 342 ms total
  2 errors | 1 warning | 1 info | 5 passed
------------------------------------------------------------------------
```

### Configuration

The console reporter is enabled by default. No explicit configuration is needed.

```yaml
query-audit:
  report:
    format: console       # Default value
    show-info: true       # Show or hide INFO-level findings
```

### ANSI Color Coding

The console reporter uses ANSI escape codes to improve readability:

| Element | Color | ANSI Code |
|---|---|---|
| ERROR severity tag and text | Red | `\033[31m` |
| WARNING severity tag and text | Yellow | `\033[33m` |
| INFO severity tag and text | Cyan | `\033[36m` |
| OK / passed count | Green | `\033[32m` |
| Header dividers, section titles | Bold | `\033[1m` |
| SQL text, dim labels | Dim | `\033[2m` |

Colors are automatically disabled when:

- The `NO_COLOR` environment variable is set (following the [no-color.org](https://no-color.org) convention).
- There is no interactive console attached (e.g., when running in a CI pipeline that
  captures output).

!!! note "Forcing colors in CI"
    Some CI systems (GitHub Actions, GitLab CI) do support ANSI colors. If colors
    are disabled but you want them, ensure `System.console()` returns non-null or
    construct `ConsoleReporter` manually with `colorsEnabled = true`.

---

## JSON Report

The JSON reporter produces a structured, machine-readable report suitable for CI artifact
storage, dashboards, and downstream processing. It uses no external JSON library --
output is built with `StringBuilder` for zero extra dependencies.

### Enabling JSON Reports

```yaml
query-audit:
  report:
    format: json
    output-dir: build/reports/query-audit
```

### Example Output

```json
{
  "testClass": "com.example.OrderServiceTest",
  "testName": "findRecentOrders_shouldUseIndex",
  "summary": {
    "confirmedIssues": 2,
    "infoIssues": 1,
    "acknowledgedIssues": 0,
    "uniquePatterns": 4,
    "totalQueries": 18,
    "executionTimeMs": 342
  },
  "confirmedIssues": [
    {
      "type": "n-plus-one",
      "severity": "ERROR",
      "query": "select * from order_items where order_id = ?",
      "table": "order_items",
      "column": null,
      "detail": "Query repeated 12 times (threshold: 3)",
      "suggestion": "Use JOIN FETCH, @EntityGraph, or batch loading (IN clause)"
    },
    {
      "type": "missing-where-index",
      "severity": "ERROR",
      "query": "select * from orders where user_id = ? order by created_at desc",
      "table": "orders",
      "column": "user_id",
      "detail": "Column 'user_id' is used in WHERE clause but has no index",
      "suggestion": "CREATE INDEX idx_orders_user_id ON orders (user_id);"
    }
  ],
  "infoIssues": [],
  "acknowledgedIssues": [],
  "queries": [
    {
      "sql": "SELECT * FROM orders WHERE user_id = 42 ORDER BY created_at DESC",
      "normalizedSql": "select * from orders where user_id = ? order by created_at desc",
      "executionTimeNanos": 15234000,
      "stackTrace": "com.example.OrderService.findOrders:42"
    }
  ]
}
```

!!! tip "CI artifact storage"
    Store JSON reports as CI artifacts for trend tracking across builds. Parse them
    with `jq` or feed them into monitoring dashboards.

    ```bash
    # Extract issue count from JSON report
    jq '.summary.confirmedIssues' build/reports/query-audit/report.json

    # List all detected issue types
    jq '[.confirmedIssues[].type] | unique' build/reports/query-audit/report.json

    # Find tests with N+1 issues
    jq 'select(.confirmedIssues[] | .type == "n-plus-one") | .testName' \
        build/reports/query-audit/*.json
    ```

---

## HTML Report

The HTML report aggregator accumulates results across all test classes and produces a
self-contained HTML file at `build/reports/query-audit/index.html` after all tests complete.
The report includes expandable sections, syntax-highlighted SQL, and a visual summary.

### Features

- **Test-level drill-down** -- Expand each test to see its detected issues and queries
- **Filtering** -- Filter by severity, issue type, or test class
- **Search** -- Full-text search across SQL queries and issue descriptions
- **Summary dashboard** -- Overall counts of errors, warnings, and info findings
- **Self-contained** -- Single HTML file with embedded CSS and JavaScript, no external dependencies

### Configuration

```yaml
query-audit:
  report:
    output-dir: build/reports/query-audit    # Where to write index.html
  auto-open-report: true                     # Open in browser after tests
```

Or via annotation:

```java
@QueryAudit(autoOpenReport = true)
```

Or via system property:

```bash
./gradlew test -Dqueryaudit.autoOpenReport=true
```

### Example HTML Report Structure

The generated HTML report contains these sections:

```
+------------------------------------------------------------+
|  QueryAudit Report                                        |
|  Generated: 2026-03-25 14:30:00                            |
+------------------------------------------------------------+
|  Summary: 12 tests | 5 errors | 3 warnings | 2 info       |
+------------------------------------------------------------+
|                                                            |
|  [v] OrderServiceTest                                      |
|      [v] findRecentOrders (2 errors, 1 warning)            |
|          [ERROR] N+1 Query detected                        |
|            Query: SELECT * FROM order_items WHERE ...       |
|          [ERROR] Missing index on WHERE column              |
|            Query: SELECT * FROM orders WHERE user_id = ...  |
|          [WARNING] SELECT * usage                          |
|      [ ] createOrder (0 issues)                            |
|                                                            |
|  [v] UserServiceTest                                       |
|      [v] findActiveUsers (1 warning)                       |
|          [WARNING] Unbounded result set                    |
+------------------------------------------------------------+
```

!!! warning "HTML report timing"
    The HTML report is generated in `@AfterAll`. If tests fail before that point
    (e.g., Spring context fails to start), no report is written. Check your test
    logs for startup failures.

---

## Report Sections Explained

### Header

```
------------------------------------------------------------------------
  QUERY AUDIT REPORT
  Test: findRecentOrders_shouldUseIndex
------------------------------------------------------------------------
```

Shows the name of the test method that was analyzed.

### CONFIRMED (100% reliable)

```
--- CONFIRMED (100% reliable) ---
```

Issues in this section are determined purely from SQL parsing and index metadata --
they do not depend on data volume or query planner behavior. These are the issues
that cause the test to fail when `failOnDetection` is `true`.

Confirmed issues have either **ERROR** or **WARNING** severity:

- **ERROR** -- high-confidence performance problems (N+1, missing WHERE/JOIN index,
  function on indexed column)
- **WARNING** -- likely problems that may be intentional in some cases (SELECT *,
  excessive OR clauses, large OFFSET pagination, missing ORDER BY/GROUP BY index)

### INFO (may vary with data volume)

```
--- INFO (may vary with data volume) ---
```

INFO-level issues come from EXPLAIN analysis and depend on the query planner's
decisions, which can vary with data volume. These are shown for awareness but
never cause a test failure. Examples include full table scans, filesort, and
temporary table usage.

!!! tip
    Set `report.show-info: false` in `application.yml` to hide this section if
    your tests use small datasets where these findings are not actionable.

### OK

```
[OK] 5 queries passed
```

Shows how many queries had no detected issues.

### Summary

```
------------------------------------------------------------------------
  4 unique patterns | 18 total queries | 342 ms total
  2 errors | 1 warning | 1 info | 5 passed
------------------------------------------------------------------------
```

The summary footer provides:

- **Unique patterns** -- number of structurally distinct SQL statements (after
  parameter normalization).
- **Total queries** -- total number of SQL statements executed during the test
  (before deduplication).
- **Total time** -- cumulative execution time of all intercepted queries.
- **Breakdown** -- counts by severity plus passed queries.

---

## How to Read the Report Effectively

1. **Start with the summary line.** If it says `0 errors | 0 warnings`, your queries
   are clean.

2. **Focus on CONFIRMED errors first.** These are definite problems -- an N+1 that
   fires 12 times, a WHERE column with no index, etc.

3. **Review warnings.** These may be intentional (e.g., `SELECT *` in a test helper
   that actually needs all columns). If intentional, suppress them with
   `@QueryAudit(suppress = {"select-all"})`.

4. **Glance at INFO.** INFO issues flag things like full table scans that are normal
   on small test datasets. If your test uses realistic data volumes, these may be
   worth investigating.

5. **Look at the Fix suggestion.** QueryAudit provides actionable suggestions like
   `CREATE INDEX` DDL or recommendations to use JOIN FETCH.

---

## See Also

- [Configuration Reference](configuration.md) -- Configure report format and output directory
- [CI/CD Integration](ci-cd.md) -- Upload reports as CI artifacts
- [Suppressing Issues](suppressing.md) -- Suppress intentional findings from reports
