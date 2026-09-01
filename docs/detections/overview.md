# Detection Rules Overview

QueryAudit ships with **active detection rules** that catch SQL performance issues, logic
bugs, and anti-patterns during your test runs. The rules are organized by severity and
confidence model to help you prioritize fixes.

!!! info "IssueType enum"
    The `IssueType` enum currently contains **69** entries. **67** are actively emitted by
    detection rules (one rule can emit multiple issue types — `MissingIndexDetector` alone
    emits 4). The remaining 2 are [disabled or reserved](#disabled-reserved-rules).
    The full canonical list is in
    [`IssueType.java`](https://github.com/haroya01/query-audit/blob/main/query-audit-core/src/main/java/io/queryaudit/core/model/IssueType.java).

---

## Confidence Model

### Confirmed (Structural / Pattern-based)

These rules analyze **SQL structure and database schema** -- things that do not change with data
volume. The detection logic examines the SQL text, repetition patterns, or cross-references the
actual index metadata via `SHOW INDEX` / `pg_catalog`. Heuristics that depend on data distribution
live in the INFO tier instead.

!!! success "High-signal tier"
    Findings here are not gated by row counts or data distribution. False positives are tracked
    as bugs and fixed -- the design intent is that a confirmed flag is a real problem worth
    investigating. The most recent false-positive fixes are listed in
    [CHANGELOG](https://github.com/haroya01/query-audit/blob/main/CHANGELOG.md); please report
    any new one you hit.

### Info (Data-Dependent / Heuristic)

These rules rely on `EXPLAIN` output or heuristic analysis. Because MySQL's query optimizer makes
cost-based decisions that shift with table statistics, results obtained from a small test dataset
may differ from production.

!!! warning "Info = EXPLAIN-based or heuristic, may vary with data size"
    A full table scan reported in a test with 5 rows might disappear once an index becomes
    cost-effective at 100,000 rows -- or vice versa.

### Why the Distinction Matters

```
Test data: 5 rows   --> MySQL: "Full scan is faster" --> Table scan (false positive possible)
Production: 1M rows --> MySQL: "Use index"           --> Index scan

 .: Structure/pattern based = 100% reliable regardless of data volume
 .: EXPLAIN based           = may vary with test data size
```

---

## Quick Reference Table

The complete searchable reference of issue types emitted by the active detection rules.
(The full canonical list, including the newest additions, is the `IssueType` enum.)

| # | Rule Name | Code | Severity | Category | Description |
|---|-----------|------|----------|----------|-------------|
| 1 | N+1 Query | `n-plus-one` | ERROR | Query Patterns | N+1 Query detected |
| 2 | WHERE Function | `where-function` | ERROR | SQL Anti-Patterns | Function in WHERE disables index |
| 3 | Missing WHERE Index | `missing-where-index` | ERROR | Index Issues | Missing index on WHERE column |
| 4 | Missing JOIN Index | `missing-join-index` | ERROR | Index Issues | Missing index on JOIN column |
| 5 | Cartesian JOIN | `cartesian-join` | ERROR | JOIN Issues | Cartesian JOIN (missing ON condition) |
| 6 | Non-Sargable Expression | `non-sargable` | ERROR | SQL Anti-Patterns | Arithmetic on column prevents index usage |
| 7 | NULL Comparison | `null-comparison` | ERROR | SQL Anti-Patterns | Comparison with NULL using = or != (always UNKNOWN) |
| 8 | FOR UPDATE No Index | `for-update-no-index` | ERROR | Locking Risks | FOR UPDATE without index may lock entire table |
| 9 | UPDATE/DELETE No WHERE | `update-without-where` | ERROR | DML Safety | UPDATE/DELETE without WHERE affects all rows |
| 10 | ORDER BY RAND() | `order-by-rand` | ERROR | SQL Anti-Patterns | ORDER BY RAND() causes full table scan and sort |
| 11 | NOT IN Subquery | `not-in-subquery` | ERROR | SQL Anti-Patterns | NOT IN (subquery) returns empty when subquery contains NULL |
| 12 | Missing ORDER BY Index | `missing-order-by-index` | WARNING | Index Issues | Missing index on ORDER BY column |
| 13 | Missing GROUP BY Index | `missing-group-by-index` | WARNING | Index Issues | Missing index on GROUP BY column |
| 14 | Composite Index Leading | `composite-index-leading` | WARNING | Index Issues | Composite index leading column not used |
| 15 | Redundant Index | `redundant-index` | WARNING | Index Issues | Redundant index (prefix of another index) |
| 16 | Write Amplification | `write-amplification` | WARNING | Index Issues | Too many indexes cause write amplification |
| 17 | Implicit Type Conversion | `implicit-type-conversion` | WARNING | SQL Anti-Patterns | Implicit type conversion disables index |
| 18 | ORDER BY+LIMIT No Index | `order-by-limit-no-index` | WARNING | Index Issues | ORDER BY with LIMIT without index causes full filesort |
| 19 | OR Abuse | `or-abuse` | WARNING | SQL Anti-Patterns | Excessive OR conditions in WHERE clause |
| 20 | OFFSET Pagination | `offset-pagination` | WARNING | SQL Anti-Patterns | Large OFFSET pagination |
| 21 | LIKE Leading Wildcard | `like-leading-wildcard` | WARNING | SQL Anti-Patterns | Leading wildcard in LIKE disables index |
| 22 | Large IN List | `large-in-list` | WARNING | SQL Anti-Patterns | IN clause with too many values |
| 23 | DISTINCT Misuse | `distinct-misuse` | WARNING | SQL Anti-Patterns | Potentially unnecessary DISTINCT usage |
| 24 | HAVING Misuse | `having-misuse` | WARNING | SQL Anti-Patterns | HAVING on non-aggregate column should be WHERE |
| 25 | Slow Query | `slow-query` | WARNING | Query Patterns | Slow query detected |
| 26 | Unbounded Result Set | `unbounded-result-set` | WARNING | SQL Anti-Patterns | SELECT without LIMIT could return unbounded rows |
| 27 | Repeated Single INSERT | `repeated-single-insert` | WARNING | DML Safety | Repeated single-row INSERT should use batch |
| 28 | Repeated Single UPDATE | `repeated-single-update` | WARNING | DML Safety | Repeated UPDATEs scoped by a unique key should use a set-based statement or batch |
| 29 | Query Count Regression | `query-count-regression` | WARNING | Query Patterns | Query count regression detected |
| 30 | DML Without Index | `dml-without-index` | WARNING | DML Safety | UPDATE/DELETE WHERE column has no index |
| 31 | INSERT SELECT * | `insert-select-all` | WARNING | DML Safety | INSERT with SELECT * is fragile |
| 32 | INSERT ON DUPLICATE KEY | `insert-on-duplicate-key` | WARNING | DML Safety | INSERT ON DUPLICATE KEY UPDATE may cause deadlocks |
| 33 | Subquery in DML | `subquery-in-dml` | WARNING | DML Safety | Subquery in UPDATE/DELETE cannot use semijoin |
| 34 | Implicit Columns INSERT | `implicit-columns-insert` | WARNING | DML Safety | INSERT without explicit column list is fragile |
| 35 | Correlated Subquery | `correlated-subquery` | WARNING | JOIN Issues | Correlated subquery in SELECT clause |
| 36 | Too Many JOINs | `too-many-joins` | WARNING | JOIN Issues | Query has too many JOINs |
| 37 | Implicit JOIN | `implicit-join` | WARNING | JOIN Issues | Implicit comma-separated join syntax |
| 38 | Unused JOIN | `unused-join` | WARNING | JOIN Issues | LEFT JOIN table is never referenced |
| 39 | FOR UPDATE Non-Unique | `for-update-non-unique` | WARNING | Locking Risks | FOR UPDATE on non-unique index causes gap locks |
| 40 | Range Lock Risk | `range-lock-risk` | WARNING | Locking Risks | Range + FOR UPDATE on unindexed column may gap lock |
| 41 | String Concat in WHERE | `string-concat-where` | WARNING | MySQL-Specific | String concatenation in WHERE prevents index |
| 42 | GROUP BY Function | `group-by-function` | WARNING | MySQL-Specific | Function in GROUP BY prevents index usage |
| 43 | REGEXP Usage | `regexp-usage` | WARNING | MySQL-Specific | REGEXP/RLIKE prevents index usage |
| 44 | FIND_IN_SET | `find-in-set` | WARNING | MySQL-Specific | FIND_IN_SET indicates comma-separated values violating 1NF |
| 45 | Collection Delete+Reinsert | `collection-delete-reinsert` | WARNING | Hibernate/ORM | DELETE-all + re-INSERT pattern |
| 46 | Derived Delete Loads Entities | `derived-delete-loads-entities` | WARNING | Hibernate/ORM | Derived delete loads entities before deletes |
| 47 | LIMIT Without ORDER BY | `limit-without-order-by` | WARNING | Query Structure | LIMIT without ORDER BY returns non-deterministic rows |
| 48 | Window No PARTITION | `window-no-partition` | WARNING | Query Structure | Window function without PARTITION BY |
| 49 | FOR UPDATE No Timeout | `for-update-no-timeout` | WARNING | Locking Risks | FOR UPDATE without NOWAIT/SKIP LOCKED |
| 50 | CASE in WHERE | `case-in-where` | WARNING | SQL Anti-Patterns | CASE expression in WHERE prevents index usage |
| 51 | SELECT * | `select-all` | INFO | SQL Anti-Patterns | SELECT * usage |
| 52 | Redundant Filter | `redundant-filter` | INFO | SQL Anti-Patterns | Redundant duplicate WHERE condition |
| 53 | COUNT vs EXISTS | `count-instead-of-exists` | INFO | SQL Anti-Patterns | COUNT used where EXISTS would be more efficient |
| 54 | UNION Without ALL | `union-without-all` | INFO | SQL Anti-Patterns | UNION without ALL forces deduplication sort |
| 55 | Covering Index Opportunity | `covering-index-opportunity` | INFO | Index Issues | Query could benefit from a covering index |
| 56 | COUNT(*) No WHERE | `count-star-no-where` | INFO | SQL Anti-Patterns | COUNT(*) without WHERE scans entire table |
| 57 | INSERT SELECT Locks Source | `insert-select-locks-source` | INFO | DML Safety | INSERT...SELECT locks source table rows |
| 58 | Excessive Column Fetch | `excessive-column-fetch` | INFO | SQL Anti-Patterns | Too many columns fetched, consider DTO projection |
| 59 | Mergeable Queries | `mergeable-queries` | INFO | Query Patterns | Multiple queries to same table could be merged |
| 60 | Non-Deterministic Pagination | `non-deterministic-pagination` | INFO | SQL Anti-Patterns | ORDER BY+LIMIT on non-unique column |
| 61 | Force Index Hint | `force-index-hint` | INFO | SQL Anti-Patterns | FORCE/USE/IGNORE INDEX hint overrides optimizer |
| 62 | N+1 Suspect | `n-plus-one-suspect` | INFO | Query Patterns | N+1 Query suspected (SQL-level heuristic) |
| 63 | findById for Association | `find-by-id-for-association` | INFO | Hibernate/ORM | findById() used only for FK association; consider getReferenceById() |
| 64 | Filesort | `filesort` | INFO | EXPLAIN-Based | Filesort detected in EXPLAIN output |
| 65 | Temporary Table | `temporary-table` | INFO | EXPLAIN-Based | Temporary table usage in EXPLAIN output |
| 66 | Read-Modify-Write | `read-modify-write` | INFO | Locking Risks | SELECT without a lock followed by INSERT/UPDATE on the same table, with no unique-constraint backing, upsert, atomic SET, or version column |
| 67 | Connection Held Idle | `connection-held-idle` | INFO | Connection Lifecycle | Connection held while non-database work runs -- the pool-exhaustion shape |

!!! note "Rule numbering"
    Rules 51-67 are INFO severity. The table numbers are for reference only and do not correspond
    to priority. Rules 1-11 are ERROR severity and should always be addressed. Rules 12-50 are
    WARNING severity and should be reviewed.

---

## Rules by Severity

### ERROR Severity (11 issue types)

Critical issues -- logic bugs, full table locks, or guaranteed performance degradation.
**These should always be fixed.**

| Code | Description | Category | Detection Method |
|------|-------------|----------|-----------------|
| `n-plus-one` | N+1 Query detected | Query Patterns | Normalize SQL, group by pattern, check count >= threshold |
| `where-function` | Function in WHERE disables index | SQL Anti-Patterns | Parse WHERE clause, detect function-wrapped columns |
| `missing-where-index` | Missing index on WHERE column | Index Issues | Extract WHERE columns + `SHOW INDEX` verification |
| `missing-join-index` | Missing index on JOIN column | Index Issues | Extract JOIN columns + `SHOW INDEX` verification |
| `cartesian-join` | Cartesian JOIN (missing ON condition) | JOIN Issues | Parse JOINs for missing ON/USING clause |
| `non-sargable` | Arithmetic on column prevents index usage | SQL Anti-Patterns | Detect arithmetic expressions wrapping indexed columns |
| `null-comparison` | Comparison with NULL using = or != | SQL Anti-Patterns | Detect `= NULL` or `!= NULL` instead of `IS [NOT] NULL` |
| `for-update-no-index` | FOR UPDATE without index may lock entire table | Locking Risks | Cross-check FOR UPDATE query against index metadata |
| `update-without-where` | UPDATE/DELETE without WHERE affects all rows | DML Safety | Parse SQL for WHERE clause presence |
| `order-by-rand` | ORDER BY RAND() causes full table scan | SQL Anti-Patterns | Detect RAND() in ORDER BY clause |
| `not-in-subquery` | NOT IN (subquery) returns empty when NULL | SQL Anti-Patterns | Detect NOT IN with subquery pattern |

!!! tip "MissingIndexDetector"
    `missing-where-index` and `missing-join-index` are both emitted by the single
    `MissingIndexDetector` rule. See [Missing Index Detection](missing-index.md) for details.

---

### WARNING Severity (39 issue types)

Important issues that should be reviewed and typically fixed.

#### Index Issues (6 issue types)

| Code | Description | Detection Method |
|------|-------------|-----------------|
| `missing-order-by-index` | Missing index on ORDER BY column | Extract ORDER BY columns + `SHOW INDEX` verification |
| `missing-group-by-index` | Missing index on GROUP BY column | Extract GROUP BY columns + `SHOW INDEX` verification |
| `composite-index-leading` | Composite index leading column not used | Parse WHERE columns + composite index column order check |
| `redundant-index` | Redundant index (prefix of another index) | Compare index definitions for prefix overlap |
| `write-amplification` | Too many indexes cause write amplification | Count indexes per table, flag when excessive |
| `order-by-limit-no-index` | ORDER BY+LIMIT without index causes filesort | Cross-check ORDER BY + LIMIT against index metadata |

!!! note "MissingIndexDetector WARNING issue types"
    `missing-order-by-index` and `missing-group-by-index` are also emitted by `MissingIndexDetector`.
    Combined with the 2 ERROR-level issue types above, this single detector emits 4 issue types total.

#### SQL Anti-Patterns (9 issue types)

| Code | Description | Detection Method |
|------|-------------|-----------------|
| `implicit-type-conversion` | Implicit type conversion disables index | Detect column-type vs literal-type mismatch |
| `or-abuse` | Excessive OR conditions in WHERE clause | Count OR conditions, compare to threshold |
| `offset-pagination` | Large OFFSET pagination | Parse OFFSET value, compare to threshold |
| `like-leading-wildcard` | Leading wildcard in LIKE disables index | Detect `LIKE '%...'` pattern |
| `large-in-list` | IN clause with too many values | Count values in IN clause, compare to threshold |
| `distinct-misuse` | Potentially unnecessary DISTINCT | Detect DISTINCT when context suggests uniqueness |
| `having-misuse` | HAVING on non-aggregate column should be WHERE | Detect non-aggregate expressions in HAVING |
| `unbounded-result-set` | SELECT without LIMIT could return unbounded rows | Detect SELECT without LIMIT clause |
| `case-in-where` | CASE expression in WHERE prevents index | Detect CASE expressions within WHERE predicates |

#### DML Safety (5 issue types)

| Code | Description | Detection Method |
|------|-------------|-----------------|
| `dml-without-index` | UPDATE/DELETE WHERE column has no index | Extract WHERE columns from DML + `SHOW INDEX` |
| `insert-select-all` | INSERT with SELECT * is fragile | Regex match for `INSERT ... SELECT *` |
| `insert-on-duplicate-key` | INSERT ON DUPLICATE KEY may deadlock | Detect INSERT ... ON DUPLICATE KEY UPDATE pattern |
| `subquery-in-dml` | Subquery in UPDATE/DELETE can't use semijoin | Detect subqueries in UPDATE/DELETE statements |
| `implicit-columns-insert` | INSERT without column list is fragile | Detect INSERT without column specification |

#### Query Patterns (4 issue types)

| Code | Description | Detection Method |
|------|-------------|-----------------|
| `slow-query` | Slow query detected | Compare execution time to configured threshold |
| `repeated-single-insert` | Repeated single-row INSERT should batch | Normalize INSERT, group by pattern, check count |
| `repeated-single-update` | Repeated UPDATEs scoped by a unique key should use a set-based statement or batch | Normalize UPDATE, require equality predicates that cover a unique index, group by pattern |
| `query-count-regression` | Query count regression detected | Compare query count against baseline |

#### JOIN Issues (4 issue types)

| Code | Description | Detection Method |
|------|-------------|-----------------|
| `correlated-subquery` | Correlated subquery in SELECT clause | Detect correlated subqueries referencing outer tables |
| `too-many-joins` | Query has too many JOINs | Count JOIN clauses, compare to threshold |
| `implicit-join` | Implicit comma-separated join syntax | Detect comma-separated tables in FROM clause |
| `unused-join` | LEFT JOIN table is never referenced | Detect LEFT JOIN tables unused in SELECT/WHERE |

#### Locking Risks (3 issue types)

| Code | Description | Detection Method |
|------|-------------|-----------------|
| `for-update-non-unique` | FOR UPDATE on non-unique index causes gap locks | Cross-check FOR UPDATE against unique index metadata |
| `range-lock-risk` | Range + FOR UPDATE on unindexed column | Detect range predicates with FOR UPDATE |
| `for-update-no-timeout` | FOR UPDATE without NOWAIT/SKIP LOCKED | Detect FOR UPDATE without timeout modifier |

#### MySQL-Specific (4 issue types)

| Code | Description | Detection Method |
|------|-------------|-----------------|
| `string-concat-where` | String concatenation in WHERE prevents index | Detect CONCAT() or \|\| in WHERE clause |
| `group-by-function` | Function in GROUP BY prevents index usage | Detect function calls in GROUP BY clause |
| `regexp-usage` | REGEXP/RLIKE prevents index usage | Detect REGEXP or RLIKE in query |
| `find-in-set` | FIND_IN_SET indicates comma-separated values | Detect FIND_IN_SET function usage |

#### Hibernate / ORM Patterns (2 issue types)

| Code | Description | Detection Method |
|------|-------------|-----------------|
| `collection-delete-reinsert` | DELETE-all + re-INSERT pattern | Detect DELETE + re-INSERT sequence on same table |
| `derived-delete-loads-entities` | Derived delete loads entities before deletes | Detect SELECT followed by individual DELETE pattern |

#### Query Structure (2 issue types)

| Code | Description | Detection Method |
|------|-------------|-----------------|
| `limit-without-order-by` | LIMIT without ORDER BY is non-deterministic | Detect LIMIT clause without corresponding ORDER BY |
| `window-no-partition` | Window function without PARTITION BY | Detect window functions missing PARTITION BY clause |

---

### INFO Severity (17 issue types)

Best-practice suggestions and heuristic checks. These won't fail your build by default
but are worth reviewing.

| Code | Description | Detection Method |
|------|-------------|-----------------|
| `select-all` | SELECT * usage | Regex match on parsed SQL |
| `redundant-filter` | Redundant duplicate WHERE condition | Detect duplicate predicates in WHERE clause |
| `count-instead-of-exists` | COUNT used where EXISTS is better | Detect `COUNT(*)` in conditional context. Off by default; enable via `query-audit.count-instead-of-exists.enabled: true`. |
| `union-without-all` | UNION without ALL forces dedup sort | Detect UNION without ALL keyword |
| `covering-index-opportunity` | Query could benefit from covering index | Analyze SELECT columns vs available indexes |
| `count-star-no-where` | COUNT(*) without WHERE scans full table | Detect `COUNT(*)` without WHERE clause |
| `insert-select-locks-source` | INSERT...SELECT locks source rows | Detect INSERT ... SELECT pattern |
| `excessive-column-fetch` | Too many columns, use DTO projection | Count selected columns, compare to threshold |
| `mergeable-queries` | Multiple queries could be merged | Detect multiple simple SELECTs to same table |
| `non-deterministic-pagination` | ORDER BY+LIMIT on non-unique column | Detect ORDER BY + LIMIT on non-unique columns |
| `force-index-hint` | FORCE/USE/IGNORE INDEX overrides optimizer | Detect index hint keywords in query |
| `find-by-id-for-association` | `findById()` used only for FK association — consider `getReferenceById()` to skip the SELECT | Spring Data return-type and call-site analysis |
| `read-modify-write` | Check-then-act race: unlocked SELECT then INSERT/UPDATE on the same table | Sequence analysis + unique-index cross-check; exempts FOR UPDATE, upserts, @Version columns, atomic `SET col = col - ?`, non-overlapping predicates |
| `connection-held-idle` | Connection held while non-database work runs | held − database-work time per connection checkout, from JDBC lifecycle events; threshold `connection-held-idle.threshold-ms` (200ms default) |
| `n-plus-one-suspect` | Same-structure query repeated at the SQL level — suspect only; the Hibernate-level tracker is authoritative | SQL pattern repetition heuristic |
| `filesort` | Filesort detected in the execution plan | MySQL/PostgreSQL EXPLAIN analyzers |
| `temporary-table` | Temporary table usage in the execution plan | MySQL/PostgreSQL EXPLAIN analyzers |

!!! info "Info rules are still useful"
    Even though they can produce false positives with small test data, they serve as early
    warning signals. When combined with Confirmed findings (e.g., a full scan **and** a missing
    index), the diagnosis becomes highly reliable.

---

## Disabled & Reserved Rules

The `IssueType` enum currently has **69 entries**. **67 are actively emitted** by detection
rules. The remaining 2 entries fall into two categories:

### Disabled Rules (1 entry)

| Code | Reason |
|------|--------|
| `duplicate-query` | **Disabled in code.** datasource-proxy provides SQL with `?` placeholders, making it impossible to distinguish "same query, same params" from "same query, different params." The N+1 detector already covers repeated patterns. Will be re-enabled when parameter tracking is added. |

!!! warning "DuplicateQueryDetector"
    The `DuplicateQueryDetector` class exists in the codebase but is intentionally omitted from
    `DetectionRuleRegistry.createBuiltInRules()`. The `DUPLICATE_QUERY` IssueType remains in the
    enum for forward compatibility.

### Reserved for Future EXPLAIN-based Detection (1 entry)

| Code | Description | Status |
|------|-------------|--------|
| `full-scan` | Full table scan detected | Reserved -- not yet emitted by the EXPLAIN analyzers |

This IssueType exists in the enum but is not emitted yet. It is a placeholder
for full-table-scan detection in the EXPLAIN analyzers.

### Accounting

| Category | Count |
|----------|-------|
| Active issue types emitted by detectors | **67** |
| Disabled (DuplicateQueryDetector) | 1 |
| Reserved (full-scan) | 1 |
| **Total IssueType enum entries** | **69** |

!!! note "Why fewer detector classes than active issue types?"
    A single detector can emit multiple issue types. The biggest example is
    `MissingIndexDetector`, which is registered as one detection rule but emits 4 different
    `IssueType`s (`missing-where-index`, `missing-join-index`, `missing-order-by-index`,
    `missing-group-by-index`) -- one per SQL clause it analyzes. On top of the core rules,
    the MySQL/PostgreSQL EXPLAIN analyzers emit `filesort` and `temporary-table`, the
    Hibernate-level trackers emit `find-by-id-for-association` and the authoritative N+1
    signal, and the connection lifecycle tracker emits `connection-held-idle` -- these run
    outside `DetectionRuleRegistry.createBuiltInRules()`.

---

## Rules by Category

### Query Patterns
- [`n-plus-one`](n-plus-one.md) -- N+1 Query detection (ERROR)
- `n-plus-one-suspect` -- SQL-level N+1 heuristic; Hibernate-level tracking is authoritative (INFO)
- `slow-query` -- Slow query detection (WARNING)
- `query-count-regression` -- Query count regression (WARNING)
- `mergeable-queries` -- Mergeable queries detection (INFO)

### Index Issues
- [`missing-where-index`](missing-index.md) -- Missing WHERE index (ERROR)
- [`missing-join-index`](missing-index.md) -- Missing JOIN index (ERROR)
- [`missing-order-by-index`](missing-index.md) -- Missing ORDER BY index (WARNING)
- [`missing-group-by-index`](missing-index.md) -- Missing GROUP BY index (WARNING)
- `composite-index-leading` -- Composite index leading column (WARNING)
- `redundant-index` -- Redundant index detection (WARNING)
- `write-amplification` -- Write amplification warning (WARNING)
- `order-by-limit-no-index` -- ORDER BY + LIMIT without index (WARNING)
- `covering-index-opportunity` -- Covering index opportunity (INFO)

### SQL Anti-Patterns
- [`where-function`](sql-anti-patterns.md) -- Function in WHERE (ERROR)
- [`non-sargable`](sql-anti-patterns.md) -- Non-sargable expressions (ERROR)
- [`null-comparison`](sql-anti-patterns.md) -- NULL comparison bugs (ERROR)
- [`order-by-rand`](sql-anti-patterns.md) -- ORDER BY RAND() (ERROR)
- [`not-in-subquery`](sql-anti-patterns.md) -- NOT IN subquery NULL trap (ERROR)
- [`implicit-type-conversion`](sql-anti-patterns.md) -- Implicit type conversion (WARNING)
- [`or-abuse`](sql-anti-patterns.md) -- OR abuse (WARNING)
- [`offset-pagination`](sql-anti-patterns.md) -- OFFSET pagination (WARNING)
- [`like-leading-wildcard`](sql-anti-patterns.md) -- LIKE leading wildcard (WARNING)
- [`large-in-list`](sql-anti-patterns.md) -- Large IN list (WARNING)
- [`distinct-misuse`](sql-anti-patterns.md) -- DISTINCT misuse (WARNING)
- [`having-misuse`](sql-anti-patterns.md) -- HAVING misuse (WARNING)
- [`unbounded-result-set`](sql-anti-patterns.md) -- Unbounded result set (WARNING)
- [`case-in-where`](sql-anti-patterns.md) -- CASE in WHERE (WARNING)
- [`select-all`](sql-anti-patterns.md) -- SELECT * (INFO)
- [`redundant-filter`](sql-anti-patterns.md) -- Redundant filter (INFO)
- [`count-instead-of-exists`](sql-anti-patterns.md) -- COUNT vs EXISTS (INFO)
- [`union-without-all`](sql-anti-patterns.md) -- UNION without ALL (INFO)
- [`count-star-no-where`](sql-anti-patterns.md) -- COUNT(*) without WHERE (INFO)
- [`excessive-column-fetch`](sql-anti-patterns.md) -- Excessive column fetch (INFO)
- [`non-deterministic-pagination`](sql-anti-patterns.md) -- Non-deterministic pagination (INFO)
- [`force-index-hint`](sql-anti-patterns.md) -- Force index hint (INFO)
- See [SQL Anti-Patterns](sql-anti-patterns.md) for the full list

### DML Safety
- [`update-without-where`](dml-anti-patterns.md) -- UPDATE/DELETE without WHERE (ERROR)
- [`dml-without-index`](dml-anti-patterns.md) -- DML without index (WARNING)
- [`repeated-single-insert`](dml-anti-patterns.md) -- Repeated single INSERT (WARNING)
- [`repeated-single-update`](dml-anti-patterns.md) -- Repeated single-row UPDATE (WARNING)
- [`insert-select-all`](dml-anti-patterns.md) -- INSERT SELECT * (WARNING)
- [`insert-on-duplicate-key`](dml-anti-patterns.md) -- INSERT ON DUPLICATE KEY (WARNING)
- [`subquery-in-dml`](dml-anti-patterns.md) -- Subquery in DML (WARNING)
- [`implicit-columns-insert`](dml-anti-patterns.md) -- Implicit columns INSERT (WARNING)
- [`insert-select-locks-source`](dml-anti-patterns.md) -- INSERT SELECT locks source (INFO)
- See [DML Anti-Patterns](dml-anti-patterns.md) for the full list

### JOIN Issues
- `cartesian-join` -- Cartesian JOIN (ERROR)
- `correlated-subquery` -- Correlated subquery (WARNING)
- `too-many-joins` -- Too many JOINs (WARNING)
- `implicit-join` -- Implicit JOIN syntax (WARNING)
- `unused-join` -- Unused LEFT JOIN (WARNING)

### Locking Risks
- `for-update-no-index` -- FOR UPDATE without index (ERROR)
- `for-update-non-unique` -- FOR UPDATE on non-unique index (WARNING)
- `range-lock-risk` -- Range lock risk (WARNING)
- `for-update-no-timeout` -- FOR UPDATE without timeout (WARNING)
- `read-modify-write` -- Check-then-act race without lock or unique-constraint backing (INFO)

### Connection Lifecycle
- `connection-held-idle` -- Connection held while non-database work runs, the pool-exhaustion shape (INFO)

### MySQL-Specific
- `string-concat-where` -- String concatenation in WHERE (WARNING)
- `group-by-function` -- Function in GROUP BY (WARNING)
- `regexp-usage` -- REGEXP/RLIKE usage (WARNING)
- `find-in-set` -- FIND_IN_SET usage (WARNING)

### Hibernate / ORM Patterns
- [`collection-delete-reinsert`](dml-anti-patterns.md) -- DELETE-all + re-INSERT (WARNING)
- [`derived-delete-loads-entities`](dml-anti-patterns.md) -- Derived delete loads entities (WARNING)
- `find-by-id-for-association` -- findById() used only for FK association (INFO)

### Query Structure
- `limit-without-order-by` -- LIMIT without ORDER BY (WARNING)
- `window-no-partition` -- Window function without PARTITION BY (WARNING)

### EXPLAIN-Based
- `filesort` -- Filesort detected in EXPLAIN output (INFO)
- `temporary-table` -- Temporary table usage in EXPLAIN output (INFO)

---

## Summary

| Severity | Issue Types | Action |
|----------|-------------|--------|
| ERROR | 11 | Must fix -- logic bugs or guaranteed performance degradation |
| WARNING | 39 | Should fix -- important issues that typically need attention |
| INFO | 17 | Review -- best-practice suggestions, may have false positives |
| **Active Total** | **67 issue types** | Emitted by the active detector set |
| Disabled | 1 | DuplicateQueryDetector (awaiting parameter tracking) |
| Reserved | 1 | full-scan (EXPLAIN full-table-scan detection planned) |

---

## Future Phases

| Phase | Focus | Status |
|-------|-------|--------|
| Phase 3 | Slow query log integration, execution time thresholds | Planned |
| Phase 4 | Multi-database support (MariaDB, Oracle, SQL Server) | Planned |
| Phase 5 | AI-assisted query rewrite suggestions | Research |

!!! success "Completed"
    **PostgreSQL support** is fully implemented in the `query-audit-postgresql` module.

!!! note "Contribute"
    Have an idea for a new detection rule? See the
    [Contributing Guide](../architecture/contributing.md) to learn how to implement one.
