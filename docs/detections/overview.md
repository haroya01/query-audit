# Detection Rules Overview

Query Guard ships with **57 active detection rules** that catch SQL performance issues,
logic bugs, and anti-patterns during your test runs. These 57 rules emit **60 distinct
issue types** (because `MissingIndexDetector` alone emits 4 different issue types).
Rules are organized by severity and confidence model to help you prioritize fixes.

!!! info "IssueType enum vs active rules"
    The `IssueType` enum contains **64** entries. Of those, **60** are actively emitted by
    **57 detection rules**. The remaining 4 are [disabled or reserved](#disabled-reserved-rules).

---

## Confidence Model

### Confirmed (100% Reliable)

These rules analyze **SQL structure and database schema** -- things that do not change with data
volume. If Query Guard reports a Confirmed issue, it is a real problem regardless of whether you
are running against 5 test rows or 5 million production rows.

!!! success "Confirmed = Structural / Pattern-based"
    The detection logic examines the SQL text, repetition patterns, or cross-references the
    actual index metadata via `SHOW INDEX`. None of these depend on row counts or data
    distribution.

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

The complete searchable reference of all 60 issue types emitted by 57 active detection rules.

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
| 28 | Query Count Regression | `query-count-regression` | WARNING | Query Patterns | Query count regression detected |
| 29 | DML Without Index | `dml-without-index` | WARNING | DML Safety | UPDATE/DELETE WHERE column has no index |
| 30 | INSERT SELECT * | `insert-select-all` | WARNING | DML Safety | INSERT with SELECT * is fragile |
| 31 | INSERT ON DUPLICATE KEY | `insert-on-duplicate-key` | WARNING | DML Safety | INSERT ON DUPLICATE KEY UPDATE may cause deadlocks |
| 32 | Subquery in DML | `subquery-in-dml` | WARNING | DML Safety | Subquery in UPDATE/DELETE cannot use semijoin |
| 33 | Implicit Columns INSERT | `implicit-columns-insert` | WARNING | DML Safety | INSERT without explicit column list is fragile |
| 34 | Correlated Subquery | `correlated-subquery` | WARNING | JOIN Issues | Correlated subquery in SELECT clause |
| 35 | Too Many JOINs | `too-many-joins` | WARNING | JOIN Issues | Query has too many JOINs |
| 36 | Implicit JOIN | `implicit-join` | WARNING | JOIN Issues | Implicit comma-separated join syntax |
| 37 | Unused JOIN | `unused-join` | WARNING | JOIN Issues | LEFT JOIN table is never referenced |
| 38 | FOR UPDATE Non-Unique | `for-update-non-unique` | WARNING | Locking Risks | FOR UPDATE on non-unique index causes gap locks |
| 39 | Range Lock Risk | `range-lock-risk` | WARNING | Locking Risks | Range + FOR UPDATE on unindexed column may gap lock |
| 40 | String Concat in WHERE | `string-concat-where` | WARNING | MySQL-Specific | String concatenation in WHERE prevents index |
| 41 | GROUP BY Function | `group-by-function` | WARNING | MySQL-Specific | Function in GROUP BY prevents index usage |
| 42 | REGEXP Usage | `regexp-usage` | WARNING | MySQL-Specific | REGEXP/RLIKE prevents index usage |
| 43 | FIND_IN_SET | `find-in-set` | WARNING | MySQL-Specific | FIND_IN_SET indicates comma-separated values violating 1NF |
| 44 | Collection Delete+Reinsert | `collection-delete-reinsert` | WARNING | Hibernate/ORM | DELETE-all + re-INSERT pattern |
| 45 | Derived Delete Loads Entities | `derived-delete-loads-entities` | WARNING | Hibernate/ORM | Derived delete loads entities before deletes |
| 46 | LIMIT Without ORDER BY | `limit-without-order-by` | WARNING | Query Structure | LIMIT without ORDER BY returns non-deterministic rows |
| 47 | Window No PARTITION | `window-no-partition` | WARNING | Query Structure | Window function without PARTITION BY |
| 48 | FOR UPDATE No Timeout | `for-update-no-timeout` | WARNING | Locking Risks | FOR UPDATE without NOWAIT/SKIP LOCKED |
| 49 | CASE in WHERE | `case-in-where` | WARNING | SQL Anti-Patterns | CASE expression in WHERE prevents index usage |
| 50 | SELECT * | `select-all` | INFO | SQL Anti-Patterns | SELECT * usage |
| 51 | Redundant Filter | `redundant-filter` | INFO | SQL Anti-Patterns | Redundant duplicate WHERE condition |
| 52 | COUNT vs EXISTS | `count-instead-of-exists` | INFO | SQL Anti-Patterns | COUNT used where EXISTS would be more efficient |
| 53 | UNION Without ALL | `union-without-all` | INFO | SQL Anti-Patterns | UNION without ALL forces deduplication sort |
| 54 | Covering Index Opportunity | `covering-index-opportunity` | INFO | Index Issues | Query could benefit from a covering index |
| 55 | COUNT(*) No WHERE | `count-star-no-where` | INFO | SQL Anti-Patterns | COUNT(*) without WHERE scans entire table |
| 56 | INSERT SELECT Locks Source | `insert-select-locks-source` | INFO | DML Safety | INSERT...SELECT locks source table rows |
| 57 | Excessive Column Fetch | `excessive-column-fetch` | INFO | SQL Anti-Patterns | Too many columns fetched, consider DTO projection |
| 58 | Mergeable Queries | `mergeable-queries` | INFO | Query Patterns | Multiple queries to same table could be merged |
| 59 | Non-Deterministic Pagination | `non-deterministic-pagination` | INFO | SQL Anti-Patterns | ORDER BY+LIMIT on non-unique column |
| 60 | Force Index Hint | `force-index-hint` | INFO | SQL Anti-Patterns | FORCE/USE/IGNORE INDEX hint overrides optimizer |

!!! note "Rule numbering"
    Rules 50-60 are INFO severity. The table numbers are for reference only and do not correspond
    to priority. Rules 1-11 are ERROR severity and should always be addressed. Rules 12-49 are
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

### WARNING Severity (38 issue types)

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

#### Query Patterns (3 issue types)

| Code | Description | Detection Method |
|------|-------------|-----------------|
| `slow-query` | Slow query detected | Compare execution time to configured threshold |
| `repeated-single-insert` | Repeated single-row INSERT should batch | Normalize INSERT, group by pattern, check count |
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

### INFO Severity (11 issue types)

Best-practice suggestions and heuristic checks. These won't fail your build by default
but are worth reviewing.

| Code | Description | Detection Method |
|------|-------------|-----------------|
| `select-all` | SELECT * usage | Regex match on parsed SQL |
| `redundant-filter` | Redundant duplicate WHERE condition | Detect duplicate predicates in WHERE clause |
| `count-instead-of-exists` | COUNT used where EXISTS is better | Detect `COUNT(*)` in conditional context |
| `union-without-all` | UNION without ALL forces dedup sort | Detect UNION without ALL keyword |
| `covering-index-opportunity` | Query could benefit from covering index | Analyze SELECT columns vs available indexes |
| `count-star-no-where` | COUNT(*) without WHERE scans full table | Detect `COUNT(*)` without WHERE clause |
| `insert-select-locks-source` | INSERT...SELECT locks source rows | Detect INSERT ... SELECT pattern |
| `excessive-column-fetch` | Too many columns, use DTO projection | Count selected columns, compare to threshold |
| `mergeable-queries` | Multiple queries could be merged | Detect multiple simple SELECTs to same table |
| `non-deterministic-pagination` | ORDER BY+LIMIT on non-unique column | Detect ORDER BY + LIMIT on non-unique columns |
| `force-index-hint` | FORCE/USE/IGNORE INDEX overrides optimizer | Detect index hint keywords in query |

!!! info "Info rules are still useful"
    Even though they can produce false positives with small test data, they serve as early
    warning signals. When combined with Confirmed findings (e.g., a full scan **and** a missing
    index), the diagnosis becomes highly reliable.

---

## Disabled & Reserved Rules

The `IssueType` enum has **64 entries**, but only **60 are actively emitted** by **57 detection
rules**. The remaining 4 entries fall into two categories:

### Disabled Rules (1 entry)

| Code | Reason |
|------|--------|
| `duplicate-query` | **Disabled in code.** datasource-proxy provides SQL with `?` placeholders, making it impossible to distinguish "same query, same params" from "same query, different params." The N+1 detector already covers repeated patterns. Will be re-enabled when parameter tracking is added. |

!!! warning "DuplicateQueryDetector"
    The `DuplicateQueryDetector` class exists in the codebase but is commented out in
    `QueryAuditAnalyzer.createRules()`. The `DUPLICATE_QUERY` IssueType remains in the enum
    for forward compatibility.

### Reserved for Future EXPLAIN-based Detection (3 entries)

| Code | Description | Status |
|------|-------------|--------|
| `full-scan` | Full table scan detected | Reserved -- requires EXPLAIN integration |
| `filesort` | Filesort detected | Reserved -- requires EXPLAIN integration |
| `temporary-table` | Temporary table usage | Reserved -- requires EXPLAIN integration |

These three IssueTypes exist in the enum but no active detector emits them. They are placeholders
for a planned EXPLAIN-based detection phase.

### Accounting

| Category | Count |
|----------|-------|
| Active detection rules (detectors) in `QueryAuditAnalyzer` | **57** |
| Active issue types emitted by those rules | **60** |
| Disabled (DuplicateQueryDetector) | 1 |
| Reserved EXPLAIN-based (full-scan, filesort, temporary-table) | 3 |
| **Total IssueType enum entries** | **64** |

!!! note "Why 57 rules but 60 issue types?"
    The `MissingIndexDetector` is registered as a **single detection rule** but emits **4
    different IssueTypes** (`missing-where-index`, `missing-join-index`, `missing-order-by-index`,
    `missing-group-by-index`), one for each SQL clause it analyzes. This accounts for the
    difference: 57 rules + 3 extra IssueTypes from MissingIndexDetector = 60 active IssueTypes.

---

## Rules by Category

### Query Patterns
- [`n-plus-one`](n-plus-one.md) -- N+1 Query detection (ERROR)
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

### MySQL-Specific
- `string-concat-where` -- String concatenation in WHERE (WARNING)
- `group-by-function` -- Function in GROUP BY (WARNING)
- `regexp-usage` -- REGEXP/RLIKE usage (WARNING)
- `find-in-set` -- FIND_IN_SET usage (WARNING)

### Hibernate / ORM Patterns
- [`collection-delete-reinsert`](dml-anti-patterns.md) -- DELETE-all + re-INSERT (WARNING)
- [`derived-delete-loads-entities`](dml-anti-patterns.md) -- Derived delete loads entities (WARNING)

### Query Structure
- `limit-without-order-by` -- LIMIT without ORDER BY (WARNING)
- `window-no-partition` -- Window function without PARTITION BY (WARNING)

---

## Summary

| Severity | Issue Types | Action |
|----------|-------------|--------|
| ERROR | 11 | Must fix -- logic bugs or guaranteed performance degradation |
| WARNING | 38 | Should fix -- important issues that typically need attention |
| INFO | 11 | Review -- best-practice suggestions, may have false positives |
| **Active Total** | **60 issue types** | **Emitted by 57 detection rules** |
| Disabled | 1 | DuplicateQueryDetector (awaiting parameter tracking) |
| Reserved | 3 | EXPLAIN-based detectors (planned) |

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
