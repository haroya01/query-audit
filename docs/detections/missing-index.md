# Missing Index Detection

| | |
|---|---|
| **Issue codes** | `missing-where-index`, `missing-join-index`, `missing-order-by-index`, `missing-group-by-index` |
| **Severity** | ERROR (WHERE, JOIN) / WARNING (ORDER BY, GROUP BY) |
| **Confidence** | Confirmed (100%) |

The `MissingIndexDetector` is a single detector that emits **4 different IssueTypes**, each
targeting a specific SQL clause where a missing index causes performance degradation.

!!! note "One rule, four issue types"
    Although `MissingIndexDetector` is registered as a single detection rule in
    `QueryAuditAnalyzer`, it produces 4 separate issue types. This is why Query Guard has
    57 active rules but 60 active issue types. See the
    [Detection Rules Overview](overview.md#accounting) for the full accounting.

---

## Core Mechanism

Missing Index detection works by cross-referencing two sources of truth:

1. **SQL column extraction** -- Parse the query to identify which columns are used in WHERE,
   JOIN, ORDER BY, and GROUP BY clauses
2. **`SHOW INDEX` verification** -- Query the database for actual index metadata and check
   whether each referenced column has an index

Because this compares **SQL structure** against **schema metadata** (not `EXPLAIN` output),
the result is deterministic and 100% reliable regardless of data volume.

```
Query: SELECT * FROM orders WHERE status = ?

Step 1: Extract WHERE column  --> status
Step 2: SHOW INDEX FROM orders --> check if 'status' has an index
Step 3: No index found         --> ERROR (confirmed, 100%)
```

---

## The 4 Sub-Types

### 1. Missing WHERE Index (`missing-where-index`)

!!! danger "Severity: ERROR"
    A missing WHERE index means **every query** touching this condition performs a full table
    scan on that column. This is the highest-impact missing index scenario.

Detects columns used in `WHERE` conditions that have no index.

**Why 100% reliable:** The column appears in the WHERE clause (structural fact) and `SHOW INDEX`
confirms no index exists (schema fact). Neither depends on data.

#### Example

```sql
-- Query
SELECT * FROM orders WHERE status = 'pending';
```

=== "MySQL SHOW INDEX output"

    ```
    -- SHOW INDEX FROM orders
    +--------+----------+-----------+
    | Table  | Key_name | Column    |
    +--------+----------+-----------+
    | orders | PRIMARY  | id        |
    +--------+----------+-----------+
    -- 'status' column: no index found
    ```

=== "PostgreSQL equivalent"

    ```sql
    -- \di+ orders
    -- or: SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'orders';
    +------------+-------------------------------------------+
    | indexname  | indexdef                                  |
    +------------+-------------------------------------------+
    | orders_pkey| CREATE UNIQUE INDEX orders_pkey ON orders (id) |
    +------------+-------------------------------------------+
    -- No index covering 'status'
    ```

#### Report Output

```
[ERROR] Missing index on WHERE column
  Query  : SELECT * FROM orders WHERE status = ?
  Table  : orders
  Column : status
  Detail : SHOW INDEX FROM orders -> status column has no index
  Fix    : ALTER TABLE orders ADD INDEX idx_status (status)
```

#### Fix

=== "MySQL"

    ```sql
    ALTER TABLE orders ADD INDEX idx_status (status);
    ```

=== "PostgreSQL"

    ```sql
    CREATE INDEX idx_status ON orders (status);
    ```

=== "With composite index suggestion"

    When other WHERE columns already have indexes, Query Guard suggests a composite index:

    ```sql
    -- If 'customer_id' already has an index, and you also filter on 'status':
    ALTER TABLE orders ADD INDEX idx_customer_id_status (customer_id, status);
    ```

---

### 2. Missing JOIN Index (`missing-join-index`)

!!! danger "Severity: ERROR"
    Without an index on the join column, MySQL performs a nested loop with a full scan of the
    inner table **for every row** in the outer table.

Detects columns used in `JOIN ON` conditions that have no index.

**Why 100% reliable:** The column is explicitly named in the ON clause, and `SHOW INDEX`
confirms no index.

#### Example

```sql
SELECT o.id, m.name
FROM orders o
JOIN members m ON o.member_id = m.id;
```

If `orders.member_id` has no index:

=== "MySQL output"

    ```
    [ERROR] Missing index on JOIN column
      Query  : SELECT o.id, m.name FROM orders o JOIN members m ON o.member_id = m.id
      Table  : orders
      Column : member_id
      Detail : SHOW INDEX FROM orders -> member_id column has no index
      Fix    : ALTER TABLE orders ADD INDEX idx_member_id (member_id)
    ```

=== "PostgreSQL output"

    ```
    [ERROR] Missing index on JOIN column
      Query  : SELECT o.id, m.name FROM orders o JOIN members m ON o.member_id = m.id
      Table  : orders
      Column : member_id
      Detail : No index found on orders.member_id
      Fix    : CREATE INDEX idx_member_id ON orders (member_id)
    ```

#### Fix

=== "MySQL"

    ```sql
    ALTER TABLE orders ADD INDEX idx_member_id (member_id);

    -- This is typically a foreign key -- consider adding a FK constraint too:
    ALTER TABLE orders ADD CONSTRAINT fk_orders_member
      FOREIGN KEY (member_id) REFERENCES members(id);
    ```

=== "PostgreSQL"

    ```sql
    CREATE INDEX idx_member_id ON orders (member_id);

    -- Add foreign key if not already present:
    ALTER TABLE orders ADD CONSTRAINT fk_orders_member
      FOREIGN KEY (member_id) REFERENCES members(id);
    ```

!!! tip "MySQL auto-creates indexes for foreign keys"
    In MySQL, adding a `FOREIGN KEY` constraint automatically creates an index if one does
    not already exist. If you define FKs, the `missing-join-index` issue typically does not
    appear for those columns.

---

### 3. Missing ORDER BY Index (`missing-order-by-index`)

!!! warning "Severity: WARNING"
    While not always critical (small result sets sort quickly), a missing ORDER BY index on a
    large table forces MySQL to sort all matching rows in memory or on disk (filesort).

Detects columns used in `ORDER BY` that have no index.

**Why 100% reliable:** The column is in the ORDER BY clause (structural) and has no index
(schema).

#### Example

```sql
SELECT * FROM products ORDER BY created_at DESC LIMIT 20;
```

=== "MySQL output"

    ```
    [WARNING] Missing index on ORDER BY column
      Query  : SELECT * FROM products ORDER BY created_at DESC LIMIT ?
      Table  : products
      Column : created_at
      Detail : SHOW INDEX FROM products -> created_at column has no index
      Fix    : ALTER TABLE products ADD INDEX idx_created_at (created_at)
    ```

=== "PostgreSQL output"

    ```
    [WARNING] Missing index on ORDER BY column
      Query  : SELECT * FROM products ORDER BY created_at DESC LIMIT 20
      Table  : products
      Column : created_at
      Detail : No index found on products.created_at
      Fix    : CREATE INDEX idx_created_at ON products (created_at)
    ```

#### Fix

=== "Simple index"

    ```sql
    -- MySQL
    ALTER TABLE products ADD INDEX idx_created_at (created_at);

    -- PostgreSQL
    CREATE INDEX idx_created_at ON products (created_at);
    ```

=== "Composite index (WHERE + ORDER BY)"

    When your query has both WHERE and ORDER BY, a composite index eliminates both the scan
    and the filesort:

    ```sql
    -- Query: SELECT * FROM products WHERE category = ? ORDER BY created_at DESC

    -- MySQL
    ALTER TABLE products ADD INDEX idx_category_created_at (category, created_at);

    -- PostgreSQL
    CREATE INDEX idx_category_created_at ON products (category, created_at);
    ```

    !!! tip "Column order matters"
        Always put the WHERE column(s) first, then the ORDER BY column(s). This lets MySQL
        use the index for filtering AND avoids the filesort.

---

### 4. Missing GROUP BY Index (`missing-group-by-index`)

!!! warning "Severity: WARNING"
    GROUP BY without an index requires building a temporary table to compute groups, which
    is expensive on large datasets.

Detects columns used in `GROUP BY` that have no index, forcing MySQL to use a temporary table
for grouping.

**Why 100% reliable:** The column is in the GROUP BY clause (structural) and has no index
(schema).

#### Example

```sql
SELECT status, COUNT(*) FROM orders GROUP BY status;
```

=== "MySQL output"

    ```
    [WARNING] Missing index on GROUP BY column
      Query  : SELECT status, COUNT(*) FROM orders GROUP BY status
      Table  : orders
      Column : status
      Detail : SHOW INDEX FROM orders -> status column has no index
      Fix    : ALTER TABLE orders ADD INDEX idx_status (status)
    ```

=== "PostgreSQL output"

    ```
    [WARNING] Missing index on GROUP BY column
      Query  : SELECT status, COUNT(*) FROM orders GROUP BY status
      Table  : orders
      Column : status
      Detail : No index found on orders.status
      Fix    : CREATE INDEX idx_status ON orders (status)
    ```

#### Fix

=== "Simple index"

    ```sql
    -- MySQL
    ALTER TABLE orders ADD INDEX idx_status (status);

    -- PostgreSQL
    CREATE INDEX idx_status ON orders (status);
    ```

=== "Composite index (WHERE + GROUP BY)"

    ```sql
    -- Query: SELECT region, COUNT(*) FROM orders WHERE status = 'active' GROUP BY region

    -- MySQL
    ALTER TABLE orders ADD INDEX idx_status_region (status, region);

    -- PostgreSQL
    CREATE INDEX idx_status_region ON orders (status, region);
    ```

    !!! tip "WHERE columns first, then GROUP BY columns"
        Same principle as WHERE + ORDER BY: put the filtering columns first in the composite
        index so MySQL can both filter and group using the index.

---

## Sub-Type Summary

| Sub-Type | Code | Severity | SQL Clause | Impact Without Index |
|----------|------|----------|-----------|---------------------|
| WHERE | `missing-where-index` | ERROR | `WHERE col = ?` | Full table scan on every query |
| JOIN | `missing-join-index` | ERROR | `JOIN ON col = ?` | Nested loop full scan per outer row |
| ORDER BY | `missing-order-by-index` | WARNING | `ORDER BY col` | In-memory or on-disk filesort |
| GROUP BY | `missing-group-by-index` | WARNING | `GROUP BY col` | Temporary table for grouping |

---

## Composite Index Leading Column (`composite-index-leading`)

While not part of the core 4 sub-types, the `CompositeIndexDetector` works closely with
`MissingIndexDetector` and deserves mention here.

!!! warning "Severity: WARNING"
    The composite index exists but is effectively invisible to this query because the leading
    column is not referenced.

Detects queries that use a non-leading column of a composite index in the WHERE clause while
**skipping** the leading column. In MySQL's B-Tree indexes, this means the index cannot be used.

### How B-Tree Composite Indexes Work

A composite index `(a, b, c)` is organized as a B-Tree sorted first by `a`, then by `b` within
each `a`, then by `c` within each `(a, b)`:

```
            Composite Index: idx_abc (a, b, c)

            B-Tree sorted by: a -> b -> c

    +-----------------------------------------------------+
    |  a=1, b=1, c=1                                      |
    |  a=1, b=1, c=2                                      |
    |  a=1, b=2, c=1    <-- b=2 values clustered          |
    |  a=1, b=2, c=3        under a=1                     |
    |  a=2, b=1, c=1                                      |
    |  a=2, b=3, c=2                                      |
    |  a=3, b=1, c=1                                      |
    +-----------------------------------------------------+

    WHERE a = 1 AND b = 2       --> Can use index (leftmost prefix)
    WHERE a = 1                 --> Can use index (leftmost prefix)
    WHERE b = 2                 --> CANNOT use index (skips 'a')
    WHERE b = 2 AND c = 1      --> CANNOT use index (skips 'a')
    WHERE a = 1 AND c = 1      --> Partial use (a only, c skipped)
```

### Example

Assume a composite index `idx_status_date (status, created_at)` exists on `orders`:

```sql
-- This query skips the leading column 'status'
SELECT * FROM orders WHERE created_at > '2024-01-01';
```

```
[WARNING] Composite index leading column not used
  Query  : SELECT * FROM orders WHERE created_at > ?
  Table  : orders
  Column : created_at
  Detail : Index idx_status_date(status, created_at) exists but leading column
           status is not in WHERE clause
  Fix    : Include leading column in WHERE or create a separate index on created_at
```

### Fix Options

=== "Add the leading column"

    ```sql
    -- If you can add the leading column to the query
    SELECT * FROM orders
    WHERE status = 'active' AND created_at > '2024-01-01';
    ```

=== "Create a separate index"

    ```sql
    -- If the query legitimately does not filter by status

    -- MySQL
    ALTER TABLE orders ADD INDEX idx_created_at (created_at);

    -- PostgreSQL
    CREATE INDEX idx_created_at ON orders (created_at);
    ```

---

## False Positive Protections

The `MissingIndexDetector` includes several intelligent filters to reduce false positives.
Understanding these helps you trust the results and avoid unnecessary index creation.

### 1. Soft-Delete Column Detection

Columns matching soft-delete patterns (`deleted_at`, `is_deleted`, `deleted`, `removed_at`, etc.)
receive special treatment:

| Scenario | Behavior |
|----------|----------|
| Soft-delete column is the **only** WHERE filter | Downgraded to **INFO** severity with explanation |
| Soft-delete column appears **alongside** other filters | **Suppressed entirely** -- the other filter provides selectivity |

!!! info "Why?"
    Soft-delete columns like `deleted_at IS NULL` typically match 99%+ of rows, making a
    standalone B-tree index useless. MySQL would prefer a full table scan over an index that
    returns nearly every row.

```sql
-- Downgraded to INFO (sole WHERE condition):
SELECT * FROM users WHERE deleted_at IS NULL;

-- Suppressed entirely (other filter exists):
SELECT * FROM users WHERE email = 'test@example.com' AND deleted_at IS NULL;
```

**Recognized soft-delete column names:**
`deleted_at`, `deleted`, `is_deleted`, `removed_at`, `deactivated_at`, `discarded_at`, `unsuspended_at`

### 2. Low Cardinality Column Detection

Columns that likely have few distinct values receive special treatment:

| Scenario | Behavior |
|----------|----------|
| Low-cardinality column with **another indexed** WHERE column | **Suppressed entirely** |
| Low-cardinality column as **sole** WHERE filter | Downgraded to **INFO** severity |

!!! info "Why?"
    A B-tree index on a column with only 3-5 distinct values (e.g., `status`, `type`, `gender`)
    has poor selectivity. MySQL often ignores such indexes in favor of a full table scan.

**Detection methods:**

- **Name-based:** Exact matches (`type`, `status`, `role`, `gender`, `category`, `level`, `kind`, `state`, `enabled`, `active`, `visible`, `locked`, `verified`, `approved`, `published`, `archived`)
- **Prefix-based:** Columns starting with `is_`, `has_`, or `flag`
- **Suffix-based:** Columns ending with `_type` or `_status`
- **Cardinality-based:** Index metadata shows cardinality <= 10

### 3. Unique/Primary Key Short-Circuit

When another WHERE column on the same table has a **unique or primary key** index with an equality
condition, the result set is guaranteed to be at most 1 row. In that case:

- All other missing WHERE index warnings for that table are **skipped**
- Missing ORDER BY index warnings are **skipped** (sorting 0-1 rows is free)

```sql
-- No missing index warning for 'status' because 'id' is the primary key:
SELECT * FROM orders WHERE id = 123 AND status = 'pending';
```

### 4. LIKE Operator Exclusion

Columns with `LIKE` operators are **skipped** by `MissingIndexDetector`. The `LikeWildcardDetector`
handles leading-wildcard LIKE patterns separately, and for non-leading-wildcard LIKE, the
effectiveness of a B-tree index depends on the pattern, which cannot be determined from
normalized SQL.

### 5. IS NULL / IS NOT NULL Exclusion

NULL checks on non-soft-delete columns are **skipped** because they have poor selectivity and
rarely benefit from B-tree indexes.

### 6. Composite Index Member Exclusion

If a column appears in **any** composite index for the table, `MissingIndexDetector` skips it and
defers to `CompositeIndexDetector` to report whether the composite index is being used correctly.

### 7. GROUP BY with PK or Indexed WHERE

GROUP BY columns are skipped when:

- All primary key columns of the table are present in GROUP BY (functional dependency)
- A WHERE column on the same table already has an index (narrow result set makes GROUP BY
  indexing less impactful)

### False Positive Protection Summary

| Protection | Trigger Condition | Result |
|------------|-------------------|--------|
| Soft-delete (sole filter) | `deleted_at IS NULL` as only WHERE condition | Downgrade to INFO |
| Soft-delete (with other filter) | `deleted_at IS NULL AND email = ?` | Suppress entirely |
| Low cardinality (sole filter) | `status = ?` as only WHERE condition | Downgrade to INFO |
| Low cardinality (with indexed column) | `status = ?` with another indexed WHERE column | Suppress entirely |
| Unique/PK short-circuit | `id = ?` in WHERE (primary key) | Skip all other missing index warnings for that table |
| LIKE operator | `name LIKE ?` | Skip (deferred to LikeWildcardDetector) |
| IS NULL / IS NOT NULL | `phone IS NULL` | Skip (poor selectivity) |
| Composite index member | Column in any composite index | Skip (deferred to CompositeIndexDetector) |
| GROUP BY with PK | All PK columns in GROUP BY | Skip |
| GROUP BY with indexed WHERE | Indexed WHERE column on same table | Skip |

---

## The SHOW INDEX Cross-Verification Flow

The complete detection flow for every query:

```
                        +----------------------+
                        |  Intercepted Query   |
                        +---------+------------+
                                  |
                        +---------v------------+
                        |  Is it a SELECT?     |
                        +---------+------------+
                             Yes  |
                        +---------v------------+
                        |  Parse SQL:          |
                        |  - WHERE columns     |
                        |  - JOIN ON columns   |
                        |  - ORDER BY columns  |
                        |  - GROUP BY columns  |
                        +---------+------------+
                                  |
                        +---------v------------+
                        |  Resolve aliases     |
                        |  (e.g., o -> orders) |
                        +---------+------------+
                                  |
                  +---------------+---------------+
                  v               v               v
         +---------------+ +------------+ +---------------+
         |  For each     | | For each   | | For each      |
         |  WHERE col:   | | JOIN col:  | | ORDER BY /    |
         |               | |            | | GROUP BY col: |
         |  Apply false  | | SHOW INDEX | | Apply false   |
         |  positive     | | has index? | | positive      |
         |  filters,     | |            | | filters,      |
         |  SHOW INDEX   | |            | | SHOW INDEX    |
         |  has index?   | |            | | has index?    |
         +-------+-------+ +-----+------+ +-------+------+
              No |            No |             No |
                 v               v               v
           ERROR issue     ERROR issue     WARNING issue
           (or downgrade)                  (or downgrade)
```

!!! tip "Performance note"
    Index metadata is fetched once per test via `SHOW INDEX` and cached. The per-query overhead
    is only the SQL parsing and hash-map lookups -- no additional database round-trips.

---

## How to Create Suggested Indexes

### Single Column Index

=== "MySQL"

    ```sql
    ALTER TABLE orders ADD INDEX idx_status (status);
    ```

=== "PostgreSQL"

    ```sql
    CREATE INDEX idx_status ON orders (status);
    ```

### Composite Index (WHERE + WHERE)

When Query Guard detects multiple unindexed WHERE columns on the same table:

=== "MySQL"

    ```sql
    ALTER TABLE orders ADD INDEX idx_customer_status (customer_id, status);
    ```

=== "PostgreSQL"

    ```sql
    CREATE INDEX idx_customer_status ON orders (customer_id, status);
    ```

!!! tip "Leading column selection"
    Put the **most selective** column first (the one with the most distinct values). For
    example, `customer_id` before `status`.

### Composite Index (WHERE + ORDER BY)

For queries that filter and sort:

=== "MySQL"

    ```sql
    -- Query: SELECT * FROM products WHERE category = ? ORDER BY price ASC
    ALTER TABLE products ADD INDEX idx_category_price (category, price);
    ```

=== "PostgreSQL"

    ```sql
    CREATE INDEX idx_category_price ON products (category, price);
    ```

### Composite Index (WHERE + GROUP BY)

For queries that filter and group:

=== "MySQL"

    ```sql
    -- Query: SELECT region, COUNT(*) FROM orders WHERE status = ? GROUP BY region
    ALTER TABLE orders ADD INDEX idx_status_region (status, region);
    ```

=== "PostgreSQL"

    ```sql
    CREATE INDEX idx_status_region ON orders (status, region);
    ```

### Covering Index

For queries that only need specific columns, a covering index avoids table lookups entirely:

=== "MySQL"

    ```sql
    -- Query: SELECT name, email FROM users WHERE status = 'active'
    ALTER TABLE users ADD INDEX idx_status_covering (status, name, email);
    ```

=== "PostgreSQL (INCLUDE syntax)"

    ```sql
    -- PostgreSQL supports INCLUDE for non-key covering columns:
    CREATE INDEX idx_status_covering ON users (status) INCLUDE (name, email);
    ```

    !!! info "INCLUDE vs composite"
        PostgreSQL's `INCLUDE` clause adds columns to the leaf pages of the index without
        including them in the B-tree sort order. This means the index remains compact and
        efficient for `WHERE status = ?` lookups while still providing coverage for `name`
        and `email` without a table lookup.

### Partial Index (PostgreSQL only)

For soft-delete patterns where most rows match the condition:

```sql
-- Only index non-deleted rows (PostgreSQL)
CREATE INDEX idx_users_active ON users (email) WHERE deleted_at IS NULL;
```

!!! tip "MySQL alternative for partial indexes"
    MySQL does not support partial indexes directly. Consider a generated column instead:

    ```sql
    ALTER TABLE users ADD is_active TINYINT(1)
      GENERATED ALWAYS AS (CASE WHEN deleted_at IS NULL THEN 1 ELSE NULL END) STORED;
    ALTER TABLE users ADD INDEX idx_active_email (is_active, email);
    ```

### Index Creation Best Practices

| Practice | Recommendation |
|----------|---------------|
| Naming convention | Use `idx_<table>_<columns>` or `idx_<columns>` |
| Online DDL | Use `ALTER TABLE ... ADD INDEX ... ALGORITHM=INPLACE, LOCK=NONE` for MySQL 5.6+ |
| Concurrently | Use `CREATE INDEX CONCURRENTLY` for PostgreSQL to avoid locking |
| Verify after creation | Run `SHOW INDEX FROM table` (MySQL) or `\di+ table` (PostgreSQL) |
| Monitor index usage | Check `sys.schema_unused_indexes` (MySQL) or `pg_stat_user_indexes` (PostgreSQL) |

---

## Related Rules

- [`composite-index-leading`](#composite-index-leading-column-composite-index-leading) -- Composite index leading column not used
- [`dml-without-index`](dml-anti-patterns.md#dml-without-index-on-where-columns) -- Missing index on UPDATE/DELETE WHERE columns
- [`for-update-no-index`](overview.md) -- FOR UPDATE without index (locking risk)
- [`covering-index-opportunity`](overview.md) -- Covering index suggestion
- [`write-amplification`](overview.md) -- Too many indexes cause write amplification
