# SQL Anti-Pattern Detection

QueryAudit detects SQL anti-patterns by analyzing the **structure** of your queries. This page
covers all SQL-related detectors organized by severity, including MySQL-specific patterns,
JOIN issues, locking risks, and query structure rules.

---

## ERROR Severity

These anti-patterns are logic bugs or guaranteed performance killers. **Always fix these.**

---

### WHERE Function Usage

| | |
|---|---|
| **Issue code** | `where-function` |
| **Severity** | ERROR |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

Wrapping a column in a function inside a `WHERE` clause **disables any index** on that column.
MySQL cannot use an index to satisfy `WHERE DATE(created_at) = '2024-01-01'` even if
`created_at` is indexed, because the index stores raw `created_at` values, not `DATE(created_at)`
values.

```
Index on created_at:

    2024-01-01 00:00:00
    2024-01-01 08:30:00    <-- Sorted by raw value
    2024-01-01 15:45:22
    2024-01-02 09:00:00

WHERE DATE(created_at) = '2024-01-01'
  --> MySQL must apply DATE() to EVERY row, then compare
  --> Full table scan (index is useless)
```

#### Detection

QueryAudit parses the WHERE clause and detects function calls that wrap column names.
Common functions caught: `DATE()`, `YEAR()`, `MONTH()`, `LOWER()`, `UPPER()`, `TRIM()`,
`SUBSTRING()`, `CAST()`, and others.

#### Examples and Fixes

=== "DATE() function"

    ```sql
    -- Bad: index on created_at is useless
    SELECT * FROM orders
    WHERE DATE(created_at) = '2024-01-01';

    -- Good: range query uses the index
    SELECT * FROM orders
    WHERE created_at >= '2024-01-01 00:00:00'
      AND created_at <  '2024-01-02 00:00:00';
    ```

=== "YEAR() / MONTH()"

    ```sql
    -- Bad
    SELECT * FROM orders
    WHERE YEAR(created_at) = 2024 AND MONTH(created_at) = 1;

    -- Good
    SELECT * FROM orders
    WHERE created_at >= '2024-01-01' AND created_at < '2024-02-01';
    ```

=== "LOWER() / UPPER()"

    ```sql
    -- Bad
    SELECT * FROM users WHERE LOWER(email) = 'user@example.com';

    -- Good (option 1): use case-insensitive collation
    SELECT * FROM users WHERE email = 'user@example.com';
    -- Ensure the column uses utf8mb4_general_ci collation

    -- Good (option 2): generated column with index (MySQL 5.7+)
    ALTER TABLE users
      ADD email_lower VARCHAR(255) GENERATED ALWAYS AS (LOWER(email)) STORED;
    ALTER TABLE users ADD INDEX idx_email_lower (email_lower);

    -- Good (option 3): expression index (PostgreSQL)
    CREATE INDEX idx_email_lower ON users (LOWER(email));
    ```

#### Report Output

```
[ERROR] Function usage in WHERE clause disables index
  Query  : SELECT * FROM orders WHERE DATE(created_at) = ?
  Table  : orders
  Column : created_at
  Detail : DATE() wrapping disables index on column created_at
  Fix    : Rewrite without function wrapper
```

#### Configuration

No threshold -- this rule is always active. Suppress per-test if needed:

```java
@QueryAudit(suppress = {"where-function"})
```

---

### Non-Sargable Expressions

| | |
|---|---|
| **Issue code** | `non-sargable` |
| **Severity** | ERROR |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

When arithmetic operations are applied to an indexed column, MySQL cannot use the index.
The term "sargable" comes from **S**earch **ARG**ument **ABLE** -- the expression must isolate
the column on one side for the index to be usable.

#### Detection

QueryAudit detects patterns where a column is wrapped in arithmetic (`+`, `-`, `*`, `/`)
before being compared.

#### Examples and Fixes

=== "Addition"

    ```sql
    -- Bad: index on price is useless
    SELECT * FROM products WHERE price + 10 > 100;

    -- Good: isolate the column
    SELECT * FROM products WHERE price > 90;
    ```

=== "Multiplication"

    ```sql
    -- Bad
    SELECT * FROM orders WHERE amount * 2 > 1000;

    -- Good
    SELECT * FROM orders WHERE amount > 500;
    ```

=== "Division"

    ```sql
    -- Bad
    SELECT * FROM metrics WHERE value / 100 = 5;

    -- Good
    SELECT * FROM metrics WHERE value = 500;
    ```

=== "Concatenation"

    ```sql
    -- Bad: concatenation on column prevents index
    SELECT * FROM users WHERE first_name || ' ' || last_name = 'John Doe';

    -- Good: use separate conditions or a generated column
    SELECT * FROM users WHERE first_name = 'John' AND last_name = 'Doe';
    ```

#### Report Output

```
[ERROR] Arithmetic on column prevents index usage
  Query  : SELECT * FROM products WHERE price + ? > ?
  Table  : products
  Column : price
  Detail : Arithmetic on column 'price' prevents index usage
  Fix    : Move the operation to the other side of the comparison
```

---

### NULL Comparison Bug

| | |
|---|---|
| **Issue code** | `null-comparison` |
| **Severity** | ERROR |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

In SQL's three-valued logic, comparing anything to NULL with `=` or `!=` always evaluates
to **UNKNOWN** (not TRUE or FALSE). This means:

- `WHERE status = NULL` matches **zero rows** (even rows where status IS NULL)
- `WHERE status != NULL` also matches **zero rows**

This is a **logic bug**, not just a performance issue.

#### Examples and Fixes

```sql
-- Bad: always returns 0 rows (logic bug!)
SELECT * FROM users WHERE email = NULL;
SELECT * FROM users WHERE email != NULL;
SELECT * FROM users WHERE email <> NULL;

-- Good: correct NULL comparison
SELECT * FROM users WHERE email IS NULL;
SELECT * FROM users WHERE email IS NOT NULL;
```

!!! danger "This is always a bug"
    Unlike other anti-patterns that merely degrade performance, `= NULL` is a **logic bug**
    that produces incorrect results. There is no legitimate use case for `column = NULL`.

#### Report Output

```
[ERROR] Comparison with NULL using = or !=
  Query  : SELECT * FROM users WHERE email = NULL
  Table  : users
  Column : email
  Detail : Comparison 'email = NULL' always evaluates to UNKNOWN. Use 'IS NULL' instead.
  Fix    : Change 'email = NULL' to 'email IS NULL'
```

---

### ORDER BY RAND()

| | |
|---|---|
| **Issue code** | `order-by-rand` |
| **Severity** | ERROR |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

`ORDER BY RAND()` forces MySQL to:

1. Generate a random number for every row in the result set
2. Sort **all rows** by that random number
3. Return the requested number of rows

This is always a full table scan + full sort, regardless of indexes.

#### Examples and Fixes

=== "Bad"

    ```sql
    -- Full table scan + sort on EVERY execution
    SELECT * FROM products ORDER BY RAND() LIMIT 5;
    ```

=== "Good: application-side random"

    ```sql
    -- Step 1: Get the count
    SELECT COUNT(*) FROM products;

    -- Step 2: Generate random offset in application
    int offset = random.nextInt(count - 5);

    -- Step 3: Fetch with calculated offset
    SELECT * FROM products LIMIT 5 OFFSET :offset;
    ```

=== "Good: random ID range"

    ```sql
    -- If IDs are sequential:
    SELECT * FROM products
    WHERE id >= FLOOR(RAND() * (SELECT MAX(id) FROM products))
    ORDER BY id LIMIT 5;
    ```

---

### NOT IN Subquery

| | |
|---|---|
| **Issue code** | `not-in-subquery` |
| **Severity** | ERROR |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

`NOT IN (subquery)` has a dangerous behavior: if the subquery returns **any NULL value**, the
entire `NOT IN` condition evaluates to UNKNOWN, returning **zero rows**.

#### Examples and Fixes

=== "Bad"

    ```sql
    -- Returns ZERO rows if any manager_id is NULL
    SELECT * FROM employees
    WHERE department_id NOT IN (SELECT manager_id FROM departments);
    ```

=== "Good: NOT EXISTS"

    ```sql
    SELECT * FROM employees e
    WHERE NOT EXISTS (
        SELECT 1 FROM departments d
        WHERE d.manager_id = e.department_id
    );
    ```

=== "Good: explicit NULL exclusion"

    ```sql
    SELECT * FROM employees
    WHERE department_id NOT IN (
        SELECT manager_id FROM departments WHERE manager_id IS NOT NULL
    );
    ```

=== "Good: LEFT JOIN + IS NULL"

    ```sql
    SELECT e.*
    FROM employees e
    LEFT JOIN departments d ON e.department_id = d.manager_id
    WHERE d.manager_id IS NULL;
    ```

---

### Cartesian JOIN

| | |
|---|---|
| **Issue code** | `cartesian-join` |
| **Severity** | ERROR |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

A Cartesian JOIN (cross join) occurs when a JOIN is missing its ON or USING condition. This
produces a result set of `rows_A x rows_B`, which explodes rapidly:

```
Table A: 1,000 rows   x   Table B: 1,000 rows   =   1,000,000 rows
```

#### Examples and Fixes

=== "Bad: missing ON condition"

    ```sql
    -- Produces rows_A x rows_B results
    SELECT o.id, c.name
    FROM orders o
    JOIN customers c;  -- missing ON clause!
    ```

=== "Good: explicit JOIN condition"

    ```sql
    SELECT o.id, c.name
    FROM orders o
    JOIN customers c ON o.customer_id = c.id;
    ```

=== "Intentional CROSS JOIN"

    If you genuinely need a cross join, use the explicit `CROSS JOIN` syntax to document intent:

    ```sql
    SELECT p.name, s.size
    FROM products p
    CROSS JOIN sizes s;
    ```

---

### FOR UPDATE Without Index

| | |
|---|---|
| **Issue code** | `for-update-no-index` |
| **Severity** | ERROR |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

`SELECT ... FOR UPDATE` without an index on the WHERE column causes InnoDB to scan the entire
table, acquiring **row locks on every row**. This effectively locks the entire table.

#### Examples and Fixes

```sql
-- Bad: locks entire table if 'status' has no index
SELECT * FROM orders WHERE status = 'pending' FOR UPDATE;

-- Good: ensure index exists
ALTER TABLE orders ADD INDEX idx_status (status);
SELECT * FROM orders WHERE status = 'pending' FOR UPDATE;
```

---

## WARNING Severity

These patterns indicate issues that should be reviewed and typically fixed.

---

### Implicit Type Conversion

| | |
|---|---|
| **Issue code** | `implicit-type-conversion` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

When a string column is compared to a numeric literal, MySQL performs implicit type conversion
on the column, which disables index usage.

#### Examples and Fixes

```sql
-- Bad: phone_code is a string column compared to a number
SELECT * FROM users WHERE phone_code = 82;

-- Good: use string comparison
SELECT * FROM users WHERE phone_code = '82';
```

!!! info "Detection scope"
    This detector only flags columns whose names contain obvious string indicators
    (`_name`, `_email`, `_phone`, `_code`, `_token`, `_key`, `_slug`, `_type`, etc.)
    to minimize false positives.

---

### OR Abuse

| | |
|---|---|
| **Issue code** | `or-abuse` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |
| **Default threshold** | 3 OR conditions |

#### Why It Matters

Excessive `OR` conditions limit MySQL's ability to use indexes effectively. MySQL can sometimes
use Index Merge optimization, but this becomes increasingly unlikely as OR conditions grow.

#### Examples and Fixes

=== "Same column: use IN"

    ```sql
    -- Bad: 10 OR conditions
    SELECT * FROM products
    WHERE category = 'electronics' OR category = 'books' OR category = 'clothing' ...;

    -- Good: single IN
    SELECT * FROM products
    WHERE category IN ('electronics', 'books', 'clothing', 'toys', 'food');
    ```

=== "Different columns: use UNION"

    ```sql
    -- Bad: OR across different columns (index merge unlikely)
    SELECT * FROM users
    WHERE email = 'a@test.com' OR phone = '010-1234' OR username = 'testuser';

    -- Good: UNION lets each sub-query use its own index
    SELECT * FROM users WHERE email = 'a@test.com'
    UNION
    SELECT * FROM users WHERE phone = '010-1234'
    UNION
    SELECT * FROM users WHERE username = 'testuser';
    ```

#### Configuration

```yaml
query-audit:
  or-clause:
    threshold: 3   # default
```

---

### OFFSET Pagination

| | |
|---|---|
| **Issue code** | `offset-pagination` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |
| **Default threshold** | 1000 |

#### Why It Matters

`LIMIT 20 OFFSET 100000` reads and discards 100,000 rows before returning 20. Performance
degrades linearly with OFFSET size.

```
OFFSET 0       -->  Read 20 rows, return 20      (fast)
OFFSET 1,000   -->  Read 1,020 rows, return 20   (ok)
OFFSET 100,000 -->  Read 100,020 rows, return 20  (slow)
```

#### Examples and Fixes

=== "Cursor-based pagination"

    ```sql
    -- Instead of:
    SELECT * FROM products ORDER BY id LIMIT 20 OFFSET 100000;

    -- Use the last seen ID as a cursor:
    SELECT * FROM products WHERE id > 98765 ORDER BY id LIMIT 20;
    ```

    O(1) regardless of page depth -- MySQL seeks directly to `id > 98765`.

=== "Deferred join"

    ```sql
    SELECT p.*
    FROM products p
    INNER JOIN (
        SELECT id FROM products ORDER BY id LIMIT 20 OFFSET 100000
    ) AS sub ON p.id = sub.id;
    ```

    The subquery scans the narrow primary key index, not full rows.

=== "Java implementation"

    ```java
    @Query("SELECT p FROM Product p WHERE p.id > :lastId ORDER BY p.id")
    List<Product> findNextPage(@Param("lastId") Long lastId, Pageable pageable);
    ```

#### Configuration

```yaml
query-audit:
  offset-pagination:
    threshold: 1000   # default
```

---

### LIKE Leading Wildcard

| | |
|---|---|
| **Issue code** | `like-leading-wildcard` |
| **Severity** | WARNING for literal leading-wildcard (`LIKE '%foo'`) / INFO for parameterized `LIKE ?` |
| **Confidence** | Confirmed for literals; suggestive for `LIKE ?` (runtime value is unknown) |

#### Why It Matters

`LIKE '%keyword'` prevents B-tree index usage because the search pattern starts with an unknown
prefix. MySQL must scan every row.

Parameterized `LIKE ?` cannot be checked statically — the runtime binding may begin with `%` and
cause the same full scan. The detector emits an INFO-level heads-up for this case.

#### Examples and Fixes

=== "Bad"

    ```sql
    -- Full table scan
    SELECT * FROM products WHERE name LIKE '%phone';
    ```

=== "Good: trailing wildcard"

    ```sql
    SELECT * FROM products WHERE name LIKE 'phone%';
    ```

=== "Good: fulltext search"

    ```sql
    ALTER TABLE products ADD FULLTEXT INDEX ft_name (name);
    SELECT * FROM products WHERE MATCH(name) AGAINST('phone');
    ```

=== "Good: reverse index trick"

    ```sql
    -- For suffix search, store a reversed copy
    ALTER TABLE products ADD name_reversed VARCHAR(255)
      GENERATED ALWAYS AS (REVERSE(name)) STORED;
    ALTER TABLE products ADD INDEX idx_name_reversed (name_reversed);

    -- Search for names ending in 'phone':
    SELECT * FROM products WHERE name_reversed LIKE CONCAT(REVERSE('phone'), '%');
    ```

---

### Large IN List

| | |
|---|---|
| **Issue code** | `large-in-list` |
| **Severity** | WARNING (>100 values) / ERROR (>1000 values) |
| **Confidence** | Confirmed (100%) |
| **Default threshold** | 100 values |

#### Why It Matters

Large IN lists cause optimizer overhead and can exceed parser limits. IN lists composed entirely
of `?` placeholders are automatically skipped (Hibernate IN-clause padding).

#### Examples and Fixes

=== "Bad"

    ```sql
    -- 500 values in IN clause
    SELECT * FROM products WHERE id IN (1, 2, 3, ..., 500);
    ```

=== "Good: temporary table JOIN"

    ```sql
    CREATE TEMPORARY TABLE tmp_ids (id BIGINT PRIMARY KEY);
    INSERT INTO tmp_ids VALUES (1), (2), (3), ...;
    SELECT p.* FROM products p JOIN tmp_ids t ON p.id = t.id;
    ```

=== "Good: batch processing"

    ```java
    // Split into multiple queries with smaller IN lists
    Lists.partition(ids, 100).forEach(batch -> {
        productRepository.findAllById(batch);
    });
    ```

#### Configuration

```yaml
query-audit:
  large-in-list:
    threshold: 100   # WARNING threshold; ERROR at 10x this value
```

---

### DISTINCT Misuse

| | |
|---|---|
| **Issue code** | `distinct-misuse` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

`DISTINCT` forces a sort/hash to deduplicate results. When the query context already guarantees
uniqueness (e.g., selecting a primary key), DISTINCT adds unnecessary overhead.

#### Examples and Fixes

=== "Bad: unnecessary DISTINCT on PK"

    ```sql
    SELECT DISTINCT * FROM orders WHERE id = 123;

    -- Good
    SELECT * FROM orders WHERE id = 123;
    ```

=== "Bad: DISTINCT hiding a JOIN problem"

    ```sql
    SELECT DISTINCT o.id FROM orders o JOIN items i ON o.id = i.order_id;

    -- Good: fix the query logic
    SELECT o.id FROM orders o WHERE EXISTS (
        SELECT 1 FROM items i WHERE i.order_id = o.id
    );
    ```

---

### HAVING Misuse

| | |
|---|---|
| **Issue code** | `having-misuse` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

`HAVING` is evaluated **after** grouping. When a condition does not involve an aggregate function,
it should be in `WHERE` (evaluated **before** grouping) for better performance.

#### Examples and Fixes

```sql
-- Bad: non-aggregate condition in HAVING
SELECT department, COUNT(*) FROM employees
GROUP BY department HAVING department = 'Engineering';

-- Good: move to WHERE
SELECT department, COUNT(*) FROM employees
WHERE department = 'Engineering'
GROUP BY department;
```

---

### Unbounded Result Set

| | |
|---|---|
| **Issue code** | `unbounded-result-set` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

A `SELECT` without `LIMIT` could return millions of rows, consuming memory and network bandwidth
unexpectedly.

#### Examples and Fixes

```sql
-- Bad: could return unbounded rows
SELECT * FROM logs WHERE level = 'ERROR';

-- Good: add a LIMIT
SELECT * FROM logs WHERE level = 'ERROR' LIMIT 1000;

-- Good: use pagination
SELECT * FROM logs WHERE level = 'ERROR'
ORDER BY created_at DESC LIMIT 20 OFFSET 0;
```

---

### Slow Query

| | |
|---|---|
| **Issue code** | `slow-query` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

Detects queries whose execution time exceeds the configured threshold. Configurable via:

```yaml
query-audit:
  slow-query:
    warning-ms: 500   # WARNING threshold
    error-ms: 3000    # ERROR threshold
```

---

### CASE in WHERE

| | |
|---|---|
| **Issue code** | `case-in-where` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

CASE expressions in WHERE prevent index usage because MySQL cannot predict the output at
plan time.

#### Examples and Fixes

```sql
-- Bad: CASE in WHERE prevents index
SELECT * FROM orders
WHERE CASE WHEN status = 'active' THEN priority ELSE 0 END > 5;

-- Good: rewrite as explicit conditions
SELECT * FROM orders
WHERE (status = 'active' AND priority > 5);
```

---

### Correlated Subquery

| | |
|---|---|
| **Issue code** | `correlated-subquery` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

A correlated subquery in the SELECT clause executes once **per row** of the outer query,
similar to the N+1 problem.

#### Examples and Fixes

=== "Bad"

    ```sql
    SELECT o.id,
           (SELECT COUNT(*) FROM items i WHERE i.order_id = o.id) as item_count
    FROM orders o;
    ```

=== "Good: JOIN with GROUP BY"

    ```sql
    SELECT o.id, COUNT(i.id) as item_count
    FROM orders o
    LEFT JOIN items i ON o.id = i.order_id
    GROUP BY o.id;
    ```

---

### Too Many JOINs

| | |
|---|---|
| **Issue code** | `too-many-joins` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

Queries with excessive JOINs become exponentially harder for the optimizer and may indicate
a design issue. Consider denormalization or materialized views for read-heavy workloads.

---

### Implicit JOIN

| | |
|---|---|
| **Issue code** | `implicit-join` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

Comma-separated join syntax makes it easy to accidentally create Cartesian products and is
harder to read than explicit JOIN syntax.

#### Examples and Fixes

```sql
-- Bad: implicit join (easy to miss the join condition)
SELECT o.id, c.name
FROM orders o, customers c
WHERE o.customer_id = c.id;

-- Good: explicit JOIN
SELECT o.id, c.name
FROM orders o
JOIN customers c ON o.customer_id = c.id;
```

---

### Unused JOIN

| | |
|---|---|
| **Issue code** | `unused-join` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

A LEFT JOIN whose table is never referenced in SELECT, WHERE, or ORDER BY adds unnecessary
overhead without contributing to the result.

#### Examples and Fixes

```sql
-- Bad: 'categories' table is joined but never used
SELECT p.name, p.price
FROM products p
LEFT JOIN categories c ON p.category_id = c.id;

-- Good: remove the unused JOIN
SELECT p.name, p.price
FROM products p;
```

---

### FOR UPDATE on Non-Unique Index

| | |
|---|---|
| **Issue code** | `for-update-non-unique` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

`FOR UPDATE` on a non-unique index causes InnoDB to acquire **gap locks** in addition to
record locks, potentially blocking inserts and updates on adjacent ranges.

#### Examples and Fixes

```sql
-- Bad: gap locks on non-unique index
SELECT * FROM orders WHERE status = 'pending' FOR UPDATE;

-- Good: lock by unique key when possible
SELECT * FROM orders WHERE id = 12345 FOR UPDATE;
```

---

### Range Lock Risk

| | |
|---|---|
| **Issue code** | `range-lock-risk` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

Range predicates (`>`, `<`, `BETWEEN`) combined with `FOR UPDATE` on unindexed columns
can cause extensive gap locking.

---

### FOR UPDATE Without Timeout

| | |
|---|---|
| **Issue code** | `for-update-no-timeout` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

`FOR UPDATE` without `NOWAIT` or `SKIP LOCKED` will block indefinitely until the lock is
acquired, risking deadlocks and application hangs.

#### Examples and Fixes

```sql
-- Bad: blocks indefinitely
SELECT * FROM orders WHERE id = 123 FOR UPDATE;

-- Good: fail immediately if locked
SELECT * FROM orders WHERE id = 123 FOR UPDATE NOWAIT;

-- Good: skip locked rows (for queue-like patterns)
SELECT * FROM orders WHERE status = 'pending'
FOR UPDATE SKIP LOCKED LIMIT 10;
```

---

### MySQL-Specific Patterns

#### String Concatenation in WHERE

| | |
|---|---|
| **Issue code** | `string-concat-where` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

String concatenation in WHERE prevents index usage, similar to function wrapping.

```sql
-- Bad
SELECT * FROM users WHERE CONCAT(first_name, ' ', last_name) = 'John Doe';

-- Good: use separate conditions
SELECT * FROM users WHERE first_name = 'John' AND last_name = 'Doe';
```

---

#### Function in GROUP BY

| | |
|---|---|
| **Issue code** | `group-by-function` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

Function calls in GROUP BY prevent the use of indexes for grouping.

```sql
-- Bad: index on created_at cannot be used for grouping
SELECT DATE(created_at) as day, COUNT(*)
FROM orders GROUP BY DATE(created_at);

-- Good: use a generated column (MySQL 5.7+)
ALTER TABLE orders ADD created_date DATE
  GENERATED ALWAYS AS (DATE(created_at)) STORED;
ALTER TABLE orders ADD INDEX idx_created_date (created_date);
SELECT created_date, COUNT(*) FROM orders GROUP BY created_date;
```

---

#### REGEXP Usage

| | |
|---|---|
| **Issue code** | `regexp-usage` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

`REGEXP` and `RLIKE` always require a full table scan -- indexes cannot be used.

```sql
-- Bad: full table scan
SELECT * FROM products WHERE name REGEXP '^phone[0-9]+';

-- Good: use LIKE when possible
SELECT * FROM products WHERE name LIKE 'phone%';
```

---

#### FIND_IN_SET

| | |
|---|---|
| **Issue code** | `find-in-set` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

`FIND_IN_SET` indicates comma-separated values stored in a single column, violating First Normal
Form (1NF). This prevents indexing and efficient querying.

```sql
-- Bad: comma-separated values
SELECT * FROM users WHERE FIND_IN_SET('admin', roles);

-- Good: normalize into a separate table
SELECT u.* FROM users u
JOIN user_roles ur ON u.id = ur.user_id
WHERE ur.role = 'admin';
```

---

## INFO Severity

Best-practice suggestions. These won't fail your build by default.

---

### SELECT *

| | |
|---|---|
| **Issue code** | `select-all` |
| **Severity** | INFO |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

`SELECT *` fetches every column, wasting bandwidth, memory, and defeating covering indexes.

#### Examples and Fixes

=== "Explicit columns"

    ```sql
    -- Bad
    SELECT * FROM orders WHERE status = 'pending';

    -- Good
    SELECT id, status, total, created_at
    FROM orders WHERE status = 'pending';
    ```

=== "JPA DTO Projection"

    ```java
    public interface OrderSummary {
        Long getId();
        String getStatus();
        BigDecimal getTotal();
    }

    @Query("SELECT o.id, o.status, o.total FROM Order o WHERE o.status = :status")
    List<OrderSummary> findSummaryByStatus(@Param("status") String status);
    ```

---

### Redundant Filter

| | |
|---|---|
| **Issue code** | `redundant-filter` |
| **Severity** | INFO |
| **Confidence** | Confirmed (100%) |

Detects duplicate predicates in WHERE clause.

```sql
-- Bad: redundant condition
SELECT * FROM users WHERE status = 'active' AND status = 'active';

-- Good: remove duplicate
SELECT * FROM users WHERE status = 'active';
```

---

### COUNT vs EXISTS

| | |
|---|---|
| **Issue code** | `count-instead-of-exists` |
| **Severity** | INFO |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

`COUNT(*)` must scan all matching rows, while `EXISTS` can stop after finding the first match.

=== "Bad"

    ```sql
    SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM orders WHERE user_id = ?;
    ```

=== "Good"

    ```sql
    SELECT EXISTS(SELECT 1 FROM orders WHERE user_id = ?);
    ```

=== "Spring Data JPA"

    ```java
    // Bad: counts all matching rows
    long count = orderRepository.countByUserId(userId);
    boolean exists = count > 0;

    // Good: stops at first match
    boolean exists = orderRepository.existsByUserId(userId);
    ```

---

### UNION Without ALL

| | |
|---|---|
| **Issue code** | `union-without-all` |
| **Severity** | INFO |
| **Confidence** | Confirmed (100%) |

`UNION` forces a sort to deduplicate results. If duplicates are acceptable or impossible,
use `UNION ALL`.

```sql
-- Bad: unnecessary deduplication sort
SELECT id FROM active_users UNION SELECT id FROM premium_users;

-- Good: skip dedup when not needed
SELECT id FROM active_users UNION ALL SELECT id FROM premium_users;
```

---

### COUNT(*) Without WHERE

| | |
|---|---|
| **Issue code** | `count-star-no-where` |
| **Severity** | INFO |
| **Confidence** | Confirmed (100%) |

`COUNT(*)` without WHERE scans the entire table. In InnoDB, this is always a full scan
(unlike MyISAM which stores the count).

```sql
-- Slow on large InnoDB tables
SELECT COUNT(*) FROM orders;

-- Consider: cached counter, approximate count, or filtered count
SELECT COUNT(*) FROM orders WHERE status = 'active';
```

---

### Excessive Column Fetch

| | |
|---|---|
| **Issue code** | `excessive-column-fetch` |
| **Severity** | INFO |
| **Confidence** | Confirmed (100%) |

Flags queries that fetch too many columns. Consider using DTO projection to select only
the columns you need.

```sql
-- Bad: fetching 30 columns when you only need 3
SELECT * FROM user_profiles WHERE user_id = ?;

-- Good: select only needed columns
SELECT display_name, avatar_url, bio FROM user_profiles WHERE user_id = ?;
```

---

### Mergeable Queries

| | |
|---|---|
| **Issue code** | `mergeable-queries` |
| **Severity** | INFO |
| **Confidence** | Confirmed (100%) |

Detects multiple simple queries to the same table that could be merged into one.

```java
// Bad: 3 separate queries
User user = userRepository.findById(1L);
User user2 = userRepository.findById(2L);
User user3 = userRepository.findById(3L);

// Good: single query
List<User> users = userRepository.findAllById(List.of(1L, 2L, 3L));
```

---

### Non-Deterministic Pagination

| | |
|---|---|
| **Issue code** | `non-deterministic-pagination` |
| **Severity** | INFO |
| **Confidence** | Confirmed (100%) |

`ORDER BY + LIMIT` on a non-unique column produces inconsistent pagination results.

```sql
-- Bad: created_at may have duplicates, page boundaries shift
SELECT * FROM products ORDER BY created_at LIMIT 20 OFFSET 40;

-- Good: add a tiebreaker column
SELECT * FROM products ORDER BY created_at, id LIMIT 20 OFFSET 40;
```

---

### Force Index Hint

| | |
|---|---|
| **Issue code** | `force-index-hint` |
| **Severity** | INFO |
| **Confidence** | Confirmed (100%) |

`FORCE INDEX`, `USE INDEX`, and `IGNORE INDEX` hints override the optimizer. These may become
stale as the schema evolves and should be reviewed periodically.

```sql
-- Flagged: optimizer hint may become stale
SELECT * FROM orders FORCE INDEX (idx_status) WHERE status = 'pending';

-- Better: let the optimizer decide, ensure proper indexes exist
SELECT * FROM orders WHERE status = 'pending';
```

---

### LIMIT Without ORDER BY

| | |
|---|---|
| **Issue code** | `limit-without-order-by` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

`LIMIT` without `ORDER BY` returns an arbitrary subset of rows. The result is non-deterministic
and may vary between executions.

```sql
-- Bad: which 10 rows?
SELECT * FROM users LIMIT 10;

-- Good: deterministic ordering
SELECT * FROM users ORDER BY id LIMIT 10;
```

---

### Window Function Without PARTITION BY

| | |
|---|---|
| **Issue code** | `window-no-partition` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

Window functions without `PARTITION BY` operate over the **entire result set**, which may
indicate a missing partition clause.

```sql
-- Flagged: ROW_NUMBER over entire table
SELECT id, ROW_NUMBER() OVER (ORDER BY created_at) as rn FROM orders;

-- If you intended per-customer numbering:
SELECT id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at) as rn
FROM orders;
```

---

## Summary Table

| Code | Severity | Category | Description |
|------|----------|----------|-------------|
| `where-function` | ERROR | Index | Function in WHERE disables index |
| `non-sargable` | ERROR | Index | Arithmetic on column prevents index |
| `null-comparison` | ERROR | Logic Bug | = NULL always returns UNKNOWN |
| `order-by-rand` | ERROR | Performance | Full table scan + sort |
| `not-in-subquery` | ERROR | Logic Bug | NOT IN fails with NULL subquery results |
| `cartesian-join` | ERROR | JOIN | Missing ON condition |
| `for-update-no-index` | ERROR | Locking | FOR UPDATE without index locks table |
| `implicit-type-conversion` | WARNING | Index | Type mismatch disables index |
| `or-abuse` | WARNING | Performance | Excessive OR prevents index usage |
| `offset-pagination` | WARNING | Performance | Large OFFSET reads and discards rows |
| `like-leading-wildcard` | WARNING | Index | Leading wildcard prevents index |
| `large-in-list` | WARNING | Performance | Too many IN values |
| `distinct-misuse` | WARNING | Performance | Unnecessary deduplication |
| `having-misuse` | WARNING | Performance | Non-aggregate in HAVING |
| `unbounded-result-set` | WARNING | Safety | No LIMIT on SELECT |
| `slow-query` | WARNING | Performance | Execution time exceeded threshold |
| `case-in-where` | WARNING | Index | CASE prevents index usage |
| `correlated-subquery` | WARNING | Performance | Per-row subquery execution |
| `too-many-joins` | WARNING | Performance | Excessive JOINs |
| `implicit-join` | WARNING | Readability | Comma-separated join syntax |
| `unused-join` | WARNING | Performance | Unreferenced LEFT JOIN |
| `for-update-non-unique` | WARNING | Locking | Gap locks on non-unique index |
| `range-lock-risk` | WARNING | Locking | Range + FOR UPDATE gap locks |
| `for-update-no-timeout` | WARNING | Locking | No NOWAIT/SKIP LOCKED |
| `string-concat-where` | WARNING | MySQL-Specific | Concatenation prevents index |
| `group-by-function` | WARNING | MySQL-Specific | Function prevents index grouping |
| `regexp-usage` | WARNING | MySQL-Specific | REGEXP prevents index usage |
| `find-in-set` | WARNING | MySQL-Specific | Comma-separated values violate 1NF |
| `limit-without-order-by` | WARNING | Query Structure | Non-deterministic LIMIT |
| `window-no-partition` | WARNING | Query Structure | Missing PARTITION BY |
| `select-all` | INFO | Best Practice | SELECT * usage |
| `redundant-filter` | INFO | Best Practice | Duplicate WHERE condition |
| `count-instead-of-exists` | INFO | Best Practice | COUNT where EXISTS suffices |
| `union-without-all` | INFO | Best Practice | Unnecessary dedup sort |
| `count-star-no-where` | INFO | Best Practice | Full table COUNT |
| `excessive-column-fetch` | INFO | Best Practice | Too many columns selected |
| `mergeable-queries` | INFO | Best Practice | Multiple queries could be one |
| `non-deterministic-pagination` | INFO | Best Practice | Non-unique ORDER BY + LIMIT |
| `force-index-hint` | INFO | Best Practice | Optimizer hint may become stale |

---

## Related Pages

- [Missing Index Detection](missing-index.md) -- Index-specific detectors
- [DML Anti-Patterns](dml-anti-patterns.md) -- INSERT/UPDATE/DELETE patterns
- [N+1 Query Detection](n-plus-one.md) -- Repeated query patterns
- [Detection Rules Overview](overview.md) -- Complete reference table
