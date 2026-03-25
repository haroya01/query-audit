# DML Anti-Pattern Detection

QueryAudit detects performance and safety issues in **INSERT, UPDATE, DELETE** statements.
These rules analyze SQL structure and repetition patterns, not `EXPLAIN` output, making them
100% reliable.

This page covers all 10 DML-related issue types organized by severity, including
Hibernate/ORM-specific patterns.

---

## ERROR Severity

---

### UPDATE/DELETE Without WHERE

| | |
|---|---|
| **Issue code** | `update-without-where` |
| **Severity** | ERROR |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

An `UPDATE` or `DELETE` without a `WHERE` clause affects **every row** in the table.
This is almost always unintentional and can cause catastrophic data loss.

In InnoDB, a WHERE-less DML statement also acquires **row locks on every row in the table**,
blocking all concurrent writes until the statement completes.

!!! danger "Data safety"
    MySQL provides `sql_safe_updates` as a built-in safety mechanism for exactly this reason.
    QueryAudit catches it at test time before it reaches production.

#### Detection

QueryAudit checks whether an UPDATE or DELETE statement contains a WHERE clause.
This is a pure syntax check.

#### Examples and Fixes

=== "Bad"

    ```sql
    -- Affects ALL rows in the table
    UPDATE users SET status = 'inactive';
    DELETE FROM sessions;
    ```

=== "Good"

    ```sql
    -- Targeted updates
    UPDATE users SET status = 'inactive' WHERE last_login < '2024-01-01';
    DELETE FROM sessions WHERE expired_at < NOW();

    -- If you genuinely intend to affect all rows:
    TRUNCATE TABLE sessions;  -- For DELETE all (faster, no row-level locks)
    ```

=== "Spring Data JPA"

    ```java
    // Bad: Spring Data's deleteAll() generates DELETE without WHERE
    sessionRepository.deleteAll();

    // Good: use @Modifying with WHERE clause
    @Modifying
    @Query("DELETE FROM Session s WHERE s.expiredAt < :cutoff")
    int deleteExpired(@Param("cutoff") LocalDateTime cutoff);

    // Good: for genuine truncation (raw SQL)
    @Modifying
    @Query(value = "TRUNCATE TABLE sessions", nativeQuery = true)
    void truncateSessions();
    ```

#### Report Output

```
[ERROR] UPDATE/DELETE without WHERE clause affects all rows
  Query  : update users set status = ?
  Table  : users
  Detail : UPDATE without WHERE clause will modify all rows in table 'users'
  Fix    : Add a WHERE clause to limit the affected rows
```

#### Configuration

No threshold. Suppress if intentional:

```java
@QueryAudit(suppress = {"update-without-where"})
```

---

## WARNING Severity

---

### DML Without Index on WHERE Columns

| | |
|---|---|
| **Issue code** | `dml-without-index` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

When an `UPDATE` or `DELETE` has a WHERE clause but the filtered columns have **no index**,
InnoDB must perform a full table scan. Unlike a SELECT full scan which is merely slow,
a **DML full scan acquires row locks on every scanned row**:

!!! quote "MySQL 8.0 Reference Manual"
    "A locking read, an UPDATE, or a DELETE generally set record locks on every index record
    that is scanned in the processing of an SQL statement."

    "If you have no indexes suitable for your statement and MySQL must scan the entire table
    to process the statement, every row of the table becomes locked, which in turn blocks all
    inserts by other users to the table."

This means a single slow UPDATE can lock the entire table and cause cascading timeouts.

#### Detection

QueryAudit extracts WHERE columns from UPDATE/DELETE statements and cross-references
them against the actual index metadata (via `SHOW INDEX`). If none of the WHERE columns
match the leading column of any index, the issue is flagged.

#### Examples and Fixes

=== "Bad"

    ```sql
    -- 'status' column has no index -> full table scan with row locks
    UPDATE orders SET processed = true WHERE status = 'pending';
    ```

=== "Good"

    ```sql
    -- Add an index first
    ALTER TABLE orders ADD INDEX idx_status (status);

    -- Now the UPDATE uses the index, locking only matching rows
    UPDATE orders SET processed = true WHERE status = 'pending';
    ```

=== "Already safe"

    ```sql
    -- 'id' is the primary key -> no issue
    DELETE FROM orders WHERE id = 12345;
    ```

=== "Batch with LIMIT"

    When updating large numbers of rows, combine with LIMIT to reduce lock contention:

    ```sql
    -- Process in chunks to avoid long-held locks
    UPDATE orders SET processed = true
    WHERE status = 'pending'
    ORDER BY id
    LIMIT 1000;
    -- Repeat until 0 rows affected
    ```

#### Report Output

```
[WARNING] UPDATE/DELETE WHERE column has no index causes full table scan
  Query  : update orders set processed = ? where status = ?
  Table  : orders
  Column : status
  Detail : UPDATE on 'orders' WHERE columns [status] have no matching index.
           This causes a full table scan with row locks.
  Fix    : Add an index on the WHERE columns to avoid full table scan.
```

---

### Repeated Single-Row INSERT

| | |
|---|---|
| **Issue code** | `repeated-single-insert` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |
| **Default threshold** | 3 identical INSERT patterns |

#### Why It Matters

Each single-row INSERT incurs fixed overhead that dominates the actual row insertion time:

| Operation | Relative Cost |
|-----------|--------------|
| Connecting | 3 |
| Sending query to server | 2 |
| Parsing query | 2 |
| **Inserting row** | **1 x row size** |
| Inserting indexes | 1 x number of indexes |
| Closing | 1 |

The overhead is **~8x** the cost of the actual row insertion. Additionally, each single INSERT
with autocommit triggers a **separate redo log flush**.

#### Detection

QueryAudit normalizes INSERT statements and groups them by pattern. If the same INSERT
pattern (same table, same columns) appears 3 or more times in a single test, it is flagged.
Multi-row INSERT statements (`VALUES (...), (...)`) are excluded.

#### Examples and Fixes

=== "Bad: loop of individual saves"

    ```java
    // 100 individual inserts
    for (User user : users) {
        userRepository.save(user);  // INSERT INTO users (...) VALUES (?)  x100
    }
    ```

=== "Good: JPA batch insert"

    ```yaml
    # application.yml
    spring:
      jpa:
        properties:
          hibernate:
            jdbc:
              batch_size: 50
            order_inserts: true
    ```

    ```java
    // Uses saveAll() instead of individual save() calls
    userRepository.saveAll(users);
    ```

    !!! warning "Identity generation strategy"
        JPA batch inserts do **not** work with `@GeneratedValue(strategy = GenerationType.IDENTITY)`
        because Hibernate needs to execute each INSERT individually to obtain the generated ID.
        Use `SEQUENCE` or `TABLE` strategy for batch support:

        ```java
        @Id
        @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "user_seq")
        @SequenceGenerator(name = "user_seq", sequenceName = "user_seq", allocationSize = 50)
        private Long id;
        ```

=== "Good: JDBC batch"

    ```java
    try (PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO users (name, email) VALUES (?, ?)")) {
        for (User user : users) {
            ps.setString(1, user.getName());
            ps.setString(2, user.getEmail());
            ps.addBatch();
        }
        ps.executeBatch();
    }
    ```

=== "Good: MySQL Connector/J optimization"

    ```
    jdbc:mysql://localhost:3306/mydb?rewriteBatchedStatements=true
    ```

    This rewrites JDBC batches into multi-value INSERT syntax on the wire,
    providing an additional 5-10x speedup over standard JDBC batching.

=== "Good: multi-row VALUES"

    ```sql
    -- Single statement with multiple value sets
    INSERT INTO users (name, email) VALUES
      ('Alice', 'alice@example.com'),
      ('Bob', 'bob@example.com'),
      ('Charlie', 'charlie@example.com');
    ```

#### Report Output

```
[WARNING] Repeated single-row INSERT should use batch insert
  Query  : insert into users (...) values (?, ?, ?)
  Table  : users
  Detail : Single-row INSERT executed 100 times on table 'users'.
           Each INSERT causes a separate network round-trip and log flush.
  Fix    : Use batch INSERT (addBatch/executeBatch in JDBC, or saveAll() in JPA
           with spring.jpa.properties.hibernate.jdbc.batch_size).
```

#### Configuration

```yaml
query-audit:
  repeated-insert:
    threshold: 3   # default
```

---

### INSERT ... SELECT *

| | |
|---|---|
| **Issue code** | `insert-select-all` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

`INSERT INTO ... SELECT *` relies on **column position matching** between source and target
tables. This creates two problems:

1. **Schema fragility** -- adding, removing, or reordering columns silently changes the
   data being inserted
2. **Unnecessary data transfer** -- `SELECT *` fetches all columns even if the target only
   needs a subset

#### Examples and Fixes

=== "Bad"

    ```sql
    INSERT INTO orders_archive SELECT * FROM orders WHERE created_at < '2023-01-01';
    ```

=== "Good"

    ```sql
    INSERT INTO orders_archive (id, customer_id, total, status, created_at)
    SELECT id, customer_id, total, status, created_at
    FROM orders
    WHERE created_at < '2023-01-01';
    ```

---

### INSERT ON DUPLICATE KEY UPDATE

| | |
|---|---|
| **Issue code** | `insert-on-duplicate-key` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

`INSERT ... ON DUPLICATE KEY UPDATE` and `REPLACE INTO` can cause **deadlocks** under concurrent
execution due to gap lock interactions on unique keys (MySQL-specific).

#### Detection

QueryAudit detects both `INSERT ... ON DUPLICATE KEY UPDATE` and `REPLACE INTO` patterns.

#### Examples and Fixes

=== "Bad"

    ```sql
    -- May deadlock under concurrent execution
    INSERT INTO user_settings (user_id, theme, lang)
    VALUES (123, 'dark', 'en')
    ON DUPLICATE KEY UPDATE theme = 'dark', lang = 'en';
    ```

=== "Good: SELECT FOR UPDATE first"

    ```sql
    -- Check first, then INSERT or UPDATE
    START TRANSACTION;
    SELECT * FROM user_settings WHERE user_id = 123 FOR UPDATE;
    -- If exists: UPDATE; else: INSERT
    COMMIT;
    ```

=== "Good: application-level upsert"

    ```java
    @Transactional
    public void upsertSettings(long userId, String theme, String lang) {
        UserSettings existing = settingsRepository.findByUserId(userId);
        if (existing != null) {
            existing.setTheme(theme);
            existing.setLang(lang);
        } else {
            settingsRepository.save(new UserSettings(userId, theme, lang));
        }
    }
    ```

=== "Good: PostgreSQL ON CONFLICT"

    PostgreSQL's `ON CONFLICT` has better locking behavior than MySQL's equivalent:

    ```sql
    INSERT INTO user_settings (user_id, theme, lang)
    VALUES (123, 'dark', 'en')
    ON CONFLICT (user_id)
    DO UPDATE SET theme = EXCLUDED.theme, lang = EXCLUDED.lang;
    ```

---

### Subquery in DML

| | |
|---|---|
| **Issue code** | `subquery-in-dml` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

MySQL cannot use semijoin or materialization optimizations for subqueries in UPDATE/DELETE
statements. This is a documented MySQL limitation.

!!! quote "MySQL Documentation"
    "A limitation on UPDATE and DELETE statements that use a subquery to modify a single table
    is that the optimizer does not use semijoin or materialization subquery optimizations."

#### Examples and Fixes

=== "Bad"

    ```sql
    -- Cannot use semijoin optimization
    UPDATE orders SET status = 'cancelled'
    WHERE customer_id IN (SELECT id FROM customers WHERE region = 'inactive');

    DELETE FROM order_items
    WHERE order_id IN (SELECT id FROM orders WHERE status = 'expired');
    ```

=== "Good: multi-table UPDATE/DELETE with JOIN"

    ```sql
    -- MySQL multi-table UPDATE
    UPDATE orders o
    JOIN customers c ON o.customer_id = c.id
    SET o.status = 'cancelled'
    WHERE c.region = 'inactive';

    -- MySQL multi-table DELETE
    DELETE oi FROM order_items oi
    JOIN orders o ON oi.order_id = o.id
    WHERE o.status = 'expired';
    ```

=== "Good: PostgreSQL-style"

    ```sql
    -- PostgreSQL supports UPDATE ... FROM
    UPDATE orders
    SET status = 'cancelled'
    FROM customers
    WHERE orders.customer_id = customers.id
      AND customers.region = 'inactive';

    -- PostgreSQL DELETE ... USING
    DELETE FROM order_items
    USING orders
    WHERE order_items.order_id = orders.id
      AND orders.status = 'expired';
    ```

---

### Implicit Columns INSERT

| | |
|---|---|
| **Issue code** | `implicit-columns-insert` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

`INSERT INTO table VALUES (...)` without specifying column names is fragile. Adding, removing,
or reordering columns in the table silently breaks the INSERT statement.

#### Examples and Fixes

=== "Bad"

    ```sql
    -- Breaks if columns are added or reordered
    INSERT INTO users VALUES (1, 'John', 'john@example.com');
    ```

=== "Good"

    ```sql
    -- Explicit columns -- safe against schema changes
    INSERT INTO users (id, name, email) VALUES (1, 'John', 'john@example.com');
    ```

=== "JPA entity mapping"

    JPA always generates INSERT with explicit column lists, so this issue typically appears
    only with native queries:

    ```java
    // Bad: native query without column list
    @Query(value = "INSERT INTO audit_log VALUES (?, ?, ?)", nativeQuery = true)
    void insertLog(Long id, String action, String detail);

    // Good: explicit column list
    @Query(value = "INSERT INTO audit_log (id, action, detail) VALUES (?, ?, ?)",
           nativeQuery = true)
    void insertLog(Long id, String action, String detail);
    ```

---

## INFO Severity

---

### INSERT...SELECT Locks Source

| | |
|---|---|
| **Issue code** | `insert-select-locks-source` |
| **Severity** | INFO |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

`INSERT INTO ... SELECT FROM source_table` acquires shared locks on the rows read from the
source table, potentially blocking concurrent writes to that table.

#### Examples and Fixes

=== "Bad"

    ```sql
    -- Locks rows in 'orders' while inserting into 'archive'
    INSERT INTO orders_archive (id, customer_id, total)
    SELECT id, customer_id, total FROM orders WHERE created_at < '2023-01-01';
    ```

=== "Good: batch with explicit locking control"

    ```java
    // Process in smaller batches to reduce lock duration
    @Transactional
    public void archiveOrders(LocalDate cutoff) {
        List<Long> ids = orderRepository.findIdsBeforeDate(cutoff, PageRequest.of(0, 1000));
        while (!ids.isEmpty()) {
            orderRepository.archiveBatch(ids);
            orderRepository.deleteBatch(ids);
            ids = orderRepository.findIdsBeforeDate(cutoff, PageRequest.of(0, 1000));
        }
    }
    ```

=== "Good: read committed isolation"

    ```sql
    -- Reduce locking by using READ COMMITTED
    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
    START TRANSACTION;
    INSERT INTO orders_archive (id, customer_id, total)
    SELECT id, customer_id, total FROM orders WHERE created_at < '2023-01-01';
    COMMIT;
    ```

---

## Hibernate / ORM Specific DML Patterns

These rules detect Hibernate-specific DML patterns that indicate inefficient entity management.

---

### Collection Delete + Reinsert

| | |
|---|---|
| **Issue code** | `collection-delete-reinsert` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

When Hibernate manages a `@OneToMany` collection without proper ordering, it may DELETE all
child rows and re-INSERT them whenever the collection is modified. This is extremely wasteful.

#### Detection

QueryAudit detects a DELETE followed by re-INSERT sequence on the same table within a single
test execution.

#### Examples and Fixes

=== "Bad: List without @OrderColumn"

    ```java
    @Entity
    public class Post {
        @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true)
        private List<Comment> comments = new ArrayList<>();  // No @OrderColumn!
    }

    // Adding one comment causes: DELETE all comments + re-INSERT all comments
    post.getComments().add(newComment);
    ```

    Generated SQL:

    ```sql
    DELETE FROM post_comments WHERE post_id = 1;
    INSERT INTO post_comments (post_id, comment_id) VALUES (1, 10);
    INSERT INTO post_comments (post_id, comment_id) VALUES (1, 11);
    INSERT INTO post_comments (post_id, comment_id) VALUES (1, 12);
    -- ... re-inserts ALL comments, not just the new one
    ```

=== "Good: Add @OrderColumn"

    ```java
    @Entity
    public class Post {
        @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true)
        @OrderColumn(name = "position")
        private List<Comment> comments = new ArrayList<>();
    }
    ```

=== "Good: Use Set instead of List"

    ```java
    @Entity
    public class Post {
        @OneToMany(cascade = CascadeType.ALL, orphanRemoval = true, mappedBy = "post")
        private Set<Comment> comments = new HashSet<>();
    }
    ```

    !!! tip "Set is generally preferred"
        Using `Set` instead of `List` for `@OneToMany` avoids the delete-reinsert problem
        entirely and more accurately models the relationship (child order rarely matters
        at the database level).

=== "Good: Bidirectional mapping"

    ```java
    @Entity
    public class Post {
        @OneToMany(mappedBy = "post", cascade = CascadeType.ALL, orphanRemoval = true)
        private List<Comment> comments = new ArrayList<>();

        public void addComment(Comment comment) {
            comments.add(comment);
            comment.setPost(this);  // Maintain both sides
        }
    }

    @Entity
    public class Comment {
        @ManyToOne(fetch = FetchType.LAZY)
        private Post post;
    }
    ```

---

### Derived Delete Loads Entities

| | |
|---|---|
| **Issue code** | `derived-delete-loads-entities` |
| **Severity** | WARNING |
| **Confidence** | Confirmed (100%) |

#### Why It Matters

Spring Data's derived delete methods (e.g., `deleteByStatus(...)`) first SELECT all matching
entities, then issue individual DELETE statements for each. This is the N+1 pattern applied
to deletes.

#### Detection

QueryAudit detects a SELECT followed by individual DELETE statements on the same table.

#### Examples and Fixes

=== "Bad: derived delete"

    ```java
    // Generates: SELECT * FROM orders WHERE status = ?
    // Then: DELETE FROM orders WHERE id = ?  (x N times)
    orderRepository.deleteByStatus("expired");
    ```

    Generated SQL:

    ```sql
    SELECT o.id, o.status, o.total, o.customer_id, o.created_at
    FROM orders o WHERE o.status = 'expired';

    DELETE FROM orders WHERE id = 101;
    DELETE FROM orders WHERE id = 102;
    DELETE FROM orders WHERE id = 103;
    -- ... one DELETE per matched row
    ```

=== "Good: @Modifying query"

    ```java
    @Modifying
    @Query("DELETE FROM Order o WHERE o.status = :status")
    int deleteAllByStatus(@Param("status") String status);
    ```

    Generated SQL:

    ```sql
    DELETE FROM orders WHERE status = 'expired';
    -- Single statement, no SELECT needed
    ```

=== "Good: JPQL bulk delete"

    ```java
    @Transactional
    public int bulkDelete(String status) {
        return entityManager.createQuery("DELETE FROM Order o WHERE o.status = :status")
            .setParameter("status", status)
            .executeUpdate();
    }
    ```

!!! warning "Cascade and lifecycle callbacks"
    Bulk delete via `@Modifying` or JPQL **bypasses** Hibernate lifecycle callbacks
    (`@PreRemove`, `@PostRemove`) and cascade operations. If you need these, consider
    using `deleteAllInBatch()` or loading entities selectively.

---

## Summary Table

| Code | Severity | Category | Description |
|------|----------|----------|-------------|
| `update-without-where` | ERROR | Safety | UPDATE/DELETE without WHERE affects all rows |
| `dml-without-index` | WARNING | Performance | DML WHERE column has no index (full table lock) |
| `repeated-single-insert` | WARNING | Performance | Repeated single-row INSERT should batch |
| `insert-select-all` | WARNING | Safety | INSERT with SELECT * is fragile |
| `insert-on-duplicate-key` | WARNING | Concurrency | ON DUPLICATE KEY may cause deadlocks |
| `subquery-in-dml` | WARNING | Performance | Subquery in DML can't use semijoin |
| `implicit-columns-insert` | WARNING | Safety | INSERT without column list is fragile |
| `insert-select-locks-source` | INFO | Concurrency | INSERT...SELECT locks source rows |
| `collection-delete-reinsert` | WARNING | Hibernate | DELETE-all + re-INSERT pattern |
| `derived-delete-loads-entities` | WARNING | Hibernate | Derived delete loads entities first |

---

## Related Pages

- [Missing Index Detection](missing-index.md) -- Index detection for SELECT queries
- [SQL Anti-Patterns](sql-anti-patterns.md) -- SQL query anti-patterns
- [N+1 Query Detection](n-plus-one.md) -- Repeated query patterns
- [Detection Rules Overview](overview.md) -- Complete reference table
