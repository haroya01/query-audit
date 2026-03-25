# N+1 Query Detection

| | |
|---|---|
| **Issue code** | `n-plus-one` |
| **Severity** | ERROR |
| **Confidence** | Confirmed (100%) |
| **Default threshold** | 3 repetitions |

## What Is the N+1 Problem?

The N+1 problem occurs when your application executes **1 query** to fetch a list of entities,
then **N additional queries** -- one per entity -- to load a related association.

```
1 query:  SELECT * FROM orders                          -- fetch all orders
N queries: SELECT * FROM members WHERE id = ?            -- one per order (lazy load)
           SELECT * FROM members WHERE id = ?
           SELECT * FROM members WHERE id = ?
           ...
```

With 100 orders this becomes 101 queries. With 10,000 orders it becomes 10,001. The problem
scales linearly and is one of the most common causes of slow application performance.

```
             Application                          Database
                 |                                    |
                 |--- SELECT * FROM orders ---------> |
                 |<-- 100 rows ---------------------- |
                 |                                    |
                 |--- SELECT * FROM members (id=1) -> |  \
                 |<-- 1 row ------------------------- |   |
                 |--- SELECT * FROM members (id=2) -> |   |  N times
                 |<-- 1 row ------------------------- |   |  (one per order)
                 |--- SELECT * FROM members (id=3) -> |   |
                 |<-- 1 row ------------------------- |  /
                 |           ...                      |
```

---

## How Query Guard Detects It

The detection algorithm is purely **pattern-based** and does not rely on `EXPLAIN`, making it
100% reliable regardless of test data size.

### Step-by-Step

1. **Normalize SQL** -- Replace all literal values with `?` placeholders

    ```
    SELECT * FROM members WHERE id = 42   --> SELECT * FROM members WHERE id = ?
    SELECT * FROM members WHERE id = 77   --> SELECT * FROM members WHERE id = ?
    SELECT * FROM members WHERE id = 103  --> SELECT * FROM members WHERE id = ?
    ```

2. **Group by normalized pattern** -- Identical normalized queries are grouped together

3. **Count** -- If the same pattern appears **>= threshold** times (default: 3), it is flagged
   as N+1

```java title="NPlusOneDetector.java (simplified)"
Map<String, List<QueryRecord>> grouped = new LinkedHashMap<>();
for (QueryRecord query : queries) {
    grouped.computeIfAbsent(query.normalizedSql(), k -> new ArrayList<>()).add(query);
}

for (var entry : grouped.entrySet()) {
    if (entry.getValue().size() >= threshold) {
        // --> N+1 detected
    }
}
```

---

## How to Diagnose

When Query Guard reports an N+1 issue, follow these steps to find and fix the root cause:

### Step 1: Identify the Repeated Query

The report tells you the **normalized SQL pattern** and the **table** being hit repeatedly:

```
[ERROR] N+1 Query detected
  Pattern : SELECT * FROM members WHERE id = ?
  Table   : members
  Detail  : Executed 100 times
```

### Step 2: Find the Triggering Code

Look for code that:

- Iterates over a collection of entities
- Accesses a lazy-loaded association inside the loop
- Calls a repository method inside a loop

!!! tip "Stack trace"
    Query Guard captures the stack trace at query execution time. Check the report output
    for the originating line of code -- it typically points to the getter that triggers the
    lazy load.

### Step 3: Check the Entity Mapping

```java
@Entity
public class Order {
    @ManyToOne(fetch = FetchType.LAZY)  // <-- lazy loading = N+1 risk
    private Member member;
}
```

### Step 4: Check for Common Code Patterns

N+1 issues often hide in non-obvious places. Look for these patterns:

=== "Service layer loop"

    ```java
    // The classic: accessing a lazy relation in a loop
    for (Order order : orderRepository.findAll()) {
        order.getMember().getName();  // <-- triggers lazy load
    }
    ```

=== "Stream / map operations"

    ```java
    // Same problem but harder to spot in functional style
    List<String> names = orders.stream()
        .map(o -> o.getMember().getName())  // <-- lazy load per element
        .collect(Collectors.toList());
    ```

=== "Repository call in a loop"

    ```java
    // Not an ORM issue -- explicit queries in a loop
    for (Long memberId : memberIds) {
        Member m = memberRepository.findById(memberId).orElseThrow();
        results.add(m);
    }
    ```

=== "Template / view layer"

    ```html
    <!-- Thymeleaf / JSP: lazy load triggered during rendering -->
    <tr th:each="order : ${orders}">
        <td th:text="${order.member.name}"/>  <!-- N+1 here -->
    </tr>
    ```

### Step 5: Choose a Fix Strategy

See the [How to Fix](#how-to-fix) section below. The right strategy depends on your use case.

---

## Real-World Examples

### Example 1: Basic JPA Lazy Loading

The most common N+1 scenario -- iterating over entities and accessing a lazy association.

=== "Problem Code"

    ```java
    // 1 query: SELECT * FROM orders
    List<Order> orders = orderRepository.findAll();

    for (Order order : orders) {
        // N queries: SELECT * FROM members WHERE id = ? (lazy loading)
        String memberName = order.getMember().getName();
        log.info("Order {} by {}", order.getId(), memberName);
    }
    ```

=== "Generated SQL"

    ```sql
    -- 1st query
    SELECT o.id, o.status, o.member_id, o.total FROM orders o;

    -- N lazy-load queries (one per order)
    SELECT m.id, m.name, m.email FROM members m WHERE m.id = 1;
    SELECT m.id, m.name, m.email FROM members m WHERE m.id = 2;
    SELECT m.id, m.name, m.email FROM members m WHERE m.id = 3;
    -- ... repeated for every order
    ```

### Example 2: Nested N+1 (Order -> Member -> Address)

A particularly severe case where N+1 is nested, causing N x M queries.

=== "Problem Code"

    ```java
    List<Order> orders = orderRepository.findAll();  // 1 query

    for (Order order : orders) {
        Member member = order.getMember();           // N queries
        Address address = member.getAddress();       // N more queries (nested!)
        log.info("{} lives at {}", member.getName(), address.getCity());
    }
    ```

=== "Query Count"

    ```
    With 100 orders:
      1  (orders)
    + 100 (members)
    + 100 (addresses)
    = 201 queries total
    ```

=== "Fix with @EntityGraph"

    ```java
    @EntityGraph(attributePaths = {"member", "member.address"})
    @Query("SELECT o FROM Order o")
    List<Order> findAllWithMemberAndAddress();
    ```

    Reduces 201 queries to **1 query**.

### Example 3: Collection Mapping (@OneToMany)

```java
@Entity
public class Department {
    @OneToMany(mappedBy = "department", fetch = FetchType.LAZY)
    private List<Employee> employees;
}
```

=== "Problem Code"

    ```java
    List<Department> departments = departmentRepository.findAll();

    for (Department dept : departments) {
        // Each call triggers: SELECT * FROM employees WHERE department_id = ?
        int headcount = dept.getEmployees().size();
    }
    ```

=== "Fix with @EntityGraph"

    ```java
    @EntityGraph(attributePaths = {"employees"})
    @Query("SELECT d FROM Department d")
    List<Department> findAllWithEmployees();
    ```

=== "Fix with JOIN FETCH"

    ```java
    @Query("SELECT DISTINCT d FROM Department d JOIN FETCH d.employees")
    List<Department> findAllWithEmployees();
    ```

    !!! tip "Use DISTINCT with JOIN FETCH on collections"
        Without `DISTINCT`, a `JOIN FETCH` on a `@OneToMany` produces duplicate parent
        entities (one per child row). Hibernate 6+ handles this automatically, but Hibernate 5
        requires explicit `DISTINCT`.

### Example 4: MyBatis Nested Select

=== "Problem Mapper (XML)"

    ```xml
    <!-- OrderMapper.xml -->
    <resultMap id="orderWithMember" type="Order">
        <id property="id" column="id"/>
        <!-- This triggers a separate SELECT for each order -->
        <association property="member" column="member_id"
                     select="com.example.mapper.MemberMapper.selectById"/>
    </resultMap>

    <select id="selectAll" resultMap="orderWithMember">
        SELECT * FROM orders
    </select>
    ```

=== "Fix: Use JOIN in SQL"

    ```xml
    <resultMap id="orderWithMember" type="Order">
        <id property="id" column="id"/>
        <association property="member" javaType="Member">
            <id property="id" column="member_id"/>
            <result property="name" column="member_name"/>
        </association>
    </resultMap>

    <select id="selectAllWithMember" resultMap="orderWithMember">
        SELECT o.*, m.name as member_name
        FROM orders o
        JOIN members m ON o.member_id = m.id
    </select>
    ```

### Example 5: Spring Data REST / JSON Serialization

A hidden N+1 that occurs during JSON serialization:

```java
@RestController
public class OrderController {
    @GetMapping("/orders")
    public List<Order> getOrders() {
        // The N+1 happens during Jackson serialization, not here!
        return orderRepository.findAll();
    }
}
```

Jackson calls `getMember()` on each `Order` during serialization, triggering lazy loads.

!!! danger "This is hard to spot"
    The N+1 does not appear in your controller code. It happens inside the JSON serializer.
    Query Guard catches it because it monitors all queries regardless of where they originate.

**Fix:** Use a DTO projection or `@EntityGraph` in the repository method:

```java
@EntityGraph(attributePaths = {"member"})
List<Order> findAll();
```

### Example 6: Spring Data JPA Derived Query

Spring Data derived queries do not support `JOIN FETCH`. This is a common trap:

=== "Problem Code"

    ```java
    // Derived query: generates SELECT * FROM orders WHERE status = ?
    // No way to add JOIN FETCH via method naming
    List<Order> orders = orderRepository.findByStatus("pending");

    for (Order order : orders) {
        order.getMember().getName();  // N+1
    }
    ```

=== "Fix: @EntityGraph on derived query"

    ```java
    @EntityGraph(attributePaths = {"member"})
    List<Order> findByStatus(String status);
    ```

=== "Fix: @Query with JOIN FETCH"

    ```java
    @Query("SELECT o FROM Order o JOIN FETCH o.member WHERE o.status = :status")
    List<Order> findByStatusWithMember(@Param("status") String status);
    ```

### Example 7: Hibernate Second-Level Cache Miss

When using second-level cache, N+1 can resurface after a cache eviction or restart:

```java
@Entity
@Cacheable
@org.hibernate.annotations.Cache(usage = CacheConcurrencyStrategy.READ_WRITE)
public class Member {
    // ...
}
```

```java
// Works fine when cache is warm (no SQL at all for members)
// After cache eviction or restart: full N+1 occurs
for (Order order : orders) {
    order.getMember().getName();  // Cache miss = SQL query
}
```

!!! warning "Cache is not a fix for N+1"
    Cache can mask N+1 problems. The issue returns after cache eviction, restart, or when
    the data changes. Always fix the underlying N+1 with a proper fetch strategy.

---

## How to Fix

=== "JOIN FETCH (JPQL)"

    ```java
    @Query("SELECT o FROM Order o JOIN FETCH o.member")
    List<Order> findAllWithMember();
    ```

    Produces a single query with a JOIN -- eliminates all N extra queries.

    !!! warning "Pagination limitation"
        `JOIN FETCH` with `@OneToMany` collections and `Pageable` causes Hibernate to fetch
        **all results in memory** and paginate in the application (HHH000104 warning).
        Use `@EntityGraph` or `@BatchSize` instead for paginated queries.

=== "@EntityGraph"

    ```java
    @EntityGraph(attributePaths = {"member"})
    @Query("SELECT o FROM Order o")
    List<Order> findAllWithMember();
    ```

    Tells JPA to eagerly fetch the `member` association in the same query.

    For nested associations:

    ```java
    @EntityGraph(attributePaths = {"member", "member.address"})
    @Query("SELECT o FROM Order o")
    List<Order> findAllWithMemberAndAddress();
    ```

    For named entity graphs defined on the entity:

    ```java
    @NamedEntityGraph(
        name = "Order.withMember",
        attributeNodes = @NamedAttributeNode("member")
    )
    @Entity
    public class Order { ... }

    // In repository:
    @EntityGraph("Order.withMember")
    List<Order> findAll();
    ```

=== "@BatchSize"

    ```java
    @Entity
    public class Order {
        @ManyToOne(fetch = FetchType.LAZY)
        @BatchSize(size = 100)
        private Member member;
    }
    ```

    Instead of N queries, Hibernate batches them into `ceil(N / batchSize)` queries using
    `WHERE id IN (?, ?, ?, ...)`. Not a single query, but a dramatic reduction.

    ```
    Before: 100 queries  (SELECT ... WHERE id = ?)
    After:  2 queries    (SELECT ... WHERE id IN (?, ?, ..., ?))  -- batch of 50
    ```

    !!! tip "Global default batch size"
        Set a global default in `application.yml` to protect against all lazy-load N+1:

        ```yaml
        spring:
          jpa:
            properties:
              hibernate:
                default_batch_fetch_size: 100
        ```

=== "Subselect Fetch"

    ```java
    @Entity
    public class Order {
        @ManyToOne(fetch = FetchType.LAZY)
        @Fetch(FetchMode.SUBSELECT)
        private Member member;
    }
    ```

    Hibernate loads all related entities in a single subselect query:

    ```sql
    SELECT m.* FROM members m
    WHERE m.id IN (SELECT o.member_id FROM orders o)
    ```

=== "DTO Projection"

    ```java
    public record OrderSummary(Long orderId, String memberName, BigDecimal total) {}

    @Query("""
        SELECT new com.example.dto.OrderSummary(o.id, m.name, o.total)
        FROM Order o JOIN o.member m
        """)
    List<OrderSummary> findOrderSummaries();
    ```

    The most efficient approach -- fetches only the columns you need in a single query,
    with no entity management overhead.

### Fix Strategy Decision Guide

```
Do you need to modify the entities?
  |
  +-- YES --> Use JOIN FETCH or @EntityGraph
  |             |
  |             +-- Is it a @OneToMany? --> Use @EntityGraph (avoids pagination issues)
  |             +-- Is it a @ManyToOne? --> Either JOIN FETCH or @EntityGraph
  |
  +-- NO (read-only) --> Use DTO Projection (most efficient)

Need a safety net for all lazy loads?
  +-- Use @BatchSize or hibernate.default_batch_fetch_size
```

### Before/After Comparison

```
+---------------------------------------------------------------------+
|  BEFORE (N+1)                    |  AFTER (JOIN FETCH)              |
|                                  |                                  |
|  SELECT * FROM orders;           |  SELECT o.*, m.*                 |
|  SELECT * FROM members           |  FROM orders o                   |
|    WHERE id = 1;                 |  JOIN members m                  |
|  SELECT * FROM members           |    ON o.member_id = m.id;        |
|    WHERE id = 2;                 |                                  |
|  SELECT * FROM members           |  -- 1 query total                |
|    WHERE id = 3;                 |  -- All data in one round-trip   |
|  ... (97 more)                   |                                  |
|                                  |                                  |
|  101 queries total               |                                  |
|  101 network round-trips         |                                  |
+---------------------------------------------------------------------+
```

---

## Query Guard Report Output

```
============================================================
 QUERY GUARD REPORT
============================================================

 [ERROR] N+1 Query detected
   Pattern : SELECT * FROM members WHERE id = ?
   Table   : members
   Detail  : Executed 100 times
   Fix     : Use JOIN FETCH or @EntityGraph
------------------------------------------------------------
```

---

## Configuration

### Threshold

The minimum number of repeated patterns to trigger an N+1 detection.

=== "application.yml"

    ```yaml
    query-audit:
      n-plus-one:
        threshold: 3   # default
    ```

=== "Programmatic"

    ```java
    QueryAuditConfig config = QueryAuditConfig.builder()
        .nPlusOneThreshold(5)
        .build();
    ```

!!! tip "Choosing a threshold"
    The default of **3** catches most real N+1 scenarios while avoiding false positives from
    queries that legitimately run twice (e.g., cache warm-up). Increase to **5** if you have
    many intentionally repeated queries.

### Suppressing

If a detected N+1 is intentional (e.g., a batch processor that deliberately issues per-row
queries), suppress it:

=== "Annotation"

    ```java
    @QueryAudit(suppress = {"n-plus-one"})
    @Test
    void batchProcessorTest() {
        // N+1 issues will not cause test failure
    }
    ```

=== "application.yml"

    ```yaml
    query-audit:
      suppress-patterns:
        - "n-plus-one"
    ```

=== "Suppress specific table"

    ```yaml
    query-audit:
      suppress-patterns:
        - "n-plus-one:members"
    ```

---

## Related Rules

- [`duplicate-query`](overview.md#disabled-rules-1-entry) -- Detects identical queries (currently disabled)
- [`repeated-single-insert`](dml-anti-patterns.md#repeated-single-row-insert) -- Similar pattern for INSERT statements
- [`mergeable-queries`](overview.md) -- Multiple queries to same table that could be combined
