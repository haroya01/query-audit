---
title: N+1 쿼리 탐지
source_digest: 9c350f830d29
---

# N+1 쿼리 탐지 {#n1-query-detection}

조회 테스트가 쿼리 예산을 넘으면, 반복된 SQL을 확인하고 애플리케이션이 읽는 연관관계를 실제로 실행해
보세요. `@EnableQueryInspector`를 쓰면 캡처된 문장과 Hibernate 이벤트를 검토하는 동안 탐지 결과는 참고용으로만
남아요. 고친 접근 방식은 예산이나 검토를 거친 N+1 탐지 정책으로 테스트에 고정하세요.
[작업 흐름 가이드](../guide/choose-your-workflow.md)를 참고하세요.

| | |
|---|---|
| **이슈 코드** | `n-plus-one` |
| **심각도** | ERROR |
| **기본 기준값** | 3회 반복 |

## N+1 문제란? {#what-is-the-n1-problem}

N+1 문제는 애플리케이션이 엔티티 목록을 가져오는 **쿼리 1개**를 실행한 뒤, 연관관계를 불러오려고 엔티티마다
**쿼리 N개**를 더 실행할 때 생겨요.

```
1 query:  SELECT * FROM orders                          -- fetch all orders
N queries: SELECT * FROM members WHERE id = ?            -- one per order (lazy load)
           SELECT * FROM members WHERE id = ?
           SELECT * FROM members WHERE id = ?
           ...
```

주문이 100개면 쿼리가 101개, 10,000개면 10,001개가 돼요. 데이터에 비례해서 늘어나고, 애플리케이션이
느려지는 가장 흔한 원인 중 하나예요.

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

## QueryAudit이 찾는 방식 {#how-queryaudit-detects-it}

0.7.0부터 확정된 `n-plus-one` 탐지 결과는 JDBC 증거로만 판단하고, 기본 `recommended` 프로필에서 실행되는
유일한 내장 규칙이에요. `EXPLAIN`도 Hibernate도 필요 없어요.

1. **SQL 정규화** -- 리터럴 값을 `?` 자리표시자로 바꿔요.

    ```
    SELECT * FROM members WHERE id = 42   --> SELECT * FROM members WHERE id = ?
    SELECT * FROM members WHERE id = 77   --> SELECT * FROM members WHERE id = ?
    ```

2. **호출 위치로 묶기** -- 정규화된 SQL과 애플리케이션 호출 스택 전체로 문장을 묶어요. 프록시, 리플렉션,
   CGLIB, 프레임워크 프레임은 무시해서, 같은 리포지토리 메서드라도 서로 다른 두 곳에서 부르면 서로 다른
   두 묶음이 돼요.

3. **세기** -- 서로 다른 값을 바인딩한 SELECT가 한 묶음에 **기준값 이상**(기본 3개) 있으면 ERROR
   `n-plus-one` 탐지 결과가 돼요. 다음 세 경우는 해당하지 않아요.

    - 자리표시자가 여러 개인 `IN (?, ?, ...)` 목록이 있는 문장은 묶어서 가져오는 것이라서, `@BatchSize`와
      배치 페치는 테스트를 실패시키지 않아요.
    - `OFFSET`으로 페이지를 넘기는 문장(`LIMIT ? OFFSET ?`, `OFFSET ? ROWS`, `LIMIT ?, ?`)은 다음 행의
      연관관계가 아니라 다음 페이지를 읽는 거예요.
    - 묶음 안의 실행이 모두 같은 값을 바인딩하면 같은 조회를 반복한 거예요. 예를 들어 테스트가 같은 요청을
      반복문으로 보내거나, 페이지 반복문의 count 쿼리가 그래요. 이런 경우는 테스트를 실패시키지 않는 INFO
      `n-plus-one` 탐지 결과로 보고해서, 운영 코드가 한 작업 안에서 같은 조회를 반복하는 것도 눈에 보이게
      해요. QueryAudit은 바인딩한 값의 해시만 비교하고 값 자체는 보관하지 않아요.

   키셋 페이지네이션(`WHERE id > ? ORDER BY id LIMIT ?`)은 페이지마다 새 값을 바인딩하기 때문에 보고돼요.
   의도한 반복문이라면 그 테스트에서 `n-plus-one`을 억제하세요.

Hibernate 연동이 켜져 있으면 지연 로딩 이벤트도 기록해요. 이 이벤트는 컬렉션이나 프록시 이름과 함께
추가할 페치를 제안하는 INFO `n-plus-one` 탐지 결과가 돼요. 확정된 탐지 결과를 설명해 주지만, 이벤트만으로
확정하지는 않아요. 예전의 SQL 전용 `n-plus-one-suspect` 규칙은 `strict`나 `enabled-rules`로 여전히 쓸 수 있어요.

`@QueryAudit`은 확정된 탐지 결과가 있으면 테스트를 실패시키고, `@EnableQueryInspector`는 보고만 해요.
다른 규칙을 켰을 때 N+1에서만 실패하게 하려면 `@QueryAudit(failOn = IssueType.N_PLUS_ONE)`을 쓰세요.

---

## 진단하는 방법 {#how-to-diagnose}

QueryAudit이 N+1을 보고하면, 다음 순서로 근본 원인을 찾아 고치세요.

### 1단계: 반복된 쿼리 찾기 {#step-1-identify-the-repeated-query}

콘솔 리포트에 반복된 SELECT, 그걸 실행한 줄, 읽는 테이블이 나와요(일부).

```
[ERROR] N+1 Query detected
  Query:  select m1_0.id,m1_0.name from members m1_0 where m1_0.id=?
  Source: com.example.OrderService.lambda$findOrders$0:42
  Target: members
  Detail: The same SELECT ran 100 times from one call site
```

### 2단계: 원인이 된 코드 찾기 {#step-2-find-the-triggering-code}

다음과 같은 코드를 찾으세요.

- 엔티티 컬렉션을 반복하는 코드
- 반복문 안에서 지연 로딩 연관관계에 접근하는 코드
- 반복문 안에서 리포지토리 메서드를 부르는 코드

!!! tip "스택 트레이스"
    QueryAudit은 쿼리가 실행될 때 스택 트레이스를 캡처해요. 콘솔에 나온 호출 위치가 프록시를 가리키면
    JSON 리포트에 남은 그 쿼리의 `stackTrace`에서 애플리케이션 패키지를 찾으세요. Hibernate 이벤트로 생긴
    탐지 결과에는 호출 위치가 없을 수 있어요. 같은 테스트에서 캡처된 문장과 맞춰 보세요.

### 3단계: 엔티티 매핑 확인하기 {#step-3-check-the-entity-mapping}

```java
@Entity
public class Order {
    @ManyToOne(fetch = FetchType.LAZY)  // <-- lazy loading = N+1 risk
    private Member member;
}
```

### 4단계: 흔한 코드 패턴 확인하기 {#step-4-check-for-common-code-patterns}

N+1은 잘 보이지 않는 곳에 숨어 있는 경우가 많아요. 다음 패턴을 확인하세요.

=== "서비스 계층 반복문"

    ```java
    // The classic: accessing a lazy relation in a loop
    for (Order order : orderRepository.findAll()) {
        order.getMember().getName();  // <-- triggers lazy load
    }
    ```

=== "Stream / map 연산"

    ```java
    // Same problem but harder to spot in functional style
    List<String> names = orders.stream()
        .map(o -> o.getMember().getName())  // <-- lazy load per element
        .collect(Collectors.toList());
    ```

=== "반복문 안의 리포지토리 호출"

    ```java
    // Not an ORM issue -- explicit queries in a loop
    for (Long memberId : memberIds) {
        Member m = memberRepository.findById(memberId).orElseThrow();
        results.add(m);
    }
    ```

=== "템플릿 / 뷰 계층"

    ```html
    <!-- Thymeleaf / JSP: lazy load triggered during rendering -->
    <tr th:each="order : ${orders}">
        <td th:text="${order.member.name}"/>  <!-- N+1 here -->
    </tr>
    ```

### 5단계: 고치는 방법 고르기 {#step-5-choose-a-fix-strategy}

아래 [고치는 방법](#how-to-fix)을 보세요. 알맞은 방법은 사용 사례에 따라 달라요.

---

## 실제 예시 {#real-world-examples}

### 예시 1: 기본 JPA 지연 로딩 {#example-1-basic-jpa-lazy-loading}

가장 흔한 N+1이에요. 엔티티를 반복하면서 지연 로딩 연관관계에 접근해요.

=== "문제 코드"

    ```java
    // 1 query: SELECT * FROM orders
    List<Order> orders = orderRepository.findAll();

    for (Order order : orders) {
        // N queries: SELECT * FROM members WHERE id = ? (lazy loading)
        String memberName = order.getMember().getName();
        log.info("Order {} by {}", order.getId(), memberName);
    }
    ```

=== "생성되는 SQL"

    ```sql
    -- 1st query
    SELECT o.id, o.status, o.member_id, o.total FROM orders o;

    -- N lazy-load queries (one per order)
    SELECT m.id, m.name, m.email FROM members m WHERE m.id = 1;
    SELECT m.id, m.name, m.email FROM members m WHERE m.id = 2;
    SELECT m.id, m.name, m.email FROM members m WHERE m.id = 3;
    -- ... repeated for every order
    ```

### 예시 2: 중첩된 N+1 (Order -> Member -> Address) {#example-2-nested-n1-order-member-address}

N+1이 중첩돼서 쿼리가 N x M개가 되는 특히 심한 경우예요.

=== "문제 코드"

    ```java
    List<Order> orders = orderRepository.findAll();  // 1 query

    for (Order order : orders) {
        Member member = order.getMember();           // N queries
        Address address = member.getAddress();       // N more queries (nested!)
        log.info("{} lives at {}", member.getName(), address.getCity());
    }
    ```

=== "쿼리 수"

    ```
    With 100 orders:
      1  (orders)
    + 100 (members)
    + 100 (addresses)
    = 201 queries total
    ```

=== "@EntityGraph로 고치기"

    ```java
    @EntityGraph(attributePaths = {"member", "member.address"})
    @Query("SELECT o FROM Order o")
    List<Order> findAllWithMemberAndAddress();
    ```

    쿼리 201개가 **1개**로 줄어요.

### 예시 3: 컬렉션 매핑 (@OneToMany) {#example-3-collection-mapping-onetomany}

```java
@Entity
public class Department {
    @OneToMany(mappedBy = "department", fetch = FetchType.LAZY)
    private List<Employee> employees;
}
```

=== "문제 코드"

    ```java
    List<Department> departments = departmentRepository.findAll();

    for (Department dept : departments) {
        // Each call triggers: SELECT * FROM employees WHERE department_id = ?
        int headcount = dept.getEmployees().size();
    }
    ```

=== "@EntityGraph로 고치기"

    ```java
    @EntityGraph(attributePaths = {"employees"})
    @Query("SELECT d FROM Department d")
    List<Department> findAllWithEmployees();
    ```

=== "JOIN FETCH로 고치기"

    ```java
    @Query("SELECT DISTINCT d FROM Department d JOIN FETCH d.employees")
    List<Department> findAllWithEmployees();
    ```

    !!! tip "컬렉션에 JOIN FETCH를 쓸 때는 DISTINCT"
        `DISTINCT` 없이 `@OneToMany`에 `JOIN FETCH`를 쓰면 부모 엔티티가 자식 행 수만큼 중복돼요.
        Hibernate 6 이상은 자동으로 처리하지만, Hibernate 5는 `DISTINCT`를 명시해야 해요.

### 예시 4: MyBatis 중첩 select {#example-4-mybatis-nested-select}

=== "문제 매퍼 (XML)"

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

=== "고치기: SQL에서 JOIN 쓰기"

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

### 예시 5: Spring Data REST / JSON 직렬화 {#example-5-spring-data-rest-json-serialization}

JSON 직렬화 중에 생기는, 잘 보이지 않는 N+1이에요.

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

Jackson이 직렬화하면서 `Order`마다 `getMember()`를 불러서 지연 로딩이 일어나요.

!!! danger "찾기 어려운 경우"
    이 N+1은 컨트롤러 코드에 보이지 않아요. JSON 직렬화기 안에서 일어나요. 계측된 DataSource를 쓰면서
    직렬화까지 감사 구간 안에서 실행해야 QueryAudit이 그 지연 로딩 쿼리도 캡처할 수 있어요.

**고치기:** 리포지토리 메서드에 DTO 프로젝션이나 `@EntityGraph`를 쓰세요.

```java
@EntityGraph(attributePaths = {"member"})
List<Order> findAll();
```

### 예시 6: Spring Data JPA 파생 쿼리 {#example-6-spring-data-jpa-derived-query}

Spring Data의 파생 쿼리는 `JOIN FETCH`를 지원하지 않아요. 흔히 빠지는 함정이에요.

=== "문제 코드"

    ```java
    // Derived query: generates SELECT * FROM orders WHERE status = ?
    // No way to add JOIN FETCH via method naming
    List<Order> orders = orderRepository.findByStatus("pending");

    for (Order order : orders) {
        order.getMember().getName();  // N+1
    }
    ```

=== "고치기: 파생 쿼리에 @EntityGraph"

    ```java
    @EntityGraph(attributePaths = {"member"})
    List<Order> findByStatus(String status);
    ```

=== "고치기: JOIN FETCH를 쓴 @Query"

    ```java
    @Query("SELECT o FROM Order o JOIN FETCH o.member WHERE o.status = :status")
    List<Order> findByStatusWithMember(@Param("status") String status);
    ```

### 예시 7: Hibernate 2차 캐시 미스 {#example-7-hibernate-second-level-cache-miss}

2차 캐시를 쓰면 캐시가 비워지거나 재시작한 뒤 N+1이 다시 나타날 수 있어요.

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

!!! warning "캐시는 N+1의 해결책이 아니에요"
    캐시는 N+1을 가릴 수 있어요. 캐시가 비워지거나, 재시작하거나, 데이터가 바뀌면 문제가 돌아와요.
    캡처된 접근 패턴을 검토하고 그 작업에 맞는 페치 방식을 고르세요.

---

## 고치는 방법 {#how-to-fix}

=== "JOIN FETCH (JPQL)"

    ```java
    @Query("SELECT o FROM Order o JOIN FETCH o.member")
    List<Order> findAllWithMember();
    ```

    JOIN을 쓰는 쿼리 하나로 추가 쿼리 N개를 모두 없애요.

    !!! warning "페이지네이션 제약"
        `@OneToMany` 컬렉션에 `JOIN FETCH`와 `Pageable`을 같이 쓰면 Hibernate가 **모든 결과를 메모리로**
        가져와서 애플리케이션에서 페이지를 나눠요(HHH000104 경고). 페이지를 나누는 쿼리에는 `@EntityGraph`나
        `@BatchSize`를 쓰세요.

=== "@EntityGraph"

    ```java
    @EntityGraph(attributePaths = {"member"})
    @Query("SELECT o FROM Order o")
    List<Order> findAllWithMember();
    ```

    JPA가 `member` 연관관계를 같은 쿼리에서 즉시 가져오게 해요.

    중첩된 연관관계라면:

    ```java
    @EntityGraph(attributePaths = {"member", "member.address"})
    @Query("SELECT o FROM Order o")
    List<Order> findAllWithMemberAndAddress();
    ```

    엔티티에 정의한 이름 있는 엔티티 그래프라면:

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

    Hibernate가 쿼리 N개 대신 `WHERE id IN (?, ?, ?, ...)`로 묶어서 `ceil(N / batchSize)`개의 쿼리로
    가져와요. 쿼리 하나는 아니지만 크게 줄어요.

    ```
    Before: 100 queries  (SELECT ... WHERE id = ?)
    After:  2 queries    (SELECT ... WHERE id IN (?, ?, ..., ?))  -- batch of 50
    ```

    !!! tip "전역 기본 배치 크기"
        `application.yml`에 전역 기본값을 두면 모든 지연 로딩 N+1을 줄일 수 있어요.

        ```yaml
        spring:
          jpa:
            properties:
              hibernate:
                default_batch_fetch_size: 100
        ```

=== "서브셀렉트 페치"

    ```java
    @Entity
    public class Order {
        @ManyToOne(fetch = FetchType.LAZY)
        @Fetch(FetchMode.SUBSELECT)
        private Member member;
    }
    ```

    Hibernate가 관련 엔티티를 서브셀렉트 쿼리 하나로 모두 불러와요.

    ```sql
    SELECT m.* FROM members m
    WHERE m.id IN (SELECT o.member_id FROM orders o)
    ```

=== "DTO 프로젝션"

    ```java
    public record OrderSummary(Long orderId, String memberName, BigDecimal total) {}

    @Query("""
        SELECT new com.example.dto.OrderSummary(o.id, m.name, o.total)
        FROM Order o JOIN o.member m
        """)
    List<OrderSummary> findOrderSummaries();
    ```

    가장 효율적인 방법이에요. 필요한 컬럼만 쿼리 하나로 가져오고, 엔티티 관리 비용도 없어요.

### 고치는 방법 고르기 {#fix-strategy-decision-guide}

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

엔티티를 수정해야 하면 `JOIN FETCH`나 `@EntityGraph`를 쓰세요. `@OneToMany`라면 페이지네이션 문제를 피할 수
있는 `@EntityGraph`가 낫고, `@ManyToOne`이면 둘 다 괜찮아요. 읽기만 한다면 DTO 프로젝션이 가장 효율적이에요.
모든 지연 로딩에 안전망이 필요하면 `@BatchSize`나 `hibernate.default_batch_fetch_size`를 쓰세요.

### 고치기 전과 후 {#beforeafter-comparison}

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

## QueryAudit 리포트 출력 {#queryaudit-report-output}

감사하는 테스트마다 자기 리포트를 출력해요. 일부:

```
────────────────────────────────────────────────────────────────────────
  QUERYAUDIT REPORT
  Test: findOrders()
────────────────────────────────────────────────────────────────────────

--- CONFIRMED (sorted by priority) ---

  [ERROR] N+1 Query detected
    ID:     qa-finding-v1:7452…
    Query:  select m1_0.id,m1_0.name from members m1_0 where m1_0.id=?
    Source: com.example.OrderService.lambda$findOrders$0:42
    Target: members
    Detail: The same SELECT ran 100 times from one call site
    Fix:    Load the rows once before the loop: JOIN FETCH, @EntityGraph, or one query with an IN list.
```

테스트가 실패하면 실패 메시지가 애플리케이션 호출 스택과 함께 이 탐지 결과를 다시 보여 줘요.

---

## 설정 {#configuration}

### 기준값 {#threshold}

N+1로 판단하는 최소 반복 횟수예요.

=== "application.yml"

    ```yaml
    query-audit:
      n-plus-one:
        threshold: 3   # default
    ```

=== "코드로 설정"

    ```java
    QueryAuditConfig config = QueryAuditConfig.builder()
        .nPlusOneThreshold(5)
        .build();
    ```

!!! tip "기준값 고르기"
    기본값 **3**은 대부분의 실제 N+1을 잡으면서, 캐시 예열처럼 정당하게 두 번 실행되는 쿼리의 오탐은
    피해요. 의도적으로 반복하는 쿼리가 많다면 **5**로 올리세요.

### 억제하기 {#suppressing}

탐지된 N+1이 의도한 것이라면(예: 일부러 행마다 쿼리를 실행하는 배치 처리기) 억제하세요.

=== "어노테이션"

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

=== "특정 테이블만 억제"

    ```yaml
    query-audit:
      suppress-patterns:
        - "n-plus-one:members"
    ```

---

## 관련 규칙 {#related-rules}

- [`duplicate-query`](overview.md#disabled-rules) -- 똑같은 쿼리를 찾아요(현재 비활성)
- [`repeated-single-insert`](dml-anti-patterns.md#repeated-single-row-insert) -- INSERT 문장에서 생기는 비슷한 패턴
- [`repeated-single-update`](dml-anti-patterns.md#repeated-single-row-update) -- 고유 키 기준 UPDATE에서 생기는 비슷한 패턴
- [`mergeable-queries`](overview.md) -- 같은 테이블에 대한 여러 쿼리를 합칠 수 있는 경우
