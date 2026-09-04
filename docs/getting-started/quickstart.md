---
title: Quick Start
description: Add one audited test, understand its first failure, and verify the fix.
---

# Quick Start

This walkthrough starts with one database-facing test. It reports every applicable finding but
fails only on a query budget you chose, so the first result is easy to explain and fix.

!!! note "Reporting behavior by version"
    Dependency snippets use the current Maven Central release. QueryAudit 0.5.x writes HTML and
    JSON after a session with at least one completed audited result. QueryAudit 0.6.0+ defaults to
    console-only output and writes the selected JSON or HTML report when configured below.

## 1. Add the test dependencies

For a Spring Boot test, add the starter and the module for your test database:

=== "Gradle"

    ```groovy
    dependencies {
        testImplementation 'io.github.haroya01:query-audit-spring-boot-starter:0.6.0' // x-release-please-version
        testImplementation 'io.github.haroya01:query-audit-mysql:0.6.0' // x-release-please-version
    }
    ```

=== "Maven"

    ```xml
    <dependencies>
        <dependency>
            <groupId>io.github.haroya01</groupId>
            <artifactId>query-audit-spring-boot-starter</artifactId>
            <version>0.6.0</version> <!-- x-release-please-version -->
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>io.github.haroya01</groupId>
            <artifactId>query-audit-mysql</artifactId>
            <version>0.6.0</version> <!-- x-release-please-version -->
            <scope>test</scope>
        </dependency>
    </dependencies>
    ```

Replace `query-audit-mysql` with `query-audit-postgresql` when the tests use PostgreSQL. Plain
JUnit tests use one database module plus `datasource-proxy`; copy that setup from
[Plain JUnit 5 installation](installation.md#plain-junit-5).

## 2. Put a budget on one path

Use `@EnableQueryInspector` while learning what the test does. It writes findings without making
those findings fail the test. `@ExpectQueries` remains an explicit assertion and fails when the
statement mix exceeds the declared limits.

=== "Spring Boot"

    ```java
    import io.queryaudit.junit5.EnableQueryInspector;
    import io.queryaudit.junit5.ExpectQueries;
    import java.util.List;
    import org.junit.jupiter.api.Test;
    import org.springframework.beans.factory.annotation.Autowired;
    import org.springframework.boot.test.context.SpringBootTest;

    @SpringBootTest
    @EnableQueryInspector
    class OrderServiceQueryTest {

        @Autowired
        private OrderService orderService;

        @Test
        @ExpectQueries(select = 2, insert = 0, update = 0, delete = 0)
        void loadsOrdersWithItemsWithinBudget() {
            List<Order> orders = orderService.findRecentOrders();
            orders.forEach(order -> assertThat(order.getItems()).isNotEmpty());
        }
    }
    ```

=== "Plain JUnit 5"

    ```java
    import io.queryaudit.junit5.EnableQueryInspector;
    import io.queryaudit.junit5.ExpectQueries;
    import javax.sql.DataSource;
    import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;
    import org.junit.jupiter.api.Test;

    @EnableQueryInspector
    class OrderRepositoryQueryTest {

        static final DataSource DATA_SOURCE =
                ProxyDataSourceBuilder.create(TestDatabase.dataSource())
                        .name("query-audit")
                        .build();

        private final OrderRepository repository = new JdbcOrderRepository(DATA_SOURCE);

        @Test
        @ExpectQueries(select = 2, insert = 0, update = 0, delete = 0)
        void loadsOrdersWithItemsWithinBudget() {
            repository.findRecentOrdersWithItems();
        }
    }
    ```

The plain JUnit repository must use `DATA_SOURCE`. QueryAudit attaches its listener to that proxy;
SQL executed through a different, raw `DataSource` is outside the capture path.

The budget above says this path may issue at most two SELECTs and no writes. Choose a limit that
describes the behavior you want, rather than copying `2` into every test.

## 3. Run only that test

=== "Gradle"

    ```bash
    ./gradlew test --tests OrderServiceQueryTest
    ```

=== "Maven"

    ```bash
    mvn -Dtest=OrderServiceQueryTest test
    ```

If the initial order query is followed by one item query per order, the assertion fails with a
statement list and the first captured application frame for each statement:

```text
QueryAudit: loadsOrdersWithItemsWithinBudget() exceeded its query budget.
SELECT: executed 6, expected at most 2.
  select ... from orders ...
    at com.example.OrderRepository.findRecentOrders(OrderRepository.java:31)
  select ... from order_items where order_id=?
    at com.example.OrderRepository.findItems(OrderRepository.java:42)
```

Before this assertion message, the console report groups confirmed findings and informational
advice. Depending on the executed SQL and available metadata, it may also identify the repeated
item lookup as N+1 or show an unindexed access path.

## 4. Read the evidence, then fix the cause

Use each field for a specific decision:

| Output | What to do with it |
|---|---|
| Issue type or budget summary | Identify the contract that failed |
| `Query` | Find the repeated or unsafe statement shape |
| `Source` | Open the application call site that issued the statement |
| `Target` and `Detail` | Check the table, column, threshold, or index evidence |
| `Fix` or `Suggestion` | Start with the proposed query, mapping, or schema change and review it against the application semantics |

For a JPA lazy-load loop, fetching the association with the parent is one common fix:

```java
public interface OrderRepository extends JpaRepository<Order, Long> {

    @EntityGraph(attributePaths = "items")
    List<Order> findByCreatedAtAfterOrderByCreatedAtDesc(Instant cutoff);
}
```

For JDBC, use one join or a bounded second query with an `IN` predicate instead of issuing the
same child lookup once per order. Rerun the same test. The chosen behavior should now stay within
the budget, and any N+1 finding should disappear from the report.

Do not add an index only because a suggestion names one. Check existing composite indexes,
write cost, and the production query shape first. The database module includes the collected index
state in the machine report when it is available.

## 5. Decide what CI should enforce

Once you have reviewed the report, choose the contract for this test:

- Keep `@EnableQueryInspector` when findings should remain advisory while explicit budgets fail.
- Replace it with `@QueryAudit` when confirmed findings should fail the test.
- Add `@ExpectMaxQueryCount` for a single total limit or keep `@ExpectQueries` for separate read and
  write limits.
- Record [query snapshot contracts](../guide/contracts.md) when a set of established tests should
  keep its SELECT, INSERT, UPDATE, and DELETE counts across runs.

QueryAudit 0.5.x already writes both report files. With Spring Boot on 0.6.0+, select the report CI
should retain in test configuration:

```yaml
# src/test/resources/application.yml
query-audit:
  report:
    format: json
```

Run the test again. This writes `build/reports/query-audit/report.json`, the schema-versioned input
for scripts, coding tools, and the
[report comparator](../guide/reports.md#delta-verdict-compare-two-runs). Set `format: html` when
reviewers need a browser artifact. A 0.6.0+ run selects one suite report format; the per-test
console diagnostics remain available. Plain JUnit users can pass the same setting into the test JVM
with the [Gradle or Maven setup in the CI guide](../guide/ci-cd.md#plain-junit-build-tool-setup). A
change tool can use the JSON evidence locally, while CI independently reruns the tests and decides
whether the build passes.

Keep reports from failed builds by using `if: always()`:

```yaml
- name: Run tests
  run: ./gradlew test

- name: Upload QueryAudit reports
  if: always()
  uses: actions/upload-artifact@v4
  with:
    name: query-audit-reports
    path: build/reports/query-audit/
    if-no-files-found: error
```

The [CI/CD guide](../guide/ci-cd.md) has complete MySQL and PostgreSQL service examples and GitHub
Actions annotations.

## Continue by task

| Next task | Guide |
|---|---|
| Understand which checks ran and what evidence they need | [Detection overview](../detections/overview.md) |
| Configure the starter or an existing proxy | [Spring Boot integration](spring-boot.md) |
| Choose budgets and focused annotations | [Annotations](../guide/annotations.md) |
| Select a profile, suppression, or full-suite coverage | [Configuration](../guide/configuration.md) |
| Record statement-count snapshots | [Query contracts](../guide/contracts.md) |
| Parse JSON or compare two runs | [Reports](../guide/reports.md) |
| Resolve missing capture or metadata | [Troubleshooting](../guide/troubleshooting.md) |
