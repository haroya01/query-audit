---
title: Installation
description: Choose the Spring Boot or plain JUnit setup and verify SQL capture.
---

# Installation

QueryAudit belongs on the test classpath. Choose one database module for the database used by
your tests; it provides the JUnit extension and the matching index metadata provider.

!!! note "Choose the setup for your version"
    Dependency snippets use the current Maven Central release. The manual plain-JUnit proxy works
    with 0.5.x and 0.6.0+; the simpler mutable-field setup requires QueryAudit 0.6.0 or later.

## Compatibility

| Layer | Project baseline and coverage |
|---|---|
| Java | Requires 17; CI runs on 17 and 21 |
| JUnit | JUnit 5 extension; built and tested with 5.11.4 |
| Spring Boot | 3.x and 4.x; the current suite covers 3.4.1 and 4.0.6 |
| Database metadata | MySQL and PostgreSQL; integration tests run MySQL 8.0 and PostgreSQL 16 |

You still need your normal JDBC driver and a test database. QueryAudit does not create the
database or replace your migration and fixture setup.

## Spring Boot

Add the starter and either the MySQL or PostgreSQL module. The starter discovers and wraps the
Spring `DataSource`; no proxy bean is needed in the test.

=== "Gradle · MySQL"

    ```groovy
    dependencies {
        testImplementation 'io.github.haroya01:query-audit-spring-boot-starter:0.5.0' // x-release-please-version
        testImplementation 'io.github.haroya01:query-audit-mysql:0.5.0' // x-release-please-version
    }
    ```

=== "Gradle · PostgreSQL"

    ```groovy
    dependencies {
        testImplementation 'io.github.haroya01:query-audit-spring-boot-starter:0.5.0' // x-release-please-version
        testImplementation 'io.github.haroya01:query-audit-postgresql:0.5.0' // x-release-please-version
    }
    ```

=== "Maven · MySQL"

    ```xml
    <dependencies>
        <dependency>
            <groupId>io.github.haroya01</groupId>
            <artifactId>query-audit-spring-boot-starter</artifactId>
            <version>0.5.0</version> <!-- x-release-please-version -->
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>io.github.haroya01</groupId>
            <artifactId>query-audit-mysql</artifactId>
            <version>0.5.0</version> <!-- x-release-please-version -->
            <scope>test</scope>
        </dependency>
    </dependencies>
    ```

=== "Maven · PostgreSQL"

    ```xml
    <dependencies>
        <dependency>
            <groupId>io.github.haroya01</groupId>
            <artifactId>query-audit-spring-boot-starter</artifactId>
            <version>0.5.0</version> <!-- x-release-please-version -->
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>io.github.haroya01</groupId>
            <artifactId>query-audit-postgresql</artifactId>
            <version>0.5.0</version> <!-- x-release-please-version -->
            <scope>test</scope>
        </dependency>
    </dependencies>
    ```

Verify the wiring with a test that executes one statement:

```java
@SpringBootTest
@EnableQueryInspector
class QueryAuditInstallationTest {

    @Autowired
    private JdbcTemplate jdbc;

    @Test
    @ExpectMaxQueryCount(1)
    void capturesJdbcWork() {
        assertThat(jdbc.queryForObject("select 1", Integer.class)).isEqualTo(1);
    }
}
```

Run `./gradlew test --tests QueryAuditInstallationTest` or
`mvn -Dtest=QueryAuditInstallationTest test`. The console report should show one captured query.
QueryAudit 0.5.x also writes `build/reports/query-audit/report.json` after the session. With Spring
Boot on 0.6.0+, select the machine report in `src/test/resources/application.yml`, rerun the test,
and check the same path:

```yaml
query-audit:
  report:
    format: json
```

## Plain JUnit 5

The portable plain JUnit setup exposes a `ProxyDataSource` on the test class. The repository or
JDBC code under test must use that same object so the extension can attach its capture listener.

Add the database module and a compile-time dependency on `datasource-proxy`:

=== "Gradle · MySQL"

    ```groovy
    dependencies {
        testImplementation 'io.github.haroya01:query-audit-mysql:0.5.0' // x-release-please-version
        testImplementation 'net.ttddyy:datasource-proxy:1.10'
    }
    ```

=== "Gradle · PostgreSQL"

    ```groovy
    dependencies {
        testImplementation 'io.github.haroya01:query-audit-postgresql:0.5.0' // x-release-please-version
        testImplementation 'net.ttddyy:datasource-proxy:1.10'
    }
    ```

=== "Maven · MySQL"

    ```xml
    <dependencies>
        <dependency>
            <groupId>io.github.haroya01</groupId>
            <artifactId>query-audit-mysql</artifactId>
            <version>0.5.0</version> <!-- x-release-please-version -->
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>net.ttddyy</groupId>
            <artifactId>datasource-proxy</artifactId>
            <version>1.10</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
    ```

=== "Maven · PostgreSQL"

    ```xml
    <dependencies>
        <dependency>
            <groupId>io.github.haroya01</groupId>
            <artifactId>query-audit-postgresql</artifactId>
            <version>0.5.0</version> <!-- x-release-please-version -->
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>net.ttddyy</groupId>
            <artifactId>datasource-proxy</artifactId>
            <version>1.10</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
    ```

Wrap the existing test `DataSource` once, expose the proxy as a static field, and pass that proxy
to the code under test:

```java
import io.queryaudit.junit5.EnableQueryInspector;
import io.queryaudit.junit5.ExpectMaxQueryCount;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.DataSource;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;
import org.junit.jupiter.api.Test;

@EnableQueryInspector
class OrderRepositoryQueryTest {

    static final DataSource DATA_SOURCE =
            ProxyDataSourceBuilder.create(TestDatabase.dataSource())
                    .name("query-audit")
                    .build();

    @Test
    @ExpectMaxQueryCount(1)
    void capturesJdbcWork() throws Exception {
        try (Connection connection = DATA_SOURCE.getConnection();
                Statement statement = connection.createStatement();
                ResultSet result = statement.executeQuery("select 1")) {
            result.next();
        }
    }
}
```

`TestDatabase.dataSource()` stands for the raw `DataSource` already created by your test fixture,
Testcontainers setup, or connection pool. Do not create a second pool for QueryAudit. The object
used by the repository must be `DATA_SOURCE`, not the raw object returned by the fixture.

QueryAudit 0.6.0+ can install the recording proxy for you when the test exposes the raw object in a
mutable static field declared as `javax.sql.DataSource`:

```java
@EnableQueryInspector
class OrderRepositoryQueryTest {

    static DataSource DATA_SOURCE = TestDatabase.dataSource();

    @Test
    @ExpectMaxQueryCount(1)
    void capturesJdbcWork() {
        new JdbcOrderRepository(DATA_SOURCE).findOne();
    }
}
```

Do not mark this field `final` or declare it as a concrete pool type. The extension temporarily
replaces the field value before the test and restores the original object afterward. The explicit
`ProxyDataSource` setup above remains valid for QueryAudit 0.5.x and 0.6.0+.

!!! warning "Keep the proxy in the execution path"
    If the repository keeps using the raw object instead of `DATA_SOURCE`, its SQL bypasses the
    capture listener and the report shows zero queries. Wrap once and use the proxy throughout
    that test.

## Module selection

| Module | Add it when |
|---|---|
| `query-audit-spring-boot-starter` | A Spring Boot test should receive automatic `DataSource` wrapping and property binding |
| `query-audit-mysql` | Tests use MySQL and need MySQL index metadata or EXPLAIN support |
| `query-audit-postgresql` | Tests use PostgreSQL and need PostgreSQL index metadata or EXPLAIN support |
| `query-audit-junit5` | Plain JUnit tests only need SQL capture and database-independent checks |
| `query-audit-core` | A tool consumes the model, schema, reporters, or comparator without the JUnit extension |

The MySQL and PostgreSQL modules both include `query-audit-core` and `query-audit-junit5`
transitively. Do not add those two modules again unless you need to depend on one directly.

## Next step

Continue with the [quick start](quickstart.md) to add a useful query budget, read the first
failure, fix it, and keep the reports in CI. If capture does not work, use the
[no-query checklist](../guide/troubleshooting.md#queryaudit-not-detecting-any-queries).
