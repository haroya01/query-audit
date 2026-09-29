---
title: Installation
description: Add test dependencies, enforce a read-path policy, and verify an intentional failure.
---

# Installation

| Start from | Use |
| --- | --- |
| An existing Spring Boot database test | [Spring Boot starter](#spring-boot) |
| JUnit 5 without Spring | [Plain JUnit 5](#plain-junit-5) |
| A runnable sample | [Fail an unexpected write, then pass](quickstart.md) |

Add QueryAudit to the **test classpath**. These snippets use published `0.6.0`;
[Versions](versions.md) lists its supported scope and tested combinations.

## Spring Boot

Add the starter to your existing database test. It wraps the Spring `DataSource` automatically;
keep your JDBC driver, connection settings, migrations, and fixtures.

=== "Gradle · Kotlin"

    ```kotlin
    dependencies {
        testImplementation("org.springframework.boot:spring-boot-starter-test")
        testImplementation("io.github.haroya01:query-audit-spring-boot-starter:0.6.0") // x-release-please-version
        testImplementation("io.github.haroya01:query-audit-mysql:0.6.0") // x-release-please-version
    }

    tasks.test {
        useJUnitPlatform()
    }
    ```

=== "Gradle · Groovy"

    ```groovy
    dependencies {
        testImplementation 'org.springframework.boot:spring-boot-starter-test'
        testImplementation 'io.github.haroya01:query-audit-spring-boot-starter:0.6.0' // x-release-please-version
        testImplementation 'io.github.haroya01:query-audit-mysql:0.6.0' // x-release-please-version
    }

    test {
        useJUnitPlatform()
    }
    ```

=== "Maven"

    ```xml
    <dependencies>
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-test</artifactId>
            <scope>test</scope>
        </dependency>
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

Keep Spring Boot's managed test version. If `spring-boot-starter-test` is already present,
add only the QueryAudit dependencies you need.

| Your test database | Database module to use |
| --- | --- |
| MySQL | `query-audit-mysql` as shown above |
| PostgreSQL | Replace `query-audit-mysql` with `query-audit-postgresql` |
| Query budgets and count contracts only | Omit the database module; the starter includes JUnit integration |

**Run it:** [fail a zero-SELECT budget, then apply a read-path policy](spring-boot.md#run-a-controlled-first-audit).

## Plain JUnit 5

Use `query-audit-junit5` for capture, budgets, and count contracts in Java 17+ JUnit 5 tests.
The H2 dependency supplies the [runnable sample](quickstart.md); use your existing database otherwise.

=== "Gradle · Kotlin"

    ```kotlin
    dependencies {
        testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
        testRuntimeOnly("org.junit.platform:junit-platform-launcher:1.11.4")
        testImplementation("io.github.haroya01:query-audit-junit5:0.6.0") // x-release-please-version
        testImplementation("net.ttddyy:datasource-proxy:1.10")
        testImplementation("com.h2database:h2:2.3.232")
    }

    tasks.test {
        useJUnitPlatform()
    }
    ```

=== "Gradle · Groovy"

    ```groovy
    dependencies {
        testImplementation 'org.junit.jupiter:junit-jupiter:5.11.4'
        testRuntimeOnly 'org.junit.platform:junit-platform-launcher:1.11.4'
        testImplementation 'io.github.haroya01:query-audit-junit5:0.6.0' // x-release-please-version
        testImplementation 'net.ttddyy:datasource-proxy:1.10'
        testImplementation 'com.h2database:h2:2.3.232'
    }

    test {
        useJUnitPlatform()
    }
    ```

=== "Maven"

    ```xml
    <dependencies>
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>5.11.4</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>io.github.haroya01</groupId>
            <artifactId>query-audit-junit5</artifactId>
            <version>0.6.0</version> <!-- x-release-please-version -->
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>net.ttddyy</groupId>
            <artifactId>datasource-proxy</artifactId>
            <version>1.10</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>com.h2database</groupId>
            <artifactId>h2</artifactId>
            <version>2.3.232</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
    ```

If your project already manages JUnit, keep its compatible JUnit version and launcher aligned.
Maven must already be configured to execute JUnit 5 tests; a build that discovers no tests has
not verified capture.

Expose a `ProxyDataSource` as a static test field and make the code under test use **that same
object**. Copy the [complete test and setup](quickstart.md#4-apply-the-budget-to-your-code).

For an existing MySQL or PostgreSQL test, replace `query-audit-junit5` with
`query-audit-mysql` or `query-audit-postgresql`; both include the JUnit integration. Keep
`datasource-proxy` for the explicit proxy setup and use your existing driver/fixture instead of H2.

??? info "Let QueryAudit wrap a mutable field (0.6.0+)"

    The extension can replace a raw field declared as `static javax.sql.DataSource` with its
    recording proxy during the test. The field must be mutable: a raw `static final` field or a
    concrete pool type cannot be replaced this way. Construct the repository after wrapping, using
    the current field value. A repository created earlier with the raw datasource bypasses capture.

    The explicit static proxy in the quick start avoids this replacement requirement and is also
    the portable setup for older releases.

## Run a failure, then keep the read policy

Add `@EnableQueryInspector` and this policy directly to an existing test that executes one SELECT:

```java
@ExpectQueries(select = 0, insert = 0, update = 0, delete = 0)
```

Run it and require this failure:

```text
SELECT: executed 1, expected at most 0.
```

Then set `select = 1` and rerun. The test should pass; the zero INSERT/UPDATE/DELETE budgets
remain to catch writes added to that read path. Keep its functional assertions.

Budgets are upper bounds, so a passing test alone does not prove SQL capture.
If the zero-budget run passes, follow [missing-capture troubleshooting](../guide/troubleshooting.md#queryaudit-not-detecting-any-queries).

Next, [record per-test count contracts](../guide/contracts.md) or [require the audit in CI](../guide/first-ci-check.md).

## Module selection

| Module | Use it for |
| --- | --- |
| `query-audit-spring-boot-starter` | Automatic Spring DataSource wrapping and property binding |
| `query-audit-junit5` | Plain JUnit capture, budgets, count contracts, and database-independent checks |
| `query-audit-mysql` | JUnit integration plus MySQL index metadata and EXPLAIN |
| `query-audit-postgresql` | JUnit integration plus PostgreSQL index metadata and EXPLAIN |
| `query-audit-core` | Programmatic analysis, models, reporters, or comparison without JUnit |

The starter and database modules include `query-audit-junit5` and `query-audit-core` transitively.
Add a direct dependency only when your code needs that module's public API.

## Compatibility

Use **Java 17+ and JUnit 5**. See [tested combinations](versions.md#tested-combinations) and
[known limitations](../guide/limitations.md) for the published release’s scope.

### SQL parser dependency

From 0.6.0, JSqlParser 5.3 is a required transitive dependency. Do not exclude it. Missing,
incompatible, or unidentifiable parser versions fail initialization.

??? info "Parser overrides and fallback"

    Structural extraction uses JSqlParser first. Unsupported statements and statements longer than
    10,000 characters use the built-in fallback for that statement; simple pattern checks and
    normalization also use built-in parsing. A fallback does not establish complete SQL support.

    If dependency management overrides JSqlParser, check `EnhancedSqlParser.parserVersion()`.
    Repackaged JARs must retain its Maven version metadata. See
    [parser troubleshooting](../guide/troubleshooting.md#sql-is-too-complex-for-the-parser).

## Next step

[Prove your first budget](quickstart.md), then [add your first CI check](../guide/first-ci-check.md).
