---
title: Installation
description: Add QueryAudit to your Java project with Gradle or Maven.
---

# Installation

!!! abstract "What you'll learn"
    How to add QueryAudit to your project, choose the right database module, and verify the installation works.

---

## Prerequisites

| Requirement | Version |
|---|---|
| **Java** | 17+ |
| **JUnit** | 5.9+ |
| **MySQL** | 5.7+ / 8.0+ |
| **PostgreSQL** | 12+ |
| **Spring Boot** *(optional)* | 3.0+ |

!!! note "Database requirement"
    You need either MySQL **or** PostgreSQL -- not both. Pick the tab that matches your database below.

---

## Dependencies

QueryAudit uses a **database-module-centric** dependency model.
Each database module (e.g. `query-audit-mysql`, `query-audit-postgresql`) transitively pulls in `query-audit-core` and `query-audit-junit5`,
so you only need **one or two** dependencies depending on your setup.

Choose the tab that matches your database and build tool:

=== "Gradle -- MySQL (Spring Boot)"

    ```gradle
    dependencies {
        testImplementation 'io.github.haroya01:query-audit-spring-boot-starter:0.1.0'
        testImplementation 'io.github.haroya01:query-audit-mysql:0.1.0'
    }
    ```

    !!! tip "That's all you need"
        The starter auto-discovers your `DataSource` bean, wraps it in a capturing proxy, and registers the JUnit 5 extension. No additional configuration required.

=== "Gradle -- MySQL (without Spring Boot)"

    ```gradle
    dependencies {
        testImplementation 'io.github.haroya01:query-audit-mysql:0.1.0'
    }
    ```

    `query-audit-mysql` transitively includes `query-audit-core` and `query-audit-junit5`.

=== "Gradle -- PostgreSQL (Spring Boot)"

    ```gradle
    dependencies {
        testImplementation 'io.github.haroya01:query-audit-spring-boot-starter:0.1.0'
        testImplementation 'io.github.haroya01:query-audit-postgresql:0.1.0'
    }
    ```

    !!! tip "That's all you need"
        The starter auto-discovers your `DataSource` bean, wraps it in a capturing proxy, and registers the JUnit 5 extension. No additional configuration required.

=== "Gradle -- PostgreSQL (without Spring Boot)"

    ```gradle
    dependencies {
        testImplementation 'io.github.haroya01:query-audit-postgresql:0.1.0'
    }
    ```

    `query-audit-postgresql` transitively includes `query-audit-core` and `query-audit-junit5`.

=== "Maven -- MySQL (Spring Boot)"

    ```xml
    <dependencies>
        <dependency>
            <groupId>io.github.haroya01</groupId>
            <artifactId>query-audit-spring-boot-starter</artifactId>
            <version>0.1.0</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>io.github.haroya01</groupId>
            <artifactId>query-audit-mysql</artifactId>
            <version>0.1.0</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
    ```

=== "Maven -- MySQL (without Spring Boot)"

    ```xml
    <dependencies>
        <dependency>
            <groupId>io.github.haroya01</groupId>
            <artifactId>query-audit-mysql</artifactId>
            <version>0.1.0</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
    ```

=== "Maven -- PostgreSQL (Spring Boot)"

    ```xml
    <dependencies>
        <dependency>
            <groupId>io.github.haroya01</groupId>
            <artifactId>query-audit-spring-boot-starter</artifactId>
            <version>0.1.0</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>io.github.haroya01</groupId>
            <artifactId>query-audit-postgresql</artifactId>
            <version>0.1.0</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
    ```

=== "Maven -- PostgreSQL (without Spring Boot)"

    ```xml
    <dependencies>
        <dependency>
            <groupId>io.github.haroya01</groupId>
            <artifactId>query-audit-postgresql</artifactId>
            <version>0.1.0</version>
            <scope>test</scope>
        </dependency>
    </dependencies>
    ```

---

## Module Overview

```
query-audit-mysql              <-- Add this for MySQL
  +-- query-audit-core              (transitive)
  +-- query-audit-junit5            (transitive)

query-audit-postgresql         <-- Add this for PostgreSQL
  +-- query-audit-core              (transitive)
  +-- query-audit-junit5            (transitive)

query-audit-spring-boot-starter  <-- Add this too if using Spring Boot
  +-- query-audit-core              (transitive)
  +-- query-audit-junit5            (transitive)
```

| Module | Artifact ID | Description |
|---|---|---|
| **Core** | `query-audit-core` | Detection engine (57 rules), SQL parser, report generator. Always required (pulled transitively). |
| **MySQL** | `query-audit-mysql` | MySQL `IndexMetadataProvider` via `SHOW INDEX`. Includes core + junit5 transitively. |
| **PostgreSQL** | `query-audit-postgresql` | PostgreSQL `IndexMetadataProvider` via `pg_catalog`. Includes core + junit5 transitively. |
| **JUnit 5** | `query-audit-junit5` | JUnit 5 extension, `@QueryAudit` / `@DetectNPlusOne` / `@ExpectMaxQueryCount` annotations. |
| **Spring Boot Starter** | `query-audit-spring-boot-starter` | Auto-configuration for Spring Boot tests. Wraps `DataSource`, binds `application.yml` settings. |

---

## Verify Installation

Run a quick smoke test to confirm everything is wired correctly.

=== "Spring Boot"

    ```java
    @SpringBootTest
    @QueryAudit
    class InstallationVerificationTest {

        @Test
        void queryGuardIsActive() {
            // If you see a QueryAudit report in the test output,
            // the installation is working correctly.
        }
    }
    ```

=== "Without Spring Boot"

    ```java
    @EnableQueryInspector
    class InstallationVerificationTest {

        private static DataSource dataSource = createTestDataSource();

        @Test
        void queryGuardIsActive() {
            // QueryAudit auto-discovers the static DataSource field
        }
    }
    ```

Run the test. If QueryAudit is configured correctly, you will see the report header:

```
================================================================================
                          QUERY AUDIT REPORT
              InstallationVerificationTest (0 queries analyzed)
================================================================================
  0 confirmed issues | 0 info | 0 queries
================================================================================
```

!!! success "You're all set!"
    If you see this output, QueryAudit is installed and working. Move on to the Quick Start to see real detections in action.

---

## Next Steps

:material-arrow-right: [Quick Start](quickstart.md) -- Walk through a full example with real detections.
