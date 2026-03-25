# Contributing Guide

Thank you for your interest in contributing to QueryAudit. This page covers
everything you need to build, test, and submit changes.

---

## Prerequisites

- **JDK 17** or later
- **Docker** (for integration tests that use Testcontainers)
- **Git** with support for conventional commits

---

## Development Setup

### 1. Clone and Build

```bash
git clone https://github.com/haroya01/query-audit.git
cd query-audit
./gradlew build
```

This compiles all modules, runs unit tests, and produces JARs in each module's
`build/libs/` directory.

### 2. Import into IDE

Import the project as a Gradle project in your IDE:

- **IntelliJ IDEA**: File > Open > select the root `build.gradle`
- **VS Code**: Open folder, install the Java Extension Pack and Gradle for Java extensions
- **Eclipse**: Import > Gradle > Existing Gradle Project

### 3. Verify the Setup

```bash
# Run all unit tests
./gradlew test

# Run integration tests (requires Docker)
./gradlew integrationTest

# Full build with all checks
./gradlew build
```

---

## Running Tests

### Unit tests

```bash
./gradlew test
```

### Integration tests

Integration tests require Docker (Testcontainers spins up real database containers):

```bash
./gradlew integrationTest
```

### Single module

```bash
./gradlew :query-audit-core:test
./gradlew :query-audit-mysql:test
./gradlew :query-audit-postgresql:test
./gradlew :query-audit-junit5:test
./gradlew :query-audit-spring-boot-starter:test
```

---

## Project Structure

```
query-audit/
├── build.gradle                        Root build script
├── settings.gradle                     Module includes
├── gradle.properties                   Version and group
├── mkdocs.yml                          Documentation site config
│
├── query-audit-core/                   Core engine
│   └── src/main/java/io/queryaudit/core/
│       ├── analyzer/                   ExplainAnalyzer, IndexMetadataProvider
│       ├── config/                     QueryAuditConfig (builder pattern)
│       ├── detector/                   DetectionRule interface + all 57 detectors
│       ├── interceptor/                DataSourceProxyFactory, QueryInterceptor
│       ├── model/                      Issue, IssueType, Severity, QueryRecord, etc.
│       ├── parser/                     SqlParser, ColumnReference, JoinColumnPair
│       └── reporter/                   Reporter interface, ConsoleReporter, JsonReporter, HtmlReporter
│
├── query-audit-mysql/                  MySQL support
│   └── src/main/java/io/queryaudit/mysql/
│       └── MySqlIndexMetadataProvider.java
│
├── query-audit-postgresql/             PostgreSQL support
│   └── src/main/java/io/queryaudit/postgresql/
│       └── PostgreSqlIndexMetadataProvider.java
│
├── query-audit-junit5/                 JUnit 5 integration
│   └── src/main/java/io/queryaudit/junit5/
│       ├── QueryAudit.java            Annotation
│       ├── QueryAuditExtension.java   JUnit extension
│       └── QueryAuditDataSourceStore.java
│
├── query-audit-spring-boot-starter/    Spring Boot starter
│   └── src/main/java/io/queryaudit/spring/
│       ├── QueryAuditAutoConfiguration.java
│       └── QueryAuditProperties.java
│
└── docs/                               MkDocs documentation
```

---

## Commit Conventions

This project uses [Conventional Commits](https://www.conventionalcommits.org/).
All commit messages must follow this format:

```
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

### Types

| Type | When to use |
|---|---|
| `feat` | New feature |
| `fix` | Bug fix |
| `docs` | Documentation changes |
| `style` | Code style (formatting, no logic change) |
| `refactor` | Code restructuring (no feature or fix) |
| `perf` | Performance improvement |
| `test` | Adding or updating tests |
| `build` | Build system or dependencies |
| `ci` | CI/CD configuration |
| `chore` | Maintenance tasks |

### Scopes

| Scope | Module |
|---|---|
| `core` | `query-audit-core` |
| `junit5` | `query-audit-junit5` |
| `mysql` | `query-audit-mysql` |
| `postgresql` | `query-audit-postgresql` |
| `spring-boot` | `query-audit-spring-boot-starter` |

### Examples

```
feat(core): add composite index leading column detection
fix(mysql): handle empty SHOW INDEX result for views
docs: add CI/CD integration guide
test(junit5): add test for method-level @QueryAudit override
refactor(core): extract SQL normalization into dedicated class
```

Release Please uses these commits to generate changelogs and determine version bumps
(`feat` = minor, `fix` = patch, `BREAKING CHANGE` footer = major).

---

## How to Add a New Detection Rule

### Step-by-step

1. **Create a detector class** in `query-audit-core/src/main/java/io/queryaudit/core/detector/`:

    ```java
    package io.queryaudit.core.detector;

    import io.queryaudit.core.model.*;
    import java.util.*;

    public class MyNewDetector implements DetectionRule {

        @Override
        public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
            List<Issue> issues = new ArrayList<>();

            for (QueryRecord query : queries) {
                // Your detection logic here.
                // Use SqlParser to extract column references, joins, etc.
                // Use IndexMetadata to check if relevant indexes exist.

                if (problemDetected) {
                    issues.add(new Issue(
                        IssueType.MY_NEW_ISSUE,
                        Severity.WARNING,
                        query.sql(),
                        "table_name",
                        "column_name",
                        "Detailed explanation of the problem",
                        "Suggested fix"
                    ));
                }
            }

            return issues;
        }
    }
    ```

2. **Add an IssueType** enum constant in `IssueType.java`:

    ```java
    MY_NEW_ISSUE("my-new-issue", "Description of the new issue", WARNING),
    ```

3. **Register the detector** in `QueryAuditAnalyzer.createRules()`:

    ```java
    ruleList.add(new MyNewDetector());
    ```

4. **Add tests** in `query-audit-core/src/test/java/io/queryaudit/core/detector/`:

    ```java
    @Test
    void shouldDetectMyNewIssue() {
        // Arrange: create QueryRecord and IndexMetadata
        // Act: call detector.evaluate()
        // Assert: verify issues are returned
    }
    ```

5. **Update documentation** -- add the new issue type to the detection rules docs
   and the issue types table in `guide/configuration.md`.

### Realistic Example: Detecting UNION without ALL

Here is a complete, realistic example of implementing a detection rule:

```java
package io.queryaudit.core.detector;

import io.queryaudit.core.model.*;
import java.util.*;
import java.util.regex.Pattern;

public class UnionWithoutAllDetector implements DetectionRule {

    // Match UNION that is NOT followed by ALL (case-insensitive)
    private static final Pattern UNION_WITHOUT_ALL =
        Pattern.compile("\\bUNION\\b(?!\\s+ALL\\b)", Pattern.CASE_INSENSITIVE);

    @Override
    public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
        List<Issue> issues = new ArrayList<>();
        Set<String> seen = new HashSet<>();  // Deduplicate by normalized SQL

        for (QueryRecord query : queries) {
            String sql = query.normalizedSql();
            if (!sql.contains("union") || seen.contains(sql)) {
                continue;  // Fast path: skip queries without UNION
            }

            if (UNION_WITHOUT_ALL.matcher(sql).find()) {
                seen.add(sql);
                issues.add(new Issue(
                    IssueType.UNION_WITHOUT_ALL,
                    Severity.INFO,
                    sql,
                    null,   // table
                    null,   // column
                    "UNION removes duplicates (like DISTINCT), which requires "
                        + "sorting. If duplicates are acceptable, UNION ALL is faster.",
                    "Use UNION ALL unless you specifically need duplicate removal."
                ));
            }
        }

        return issues;
    }
}
```

### Test Patterns: True Positives and False Positives

Every detector needs both **true positive** (should detect) and **false positive**
(should NOT detect) tests:

```java
class UnionWithoutAllDetectorTest {

    private final UnionWithoutAllDetector detector = new UnionWithoutAllDetector();
    private final IndexMetadata emptyMetadata = new IndexMetadata(Map.of());

    // --- True positives: should detect ---

    @Test
    void shouldDetectSimpleUnionWithoutAll() {
        List<QueryRecord> queries = List.of(
            queryRecord("select id from orders union select id from archived_orders")
        );

        List<Issue> issues = detector.evaluate(queries, emptyMetadata);

        assertEquals(1, issues.size());
        assertEquals(IssueType.UNION_WITHOUT_ALL, issues.get(0).type());
    }

    @Test
    void shouldDetectUnionInSubquery() {
        List<QueryRecord> queries = List.of(
            queryRecord("select * from (select id from a union select id from b) t")
        );

        List<Issue> issues = detector.evaluate(queries, emptyMetadata);

        assertEquals(1, issues.size());
    }

    // --- False positives: should NOT detect ---

    @Test
    void shouldNotFlagUnionAll() {
        List<QueryRecord> queries = List.of(
            queryRecord("select id from orders union all select id from archived_orders")
        );

        List<Issue> issues = detector.evaluate(queries, emptyMetadata);

        assertTrue(issues.isEmpty(), "UNION ALL should not be flagged");
    }

    @Test
    void shouldNotFlagQueryWithoutUnion() {
        List<QueryRecord> queries = List.of(
            queryRecord("select * from orders where status = ?")
        );

        List<Issue> issues = detector.evaluate(queries, emptyMetadata);

        assertTrue(issues.isEmpty());
    }

    @Test
    void shouldDeduplicateSameQuery() {
        List<QueryRecord> queries = List.of(
            queryRecord("select id from a union select id from b"),
            queryRecord("select id from a union select id from b")
        );

        List<Issue> issues = detector.evaluate(queries, emptyMetadata);

        assertEquals(1, issues.size(), "Should deduplicate identical queries");
    }

    // Helper method
    private QueryRecord queryRecord(String sql) {
        return new QueryRecord(sql, sql, 1_000_000L, System.nanoTime(), "", 0);
    }
}
```

!!! tip "Test naming convention"
    Use `shouldDetect*` for true positive tests and `shouldNotFlag*` or `shouldNotDetect*`
    for false positive tests. This makes it clear at a glance what each test validates.

---

## How to Add a Custom Detection Rule (External)

Users can add custom detection rules **without modifying QueryAudit source code**
using either ServiceLoader auto-discovery or programmatic registration.

### Option 1: ServiceLoader auto-discovery

1. **Implement the `DetectionRule` interface** in your own project:

    ```java
    package com.example;

    import io.queryaudit.core.detector.DetectionRule;
    import io.queryaudit.core.model.*;
    import java.util.*;

    /**
     * Detects queries that use database-specific functions our team
     * has banned for portability reasons.
     */
    public class BannedFunctionDetector implements DetectionRule {

        private static final Set<String> BANNED_FUNCTIONS = Set.of(
            "group_concat", "find_in_set", "ifnull"
        );

        @Override
        public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
            List<Issue> issues = new ArrayList<>();
            Set<String> seen = new HashSet<>();

            for (QueryRecord query : queries) {
                String sql = query.normalizedSql();
                if (seen.contains(sql)) continue;

                for (String func : BANNED_FUNCTIONS) {
                    if (sql.contains(func + "(")) {
                        seen.add(sql);
                        issues.add(new Issue(
                            IssueType.SELECT_ALL, // Use closest built-in type
                            Severity.WARNING,
                            sql,
                            null,
                            null,
                            "Query uses banned function: " + func
                                + "(). This function is MySQL-specific and "
                                + "not portable to PostgreSQL.",
                            "Replace " + func + "() with a portable alternative."
                        ));
                        break;
                    }
                }
            }

            return issues;
        }
    }
    ```

2. **Register via `META-INF/services`** -- create the file
   `src/main/resources/META-INF/services/io.queryaudit.core.detector.DetectionRule`
   with the fully qualified class name of your detector:

    ```
    com.example.BannedFunctionDetector
    ```

3. **The rule will be auto-discovered** at runtime. When `QueryAuditAnalyzer`
   initializes, it uses `ServiceLoader` to find all `DetectionRule` implementations
   on the classpath. Your custom rule runs alongside all built-in detectors
   automatically.

### Option 2: Programmatic registration

Pass additional rules directly via the `QueryAuditAnalyzer` constructor:

```java
List<DetectionRule> customRules = List.of(new BannedFunctionDetector());

// With baseline entries
QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, baseline, customRules);

// With baseline file path
QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, baselinePath, customRules);
```

Programmatic rules are appended after built-in and ServiceLoader-discovered rules.

---

## How to Add a New Database

See [Adding Database Support](new-database.md) for a complete step-by-step guide.

In summary:

1. Create a new module (`query-audit-<database>`)
2. Implement `IndexMetadataProvider`
3. Register via `META-INF/services`
4. Optionally implement `ExplainAnalyzer`
5. Add Testcontainers-based integration tests

---

## How to Add a New Reporter

1. **Create a reporter class** implementing the `Reporter` interface from
   `query-audit-core`:

    ```java
    package io.queryaudit.core.reporter;

    import io.queryaudit.core.model.QueryAuditReport;

    public class SlackReporter implements Reporter {

        private final String webhookUrl;

        public SlackReporter(String webhookUrl) {
            this.webhookUrl = webhookUrl;
        }

        @Override
        public void report(QueryAuditReport report) {
            if (report.confirmedIssues().isEmpty()) {
                return;  // Only notify on issues
            }

            String message = String.format(
                "QueryAudit: %d issue(s) in %s.%s",
                report.confirmedIssues().size(),
                report.testClass(),
                report.testName()
            );

            // Send to Slack webhook
            // HttpClient.newHttpClient().send(...);
        }
    }
    ```

2. **Wire it in** -- currently, the `QueryAuditExtension` creates a `ConsoleReporter`
   directly. To support multiple reporters, the extension should be updated to read
   the `report.format` config and instantiate the appropriate reporter.

3. **Add tests** that verify the output format.

---

## Pull Request Process

1. **Fork** the repository and create a feature branch from `main`:

    ```bash
    git checkout -b feat/my-new-feature
    ```

2. **Make your changes** with appropriate tests. Ensure:
    - All existing tests still pass: `./gradlew test`
    - The build succeeds: `./gradlew build`
    - New public APIs have Javadoc
    - Commit messages follow conventional commit format

3. **Write a clear PR description** explaining:
    - What the change does
    - Why it is needed
    - How to test it
    - Any breaking changes

4. **Submit the PR** against the `main` branch.

5. **Address review feedback** -- maintainers may request changes. Push additional
   commits (do not force-push or squash during review).

6. Once approved, a maintainer will merge the PR.

---

## Code Style

- Follow standard Java conventions
- Use meaningful variable and method names
- Write Javadoc for all public APIs
- Keep methods focused and small
- Prefer immutable objects (records, `Collections.unmodifiable*`)
- Use `sealed` interfaces where appropriate (Java 17+)
- Use text blocks for multi-line strings (SQL queries, templates)

---

## License

By contributing, you agree that your contributions will be licensed under the
[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).

---

## See Also

- [Architecture Overview](overview.md) -- Module structure, interfaces, and extension points
- [Adding Database Support](new-database.md) -- Step-by-step guide for new database modules
- [Configuration Reference](../guide/configuration.md) -- All configuration options
