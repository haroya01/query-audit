# Contributing to QueryAudit

Thank you for considering contributing to QueryAudit! Whether it's a bug report, new detector, documentation fix, or feature request, every contribution is welcome.

---

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [How to Report a Bug](#how-to-report-a-bug)
- [How to Suggest a Feature](#how-to-suggest-a-feature)
- [Development Setup](#development-setup)
- [Building and Testing](#building-and-testing)
- [Project Structure](#project-structure)
- [Adding a New Detector](#adding-a-new-detector)
- [Adding a New Database](#adding-a-new-database)
- [SQL Parser Usage](#sql-parser-usage)
- [Coding Conventions](#coding-conventions)
- [Commit Message Guidelines](#commit-message-guidelines)
- [Pull Request Process](#pull-request-process)
- [Good First Issues](#good-first-issues)
- [License](#license)

---

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/).
Please be respectful and constructive in all interactions.

---

## How to Report a Bug

1. Check [existing issues](https://github.com/haroya01/query-audit/issues) to avoid duplicates.
2. Open a new issue using the **Bug Report** template.
3. Include:
   - QueryAudit version
   - Java version and database type (MySQL / PostgreSQL)
   - The SQL query that triggered the issue
   - Expected vs actual behavior
   - **If it's a false positive**: include why the query is correct and should not be flagged.

> **False positive reports are especially valuable.** Reducing false positives is our top priority.

---

## How to Suggest a Feature

1. Open a [GitHub Issue](https://github.com/haroya01/query-audit/issues) **before writing code**.
2. Describe the use case, not just the solution.
3. For new detection rules, include:
   - Example SQL that should be detected
   - Example SQL that should NOT be detected (false positive cases)
   - Why this pattern is problematic

---

## Development Setup

### Prerequisites

- **JDK 17** or later
- **Docker** (for integration tests using Testcontainers)
- **Git**

### Clone and Build

```bash
git clone https://github.com/haroya01/query-audit.git
cd query-audit
./gradlew build
```

### IDE Setup

| IDE | How to import |
|-----|--------------|
| **IntelliJ IDEA** | File > Open > select root `build.gradle` |
| **VS Code** | Open folder + install Java Extension Pack |
| **Eclipse** | Import > Gradle > Existing Gradle Project |

---

## Building and Testing

```bash
# Full build (compile + test)
./gradlew build

# Unit tests only
./gradlew test

# Single module
./gradlew :query-audit-core:test
./gradlew :query-audit-mysql:test
./gradlew :query-audit-postgresql:test

# Integration tests (requires Docker)
./gradlew integrationTest

# Mutation testing (70% threshold)
./gradlew :query-audit-core:pitest

# Code formatting
./gradlew spotlessApply
```

### Test Categories

| Test | Purpose |
|------|---------|
| `*DetectorTest` | Per-detector true positive + false positive cases |
| `FalsePositiveFixTest` | Regression tests for fixed false positives |
| `AdversarialFalsePositiveTest` | Edge-case false positive prevention |
| `OpenSourceCorpusTest` | Real-world queries from PetClinic, JHipster, Keycloak |
| `RealWorldCorpusBenchmarkTest` | Precision metrics measurement |

---

## Project Structure

```
query-audit/
├── query-audit-core/                Core analysis engine
│   └── io/queryaudit/core/
│       ├── detector/                DetectionRule implementations (64+ rules)
│       ├── parser/                  SqlParser & EnhancedSqlParser
│       ├── model/                   Issue, QueryRecord, Severity, IssueType
│       ├── config/                  QueryAuditConfig
│       └── reporter/               Console, JSON, HTML reporters
│
├── query-audit-mysql/               MySQL index metadata + EXPLAIN
├── query-audit-postgresql/          PostgreSQL index metadata + EXPLAIN
├── query-audit-junit5/              JUnit 5 extension (@QueryAudit)
└── query-audit-spring-boot-starter/ Spring Boot auto-configuration
```

---

## Adding a New Detector

This is the most common type of contribution. Follow these steps:

### 1. Create the detector

Create a class in `query-audit-core/src/main/java/io/queryaudit/core/detector/`:

```java
package io.queryaudit.core.detector;

import io.queryaudit.core.model.*;
import java.util.*;

public class MyNewDetector implements DetectionRule {

    @Override
    public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
        List<Issue> issues = new ArrayList<>();

        for (QueryRecord query : queries) {
            String sql = query.normalizedSql();

            if (detectsProblem(sql)) {
                issues.add(new Issue(
                    IssueType.MY_NEW_ISSUE,
                    Severity.WARNING,
                    sql,
                    "table_name",
                    "column_name",
                    "Explanation of the problem",
                    "Suggested fix"
                ));
            }
        }
        return issues;
    }
}
```

### 2. Add an IssueType

In `IssueType.java`:

```java
MY_NEW_ISSUE("my-new-issue", "Description of the issue", WARNING),
```

### 3. Register the detector

In `QueryAuditAnalyzer.createRules()`:

```java
ruleList.add(new MyNewDetector());
```

### 4. Write tests

Every detector **must** have both types of tests:

```java
// True positive: should detect
@Test
void shouldDetectProblem() {
    List<QueryRecord> queries = List.of(queryRecord("SELECT ..."));
    List<Issue> issues = detector.evaluate(queries, emptyMetadata);
    assertThat(issues).hasSize(1);
}

// False positive: should NOT detect
@Test
void shouldNotFlagNormalQuery() {
    List<QueryRecord> queries = List.of(queryRecord("SELECT ..."));
    List<Issue> issues = detector.evaluate(queries, emptyMetadata);
    assertThat(issues).isEmpty();
}
```

> **Naming convention**: Use `shouldDetect*` for true positives, `shouldNotFlag*` for false positives.

### 5. Verify false positive suites pass

```bash
./gradlew :query-audit-core:test --tests "*FalsePositive*"
./gradlew :query-audit-core:test --tests "*OpenSourceCorpus*"
```

> **Important**: If your detector triggers false positives on real-world queries,
> narrow the detection conditions or downgrade severity to `INFO`.

---

## Adding a New Database

To add support for a new database (e.g., Oracle, SQL Server):

1. Create a new module: `query-audit-<database>`
2. Implement `IndexMetadataProvider` using the database's system catalogs
3. Implement `ExplainAnalyzer` for the database's EXPLAIN format
4. Register via `META-INF/services` (Java SPI)
5. Add integration tests using Testcontainers

See [Adding Database Support](docs/architecture/new-database.md) for the full guide.

---

## SQL Parser Usage

The project has two parser classes:

| Parser | Backed by | Use when |
|--------|-----------|----------|
| `SqlParser` | Regex | Simple pattern checks (`isSelectQuery`, `hasWhereClause`, `hasSelectAll`) |
| `EnhancedSqlParser` | JSQLParser | Structural extraction (`extractWhereColumns`, `extractTableNames`, `extractJoinColumns`) |

`EnhancedSqlParser` falls back to `SqlParser` automatically when JSQLParser fails to parse.

---

## Coding Conventions

- **Detectors are stateless** — no mutable instance fields
- Use `query.normalizedSql()` for deduplication
- Use `SqlParser` static methods for SQL analysis
- Follow [Google Java Format](https://github.com/google/google-java-format) — run `./gradlew spotlessApply`
- Write Javadoc for all public classes and methods
- Prefer records and immutable collections (`List.of()`, `Map.of()`)
- Keep methods small and focused

---

## Commit Message Guidelines

We use [Conventional Commits](https://www.conventionalcommits.org/) for automated versioning and changelog generation.

### Format

```
<type>(<scope>): <short description>

[optional body]

[optional footer]
```

### Types

| Type | Description | Version bump |
|------|------------|-------------|
| `feat` | New feature | minor |
| `fix` | Bug fix | patch |
| `perf` | Performance improvement | patch |
| `docs` | Documentation only | none |
| `test` | Adding/updating tests | none |
| `refactor` | Code change (no feature/fix) | none |
| `ci` | CI/CD changes | none |
| `chore` | Maintenance | none |

### Scopes

`core`, `junit5`, `mysql`, `postgresql`, `spring-boot`

### Examples

```
feat(core): add composite index leading column detection
fix(mysql): handle empty SHOW INDEX result for views
test(core): add false positive tests for WhereFunctionDetector
docs: update installation guide for PostgreSQL
```

### Breaking Changes

Add a `BREAKING CHANGE:` footer for major version bumps:

```
feat(core): change DetectionRule return type to Stream

BREAKING CHANGE: DetectionRule.evaluate() now returns Stream<Issue> instead of List<Issue>
```

---

## Pull Request Process

1. **Fork** the repository and create a branch from `main`:
   ```bash
   git checkout -b feat/my-feature
   ```

2. **Make changes** with tests. Before submitting:
   ```bash
   ./gradlew build          # Full build passes
   ./gradlew spotlessApply  # Code is formatted
   ```

3. **Open a PR** against `main` with:
   - Clear description of what and why
   - Link to related issue (if any)
   - How to test the change

4. **CI must pass** — Java 17 and 21 matrix build.

5. **Address review feedback** — push additional commits, don't force-push during review.

6. A maintainer will merge once approved.

### PR Checklist

- [ ] `./gradlew build` passes
- [ ] New code has tests (both true positive and false positive)
- [ ] False positive test suites still pass
- [ ] Commit messages follow conventional commits format
- [ ] Javadoc added for new public APIs
- [ ] Documentation updated if user-facing behavior changed

---

## Good First Issues

Look for issues labeled [`good first issue`](https://github.com/haroya01/query-audit/labels/good%20first%20issue). Great starting points:

- Adding false positive test cases for existing detectors
- Improving detection messages and suggestions
- Documentation improvements
- Adding edge-case tests for known problematic detectors

---

## License

By contributing, you agree that your contributions will be licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
