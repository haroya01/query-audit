# Contributing to Query Guard

Thank you for your interest in contributing to Query Guard! This guide will help you get started.

## Prerequisites

- JDK 17 or later
- Docker (for integration tests with real databases)

## Building

```bash
./gradlew build
```

## Running Tests

```bash
./gradlew test
```

Integration tests that require a running database:

```bash
./gradlew integrationTest
```

## Commit Convention

This project uses [Conventional Commits](https://www.conventionalcommits.org/). All commit messages must follow this format:

```
<type>(<scope>): <description>

[optional body]

[optional footer(s)]
```

**Types:** `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`

**Scopes:** `core`, `junit5`, `mysql`, `postgresql`, `spring-boot`

Examples:

```
feat(core): add full table scan detection
fix(mysql): handle EXPLAIN output for subqueries
docs: update README with configuration options
test(junit5): add integration test for @QueryAudit annotation
```

Release Please uses these commits to automatically generate changelogs and determine version bumps.

## Adding a New Database

To add support for a new database (e.g., PostgreSQL), you need to implement two SPI interfaces from `query-audit-core`:

1. **`ExplainAnalyzer`** — Parses the database-specific `EXPLAIN` output and converts it into the common `QueryPlan` model.

2. **`IndexMetadataProvider`** — Retrieves index metadata (existing indexes, column statistics) from the database's information schema.

Steps:

1. Create a new module: `query-audit-<database>`
2. Implement `ExplainAnalyzer` for the database's EXPLAIN format
3. Implement `IndexMetadataProvider` using the database's system catalogs
4. Register implementations via `META-INF/services` (Java SPI)
5. Add integration tests using Testcontainers

## Pull Request Process

1. Fork the repository and create a feature branch from `main`
2. Make your changes with appropriate tests
3. Ensure all tests pass: `./gradlew test`
4. Ensure code compiles cleanly: `./gradlew build`
5. Write a clear PR description explaining the change
6. Submit the pull request against `main`

## Code Formatting

This project uses [Spotless](https://github.com/diffplug/spotless) with [google-java-format](https://github.com/google/google-java-format) to enforce consistent code formatting.

Before committing, run the formatter on your changes:

```bash
./gradlew spotlessApply
```

To check formatting without applying fixes:

```bash
./gradlew spotlessCheck
```

> **Note:** `spotlessCheck` is not yet enforced in CI because the existing codebase has not been fully formatted. For now, please run `spotlessApply` on any files you modify before committing to incrementally bring the codebase into compliance.

## SQL Parser Usage

The project has two parser classes with clear roles:

- **`SqlParser`** — Fast regex-based parser. Use for simple checks: `isSelectQuery`, `hasWhereClause`, `hasSelectAll`, `normalize`, etc.
- **`EnhancedSqlParser`** — JSqlParser-backed parser for complex structural extraction: `extractWhereColumns`, `extractTableNames`, `extractJoinColumns`. Falls back to `SqlParser` regex automatically when JSqlParser is not on the classpath or fails to parse.

When writing a new detector, follow this rule of thumb:
- Pattern checks (does the query have X?) → `SqlParser`
- Column/table extraction (what columns are in the WHERE clause?) → `EnhancedSqlParser`

## Code Style

- Follow [Google Java Style](https://google.github.io/styleguide/javaguide.html) (enforced by Spotless)
- Use meaningful variable and method names
- Write Javadoc for all public APIs
- Keep methods focused and small

## License

By contributing, you agree that your contributions will be licensed under the Apache License 2.0.
