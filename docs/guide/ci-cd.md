# CI/CD Integration

Query Guard is designed to catch query performance issues before they reach production.
By running as part of your test suite, it integrates naturally into any CI/CD pipeline.

## How It Works

When `fail-on-detection` is `true` (the default), Query Guard throws an `AssertionError`
on confirmed issues (ERROR or WARNING severity). This causes the test to fail, which in
turn causes the CI build to fail -- no extra configuration needed.

```
BUILD FAILED

OrderServiceTest > findRecentOrders_shouldUseIndex FAILED
    java.lang.AssertionError: QueryAudit detected 2 issue(s) in findRecentOrders_shouldUseIndex:

      [ERROR] N+1 Query detected (table: order_items)
        Detail: Query repeated 12 times (threshold: 3)
        Suggestion: Use JOIN FETCH, @EntityGraph, or batch loading (IN clause)

      [ERROR] Missing index on WHERE column (table: orders)
        Detail: Column 'user_id' is used in WHERE clause but has no index
        Suggestion: CREATE INDEX idx_orders_user_id ON orders (user_id);
```

---

## GitHub Actions

### Basic Setup with MySQL

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest

    services:
      mysql:
        image: mysql:8.0
        env:
          MYSQL_ROOT_PASSWORD: test
          MYSQL_DATABASE: testdb
        ports:
          - 3306:3306
        options: >-
          --health-cmd="mysqladmin ping -h localhost"
          --health-interval=10s
          --health-timeout=5s
          --health-retries=5

    steps:
      - uses: actions/checkout@v4

      - name: Set up JDK 17
        uses: actions/setup-java@v4
        with:
          java-version: '17'
          distribution: 'temurin'

      - name: Setup Gradle
        uses: gradle/actions/setup-gradle@v4

      - name: Run tests (including Query Guard analysis)
        run: ./gradlew test
        env:
          SPRING_DATASOURCE_URL: jdbc:mysql://localhost:3306/testdb
          SPRING_DATASOURCE_USERNAME: root
          SPRING_DATASOURCE_PASSWORD: test

      - name: Upload Query Guard reports
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: query-audit-reports
          path: build/reports/query-audit/
          if-no-files-found: ignore
```

!!! tip "The `if: always()` on the upload step"
    This ensures reports are uploaded even when tests fail, so you can review the
    Query Guard output in the build artifacts.

### With PostgreSQL

```yaml
jobs:
  test:
    runs-on: ubuntu-latest

    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_USER: test
          POSTGRES_PASSWORD: test
          POSTGRES_DB: testdb
        ports:
          - 5432:5432
        options: >-
          --health-cmd="pg_isready -U test"
          --health-interval=10s
          --health-timeout=5s
          --health-retries=5

    steps:
      - uses: actions/checkout@v4

      - name: Set up JDK 17
        uses: actions/setup-java@v4
        with:
          java-version: '17'
          distribution: 'temurin'

      - name: Setup Gradle
        uses: gradle/actions/setup-gradle@v4

      - name: Run tests
        run: ./gradlew test
        env:
          SPRING_DATASOURCE_URL: jdbc:postgresql://localhost:5432/testdb
          SPRING_DATASOURCE_USERNAME: test
          SPRING_DATASOURCE_PASSWORD: test

      - name: Upload Query Guard reports
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: query-audit-reports
          path: build/reports/query-audit/
          if-no-files-found: ignore
```

### With Baseline Files

To use baseline-based regression detection in CI, commit the baseline file and
update it explicitly when query counts change intentionally.

```yaml
      - name: Run tests with baseline
        run: ./gradlew test

      # Only update baseline on main branch merges, not PRs
      - name: Update baseline (main only)
        if: github.ref == 'refs/heads/main' && success()
        run: |
          ./gradlew test -DqueryGuard.updateBaseline=true
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add .query-audit-counts
          git diff --cached --quiet || git commit -m "chore: update query-audit baseline"
          git push
```

### With PR Comment (JSON Report Parsing)

Post a summary of Query Guard findings as a PR comment:

```yaml
      - name: Run tests with JSON report
        run: ./gradlew test
        env:
          SPRING_DATASOURCE_URL: jdbc:mysql://localhost:3306/testdb
          SPRING_DATASOURCE_USERNAME: root
          SPRING_DATASOURCE_PASSWORD: test
        continue-on-error: true

      - name: Comment on PR with Query Guard summary
        if: github.event_name == 'pull_request' && always()
        uses: actions/github-script@v7
        with:
          script: |
            const fs = require('fs');
            const glob = require('glob');
            const files = glob.sync('build/reports/query-audit/**/*.json');
            let totalErrors = 0;
            let totalWarnings = 0;
            for (const file of files) {
              const report = JSON.parse(fs.readFileSync(file, 'utf8'));
              totalErrors += report.summary?.confirmedIssues || 0;
            }
            if (totalErrors > 0) {
              await github.rest.issues.createComment({
                owner: context.repo.owner,
                repo: context.repo.repo,
                issue_number: context.issue.number,
                body: `**Query Guard Report**: ${totalErrors} issue(s) detected. Check the [build artifacts](${context.serverUrl}/${context.repo.owner}/${context.repo.repo}/actions/runs/${context.runId}) for details.`
              });
            }
```

---

## GitLab CI

### Basic Setup with MySQL

```yaml
test:
  image: eclipse-temurin:17-jdk
  services:
    - name: mysql:8.0
      alias: mysql
      variables:
        MYSQL_ROOT_PASSWORD: test
        MYSQL_DATABASE: testdb
  variables:
    SPRING_DATASOURCE_URL: "jdbc:mysql://mysql:3306/testdb"
    SPRING_DATASOURCE_USERNAME: root
    SPRING_DATASOURCE_PASSWORD: test
  script:
    - ./gradlew test
  artifacts:
    when: always
    paths:
      - build/reports/query-audit/
    expire_in: 7 days
```

### With PostgreSQL

```yaml
test:
  image: eclipse-temurin:17-jdk
  services:
    - name: postgres:16
      alias: postgres
      variables:
        POSTGRES_USER: test
        POSTGRES_PASSWORD: test
        POSTGRES_DB: testdb
  variables:
    SPRING_DATASOURCE_URL: "jdbc:postgresql://postgres:5432/testdb"
    SPRING_DATASOURCE_USERNAME: test
    SPRING_DATASOURCE_PASSWORD: test
  script:
    - ./gradlew test
  artifacts:
    when: always
    paths:
      - build/reports/query-audit/
    expire_in: 7 days
```

### With Baseline Update on Main

```yaml
test:
  image: eclipse-temurin:17-jdk
  services:
    - name: mysql:8.0
      alias: mysql
      variables:
        MYSQL_ROOT_PASSWORD: test
        MYSQL_DATABASE: testdb
  variables:
    SPRING_DATASOURCE_URL: "jdbc:mysql://mysql:3306/testdb"
    SPRING_DATASOURCE_USERNAME: root
    SPRING_DATASOURCE_PASSWORD: test
  script:
    - ./gradlew test
  artifacts:
    when: always
    paths:
      - build/reports/query-audit/
    expire_in: 7 days

update-baseline:
  image: eclipse-temurin:17-jdk
  stage: deploy
  only:
    - main
  services:
    - name: mysql:8.0
      alias: mysql
      variables:
        MYSQL_ROOT_PASSWORD: test
        MYSQL_DATABASE: testdb
  variables:
    SPRING_DATASOURCE_URL: "jdbc:mysql://mysql:3306/testdb"
    SPRING_DATASOURCE_USERNAME: root
    SPRING_DATASOURCE_PASSWORD: test
  script:
    - ./gradlew test -DqueryGuard.updateBaseline=true
    - git config user.name "GitLab CI"
    - git config user.email "ci@example.com"
    - git add .query-audit-counts
    - git diff --cached --quiet || git commit -m "chore: update query-audit baseline"
    - git push
```

---

## Jenkins

=== "Declarative Pipeline"

    ```groovy
    pipeline {
        agent any

        stages {
            stage('Test') {
                steps {
                    sh './gradlew test'
                }
            }
        }

        post {
            always {
                archiveArtifacts artifacts: 'build/reports/query-audit/**', allowEmptyArchive: true
                junit 'build/test-results/test/*.xml'
            }
        }
    }
    ```

=== "Scripted Pipeline"

    ```groovy
    node {
        stage('Checkout') {
            checkout scm
        }
        stage('Test') {
            try {
                sh './gradlew test'
            } finally {
                archiveArtifacts artifacts: 'build/reports/query-audit/**', allowEmptyArchive: true
                junit 'build/test-results/test/*.xml'
            }
        }
    }
    ```

---

## Maven Projects

All examples above use Gradle. For Maven projects, replace `./gradlew test` with:

```bash
mvn test
```

And adjust artifact paths from `build/reports/query-audit/` to `target/reports/query-audit/`.

---

## Recommended CI Configuration

For CI environments, use these application.yml settings:

```yaml
query-audit:
  enabled: true
  fail-on-detection: true
  auto-open-report: false              # No browser in CI
  report:
    format: console
    output-dir: build/reports/query-audit
  suppress-queries:
    - "SELECT 1"                       # Health-check queries
```

!!! tip "Separate CI profile"
    Create a `src/test/resources/application-ci.yml` with CI-specific settings
    and activate it with `SPRING_PROFILES_ACTIVE=ci` in your CI environment.

---

## Strategies for Gradual Adoption

If you are introducing Query Guard to a large existing project, you may not want every
pre-existing issue to break the build immediately. Here are some strategies:

### 1. Report-Only Mode First

Start with `fail-on-detection: false` to see what Query Guard finds without failing
any builds:

```yaml
query-audit:
  fail-on-detection: false
```

Review the reports, fix what you can, and then switch to `true`.

### 2. Fail Only on Critical Issues

Use `@QueryAudit(failOn = {...})` to limit which issue types cause failures:

```java
@QueryAudit(failOn = {IssueType.N_PLUS_ONE, IssueType.MISSING_WHERE_INDEX})
@SpringBootTest
class OrderServiceTest { }
```

### 3. Suppress Known Issues

Suppress issues that are known and accepted, then enable `fail-on-detection` for
everything else:

```yaml
query-audit:
  fail-on-detection: true
  suppress-patterns:
    - "select-all"
    - "missing-where-index:legacy_table.old_column"
```

### 4. Use Baseline for Regression Detection

Establish a baseline of existing issues, then only fail on new regressions:

```bash
# First run: create the baseline
./gradlew test -DqueryGuard.updateBaseline=true

# Subsequent runs: detect regressions against baseline
./gradlew test
```

Commit the `.query-audit-counts` baseline file to your repository so all team members
and CI use the same baseline.

---

## See Also

- [Configuration Reference](configuration.md) -- All CI-relevant configuration options
- [Reports](reports.md) -- Understanding report formats and output
- [Suppressing Issues](suppressing.md) -- Suppressing known issues in CI
- [Troubleshooting](troubleshooting.md) -- Fixing CI-specific issues
