# CI/CD Integration

QueryAudit runs inside the JUnit test process, so the existing test job remains the build gate. CI
adds two responsibilities: select a machine-readable report and retain it even when the tests fail.

!!! note "Version scope"
    The `outcome` checks, selectable suite format, configurable output directory, and fail-on-write
    behavior on this page require QueryAudit 0.6. After a session with at least one completed
    audited result, QueryAudit 0.5 writes both HTML and schema 1.0 JSON and does not include a suite
    outcome.

## Select JSON for CI

For Spring Boot, keep the CI settings in `src/test/resources/application-ci.yml`:

```yaml
query-audit:
  enabled: true
  fail-on-detection: true
  auto-open-report: false
  report:
    format: json
    output-dir: build/reports/query-audit
```

Activate the profile with `SPRING_PROFILES_ACTIVE=ci`. QueryAudit writes one aggregate file to
`build/reports/query-audit/report.json` after the participating test classes finish.

## Plain JUnit build-tool setup

Plain JUnit projects configure the same value as a test-JVM system property. Maven forwards a user
property passed with `-D` to the test process:

```bash
mvn test -DqueryAudit.reportFormat=json
```

Gradle does not forward command-line system properties to forked `Test` workers by default. Add a
small project-property bridge once, then use the `-P` commands throughout this guide:

=== "Groovy DSL"

    ```groovy
    def queryAuditTestProperties = [
        queryAuditReportFormat: 'queryAudit.reportFormat',
        queryAuditMode: 'queryAudit.mode',
        queryAuditUpdateBaseline: 'queryAudit.updateBaseline',
        queryAuditContractsRecord: 'queryAudit.contracts.record',
        queryAuditContractsPath: 'queryAudit.contractsPath',
        queryAuditCountBaselinePath: 'queryAudit.countBaselinePath',
        queryAuditAutoOpenReport: 'queryaudit.autoOpenReport'
    ]

    tasks.withType(Test).configureEach {
        queryAuditTestProperties.each { projectProperty, systemPropertyName ->
            def value = providers.gradleProperty(projectProperty)
            if (value.isPresent()) {
                systemProperty systemPropertyName, value.get()
            }
        }
    }
    ```

=== "Kotlin DSL"

    ```kotlin
    val queryAuditTestProperties = mapOf(
        "queryAuditReportFormat" to "queryAudit.reportFormat",
        "queryAuditMode" to "queryAudit.mode",
        "queryAuditUpdateBaseline" to "queryAudit.updateBaseline",
        "queryAuditContractsRecord" to "queryAudit.contracts.record",
        "queryAuditContractsPath" to "queryAudit.contractsPath",
        "queryAuditCountBaselinePath" to "queryAudit.countBaselinePath",
        "queryAuditAutoOpenReport" to "queryaudit.autoOpenReport"
    )

    tasks.withType<Test>().configureEach {
        for ((projectProperty, systemPropertyName) in queryAuditTestProperties) {
            providers.gradleProperty(projectProperty).orNull?.let {
                systemProperty(systemPropertyName, it)
            }
        }
    }
    ```

A plain JUnit JSON run is now:

```bash
./gradlew test -PqueryAuditReportFormat=json
```

## GitHub Actions with MySQL

This workflow preserves both test failures and the QueryAudit report. The final step rejects a
missing report, a non-`PASS` audit outcome, invalid JSON, or an unrelated test failure.

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

permissions:
  contents: read

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

      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: '17'

      - uses: gradle/actions/setup-gradle@v4

      - name: Run tests
        id: tests
        continue-on-error: true
        run: ./gradlew test
        env:
          SPRING_PROFILES_ACTIVE: ci
          SPRING_DATASOURCE_URL: jdbc:mysql://localhost:3306/testdb
          SPRING_DATASOURCE_USERNAME: root
          SPRING_DATASOURCE_PASSWORD: test

      - name: Upload QueryAudit report
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: query-audit-report
          path: build/reports/query-audit/report.json
          if-no-files-found: error

      - name: Verify test and audit outcomes
        if: always()
        env:
          TEST_OUTCOME: ${{ steps.tests.outcome }}
        run: |
          python3 - <<'PY'
          import json
          import os
          import sys
          from pathlib import Path

          path = Path("build/reports/query-audit/report.json")
          if not path.is_file():
              sys.exit("QueryAudit report.json is missing")

          report = json.loads(path.read_text())
          outcome = report.get("outcome")
          if outcome != "PASS":
              sys.exit(f"QueryAudit outcome is {outcome!r}")
          if os.environ["TEST_OUTCOME"] != "success":
              sys.exit("The test task failed")
          PY
```

When `GITHUB_ACTIONS=true`, QueryAudit also emits native error, warning, and notice commands and
adds a Markdown step summary. No comment-writing permission is needed for those annotations.

If you add a bot-authored PR comment, grant `pull-requests: write` only to the job that posts it.
Tokens for fork pull requests are read-only by default; do not move untrusted pull-request code into
a privileged workflow without reviewing the security boundary.

## PostgreSQL service

Use the same steps and replace the service and datasource variables:

```yaml
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

env:
  SPRING_PROFILES_ACTIVE: ci
  SPRING_DATASOURCE_URL: jdbc:postgresql://localhost:5432/testdb
  SPRING_DATASOURCE_USERNAME: test
  SPRING_DATASOURCE_PASSWORD: test
```

## Maven jobs

The Spring profile above works without build-tool-specific flags:

```bash
SPRING_PROFILES_ACTIVE=ci mvn test
```

The configured output directory remains `build/reports/query-audit/` for both Maven and Gradle.
Change `report.output-dir` to `target/query-audit/` if the Maven job should keep all generated
artifacts under `target`.

## Other CI systems

Use the same sequence in GitLab CI, Jenkins, Buildkite, or another runner:

1. Run the tests without preventing the artifact and verification steps from executing.
2. Upload `report.json` even when the test command fails, and treat a missing file as an error.
3. Parse the 0.6 suite `outcome` and require `PASS`.
4. Restore the original test command result so unrelated test failures still fail the job.

Do not configure an optional or empty artifact archive for a required QueryAudit gate. A test-engine
failure can happen before the suite finalizer writes the file, so absence is an incomplete result.

## Gradual adoption

Start with a contract the team can explain:

- Set `fail-on-detection: false` or use `@EnableQueryInspector` while reviewing existing findings.
- Use the `recommended` profile to omit context-dependent style rules from the first pass.
- Add `@ExpectQueries` or `@ExpectMaxQueryCount` to established paths before widening coverage.
- Move to `@QueryAudit`, `fail-on-detection: true`, or `mode: all` as the accepted surface grows.

Finding acknowledgement and query-count baselines solve different problems:

| File | Purpose | Update path |
|---|---|---|
| `.query-audit-baseline` | Acknowledge specific known findings | Review entries as suppressions; see [Suppressing issues](suppressing.md) |
| `.query-audit-counts` | Detect count increases for tests without a stronger inline budget | Record intentionally, review the count diff, then rerun normally |
| `.query-audit-contracts` | Enforce exact SELECT, INSERT, UPDATE, and DELETE counts for selected tests | Use the explicit contract recording workflow |

With the Gradle bridge above, record a query-count baseline locally with:

```bash
./gradlew test -PqueryAuditUpdateBaseline=true
```

The recording run can still report the old baseline as a regression. Review the resulting
`.query-audit-counts` diff, then rerun `./gradlew test` and commit the file in the same change that
justifies the new counts. Do not let a pull-request job push baseline changes automatically.

For snapshot contracts, use `-PqueryAuditContractsRecord=true` and follow the
[query contract workflow](contracts.md).

## See also

- [Configuration reference](configuration.md)
- [Reports and comparison](reports.md)
- [Query contracts](contracts.md)
- [Suppressing known findings](suppressing.md)
- [Troubleshooting](troubleshooting.md)
