# CI/CD Integration

Use the existing JUnit job to enforce read/write budgets and recorded count contracts. Keep its
JSON report and test log so reviewers can see the policy result and the SQL behind a failure.

| CI check | Require |
|---|---|
| Read/write policy | The test command succeeds and the fresh report has `outcome: PASS`. [First CI check](first-ci-check.md) |
| Required audits | A committed expected-test manifest has no missing evidence. [Coverage setup](audit-coverage.md) |
| Findings across runs | The comparator exits `0` for compatible, complete reports without new confirmed findings. [Compare command](reports.md#delta-verdict-compare-two-runs) |
| Intentional count changes | A reviewed `.query-audit-contracts` diff in the PR. [Record and update](contracts.md) |

The comparator reports total count changes for context; budgets and contracts enforce count
limits. Start with the [first CI check](first-ci-check.md) for a copyable single-test gate.

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
  fail-on-detection: false
  auto-open-report: false
  report:
    format: json
    output-dir: build/reports/query-audit
```

This keeps findings advisory while explicit budgets and contracts still fail tests. Enable
`fail-on-detection` after reviewing the finding rules you want to enforce.

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
        run: |
          rm -f build/reports/query-audit/report.json
          ./gradlew test --rerun-tasks --no-build-cache
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

1. Remove the previous report, then run the tests without preventing artifact and verification steps from executing.
2. Upload `report.json` even when the test command fails, and treat a missing file as an error.
3. Parse the 0.6 suite `outcome` and require `PASS`.
4. Restore the original test command result so unrelated test failures still fail the job.

Do not configure an optional or empty artifact archive for a required QueryAudit gate. A test-engine
failure can happen before the suite finalizer writes the file, so absence is an incomplete result.

## Gradual adoption

Expand the checks as tests become useful:

- Add `@ExpectQueries` to important reads and writes; use explicit zeros for forbidden write types.
- Record [contracts](contracts.md) for established tests whose exact counts should be reviewed.
- Commit an [expected-test manifest](audit-coverage.md) before relying on suite-wide results.
- Compare reports under the same reviewed settings. Retain SQL and call sites when the gate fails.
- Review additional findings with `@EnableQueryInspector` or `fail-on-detection: false`; enable
  finding failures with `@QueryAudit` or `fail-on-detection: true` when ready. `mode: all` extends
  audit activation across the suite.

Finding acknowledgement and query-count baselines solve different problems:

| File | Purpose | Update path |
|---|---|---|
| `.query-audit-baseline` | Acknowledge specific known findings | Review entries as suppressions; see [Suppressing issues](suppressing.md) |
| `.query-audit-counts` | Emit threshold-based count-regression findings | Record intentionally, review the count diff, then rerun normally |
| `.query-audit-contracts` | Enforce exact SELECT, INSERT, UPDATE, and DELETE counts for selected tests | Use the explicit contract recording workflow |

A count baseline emits findings rather than direct assertions. It reports a total-count increase
of at least 50% and five statements, or a SELECT increase of at least 100% and five statements.
These findings follow the configured rule and failure policy; with `@EnableQueryInspector` they
remain advisory in the test run. A new confirmed regression finding can fail the comparator.
Use budgets or contracts when a smaller count change must fail directly.

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
