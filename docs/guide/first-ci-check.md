---
title: Your First CI Check
description: Fail CI on unexpected reads or writes and keep the SQL evidence from the run.
---

# Your First CI Check

Run one read/write budget test, retain its SQL evidence, and require both a successful test
command and a fresh JSON report with `outcome: PASS`.

| Run result | CI result |
|---|---|
| Read/write counts stay within the budget and the audit completes | Pass |
| A listed write occurs or the SELECT limit is exceeded | Fail; inspect the SQL and captured call site in the test log |
| Capture is incomplete, the report is missing, or the test command fails | Fail; restore a complete run before accepting it |

This guide assumes your application already has a working database-backed test job and the
[test dependencies](../getting-started/installation.md). The JSON outcome gate needs the report
contract described in [Versions and compatibility](../getting-started/versions.md); a legacy
report without `outcome` will fail this check.

## 1. Verify capture with one small test

Place this class under `src/test/java` in your application's package, adding your package
declaration above the imports. It uses your existing Spring
Boot application and test database; it does not need an entity or a new table.

```java
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.queryaudit.junit5.EnableQueryInspector;
import io.queryaudit.junit5.ExpectQueries;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootTest
@EnableQueryInspector
class QueryBudgetTest {

    @Autowired
    JdbcTemplate jdbc;

    @Test
    @ExpectQueries(select = 1, insert = 0, update = 0, delete = 0)
    void oneReadDoesNotWrite() {
        assertEquals(1, jdbc.queryForObject("SELECT 1", Integer.class));
    }
}
```

The budget permits at most one SELECT and no INSERT, UPDATE, or DELETE. `@EnableQueryInspector`
keeps detected findings advisory; the explicit budget still fails when exceeded. Check the console
once to confirm that this test captured `SELECT 1`.

To check that the budget is active, temporarily execute the same SELECT a second time. The test
must fail with this diagnostic (excerpt):

```text
QueryAudit: oneReadDoesNotWrite() exceeded its query budget.
SELECT: executed 2, expected at most 1.
```

Remove the extra call before keeping this test.

After verifying the setup, replace `jdbc.queryForObject(...)` with a real service or repository
call and assert its result. Choose a budget for that operation. The smoke test alone does not
protect application behavior. For JPA write checks, flush pending changes inside the test's
transaction so the SQL executes within the audited test body.

## 2. Ask the test process for JSON

For Spring Boot, create `src/test/resources/application-query-audit-ci.yml`:

```yaml
query-audit:
  enabled: true
  fail-on-detection: false
  auto-open-report: false
  report:
    format: json
    output-dir: build/reports/query-audit
```

Activate it with `SPRING_PROFILES_ACTIVE=query-audit-ci`. If your database configuration lives in
another profile, include both, for example `SPRING_PROFILES_ACTIVE=test,query-audit-ci`. The script
below appends `query-audit-ci` to any profiles already set in the environment. Keep your existing CI
database credentials and service setup.

??? info "Plain JUnit: pass settings into the Gradle test JVM"
    Use the [plain JUnit capture setup](../getting-started/installation.md#plain-junit-5) instead of
    the Spring test above. A command such as `./gradlew test -DqueryAudit.reportFormat=json` does
    not automatically forward the property to Gradle's forked test JVM. Configure the `test` task:

    === "Kotlin DSL"

        ```kotlin
        tasks.named<Test>("test") {
            systemProperty("queryAudit.reportFormat", "json")
            systemProperty("queryAudit.reportOutputDir", "build/reports/query-audit")
            systemProperty("queryaudit.autoOpenReport", "false")
        }
        ```

    === "Groovy DSL"

        ```groovy
        tasks.named('test', Test) {
            systemProperty 'queryAudit.reportFormat', 'json'
            systemProperty 'queryAudit.reportOutputDir', 'build/reports/query-audit'
            systemProperty 'queryaudit.autoOpenReport', 'false'
        }
        ```

    The lowercase `queryaudit` in the last property is intentional. With these settings, omit the
    Spring profile variable from the script below and replace `QueryBudgetTest` in the test filter
    with your actual class name (`FirstAuditTest` for the quick start). For command-line-selectable settings and Maven,
    see [CI build-tool setup](ci-cd.md#plain-junit-build-tool-setup).

## 3. Run the test and check the artifact

Save this as `ci/query-audit.sh` and run it from the application module's directory. It requires
Bash and Python 3. The report path must match the output directory configured above.

```bash
#!/usr/bin/env bash
set -euo pipefail

# A previous successful report must not satisfy this run's gate.
rm -f build/reports/query-audit/report.json

test_exit=0
SPRING_PROFILES_ACTIVE="${SPRING_PROFILES_ACTIVE:+${SPRING_PROFILES_ACTIVE},}query-audit-ci" \
  ./gradlew test --tests '*QueryBudgetTest' --rerun-tasks --no-build-cache \
  || test_exit=$?

TEST_EXIT="$test_exit" python3 - <<'PY'
import json
import os
import sys
from pathlib import Path

path = Path("build/reports/query-audit/report.json")
if not path.is_file():
    sys.exit("QueryAudit failed: this run did not write report.json")

report = json.loads(path.read_text(encoding="utf-8"))
if not isinstance(report, dict):
    sys.exit("QueryAudit failed: expected a JSON report envelope")

errors = []
if os.environ["TEST_EXIT"] != "0":
    errors.append("the test command failed")
if report.get("outcome") != "PASS":
    errors.append(f"audit outcome is {report.get('outcome')!r}")
if not isinstance(report.get("reports"), list) or not report["reports"]:
    errors.append("no audited test results were reported")
if errors:
    sys.exit("QueryAudit failed: " + "; ".join(errors))

print("QueryAudit passed: tests succeeded and the fresh audit is PASS")
PY
```

```bash
bash ci/query-audit.sh
```

Successful gate output:

```text
QueryAudit passed: tests succeeded and the fresh audit is PASS
```

`--rerun-tasks --no-build-cache` makes Gradle execute the test instead of reusing an earlier test
result. Invalid JSON, a missing outcome, `FAIL`, and `INCONCLUSIVE` all fail the script. A successful
JUnit command alone is insufficient: report finalization can fail after the test body succeeds.
Use the final JSON outcome together with the test command's exit status as the gate. A per-test
console line such as `[OK]` can precede a later budget assertion failure and is not the final verdict.

This script targets a single-module Gradle project. In a multi-module build, use the module's test
task, such as `:orders:test`, and the matching module-relative report path in both the deletion and
Python check. If the wrapper is only at the repository root, run the script there.

For Maven, replace the Gradle command with `mvn -Dtest=QueryBudgetTest test`; keep the profile,
artifact cleanup, and Python check. The configured report directory remains the same.

## 4. Keep the report when CI fails

Add these steps to your existing GitHub Actions job after Java and the test database are ready:

```yaml
- name: Check query budget
  run: bash ci/query-audit.sh

- name: Keep QueryAudit evidence
  if: always()
  uses: actions/upload-artifact@v4
  with:
    name: query-audit-report
    path: build/reports/query-audit/report.json
    if-no-files-found: error
```

Use the same module-relative path here if you changed it in the script. The
[CI/CD guide](ci-cd.md) has full MySQL and PostgreSQL job examples.

## When the gate fails

| Result | Next step |
|---|---|
| Query budget exceeded | Read the statement list, fix the extra access, and rerun the same test. Change the budget only for an intended behavior change. |
| `INCONCLUSIVE` | Read `incompleteReasons` in the report and restore the missing capture or input. |
| Missing or invalid report | Check the selected library version, active Spring profile or test-JVM settings, output path, and test logs. |
| Test command failed with a `PASS` audit | Fix the ordinary test failure; a passing audit does not override it. |

`PASS` applies to the reported audits and configured policies. It does not prove that every
database-facing test was audited. Add an [expected-test manifest](audit-coverage.md) when missing or
skipped audits must fail the job.

For more tests, [record count contracts](contracts.md) and review their changes in the PR.
Add [report comparison](reports.md#delta-verdict-compare-two-runs) to check for new findings while
rejecting missing audit evidence or changed comparison inputs. Query-count limits remain enforced
by budgets or contracts; the comparator does not gate the total query-count delta on its own.
