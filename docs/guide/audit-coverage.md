# Audit coverage

A green build is useful only if the tests you rely on actually ran. A test filter, `@Disabled`,
a setup failure, or a removed audit annotation can otherwise leave a run with fewer findings
simply because it audited less code.

QueryAudit can check a version-controlled list of expected JUnit Jupiter test IDs against each
test execution. Any expected test that fails to provide complete audit evidence makes the run
`INCONCLUSIVE`. This check describes the tests in your manifest; it does not prove that every
database path in the application is covered.

## Create the expected-test manifest

First, run the full suite with your intended audit annotations and policies enabled, without a
coverage manifest. Do not bootstrap from a filtered `--tests` run. Select JSON output and one
test JVM so the report contains the complete selection.

For a Gradle project, configure the test task in `build.gradle`:

```groovy
tasks.named('test', Test) {
    useJUnitPlatform()
    forkEvery = 0
    maxParallelForks = 1
    systemProperty 'queryAudit.reportFormat', 'json'
    systemProperty 'queryAudit.reportOutputDir',
        layout.buildDirectory.dir('reports/query-audit').get().asFile.absolutePath
}
```

The properties above must reach the **test JVM**. Passing `-DqueryAudit.reportOutputDir=...` to
Gradle alone does not forward it to forked tests. Use the explicit `systemProperty` configuration.

Generate a candidate from the first complete suite report:

```bash
set -euo pipefail
./gradlew clean test
jq -er '.reports[].testId' build/reports/query-audit/report.json \
    | LC_ALL=C sort -u > .query-audit-tests.candidate
```

Review the candidate against the audit tests you intend to require. The report can only list
tests that produced an audit, so it cannot identify a missing annotation or an excluded test for
you. Add the reviewed file to the repository:

```bash
mv .query-audit-tests.candidate .query-audit-tests
git add .query-audit-tests
git commit -m "test: record expected query audit coverage"
```

The manifest contains one exact Jupiter unique ID per line. Blank lines and whole-line `#`
comments are allowed:

```text
# Query budgets required for the order service
[engine:junit-jupiter]/[class:com.example.OrderServiceTest]/[method:findRecentOrders()]
[engine:junit-jupiter]/[class:com.example.OrderServiceTest]/[test-template:findByStatus(java.lang.String)]/[test-template-invocation:#1]
[engine:junit-jupiter]/[class:com.example.OrderServiceTest]/[test-template:findByStatus(java.lang.String)]/[test-template-invocation:#2]
```

IDs are opaque identities, not display names or class selectors. Copy them from `reports[].testId`
instead of constructing them by hand. Wildcards and inline comments are not supported. Duplicate
IDs, an empty manifest, or malformed IDs make the manifest invalid. Parameterized tests need
one entry per invocation; changes to argument order or count may require a deliberate manifest
update.

## Enable coverage checks

QueryAudit automatically uses `.query-audit-tests` when it exists in the test JVM's working
directory. In a typical Gradle project this is the project directory. To choose another file,
pass `queryAudit.coverageManifest` to the test JVM:

```groovy
tasks.named('test', Test) {
    systemProperty 'queryAudit.coverageManifest',
        file('config/query-audit-tests.txt').absolutePath
}
```

With no explicit property and no default file, coverage is disabled and the JSON envelope
contains `"coverage": null`. This means **manifest coverage was not verified**, even when the
outcome is `PASS`. An explicitly configured path that is missing, blank, unreadable, or invalid
produces `INCONCLUSIVE` with `COVERAGE_MANIFEST_UNREADABLE`; it does not disable the check.

Coverage uses the JUnit Platform listener included in `query-audit-junit5`. Keep automatic
listener registration enabled. The listener observes the test plan even when every selected
test is disabled or no test reaches `QueryAuditExtension`.

With a manifest enabled, QueryAudit always writes `report.json`, including when the selected
report format is HTML or console. Set `queryAudit.reportOutputDir` as above to give the listener
the same output directory when no extension runs. A Spring-only output-directory setting cannot
supply that path if Spring never starts.

## Require the artifact in CI

Run the tests and require a fresh, passing report with verified coverage:

```bash
set -euo pipefail
./gradlew clean test
jq -e '.outcome == "PASS" and .coverage != null and .coverage.failedToAudit == 0' \
    build/reports/query-audit/report.json > /dev/null
```

The `clean` step prevents an old passing report from satisfying the gate after a run that produced
no artifact. If your build does not use `clean`, remove the previous report before launching the
tests. Keep both the build exit check and the JSON gate: a launcher can succeed when no expected
tests ran, and a listener cannot reliably change its exit status. A missing or unreadable JSON
artifact must fail the job.

**Do not generate or replace the manifest in CI.** Regenerating it from the current report would
accept the very test disappearance this check is intended to catch. Change the committed manifest
only after reviewing an intentional test addition, removal, rename, or parameter change.

Coverage is reconciled within one JUnit execution in one JVM. Use `forkEvery = 0` and
`maxParallelForks = 1` for the task above. If the suite is split across JVMs, modules, or CI shards,
give each execution its own manifest and output directory, and gate every report. Sharing a
whole-suite manifest across workers makes each partial execution appear incomplete; sharing an
output directory can also overwrite another worker's report. Run separate audit tasks with
separate artifacts. This feature does not add support for concurrent audit execution within a JVM.

## Read the result

Schema 1.5 includes a required, nullable `coverage` field. A fully audited manifest can produce:

```json
{
  "expected": 1,
  "executed": 1,
  "skipped": 0,
  "audited": 1,
  "failedToAudit": 0,
  "tests": [
    {
      "testId": "[engine:junit-jupiter]/[class:com.example.OrderServiceTest]/[method:findRecentOrders()]",
      "expected": true,
      "executed": true,
      "audited": true,
      "gap": null
    }
  ]
}
```

This object appears inside the [suite report envelope](reports.md#json-schema). Counts cover the
union of manifest entries and emitted audit reports, not every unrelated test in the launcher.

| Field | Meaning |
|---|---|
| `expected` | Number of IDs in the manifest. |
| `executed` | Number of entries whose test execution started. |
| `skipped` | Expected tests marked `SKIPPED`, including tests in a skipped container. |
| `audited` | Number of per-test audit reports. A failed or aborted test can still have a partial report. |
| `failedToAudit` | Expected tests with a non-null `gap`. |
| `tests` | Expected tests and any additional tests that produced reports. Extra audited tests have `expected: false` and do not themselves create a gap. |

`executed` and `audited` are separate: a test can run without its audit extension, and a test can
produce an audit report before an assertion fails. Each expected test's `gap` explains why its
evidence was incomplete:

| Gap | Meaning |
|---|---|
| `NOT_DISCOVERED` | The expected ID was absent from discovery and dynamic registration, for example because a filter excluded it or it was removed. |
| `NOT_EXECUTED` | The expected test was discovered but did not start. |
| `SETUP_FAILED` | A containing class or other test container failed before the expected test started. |
| `SKIPPED` | The expected test or its containing class was skipped or disabled. |
| `ABORTED` | The test started but aborted, for example because an assumption was not satisfied. |
| `TEST_FAILED` | The test started but failed independently of QueryAudit's audit policies. |
| `AUDIT_MISSING` | The test completed without a per-test audit report, for example after its audit annotation was removed. |

Any gap adds an `EXPECTED_TEST_MISSING` reason and makes the suite `INCONCLUSIVE`. A completed
audit that fails a query budget or another QueryAudit policy remains `FAIL` with complete
coverage. Coverage does not turn a policy failure into a pass, or make retained partial findings
disappear. If several conditions apply, the report records one gap for each expected ID.

Report comparison also checks coverage. Different expected-ID sets, or a verified manifest on
only one side, produce `COVERAGE_MANIFEST_MISMATCH`. Comparing two unverified reports remains
possible, but establishes no manifest coverage guarantee. Keep the artifact gate above even when
your workflow also compares findings against a baseline.
