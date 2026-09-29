---
title: Spring Boot read-path policy
description: Add a query policy to a Spring Boot test, verify a failure, and inspect SQL in the audit.
---

# Spring Boot read-path policy

Use your existing database test to enforce a SELECT limit and zero INSERT/UPDATE/DELETEs.
The starter wraps the Spring `DataSource`; these snippets use published `0.6.0`.

## Add the starter

[Copy the test dependencies](installation.md#spring-boot). For budgets and count contracts,
the starter is sufficient. Add a database module when you need its index or EXPLAIN checks.

## Run a controlled first audit

Create `QueryAuditInstallationTest.java` under your application's test package.
Add your package declaration above these imports:

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
class QueryAuditInstallationTest {

    @Autowired
    private JdbcTemplate jdbc;

    @Test
    @ExpectQueries(select = 0, insert = 0, update = 0, delete = 0)
    void capturesOneSelect() {
        assertEquals(1, jdbc.queryForObject("select 1", Integer.class));
    }
}
```

Run only this test:

=== "Gradle"

    ```bash
    ./gradlew test --tests '*QueryAuditInstallationTest'
    ```

=== "Maven"

    ```bash
    mvn -Dtest=QueryAuditInstallationTest test
    ```

**Expected failure:**

```text
QueryAudit: capturesOneSelect() exceeded its query budget.
SELECT: executed 1, expected at most 0.
```

Change `select = 0` to `select = 1` and rerun: **the test should pass**.
If the zero-budget run passes, use the [capture checklist](../guide/troubleshooting.md#queryaudit-not-detecting-any-queries).

## Apply the policy to an existing read test

Keep its functional assertions and add these annotations directly:

```java
@EnableQueryInspector
@ExpectQueries(select = 1, insert = 0, update = 0, delete = 0)
```

Choose the SELECT limit for the service or repository operation. If a change adds an UPDATE,
the policy fails even when the functional assertions still pass:

```text
UPDATE: executed 1, expected at most 0.
```

`@EnableQueryInspector` reports detector findings while explicit budgets remain assertions.
Verify an intentional violation through the real operation before relying on a pass.
The [runnable example](quickstart.md) demonstrates the added-write and added-SELECT failures.

!!! note "Direct annotations and stable contexts"
    Use direct annotations for this installation proof. If you share composed/inherited policies
    or replace a context with `@DirtiesContext`, verify capture through that exact setup.
    See the [reported cases and their reproduction scope](../guide/limitations.md).

## Inspect the result

Add this to the active test profile, for example `src/test/resources/application.yml`:

```yaml
query-audit:
  report:
    format: json
```

Open `build/reports/query-audit/report.json`:

| Read | Use it to |
| --- | --- |
| `outcome` | Check the final `PASS`, `FAIL`, or `INCONCLUSIVE` result |
| `reports[].queries[].sql` | Inspect captured statements |
| `reports[].queries[].stackTrace` | Locate application callers |

For multiple tests, [record their counts in a contract file](../guide/contracts.md) and review
intentional changes in its diff. [Require complete results in CI](../guide/first-ci-check.md),
with [coverage](../guide/audit-coverage.md) and [comparison checks](../guide/comparison-inputs.md)
to keep missing tests or changed audit settings visible.

## Optional settings

| Goal | Setting or annotation |
| --- | --- |
| Review detector findings; enforce explicit budgets | `@EnableQueryInspector` with `@ExpectQueries` |
| Fail on configured detector findings | `@QueryAudit` |
| Choose the rule set | `query-audit.profile: recommended` |
| Generate a browser report locally | `query-audit.report.format: html` |
| Analyze per-test setup/teardown SQL for detector findings | `@QueryAudit(includeSetupQueries = true)`; raw reports and budgets already include captured lifecycle SQL |

See [configuration](../guide/configuration.md) for defaults and overrides. JSON/Actions redaction
does not currently redact console or HTML diagnostics; see [report privacy](../guide/reports.md#machine-report-redaction).

## How DataSource capture works

The starter wraps Spring `DataSource` beans. The JUnit extension attaches the active test's
capture listener to the selected query-aware datasource. Application SQL must pass through that
same object during the audit window.

If you have several datasources, make the one used by the audited code unambiguous, usually with
`@Primary`. This does not merge every datasource into one audit. Run the capture proof through the
actual repository path when validating a multi-datasource application.

## Reuse an existing datasource-proxy

If another library already provides the query-aware Spring datasource, disable only the starter's
additional wrapper:

```yaml
query-audit:
  wrap-data-source:
    enabled: false
```

Keep QueryAudit enabled. The extension attaches to the existing datasource-proxy; a custom
interceptor bean is not required. If the context only contains a raw datasource, keep automatic
wrapping enabled. Verify one-query/zero-budget failure after changing this setting.

## Audit a full suite

??? info "Enable opt-out auditing after the first test works"

    Add to `src/test/resources/junit-platform.properties`:

    ```properties
    junit.jupiter.extensions.autodetection.enabled=true
    ```

    Then add to the active test YAML:

    ```yaml
    query-audit:
      mode: all
      profile: recommended
    ```

    Exclude intentional non-audited tests with `@QueryAuditExclude`. Autodetection alone does not
    widen the default `annotated` mode. Use an [audit coverage manifest](../guide/audit-coverage.md)
    when CI must prove that specified tests actually supplied audit evidence. Review
    [lifecycle limitations](../guide/limitations.md) before making the whole suite required.

## Disable QueryAudit

Set `query-audit.enabled: false` only when you intend to run without auditing. For a temporary
report-only run, keep it enabled and use `@EnableQueryInspector` instead; explicit budgets remain
assertions.

## Next steps

- [Review per-test count changes](../guide/contracts.md)
- [Require audited tests and compare CI results](../guide/first-ci-check.md)
- [Find a symptom and its first check](../guide/troubleshooting.md)
