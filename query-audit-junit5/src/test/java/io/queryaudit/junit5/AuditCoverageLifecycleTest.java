package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectMethod;

import io.queryaudit.core.reporter.HtmlReportAggregator;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.engine.DiscoverySelector;
import org.junit.platform.launcher.core.LauncherConfig;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class AuditCoverageLifecycleTest {

  private static final String FIXTURE_PROPERTY = "queryaudit.test.coverageFixture";
  private static final List<String> PROPERTIES =
      List.of(
          FIXTURE_PROPERTY,
          "queryAudit.coverageManifest",
          "queryAudit.reportOutputDir",
          "queryAudit.reportFormat",
          "queryAudit.contractsPath",
          "queryAudit.countBaselinePath");

  @TempDir Path directory;
  private final Map<String, String> originalProperties = new LinkedHashMap<>();

  @BeforeEach
  void configure() {
    for (String property : PROPERTIES) {
      originalProperties.put(property, System.getProperty(property));
      System.clearProperty(property);
    }
    System.setProperty(FIXTURE_PROPERTY, "true");
    System.setProperty("queryAudit.coverageManifest", directory.resolve("tests.txt").toString());
    System.setProperty("queryAudit.reportOutputDir", directory.resolve("report").toString());
    System.setProperty("queryAudit.reportFormat", "json");
    System.setProperty("queryAudit.contractsPath", directory.resolve("contracts").toString());
    System.setProperty("queryAudit.countBaselinePath", directory.resolve("counts").toString());
    HtmlReportAggregator.getInstance().reset();
    QueryAuditDataSourceStore.clear();
  }

  @AfterEach
  void restore() {
    originalProperties.forEach(
        (key, value) -> {
          if (value == null) {
            System.clearProperty(key);
          } else {
            System.setProperty(key, value);
          }
        });
    HtmlReportAggregator.getInstance().reset();
    QueryAuditDataSourceStore.clear();
  }

  @Test
  void completeManifestAllowsPass() throws Exception {
    manifest(testId(Fixture.class, "successful()"));

    TestExecutionSummary summary = execute(selectMethod(Fixture.class, "successful"));

    assertThat(summary.getTestsSucceededCount()).isEqualTo(1);
    assertReport("PASS", 1, 1, 0, 1, 0);
  }

  @Test
  void filteredExpectedTestDoesNotDisappearFromTheVerdict() throws Exception {
    manifest(testId(Fixture.class, "successful()"));

    execute(selectMethod(Fixture.class, "disabled"));

    assertReport("INCONCLUSIVE", 1, 0, 0, 0, 1);
    assertThat(report()).contains("NOT_DISCOVERED", "EXPECTED_TEST_MISSING", "successful()");
  }

  @Test
  void anEntirelyDisabledSelectionStillWritesCoverage() throws Exception {
    manifest(testId(Fixture.class, "disabled()"));

    TestExecutionSummary summary = execute(selectMethod(Fixture.class, "disabled"));

    assertThat(summary.getTestsSkippedCount()).isEqualTo(1);
    assertReport("INCONCLUSIVE", 1, 0, 1, 0, 1);
    assertThat(report()).contains("SKIPPED");
  }

  @Test
  void classSetupFailureIsRecordedBeforeAnyAuditCompletes() throws Exception {
    manifest(testId(SetupFailure.class, "unreachable()"));

    TestExecutionSummary summary = execute(selectMethod(SetupFailure.class, "unreachable"));

    assertThat(summary.getTotalFailureCount()).isEqualTo(1);
    assertReport("INCONCLUSIVE", 1, 0, 0, 0, 1);
    assertThat(report()).contains("SETUP_FAILED");
  }

  @Test
  void anAbortedBodyCannotSupplyCompleteAuditEvidence() throws Exception {
    manifest(testId(Fixture.class, "aborted()"));

    TestExecutionSummary summary = execute(selectMethod(Fixture.class, "aborted"));

    assertThat(summary.getTestsAbortedCount()).isEqualTo(1);
    assertReport("INCONCLUSIVE", 1, 1, 0, 1, 1);
    assertThat(report()).contains("ABORTED");
  }

  @Test
  void aFailedBodyCannotSupplyCompleteAuditEvidence() throws Exception {
    manifest(testId(Fixture.class, "failed()"));

    execute(selectMethod(Fixture.class, "failed"));

    assertReport("INCONCLUSIVE", 1, 1, 0, 1, 1);
    assertThat(report()).contains("TEST_FAILED");
  }

  @Test
  void removingTheAuditAnnotationIsNotACleanRun() throws Exception {
    manifest(testId(Unaudited.class, "ordinaryTest()"));

    TestExecutionSummary summary = execute(selectMethod(Unaudited.class, "ordinaryTest"));

    assertThat(summary.getTestsSucceededCount()).isEqualTo(1);
    assertReport("INCONCLUSIVE", 1, 1, 0, 0, 1);
    assertThat(report()).contains("AUDIT_MISSING");
  }

  @Test
  void policyFailureRemainsFailWhenCoverageIsComplete() throws Exception {
    manifest(testId(Fixture.class, "policyFailure()"));

    execute(selectMethod(Fixture.class, "policyFailure"));

    assertReport("FAIL", 1, 1, 0, 1, 0);
  }

  @Test
  void parameterizedInvocationsAreCountedIndividually() throws Exception {
    String template = classId(Fixture.class) + "/[test-template:parameterized(java.lang.String)]";
    manifest(
        template + "/[test-template-invocation:#1]", template + "/[test-template-invocation:#2]");

    TestExecutionSummary summary =
        execute(selectMethod(Fixture.class, "parameterized", String.class));

    assertThat(summary.getTestsSucceededCount()).isEqualTo(2);
    assertReport("PASS", 2, 2, 0, 2, 0);
  }

  @Test
  void aRemovedParameterizedInvocationIsMissing() throws Exception {
    String template = classId(Fixture.class) + "/[test-template:parameterized(java.lang.String)]";
    manifest(template + "/[test-template-invocation:#3]");

    execute(selectMethod(Fixture.class, "parameterized", String.class));

    assertReport("INCONCLUSIVE", 1, 2, 0, 2, 1);
    assertThat(report()).contains("NOT_DISCOVERED", "\"expected\": false");
  }

  @Test
  void missingExplicitManifestIsAnErrorEvenWithSuccessfulTests() throws Exception {
    execute(selectMethod(Fixture.class, "successful"));

    assertThat(report()).contains("\"outcome\": \"INCONCLUSIVE\"", "COVERAGE_MANIFEST_UNREADABLE");
  }

  @Test
  void aConfiguredManifestRequiresThePlatformListener() throws Exception {
    manifest(testId(Fixture.class, "successful()"));
    SummaryGeneratingListener summary = new SummaryGeneratingListener();

    LauncherFactory.create(
            LauncherConfig.builder().enableTestExecutionListenerAutoRegistration(false).build())
        .execute(
            LauncherDiscoveryRequestBuilder.request()
                .selectors(selectMethod(Fixture.class, "successful"))
                .build(),
            summary);

    assertThat(summary.getSummary().getTotalFailureCount()).isGreaterThan(0);
    assertThat(report()).contains("\"outcome\": \"INCONCLUSIVE\"", "AUDIT_INITIALIZATION_FAILED");
  }

  @Test
  void repeatedLaunchesDoNotReuseThePreviousCoverage() throws Exception {
    manifest(testId(Fixture.class, "successful()"));
    execute(selectMethod(Fixture.class, "successful"));
    assertReport("PASS", 1, 1, 0, 1, 0);

    execute(selectMethod(Fixture.class, "disabled"));

    assertReport("INCONCLUSIVE", 1, 0, 0, 0, 1);
    assertThat(report()).contains("NOT_DISCOVERED");
  }

  private void manifest(String... ids) throws Exception {
    Files.write(directory.resolve("tests.txt"), List.of(ids));
  }

  private TestExecutionSummary execute(DiscoverySelector selector) {
    SummaryGeneratingListener summary = new SummaryGeneratingListener();
    LauncherFactory.create()
        .execute(
            LauncherDiscoveryRequestBuilder.request()
                .selectors(selector)
                .configurationParameter("junit.jupiter.extensions.autodetection.enabled", "false")
                .build(),
            summary);
    return summary.getSummary();
  }

  private String report() throws Exception {
    return Files.readString(directory.resolve("report/report.json"));
  }

  private void assertReport(
      String outcome, int expected, int executed, int skipped, int audited, int failedToAudit)
      throws Exception {
    assertThat(report())
        .contains("\"outcome\": \"" + outcome + "\"")
        .contains("\"expected\": " + expected)
        .contains("\"executed\": " + executed)
        .contains("\"skipped\": " + skipped)
        .contains("\"audited\": " + audited)
        .contains("\"failedToAudit\": " + failedToAudit);
  }

  private static String classId(Class<?> fixture) {
    return "[engine:junit-jupiter]/[class:" + fixture.getName() + "]";
  }

  private static String testId(Class<?> fixture, String method) {
    return classId(fixture) + "/[method:" + method + "]";
  }

  private static DataSource dataSource() {
    JdbcDataSource dataSource = new JdbcDataSource();
    dataSource.setURL("jdbc:h2:mem:coverage;DB_CLOSE_DELAY=-1");
    return dataSource;
  }

  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  @QueryAudit(failOnDetection = BooleanOverride.FALSE, autoOpenReport = BooleanOverride.FALSE)
  static class Fixture {
    static DataSource dataSource = dataSource();

    @Test
    void successful() {}

    @Test
    @Disabled
    void disabled() {}

    @Test
    void aborted() {
      Assumptions.assumeTrue(false);
    }

    @Test
    void failed() {
      throw new AssertionError("Application assertion failed");
    }

    @Test
    @ExpectQueries(select = 0)
    void policyFailure() throws Exception {
      try (var connection = dataSource.getConnection();
          var statement = connection.createStatement();
          var result = statement.executeQuery("SELECT 1")) {
        assertThat(result.next()).isTrue();
      }
    }

    @ParameterizedTest
    @ValueSource(strings = {"one", "two"})
    void parameterized(String value) {}
  }

  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  @QueryAudit(failOnDetection = BooleanOverride.FALSE, autoOpenReport = BooleanOverride.FALSE)
  static class SetupFailure {
    static DataSource dataSource = dataSource();

    @BeforeAll
    static void failSetup() {
      throw new IllegalStateException("Setup failed");
    }

    @Test
    void unreachable() {}
  }

  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class Unaudited {
    @Test
    void ordinaryTest() {}
  }
}
