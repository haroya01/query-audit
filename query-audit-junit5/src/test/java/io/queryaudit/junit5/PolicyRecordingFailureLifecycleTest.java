package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.ReportFormat;
import io.queryaudit.core.regression.QueryCountBaseline;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class PolicyRecordingFailureLifecycleTest {

  private static final String FIXTURE = "queryaudit.test.policyRecordingFixture";
  private static final String OUTPUT = "queryaudit.test.policyRecordingOutput";
  private static final String BLOCK = "queryaudit.test.blockPolicyRecording";
  private static final List<String> PROPERTIES =
      List.of(
          FIXTURE,
          OUTPUT,
          BLOCK,
          "queryAudit.contractsPath",
          "queryAudit.contracts.record",
          "queryAudit.countBaselinePath",
          "queryAudit.updateBaseline",
          "queryGuard.contractsPath",
          "queryGuard.contracts.record",
          "queryGuard.countBaselinePath",
          "queryGuard.updateBaseline");

  private final Map<String, String> originalProperties = new LinkedHashMap<>();
  @TempDir Path directory;

  @BeforeEach
  void setUp() {
    for (String property : PROPERTIES) {
      originalProperties.put(property, System.getProperty(property));
      System.clearProperty(property);
    }
    HtmlReportAggregator.getInstance().reset();
    QueryAuditDataSourceStore.clear();
    System.setProperty(FIXTURE, "true");
    System.setProperty(OUTPUT, directory.resolve("reports").toString());
  }

  @AfterEach
  void tearDown() {
    originalProperties.forEach(
        (property, value) -> {
          if (value == null) {
            System.clearProperty(property);
          } else {
            System.setProperty(property, value);
          }
        });
    HtmlReportAggregator.getInstance().reset();
    QueryAuditDataSourceStore.clear();
  }

  @ParameterizedTest
  @EnumSource(Policy.class)
  void classLevelRecordingFailureFailsTheLauncher(Policy policy) throws IOException {
    DataSource original = ClassFixture.dataSource;
    Path destination = configure(policy, false);
    System.setProperty(BLOCK, destination.toString());

    TestExecutionSummary summary = run(ClassFixture.class);

    assertRecordingFailure(summary, policy);
    assertThat(summary.getTestsSucceededCount()).isEqualTo(1);
    assertThat(ClassFixture.dataSource).isSameAs(original);
  }

  @ParameterizedTest
  @EnumSource(Policy.class)
  void methodLevelRecordingFailureFailsTheLauncher(Policy policy) throws IOException {
    DataSource original = MethodFixture.dataSource;
    System.setProperty(BLOCK, configure(policy, false).toString());

    TestExecutionSummary summary = run(MethodFixture.class);

    assertRecordingFailure(summary, policy);
    assertThat(MethodFixture.dataSource).isSameAs(original);
  }

  @ParameterizedTest
  @EnumSource(Policy.class)
  void legacyRecordingPropertiesAlsoFailClosed(Policy policy) throws IOException {
    System.setProperty(BLOCK, configure(policy, true).toString());

    assertRecordingFailure(run(ClassFixture.class), policy);
  }

  @ParameterizedTest
  @EnumSource(Policy.class)
  void aWritableDestinationRecordsThePolicy(Policy policy) throws IOException {
    Path destination = configure(policy, false);

    TestExecutionSummary summary = run(ClassFixture.class);

    assertThat(summary.getTotalFailureCount()).isZero();
    assertThat(QueryCountBaseline.load(destination)).hasSize(1);
    assertThat(report()).contains("\"outcome\": \"PASS\"");
  }

  @Test
  void bothRecordingFailuresAreRetainedAndCleanupStillRuns() throws IOException {
    Path contracts = configure(Policy.CONTRACTS, false);
    Path counts = configure(Policy.COUNTS, false);
    System.setProperty(BLOCK, contracts + "\n" + counts);
    DataSource original = ClassFixture.dataSource;

    TestExecutionSummary summary = run(ClassFixture.class);

    assertThat(summary.getTotalFailureCount()).isEqualTo(1);
    assertThat(summary.getFailures().get(0).getException())
        .hasStackTraceContaining("Could not write count baseline")
        .hasStackTraceContaining("Could not write query contracts");
    assertThat(report())
        .contains("\"outcome\": \"INCONCLUSIVE\"")
        .contains("Could not write query contracts")
        .contains("Could not write count baseline");
    assertThat(ClassFixture.dataSource).isSameAs(original);
  }

  private Path configure(Policy policy, boolean legacy) {
    String prefix = legacy ? "queryGuard." : "queryAudit.";
    Path destination = directory.resolve(policy.name()).resolve("policy.txt");
    System.setProperty(prefix + policy.pathProperty, destination.toString());
    System.setProperty(prefix + policy.recordProperty, "true");
    return destination;
  }

  private void assertRecordingFailure(TestExecutionSummary summary, Policy policy)
      throws IOException {
    assertThat(summary.getTotalFailureCount()).isEqualTo(1);
    assertThat(summary.getFailures().get(0).getException())
        .hasStackTraceContaining("Could not write " + policy.description);
    assertThat(report())
        .contains("\"outcome\": \"INCONCLUSIVE\"")
        .contains("\"code\": \"POLICY_WRITE_FAILED\"")
        .doesNotContain("\"code\": \"AUDIT_ANALYSIS_FAILED\"");
  }

  private String report() throws IOException {
    return Files.readString(directory.resolve("reports/report.json"));
  }

  private static TestExecutionSummary run(Class<?> fixture) {
    SummaryGeneratingListener listener = new SummaryGeneratingListener();
    LauncherFactory.create()
        .execute(
            LauncherDiscoveryRequestBuilder.request()
                .selectors(selectClass(fixture))
                .configurationParameter("junit.jupiter.extensions.autodetection.enabled", "false")
                .build(),
            listener);
    return listener.getSummary();
  }

  private static void blockRequestedPaths() throws IOException {
    String destinations = System.getProperty(BLOCK);
    if (destinations != null) {
      for (String destination : destinations.split("\n")) {
        Files.writeString(Path.of(destination).getParent(), "not a directory");
      }
    }
  }

  private static DataSource dataSource(String name) {
    JdbcDataSource dataSource = new JdbcDataSource();
    dataSource.setURL("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1");
    return dataSource;
  }

  private enum Policy {
    CONTRACTS("contractsPath", "contracts.record", "query contracts"),
    COUNTS("countBaselinePath", "updateBaseline", "count baseline");

    final String pathProperty;
    final String recordProperty;
    final String description;

    Policy(String pathProperty, String recordProperty, String description) {
      this.pathProperty = pathProperty;
      this.recordProperty = recordProperty;
      this.description = description;
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class ReportConfiguration {
    @Bean
    QueryAuditConfig queryAuditConfig() {
      return QueryAuditConfig.builder()
          .failOnDetection(false)
          .autoOpenReport(false)
          .reportFormat(ReportFormat.JSON)
          .reportOutputDir(System.getProperty(OUTPUT))
          .build();
    }
  }

  @QueryAudit(autoOpenReport = BooleanOverride.FALSE)
  @SpringJUnitConfig(ReportConfiguration.class)
  @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
  @EnabledIfSystemProperty(named = FIXTURE, matches = "true")
  static class ClassFixture {
    static DataSource dataSource = dataSource("policy-recording-class");

    @Test
    void passesBeforeRecording() throws IOException {
      blockRequestedPaths();
    }
  }

  @SpringJUnitConfig(ReportConfiguration.class)
  @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
  @EnabledIfSystemProperty(named = FIXTURE, matches = "true")
  static class MethodFixture {
    static DataSource dataSource = dataSource("policy-recording-method");

    @Test
    @QueryAudit(autoOpenReport = BooleanOverride.FALSE)
    void passesBeforeRecording() throws IOException {
      blockRequestedPaths();
    }
  }
}
