package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.ReportFormat;
import io.queryaudit.core.regression.QueryCountBaseline;
import io.queryaudit.core.regression.QueryCounts;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@DisplayName("Stable identity policy recording lifecycle")
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class StableIdentityPolicyLifecycleTest {

  private static final String FIXTURE_PROPERTY = "queryaudit.test.identityPolicyFixture";
  private static final String OUTPUT_PATH_PROPERTY = "queryaudit.test.identityPolicyOutput";
  private static final String COUNT_PATH_PROPERTY = "queryAudit.countBaselinePath";
  private static final String UPDATE_COUNT_PROPERTY = "queryAudit.updateBaseline";
  private static final String CONTRACTS_PATH_PROPERTY = "queryAudit.contractsPath";
  private static final String RECORD_CONTRACTS_PROPERTY = "queryAudit.contracts.record";
  private static final List<String> MANAGED_PROPERTIES =
      List.of(
          FIXTURE_PROPERTY,
          OUTPUT_PATH_PROPERTY,
          COUNT_PATH_PROPERTY,
          UPDATE_COUNT_PROPERTY,
          CONTRACTS_PATH_PROPERTY,
          RECORD_CONTRACTS_PROPERTY);

  private final Map<String, String> originalProperties = new LinkedHashMap<>();

  @BeforeEach
  void setUp() {
    for (String property : MANAGED_PROPERTIES) {
      originalProperties.put(property, System.getProperty(property));
      System.clearProperty(property);
    }
    HtmlReportAggregator.getInstance().reset();
    QueryAuditDataSourceStore.clear();
  }

  @AfterEach
  void tearDown() {
    for (Map.Entry<String, String> property : originalProperties.entrySet()) {
      restoreProperty(property.getKey(), property.getValue());
    }
    originalProperties.clear();
    HtmlReportAggregator.getInstance().reset();
    QueryAuditDataSourceStore.clear();
  }

  @Test
  @DisplayName("method-level audits persist stable count and contract entries")
  void methodLevelAuditRecordsStablePolicies(@TempDir Path tempDir) {
    DataSource originalDataSource = MethodLevelFixture.dataSource;
    PolicyPaths paths = configureRecording(tempDir.resolve("method-level"));

    TestExecutionSummary summary = runFixture(MethodLevelFixture.class);

    assertThat(summary.getTotalFailureCount()).isZero();
    assertThat(summary.getTestsSucceededCount()).isEqualTo(1);
    String testId =
        "[engine:junit-jupiter]/[class:"
            + MethodLevelFixture.class.getName()
            + "]/[method:audited()]";
    assertRecorded(paths, testId);
    assertThat(MethodLevelFixture.dataSource).isSameAs(originalDataSource);
  }

  @Test
  @DisplayName("a nested-only audit owns cleanup and policy recording")
  void nestedOnlyAuditRecordsStablePolicies(@TempDir Path tempDir) {
    DataSource originalDataSource = NestedOnlyFixture.AuditedNested.dataSource;
    PolicyPaths paths = configureRecording(tempDir.resolve("nested-only"));

    TestExecutionSummary summary = runFixture(NestedOnlyFixture.class);

    assertThat(summary.getTotalFailureCount()).isZero();
    assertThat(summary.getTestsSucceededCount()).isEqualTo(1);
    String testId =
        "[engine:junit-jupiter]/[class:"
            + NestedOnlyFixture.class.getName()
            + "]/[nested-class:AuditedNested]/[method:audited()]";
    assertRecorded(paths, testId);
    assertThat(NestedOnlyFixture.AuditedNested.dataSource).isSameAs(originalDataSource);
  }

  @Test
  @DisplayName("an inline budget still participates in legacy identity migration")
  void inlineBudgetDoesNotHideAnAmbiguousLegacyContract(@TempDir Path tempDir) throws Exception {
    Path outputDirectory = tempDir.resolve("inline-contract-migration");
    Path contractsPath = outputDirectory.resolve("query-contracts.txt");
    String stableOwnerId =
        "[engine:junit-jupiter]/[class:"
            + InlineContractMigrationFixture.class.getName()
            + "]/[method:stableOwner()]";
    QueryCounts zeroQueries = new QueryCounts(0, 0, 0, 0, 0);
    QueryCountBaseline.save(
        contractsPath,
        Map.of(
            QueryCountBaseline.key(stableOwnerId),
            zeroQueries,
            QueryCountBaseline.key(
                StableIdentityPolicyLifecycleTest.class.getSimpleName(), "duplicate"),
            zeroQueries),
        "QueryAudit Query Contracts");
    System.setProperty(FIXTURE_PROPERTY, "true");
    System.setProperty(OUTPUT_PATH_PROPERTY, outputDirectory.resolve("report").toString());
    System.setProperty(CONTRACTS_PATH_PROPERTY, contractsPath.toString());

    TestExecutionSummary summary = runFixture(InlineContractMigrationFixture.class);

    assertThat(summary.getTotalFailureCount()).isEqualTo(1);
    assertThat(summary.getFailures())
        .singleElement()
        .satisfies(
            failure ->
                assertThat(failure.getException())
                    .hasStackTraceContaining("ambiguous 0.5 identity")
                    .hasStackTraceContaining("stableOwner")
                    .hasStackTraceContaining("fallbackConsumer"));
    assertThat(Files.readString(outputDirectory.resolve("report/report.json")))
        .contains("\"outcome\": \"INCONCLUSIVE\"")
        .contains("\"code\": \"AUDIT_ANALYSIS_FAILED\"");
  }

  private static PolicyPaths configureRecording(Path directory) {
    Path countPath = directory.resolve("query-counts.txt");
    Path contractsPath = directory.resolve("query-contracts.txt");
    System.setProperty(FIXTURE_PROPERTY, "true");
    System.setProperty(OUTPUT_PATH_PROPERTY, directory.resolve("report").toString());
    System.setProperty(COUNT_PATH_PROPERTY, countPath.toString());
    System.setProperty(UPDATE_COUNT_PROPERTY, "true");
    System.setProperty(CONTRACTS_PATH_PROPERTY, contractsPath.toString());
    System.setProperty(RECORD_CONTRACTS_PROPERTY, "true");
    return new PolicyPaths(countPath, contractsPath);
  }

  private static TestExecutionSummary runFixture(Class<?> fixture) {
    LauncherDiscoveryRequest request =
        LauncherDiscoveryRequestBuilder.request()
            .selectors(selectClass(fixture))
            .configurationParameter("junit.jupiter.extensions.autodetection.enabled", "false")
            .build();
    SummaryGeneratingListener listener = new SummaryGeneratingListener();
    Launcher launcher = LauncherFactory.create();
    launcher.registerTestExecutionListeners(listener);
    launcher.execute(request);
    return listener.getSummary();
  }

  private static void assertRecorded(PolicyPaths paths, String testId) {
    QueryCounts zeroQueries = new QueryCounts(0, 0, 0, 0, 0);
    assertThat(QueryCountBaseline.load(paths.countPath()))
        .containsEntry(QueryCountBaseline.key(testId), zeroQueries);
    assertThat(QueryCountBaseline.load(paths.contractsPath()))
        .containsEntry(QueryCountBaseline.key(testId), zeroQueries);
  }

  private static DataSource newDataSource(String name) {
    JdbcDataSource dataSource = new JdbcDataSource();
    dataSource.setURL("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1");
    return dataSource;
  }

  private static void restoreProperty(String key, String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }

  private record PolicyPaths(Path countPath, Path contractsPath) {}

  @Configuration(proxyBeanMethods = false)
  static class RecordingConfiguration {

    @Bean
    QueryAuditConfig queryAuditConfig() {
      return QueryAuditConfig.builder()
          .reportFormat(ReportFormat.JSON)
          .reportOutputDir(System.getProperty(OUTPUT_PATH_PROPERTY))
          .build();
    }
  }

  @SpringJUnitConfig(RecordingConfiguration.class)
  @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class MethodLevelFixture {

    static DataSource dataSource = newDataSource("stable-policy-method-level");

    @Test
    @QueryAudit(autoOpenReport = BooleanOverride.FALSE)
    void audited() {}
  }

  @SpringJUnitConfig(RecordingConfiguration.class)
  @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class NestedOnlyFixture {

    @Nested
    @QueryAudit(autoOpenReport = BooleanOverride.FALSE)
    class AuditedNested {

      static DataSource dataSource = newDataSource("stable-policy-nested-only");

      @Test
      void audited() {}
    }
  }

  @QueryAudit(autoOpenReport = BooleanOverride.FALSE)
  @SpringJUnitConfig(RecordingConfiguration.class)
  @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  static class InlineContractMigrationFixture {

    static DataSource dataSource = newDataSource("stable-policy-inline-contract");

    @Test
    @Order(1)
    @DisplayName("duplicate")
    @ExpectQueries(select = 0)
    void stableOwner() {}

    @Test
    @Order(2)
    @DisplayName("duplicate")
    void fallbackConsumer() {}
  }
}
