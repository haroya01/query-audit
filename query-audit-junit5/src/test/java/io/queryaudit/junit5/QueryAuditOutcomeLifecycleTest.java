package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.ReportFormat;
import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.platform.engine.DiscoverySelector;
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

@DisplayName("Audit outcome JUnit lifecycle")
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class QueryAuditOutcomeLifecycleTest {

  private static final String FIXTURE_PROPERTY = "queryaudit.test.outcomeLifecycleFixture";
  private static final String OUTPUT_PATH_PROPERTY = "queryaudit.test.outcomeLifecyclePath";
  private static final String CONTRACTS_PATH_PROPERTY = "queryAudit.contractsPath";
  private static final String REPORT_FORMAT_PROPERTY = "queryAudit.reportFormat";

  private String fixtureProperty;
  private String outputPathProperty;
  private String contractsPathProperty;
  private String reportFormatProperty;

  @BeforeEach
  void setUp() {
    fixtureProperty = System.getProperty(FIXTURE_PROPERTY);
    outputPathProperty = System.getProperty(OUTPUT_PATH_PROPERTY);
    contractsPathProperty = System.getProperty(CONTRACTS_PATH_PROPERTY);
    reportFormatProperty = System.getProperty(REPORT_FORMAT_PROPERTY);
    System.clearProperty(CONTRACTS_PATH_PROPERTY);
    System.clearProperty(REPORT_FORMAT_PROPERTY);
    HtmlReportAggregator.getInstance().reset();
    QueryAuditDataSourceStore.clear();
  }

  @AfterEach
  void tearDown() {
    restoreProperty(FIXTURE_PROPERTY, fixtureProperty);
    restoreProperty(OUTPUT_PATH_PROPERTY, outputPathProperty);
    restoreProperty(CONTRACTS_PATH_PROPERTY, contractsPathProperty);
    restoreProperty(REPORT_FORMAT_PROPERTY, reportFormatProperty);
    HtmlReportAggregator.getInstance().reset();
    QueryAuditDataSourceStore.clear();
  }

  @Test
  @DisplayName("a missing DataSource is written as INCONCLUSIVE")
  void missingDataSourceWritesInconclusiveEnvelope(@TempDir Path tempDir) throws IOException {
    Path outputDirectory = tempDir.resolve("missing-datasource");

    TestExecutionSummary summary = runFixture(MissingDataSourceFixture.class, outputDirectory);

    assertThat(summary.getTotalFailureCount()).isGreaterThan(0);
    assertIncomplete(outputDirectory, "DATASOURCE_UNAVAILABLE");
  }

  @Test
  @DisplayName("an unreadable contract is written as INCONCLUSIVE")
  void unreadableContractWritesInconclusiveEnvelope(@TempDir Path tempDir) throws IOException {
    Path malformedContracts = tempDir.resolve("query-contracts.txt");
    Files.writeString(malformedContracts, "OrderServiceTest | loadsOrders | invalid\n");
    System.setProperty(CONTRACTS_PATH_PROPERTY, malformedContracts.toString());
    Path outputDirectory = tempDir.resolve("unreadable-contract");

    TestExecutionSummary summary = runFixture(UnreadableContractFixture.class, outputDirectory);

    assertThat(summary.getTotalFailureCount()).isGreaterThan(0);
    assertIncomplete(outputDirectory, "CONTRACT_UNREADABLE");
  }

  @Test
  @DisplayName("a truncated query capture keeps its partial report and is INCONCLUSIVE")
  void truncatedCaptureWritesInconclusiveEnvelope(@TempDir Path tempDir) throws IOException {
    Path outputDirectory = tempDir.resolve("truncated-capture");

    TestExecutionSummary summary = runFixture(TruncatedCaptureFixture.class, outputDirectory);

    assertThat(summary.getTestsFailedCount()).isEqualTo(1);
    assertIncomplete(outputDirectory, "QUERY_LIMIT_REACHED");
    assertThat(Files.readString(outputDirectory.resolve("report.json")))
        .contains("\"totalQueries\": 1")
        .contains("\"reports\": [");
  }

  @Test
  @DisplayName("an analysis failure keeps earlier reports and makes the suite INCONCLUSIVE")
  void analysisFailureDoesNotLeaveAPartialPass(@TempDir Path tempDir) throws IOException {
    Path outputDirectory = tempDir.resolve("analysis-failure");
    TestExecutionSummary summary =
        runFixtures(outputDirectory, CompletedAuditFixture.class, AnalysisFailureFixture.class);

    assertThat(summary.getTotalFailureCount()).isGreaterThan(0);
    assertIncomplete(outputDirectory, "AUDIT_ANALYSIS_FAILED");
    assertThat(Files.readString(outputDirectory.resolve("report.json")))
        .contains("\"testName\": \"completesAudit()\"");
  }

  @Test
  @DisplayName("an initialization failure keeps earlier reports and makes the suite INCONCLUSIVE")
  void initializationFailureDoesNotLeaveAPartialPass(@TempDir Path tempDir) throws IOException {
    Path outputDirectory = tempDir.resolve("initialization-failure");

    TestExecutionSummary summary =
        runFixtures(outputDirectory, CompletedAuditFixture.class, InvalidMaxQueriesFixture.class);

    assertThat(summary.getTotalFailureCount()).isGreaterThan(0);
    assertIncomplete(outputDirectory, "AUDIT_INITIALIZATION_FAILED");
    assertThat(Files.readString(outputDirectory.resolve("report.json")))
        .contains("\"testName\": \"completesAudit()\"");
  }

  @Test
  @DisplayName("an unexpected initialization failure cannot leave an earlier partial PASS")
  void unexpectedInitializationFailureDoesNotLeaveAPartialPass(@TempDir Path tempDir)
      throws IOException {
    Path outputDirectory = tempDir.resolve("unexpected-initialization-failure");

    TestExecutionSummary summary =
        runFixtures(
            outputDirectory,
            CompletedAuditFixture.class,
            UnexpectedInitializationFailureFixture.class);

    assertThat(summary.getTotalFailureCount()).isGreaterThan(0);
    assertIncomplete(outputDirectory, "AUDIT_INITIALIZATION_FAILED");
    assertThat(Files.readString(outputDirectory.resolve("report.json")))
        .contains("\"testName\": \"completesAudit()\"");
  }

  @Test
  @DisplayName("method-level activation stays stable across the test lifecycle")
  void methodLevelActivationDoesNotReparseChangedConfiguration(@TempDir Path tempDir)
      throws IOException {
    Path outputDirectory = tempDir.resolve("method-level-activation");

    TestExecutionSummary summary =
        runFixture(MethodLevelConfigMutationFixture.class, outputDirectory);

    assertThat(summary.getTotalFailureCount()).isZero();
    assertThat(Files.readString(outputDirectory.resolve("report.json")))
        .contains("\"outcome\": \"PASS\"")
        .contains("\"testName\": \"audited()\"");
  }

  private static TestExecutionSummary runFixture(Class<?> fixture, Path outputDirectory) {
    return runFixtures(outputDirectory, fixture);
  }

  private static TestExecutionSummary runFixtures(
      Path outputDirectory, Class<?>... fixtureClasses) {
    System.setProperty(FIXTURE_PROPERTY, "true");
    System.setProperty(OUTPUT_PATH_PROPERTY, outputDirectory.toString());
    List<? extends DiscoverySelector> selectors =
        Arrays.stream(fixtureClasses).map(fixture -> selectClass(fixture)).toList();
    LauncherDiscoveryRequest request =
        LauncherDiscoveryRequestBuilder.request()
            .selectors(selectors)
            .configurationParameter("junit.jupiter.extensions.autodetection.enabled", "false")
            .build();
    SummaryGeneratingListener listener = new SummaryGeneratingListener();
    Launcher launcher = LauncherFactory.create();
    launcher.registerTestExecutionListeners(listener);
    launcher.execute(request);
    return listener.getSummary();
  }

  private static void assertIncomplete(Path outputDirectory, String reasonCode) throws IOException {
    Path reportPath = outputDirectory.resolve("report.json");
    assertThat(reportPath).exists();
    assertThat(Files.readString(reportPath))
        .contains("\"outcome\": \"INCONCLUSIVE\"")
        .contains("\"code\": \"" + reasonCode + "\"");
  }

  private static QueryAuditConfig outcomeConfig(int maxQueries) {
    return QueryAuditConfig.builder()
        .reportFormat(ReportFormat.JSON)
        .reportOutputDir(System.getProperty(OUTPUT_PATH_PROPERTY))
        .maxQueries(maxQueries)
        .build();
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

  @Configuration(proxyBeanMethods = false)
  static class StandardOutcomeConfiguration {

    @Bean
    QueryAuditConfig queryAuditConfig() {
      return outcomeConfig(QueryInterceptor.DEFAULT_MAX_QUERIES);
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class TruncatedOutcomeConfiguration {

    @Bean
    QueryAuditConfig queryAuditConfig() {
      return outcomeConfig(1);
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class InvalidMaxQueriesConfiguration {

    @Bean
    QueryAuditConfig queryAuditConfig() {
      return outcomeConfig(0);
    }
  }

  @QueryAudit(autoOpenReport = BooleanOverride.FALSE)
  @SpringJUnitConfig(StandardOutcomeConfiguration.class)
  @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class MissingDataSourceFixture {

    @Test
    void audited() {}
  }

  @QueryAudit(autoOpenReport = BooleanOverride.FALSE)
  @SpringJUnitConfig(StandardOutcomeConfiguration.class)
  @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class UnreadableContractFixture {

    static DataSource dataSource = newDataSource("outcome-unreadable-contract");

    @Test
    void audited() {}
  }

  @QueryAudit(autoOpenReport = BooleanOverride.FALSE)
  @SpringJUnitConfig(TruncatedOutcomeConfiguration.class)
  @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class TruncatedCaptureFixture {

    static DataSource dataSource = newDataSource("outcome-truncated-capture");

    @Test
    void executesTwoQueries() throws SQLException {
      try (Connection connection = dataSource.getConnection();
          Statement statement = connection.createStatement()) {
        statement.executeQuery("SELECT 1").close();
        statement.executeQuery("SELECT 2").close();
      }
    }
  }

  @QueryAudit(autoOpenReport = BooleanOverride.FALSE)
  @SpringJUnitConfig(StandardOutcomeConfiguration.class)
  @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class CompletedAuditFixture {

    static DataSource dataSource = newDataSource("outcome-completed-audit");

    @Test
    void completesAudit() {}
  }

  @QueryAudit(baselinePath = "\0", autoOpenReport = BooleanOverride.FALSE)
  @SpringJUnitConfig(StandardOutcomeConfiguration.class)
  @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class AnalysisFailureFixture {

    static DataSource dataSource = newDataSource("outcome-analysis-failure");

    @Test
    void cannotCompleteAnalysis() {}
  }

  @QueryAudit(autoOpenReport = BooleanOverride.FALSE)
  @SpringJUnitConfig(InvalidMaxQueriesConfiguration.class)
  @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class InvalidMaxQueriesFixture {

    static DataSource dataSource = newDataSource("outcome-invalid-max-queries");

    @Test
    void cannotInitializeAudit() {}
  }

  @QueryAudit(autoOpenReport = BooleanOverride.FALSE)
  @SpringJUnitConfig(StandardOutcomeConfiguration.class)
  @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class UnexpectedInitializationFailureFixture {

    static DataSource dataSource = newDataSource("outcome-unexpected-initialization-failure");
    private static String previousReportFormat;

    @BeforeAll
    static void useInvalidReportFormat() {
      previousReportFormat = System.getProperty(REPORT_FORMAT_PROPERTY);
      System.setProperty(REPORT_FORMAT_PROPERTY, "unsupported");
    }

    @AfterAll
    static void restoreReportFormat() {
      restoreProperty(REPORT_FORMAT_PROPERTY, previousReportFormat);
    }

    @Test
    void cannotInitializeAudit() {}
  }

  @SpringJUnitConfig(StandardOutcomeConfiguration.class)
  @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class MethodLevelConfigMutationFixture {

    static DataSource dataSource = newDataSource("outcome-method-level-activation");
    private static String previousReportFormat;

    @BeforeEach
    void changeReportFormatAfterAuditStarts() {
      previousReportFormat = System.getProperty(REPORT_FORMAT_PROPERTY);
      System.setProperty(REPORT_FORMAT_PROPERTY, "unsupported");
    }

    @AfterEach
    void restoreReportFormatBeforeAuditCompletes() {
      restoreProperty(REPORT_FORMAT_PROPERTY, previousReportFormat);
    }

    @Test
    @QueryAudit(autoOpenReport = BooleanOverride.FALSE)
    void audited() {}
  }
}
