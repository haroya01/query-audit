package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectMethod;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;
import org.junit.platform.launcher.core.LauncherConfig;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class AuditCoverageListenerTest {

  private static final String FIXTURE_PROPERTY = "queryaudit.test.coverageListenerFixture";
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
  private int originalRetentionLimit;

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
    originalRetentionLimit = HtmlReportAggregator.getInstance().getMaxInMemoryReports();
    HtmlReportAggregator.getInstance().reset();
    QueryAuditDataSourceStore.clear();
    InnerFixture.sessionHidden = false;
    OuterFixture.parentRestored = false;
  }

  @AfterEach
  void restore() {
    originalProperties.forEach(
        (key, value) -> {
          if (value == null) System.clearProperty(key);
          else System.setProperty(key, value);
        });
    HtmlReportAggregator.getInstance().setMaxInMemoryReports(originalRetentionLimit);
    HtmlReportAggregator.getInstance().reset();
    QueryAuditDataSourceStore.clear();
  }

  @Test
  void crossThreadFinishPreservesTheEngineFrameAndReleasesTheWorkerSession() throws Exception {
    TestPlan plan = plan(PlainFixture.class, "ordinary");
    TestIdentifier engine = plan.getRoots().iterator().next();
    TestIdentifier test =
        plan.getDescendants(engine).stream()
            .filter(TestIdentifier::isTest)
            .findFirst()
            .orElseThrow();
    manifest(test.getUniqueId());
    AuditCoverageListener listener = new AuditCoverageListener();
    listener.testPlanExecutionStarted(plan);
    listener.executionStarted(engine);
    AuditCoverageSession session = AuditCoverageListener.currentSession();
    assertThat(session).isNotNull();
    var worker = Executors.newSingleThreadExecutor();
    try {
      try {
        assertThat(
                worker
                    .submit(
                        () -> {
                          listener.executionStarted(test);
                          return AuditCoverageListener.currentSession();
                        })
                    .get())
            .isSameAs(session);

        listener.executionFinished(test, TestExecutionResult.successful());

        assertThat(AuditCoverageListener.currentSession()).isSameAs(session);
        listener.executionFinished(engine, TestExecutionResult.successful());
      } finally {
        listener.testPlanExecutionFinished(plan);
      }
      Callable<AuditCoverageSession> lookup = AuditCoverageListener::currentSession;
      assertThat(worker.submit(lookup).get()).isNull();
    } finally {
      worker.shutdownNow();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void nestedLauncherDoesNotInheritTheParentCoverage(boolean disableChildListener)
      throws Exception {
    manifest(
        "[engine:junit-jupiter]/[class:" + OuterFixture.class.getName() + "]/[method:outer()]");
    OuterFixture.disableChildListener = disableChildListener;
    SummaryGeneratingListener summary = new SummaryGeneratingListener();

    LauncherFactory.create()
        .execute(
            LauncherDiscoveryRequestBuilder.request()
                .selectors(selectMethod(OuterFixture.class, "outer"))
                .build(),
            summary);

    assertThat(summary.getSummary().getTotalFailureCount()).isZero();
    assertThat(summary.getSummary().getTestsSucceededCount()).isEqualTo(1);
    assertThat(InnerFixture.sessionHidden).isTrue();
    assertThat(OuterFixture.parentRestored).isTrue();
    assertThat(Files.readString(directory.resolve("report/report.json")))
        .contains("\"outcome\": \"PASS\"", "\"expected\": 1", "\"audited\": 1")
        .doesNotContain(InnerFixture.class.getName());
  }

  @Test
  void sessionBoundsQueryEvidenceWithoutDroppingCountsOrFindings() throws Exception {
    TestPlan plan = plan(PlainFixture.class, "ordinary");
    manifest(
        "[engine:junit-jupiter]/[class:" + PlainFixture.class.getName() + "]/[method:ordinary()]");
    HtmlReportAggregator.getInstance().setMaxInMemoryReports(1);
    AuditCoverageSession session = AuditCoverageSession.open(plan);
    Issue issue =
        new Issue(
            IssueType.SELECT_ALL,
            Severity.INFO,
            "SELECT * FROM accounts",
            "accounts",
            null,
            "Selects all columns",
            null,
            null);
    for (String name : List.of("first", "second", "third")) {
      session.audited(
          new QueryAuditReport(
              "Retention",
              name,
              List.of(),
              List.of(issue),
              List.of(),
              List.of(new QueryRecord("SELECT 1", "SELECT ?", 1, 0, null, 1)),
              1,
              1,
              1));
    }

    List<QueryAuditReport> reports = session.reports();

    assertThat(reports).hasSize(3);
    assertThat(reports.get(0).getAllQueries()).hasSize(1);
    assertThat(reports.get(1).getAllQueries()).isEmpty();
    assertThat(reports.get(2).getAllQueries()).isEmpty();
    assertThat(reports).extracting(QueryAuditReport::getTotalQueryCount).containsExactly(1, 1, 1);
    assertThat(reports)
        .allSatisfy(report -> assertThat(report.getInfoIssues()).containsExactly(issue));
  }

  private void manifest(String identity) throws Exception {
    Files.write(directory.resolve("tests.txt"), List.of(identity));
  }

  private static TestPlan plan(Class<?> fixture, String method) {
    return LauncherFactory.create()
        .discover(
            LauncherDiscoveryRequestBuilder.request()
                .selectors(selectMethod(fixture, method))
                .build());
  }

  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class PlainFixture {
    @Test
    void ordinary() {}
  }

  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  @QueryAudit(failOnDetection = BooleanOverride.FALSE, autoOpenReport = BooleanOverride.FALSE)
  static class OuterFixture {
    static DataSource dataSource = dataSource("coverage_listener_outer");
    static boolean disableChildListener;
    static boolean parentRestored;

    @Test
    void outer() {
      AuditCoverageSession parent = AuditCoverageListener.currentSession();
      String manifest = System.getProperty("queryAudit.coverageManifest");
      String output = System.getProperty("queryAudit.reportOutputDir");
      try {
        System.clearProperty("queryAudit.coverageManifest");
        System.setProperty(
            "queryAudit.reportOutputDir", Path.of(output).resolve("child").toString());
        SummaryGeneratingListener summary = new SummaryGeneratingListener();
        LauncherFactory.create(
                LauncherConfig.builder()
                    .enableTestExecutionListenerAutoRegistration(!disableChildListener)
                    .build())
            .execute(
                LauncherDiscoveryRequestBuilder.request()
                    .selectors(selectMethod(InnerFixture.class, "inner"))
                    .build(),
                summary);
        assertThat(summary.getSummary().getTotalFailureCount()).isZero();
        assertThat(summary.getSummary().getTestsSucceededCount()).isEqualTo(1);
      } finally {
        System.setProperty("queryAudit.coverageManifest", manifest);
        System.setProperty("queryAudit.reportOutputDir", output);
      }
      parentRestored = parent != null && AuditCoverageListener.currentSession() == parent;
    }
  }

  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  @QueryAudit(failOnDetection = BooleanOverride.FALSE, autoOpenReport = BooleanOverride.FALSE)
  @ExtendWith(SessionProbe.class)
  static class InnerFixture {
    static DataSource dataSource = dataSource("coverage_listener_inner");
    static boolean sessionHidden;

    @Test
    void inner() {}
  }

  static class SessionProbe implements BeforeTestExecutionCallback {
    @Override
    public void beforeTestExecution(ExtensionContext context) {
      InnerFixture.sessionHidden = AuditCoverageListener.currentSession(context) == null;
    }
  }

  private static DataSource dataSource(String name) {
    JdbcDataSource dataSource = new JdbcDataSource();
    dataSource.setURL("jdbc:h2:mem:" + name);
    return dataSource;
  }
}
