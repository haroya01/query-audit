package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import com.jayway.jsonpath.JsonPath;
import io.queryaudit.core.extension.AuditExtensions;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.core.reporter.delivery.PublishedAuditRun;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.platform.launcher.core.LauncherConfig;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

/** Public Launcher regressions: capture ownership and report state must belong to one execution. */
@Isolated("Changes system properties and shared audit accumulators around nested Launcher runs")
class AuditRunIsolationRegressionTest {
  private static final String ENABLED = "queryaudit.test.runIsolationRegression";
  private static final List<String> PROPERTIES =
      List.of(
          ENABLED,
          "queryAudit.reportFormat",
          "queryAudit.reportOutputDir",
          "queryAudit.reportRedaction",
          "queryAudit.autoOpenReport",
          "queryAudit.coverageManifest");
  private static final List<PublishedAuditRun> PUBLICATIONS = new CopyOnWriteArrayList<>();
  private static final AuditExtensions EXTENSIONS =
      AuditExtensions.builder()
          .reportSink("regression:canonical-summary", true, PUBLICATIONS::add)
          .build();
  private static CountDownLatch ready;
  private static CountDownLatch queriesFinished;
  private final Map<String, String> originalProperties = new LinkedHashMap<>();

  @BeforeEach
  void prepare() {
    for (String key : PROPERTIES) {
      originalProperties.put(key, System.getProperty(key));
      System.clearProperty(key);
    }
    System.setProperty(ENABLED, "true");
    System.setProperty("queryAudit.reportFormat", "json");
    System.setProperty("queryAudit.reportRedaction", "full");
    System.setProperty("queryAudit.autoOpenReport", "false");
    HtmlReportAggregator.getInstance().reset();
    QueryAuditDataSourceStore.clear();
    PUBLICATIONS.clear();
  }

  @AfterEach
  void restore() {
    originalProperties.forEach(
        (key, value) -> {
          if (value == null) System.clearProperty(key);
          else System.setProperty(key, value);
        });
    ParallelFirstFixture.dataSource = null;
    ParallelSecondFixture.dataSource = null;
    SequentialFirstFixture.dataSource = null;
    SequentialSecondFixture.dataSource = null;
    HtmlReportAggregator.getInstance().reset();
    QueryAuditDataSourceStore.clear();
    PUBLICATIONS.clear();
    ready = null;
    queriesFinished = null;
  }

  @Test
  void sharedProxyAcrossConcurrentClassesIsAttributedToItsExecutingTest(@TempDir Path output)
      throws Exception {
    DataSource shared = proxy();
    ParallelFirstFixture.dataSource = shared;
    ParallelSecondFixture.dataSource = shared;
    ready = new CountDownLatch(2);
    queriesFinished = new CountDownLatch(2);

    TestExecutionSummary summary =
        launch(output, true, ParallelFirstFixture.class, ParallelSecondFixture.class);
    CanonicalEvidence canonical = reportJson(output);
    assertThat(PUBLICATIONS).as("one canonical publication for this Launcher").hasSize(1);
    PublicationEvidence published = publication(PUBLICATIONS.get(0));

    Map<String, String> expectedQueries =
        Map.of(
            testId(ParallelFirstFixture.class), "SELECT 11",
            testId(ParallelSecondFixture.class), "SELECT 22");
    assertAll(
        "a passing audit must attribute each query only to its executing test",
        () -> {
          assertThat(summary.getFailures()).isEmpty();
          assertThat(summary.getTestsSucceededCount()).isEqualTo(2);
          assertThat(canonical.outcome()).isEqualTo("PASS");
        },
        () -> {
          List<QueryAuditReport> reports = HtmlReportAggregator.getInstance().getReports();
          assertThat(reports)
              .extracting(QueryAuditReport::getTestId)
              .containsExactlyInAnyOrderElementsOf(expectedQueries.keySet());
          for (QueryAuditReport report : reports) {
            assertThat(report.getTotalQueryCount()).as(report.getTestId()).isEqualTo(1);
            assertThat(report.getAllQueries())
                .extracting(QueryRecord::sql)
                .containsExactly(expectedQueries.get(report.getTestId()));
          }
        },
        () -> assertCanonicalReports(canonical, expectedQueries.keySet().stream().toList()),
        () -> assertPublishedCounts(published, 2, 2));
  }

  @Test
  void classParallelDefaultCapturesCorrectlyWhenMethodDefaultIsSameThread(@TempDir Path output)
      throws Exception {
    DataSource shared = proxy();
    SequentialFirstFixture.dataSource = shared;
    SequentialSecondFixture.dataSource = shared;
    ready = new CountDownLatch(2);
    queriesFinished = new CountDownLatch(2);

    TestExecutionSummary summary =
        launch(output, true, SequentialFirstFixture.class, SequentialSecondFixture.class);

    assertThat(PUBLICATIONS).hasSize(1);
    assertThat(summary.getFailures()).isEmpty();
    assertThat(summary.getTestsSucceededCount()).isEqualTo(2);
    assertCanonicalReports(
        reportJson(output),
        List.of(testId(SequentialFirstFixture.class), testId(SequentialSecondFixture.class)));
    assertPublishedCounts(publication(PUBLICATIONS.get(0)), 2, 2);
    assertThat(HtmlReportAggregator.getInstance().getReports())
        .allSatisfy(
            report ->
                assertThat(report.getAllQueries())
                    .extracting(QueryRecord::sql)
                    .containsExactly(
                        report.getTestId().equals(testId(SequentialFirstFixture.class))
                            ? "SELECT 1"
                            : "SELECT 2"));
  }

  @Test
  void parallelEngineWithSameThreadClassesAndMethodsKeepsCorrectAttribution(@TempDir Path output)
      throws Exception {
    DataSource shared = proxy();
    SequentialFirstFixture.dataSource = shared;
    SequentialSecondFixture.dataSource = shared;

    TestExecutionSummary summary =
        launch(
            output,
            true,
            "same_thread",
            SequentialFirstFixture.class,
            SequentialSecondFixture.class);

    assertThat(summary.getFailures()).isEmpty();
    assertThat(summary.getTestsSucceededCount()).isEqualTo(2);
    assertCanonicalReports(
        reportJson(output),
        List.of(testId(SequentialFirstFixture.class), testId(SequentialSecondFixture.class)));
    assertThat(PUBLICATIONS).hasSize(1);
    assertPublishedCounts(publication(PUBLICATIONS.get(0)), 2, 2);
    assertThat(HtmlReportAggregator.getInstance().getReports())
        .allSatisfy(
            report ->
                assertThat(report.getAllQueries())
                    .singleElement()
                    .satisfies(
                        query ->
                            assertThat(query.sql())
                                .isEqualTo(
                                    report.getTestId().equals(testId(SequentialFirstFixture.class))
                                        ? "SELECT 1"
                                        : "SELECT 2")));
  }

  @Test
  void successiveLaunchersPublishOnlyTheirOwnReportsWithoutResetBetweenRuns(@TempDir Path output)
      throws Exception {
    DataSource shared = proxy();
    SequentialFirstFixture.dataSource = shared;
    SequentialSecondFixture.dataSource = shared;
    Path firstOutput = output.resolve("first");
    Path secondOutput = output.resolve("second");

    TestExecutionSummary first = launch(firstOutput, false, SequentialFirstFixture.class);
    assertThat(first.getFailures()).isEmpty();
    assertThat(first.getTestsSucceededCount()).isEqualTo(1);
    assertCanonicalReports(reportJson(firstOutput), List.of(testId(SequentialFirstFixture.class)));
    assertThat(PUBLICATIONS).hasSize(1);
    assertPublishedCounts(publication(PUBLICATIONS.get(0)), 1, 1);

    // Deliberately keep both the aggregator and publication collector across the two Launchers.
    TestExecutionSummary second = launch(secondOutput, false, SequentialSecondFixture.class);
    assertThat(second.getFailures()).isEmpty();
    assertThat(second.getTestsSucceededCount()).isEqualTo(1);
    assertThat(PUBLICATIONS).as("each Launcher publishes exactly once").hasSize(2);
    CanonicalEvidence secondCanonical = reportJson(secondOutput);
    PublicationEvidence secondPublished = publication(PUBLICATIONS.get(1));

    assertAll(
        "the second run must not inherit reports, counts, or identities from the first",
        () ->
            assertCanonicalReports(secondCanonical, List.of(testId(SequentialSecondFixture.class))),
        () -> assertPublishedCounts(secondPublished, 1, 1),
        () -> {
          String firstPublishedId = publication(PUBLICATIONS.get(0)).tests().get(0).testId();
          assertThat(secondPublished.tests())
              .extracting(TestCount::testId)
              .doesNotContain(firstPublishedId);
        },
        () ->
            assertCanonicalReports(
                reportJson(firstOutput), List.of(testId(SequentialFirstFixture.class))));
  }

  private static TestExecutionSummary launch(Path output, boolean parallel, Class<?>... fixtures) {
    return launch(output, parallel, "concurrent", fixtures);
  }

  private static TestExecutionSummary launch(
      Path output, boolean parallel, String classMode, Class<?>... fixtures) {
    System.setProperty("queryAudit.reportOutputDir", output.toString());
    var request =
        LauncherDiscoveryRequestBuilder.request()
            .configurationParameter("junit.jupiter.extensions.autodetection.enabled", "false")
            .configurationParameter(
                "junit.jupiter.execution.parallel.enabled", Boolean.toString(parallel))
            .configurationParameter("junit.jupiter.execution.parallel.mode.default", "same_thread")
            .configurationParameter(
                "junit.jupiter.execution.parallel.mode.classes.default", classMode)
            .configurationParameter("junit.jupiter.execution.parallel.config.strategy", "fixed")
            .configurationParameter(
                "junit.jupiter.execution.parallel.config.fixed.parallelism", "2");
    for (Class<?> fixture : fixtures) request.selectors(selectClass(fixture));
    SummaryGeneratingListener listener = new SummaryGeneratingListener();
    // Exercise the supported no-manifest path independently of the outer test's listeners.
    var launcher =
        LauncherFactory.create(
            LauncherConfig.builder().enableTestExecutionListenerAutoRegistration(false).build());
    launcher.execute(request.build(), listener);
    return listener.getSummary();
  }

  private record TestCount(String testId, long queries) {}

  private record CanonicalEvidence(String outcome, int incompleteReasons, List<TestCount> tests) {}

  private record PublicationEvidence(
      String outcome, int reportedTests, long totalQueries, List<TestCount> tests) {}

  private static CanonicalEvidence reportJson(Path directory) throws Exception {
    Path file = directory.resolve("report.json");
    assertThat(file).as("canonical report for this Launcher").exists();
    Map<String, Object> document = JsonPath.parse(Files.readString(file)).json();
    List<TestCount> tests = new ArrayList<>();
    for (Object entry : (List<?>) document.get("reports")) {
      Map<?, ?> report = (Map<?, ?>) entry;
      Map<?, ?> summary = (Map<?, ?>) report.get("summary");
      tests.add(
          new TestCount(
              (String) report.get("testId"), ((Number) summary.get("totalQueries")).longValue()));
    }
    return new CanonicalEvidence(
        (String) document.get("outcome"),
        ((List<?>) document.get("incompleteReasons")).size(),
        List.copyOf(tests));
  }

  private static PublicationEvidence publication(PublishedAuditRun run) {
    Map<String, Object> document = JsonPath.parse(run.json()).json();
    List<TestCount> tests = new ArrayList<>();
    for (Object entry : (List<?>) document.get("tests")) {
      Map<?, ?> test = (Map<?, ?>) entry;
      tests.add(
          new TestCount(
              (String) test.get("testId"), ((Number) test.get("totalQueries")).longValue()));
    }
    return new PublicationEvidence(
        (String) document.get("outcome"),
        ((Number) document.get("reportedTests")).intValue(),
        ((Number) document.get("totalQueries")).longValue(),
        List.copyOf(tests));
  }

  private static void assertCanonicalReports(CanonicalEvidence document, List<String> expectedIds) {
    assertThat(document.outcome()).isEqualTo("PASS");
    assertThat(document.tests())
        .extracting(TestCount::testId)
        .containsExactlyInAnyOrderElementsOf(expectedIds);
    assertThat(document.tests()).allSatisfy(test -> assertThat(test.queries()).isEqualTo(1));
  }

  private static void assertPublishedCounts(PublicationEvidence document, int tests, int queries) {
    assertThat(document.outcome()).isEqualTo("PASS");
    assertThat(document.reportedTests()).isEqualTo(tests);
    assertThat(document.totalQueries()).isEqualTo(queries);
    assertThat(document.tests()).hasSize(tests);
    assertThat(document.tests()).allSatisfy(test -> assertThat(test.queries()).isEqualTo(1));
  }

  private static String testId(Class<?> fixture) {
    return "[engine:junit-jupiter]/[class:" + fixture.getName() + "]/[method:ownQuery()]";
  }

  private static DataSource proxy() {
    JdbcDataSource source = new JdbcDataSource();
    source.setURL("jdbc:h2:mem:run-isolation-regression");
    return ProxyDataSourceBuilder.create(source).build();
  }

  private static void execute(DataSource source, String sql) throws Exception {
    try (var connection = source.getConnection();
        var statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static void executeTogether(DataSource source, String sql) throws Exception {
    ready.countDown();
    if (!ready.await(10, TimeUnit.SECONDS)) {
      throw new AssertionError("Both fixture methods did not reach the coordination barrier");
    }
    execute(source, sql);
    queriesFinished.countDown();
    if (!queriesFinished.await(10, TimeUnit.SECONDS)) {
      throw new AssertionError("Both fixture queries did not finish before the audit boundary");
    }
  }

  private static void executeDefaultFixture(DataSource source, String sql) throws Exception {
    if (ready == null) execute(source, sql);
    else executeTogether(source, sql);
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  @Execution(ExecutionMode.CONCURRENT)
  static class ParallelFirstFixture {
    static DataSource dataSource;
    @RegisterExtension static final QueryAuditExtension audit = new QueryAuditExtension(EXTENSIONS);

    @Test
    @Execution(ExecutionMode.SAME_THREAD)
    void ownQuery() throws Exception {
      executeTogether(dataSource, "SELECT 11");
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  @Execution(ExecutionMode.CONCURRENT)
  static class ParallelSecondFixture {
    static DataSource dataSource;
    @RegisterExtension static final QueryAuditExtension audit = new QueryAuditExtension(EXTENSIONS);

    @Test
    @Execution(ExecutionMode.SAME_THREAD)
    void ownQuery() throws Exception {
      executeTogether(dataSource, "SELECT 22");
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class SequentialFirstFixture {
    static DataSource dataSource;
    @RegisterExtension static final QueryAuditExtension audit = new QueryAuditExtension(EXTENSIONS);

    @Test
    void ownQuery() throws Exception {
      executeDefaultFixture(dataSource, "SELECT 1");
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class SequentialSecondFixture {
    static DataSource dataSource;
    @RegisterExtension static final QueryAuditExtension audit = new QueryAuditExtension(EXTENSIONS);

    @Test
    void ownQuery() throws Exception {
      executeDefaultFixture(dataSource, "SELECT 2");
    }
  }
}
