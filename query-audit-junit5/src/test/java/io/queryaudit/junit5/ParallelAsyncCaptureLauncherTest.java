package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import com.jayway.jsonpath.JsonPath;
import io.queryaudit.core.extension.AuditExtensions;
import io.queryaudit.core.extension.AuditRule;
import io.queryaudit.core.extension.FindingKindId;
import io.queryaudit.core.extension.RuleContext;
import io.queryaudit.core.extension.RuleDescriptor;
import io.queryaudit.core.extension.RuleId;
import io.queryaudit.core.interceptor.QueryCaptureSession;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.core.reporter.delivery.PublishedAuditRun;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import net.ttddyy.dsproxy.support.ProxyDataSource;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.core.LauncherConfig;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

/** Public executor propagation and fail-closed async diagnostics exercised through JUnit. */
@Isolated("Owns nested Launcher properties, a worker pool, and the legacy report accumulator")
class ParallelAsyncCaptureLauncherTest {
  private static final String ENABLED = "queryaudit.test.asyncCaptureFixture";
  private static final String PRIVATE_WORKER_SQL = "SELECT 'worker-private-marker-733117'";
  private static final String PRIVATE_ANALYSIS_SQL = "SELECT 'analysis-private-marker-733117'";
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
          .auditRule("test:analysis-suppression", new AnalysisSuppressionProbe())
          .reportSink("test:async-capture", true, PUBLICATIONS::add)
          .build();
  private static Scenario scenario;
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
    scenario = new Scenario();
    FixtureBase.dataSource = scenario.proxy;
    HtmlReportAggregator.getInstance().reset();
    QueryAuditDataSourceStore.clear();
    PUBLICATIONS.clear();
  }

  @AfterEach
  void restore() throws InterruptedException {
    try {
      scenario.releaseWorker.countDown();
      scenario.executor.shutdownNow();
      assertThat(scenario.executor.awaitTermination(10, TimeUnit.SECONDS))
          .as("the fixture must not leave worker threads behind")
          .isTrue();
    } finally {
      originalProperties.forEach(
          (key, value) -> {
            if (value == null) System.clearProperty(key);
            else System.setProperty(key, value);
          });
      FixtureBase.dataSource = null;
      scenario = null;
      HtmlReportAggregator.getInstance().reset();
      QueryAuditDataSourceStore.clear();
      PUBLICATIONS.clear();
    }
  }

  @Test
  void wrappedRunnableAndCallableBelongToTheirConcurrentJUnitInvocations(@TempDir Path output)
      throws Exception {
    TestExecutionSummary summary = launch(output, WrappedFixture.class);
    assertSucceeded(summary, 2);
    Map<String, List<String>> expected =
        Map.of(
            testId(WrappedFixture.class, "callable"), List.of("SELECT 1101", "SELECT 1102"),
            testId(WrappedFixture.class, "runnable"), List.of("SELECT 2201", "SELECT 2202"));
    assertEvidence(output, expected, "PASS");
    assertThat(scenario.workerThreads).as("both executor tasks must overlap").hasSize(2);
    assertThat(scenario.junitThreads).as("both JUnit invocations must overlap").hasSize(2);
    assertThat(scenario.workerThreads).doesNotContainAnyElementsOf(scenario.junitThreads);
  }

  @Test
  void unwrappedWorkerSqlDuringParallelAuditsIsInconclusiveWithSafeIdentity(@TempDir Path output)
      throws Exception {
    TestExecutionSummary summary = launch(output, UnwrappedFixture.class);
    assertSucceeded(summary, 2);
    String id = testId(UnwrappedFixture.class, "unwrapped");
    String sibling = testId(UnwrappedFixture.class, "overlaps");
    assertEvidence(
        output,
        Map.of(id, List.of("SELECT 3301"), sibling, List.of("SELECT 3302")),
        "INCONCLUSIVE");
    assertIncompleteReason(output, id, "UNATTRIBUTED_QUERY");
    assertIncompleteReason(output, sibling, "UNATTRIBUTED_QUERY");
    assertThat(scenario.workerThreads).hasSize(1);
    assertThat(Files.readString(output.resolve("report.json"))).doesNotContain(PRIVATE_WORKER_SQL);
  }

  @Test
  void unwrappedWorkerSqlCountsTowardTheOnlyRunningAudit(@TempDir Path output) throws Exception {
    TestExecutionSummary summary = launch(output, SoleUnwrappedFixture.class);
    assertSucceeded(summary, 1);
    String id = testId(SoleUnwrappedFixture.class, "unwrapped");
    assertEvidence(output, Map.of(id, List.of("SELECT 3401", "SELECT 3402")), "PASS");
    assertThat(scenario.workerThreads).hasSize(1);
  }

  @Test
  void unfinishedWrappedWorkIsInconclusiveAndCannotContaminateTheNextRoot(@TempDir Path output)
      throws Exception {
    Path firstOutput = output.resolve("unfinished");
    TestExecutionSummary first = launch(firstOutput, UnfinishedFixture.class);
    assertSucceeded(first, 1);
    String firstId = testId(UnfinishedFixture.class, "leavesWorkRunning");
    assertEvidence(firstOutput, Map.of(firstId, List.of("SELECT 4401")), "INCONCLUSIVE");
    assertIncompleteReason(firstOutput, firstId, "ASYNC_WORK_STILL_RUNNING");
    assertThat(scenario.pending).isNotNull();
    assertThat(scenario.pending.isDone()).isTrue();
    assertThat(scenario.lateQueriesExecuted).hasValue(1);

    // Reuse the same pool, DataSource, accumulator, and sink. Only the Launcher root is new.
    Path nextOutput = output.resolve("next");
    TestExecutionSummary next = launch(nextOutput, FreshFixture.class);
    assertSucceeded(next, 1);
    String nextId = testId(FreshFixture.class, "fresh");
    assertEvidence(nextOutput, Map.of(nextId, List.of("SELECT 5501", "SELECT 5502")), "PASS");
    assertThat(PUBLICATIONS).hasSize(2);
    assertThat(Files.readString(nextOutput.resolve("report.json")))
        .doesNotContain(firstId, "SELECT 4401", "SELECT 4402", "ASYNC_WORK_STILL_RUNNING");
    assertThat(PUBLICATIONS.get(1).json()).doesNotContain(publishedId(firstId));
  }

  @Test
  void aQueuedWrapperCannotBecomeAPassingAuditBeforeItsWorkerStarts(@TempDir Path output)
      throws Exception {
    TestExecutionSummary summary = launch(output, QueuedFixture.class);
    assertSucceeded(summary, 1);
    String id = testId(QueuedFixture.class, "queuesWork");
    assertEvidence(output, Map.of(id, List.of("SELECT 7701")), "INCONCLUSIVE");
    assertIncompleteReason(output, id, "ASYNC_WORK_NOT_COMPLETED");
    assertThat(scenario.blockers)
        .hasSize(2)
        .allSatisfy(blocker -> assertThat(blocker.isDone()).isTrue());
    assertThat(scenario.pending.isDone()).isTrue();
    assertThat(scenario.lateQueriesExecuted).hasValue(1);
    assertThat(Files.readString(output.resolve("report.json"))).doesNotContain("SELECT 7702");
  }

  @Test
  void sqlDuringAfterEachAnalysisIsSuppressedWithoutPoisoningAnActiveSibling(@TempDir Path output)
      throws Exception {
    scenario.analysisProbeEnabled = true;
    TestExecutionSummary summary = launch(output, AnalysisFixture.class);
    assertSucceeded(summary, 2);
    assertThat(scenario.analysisQueriesExecuted).hasValue(1);
    assertEvidence(
        output,
        Map.of(
            testId(AnalysisFixture.class, "finishesFirst"), List.of("SELECT 6101"),
            testId(AnalysisFixture.class, "staysActive"), List.of("SELECT 6201", "SELECT 6202")),
        "PASS");
    assertThat(Files.readString(output.resolve("report.json")))
        .doesNotContain(PRIVATE_ANALYSIS_SQL, "WORK_AFTER_CAPTURE", "UNATTRIBUTED_QUERY");
  }

  private static TestExecutionSummary launch(Path output, Class<?> fixture) {
    System.setProperty("queryAudit.reportOutputDir", output.toString());
    var request =
        LauncherDiscoveryRequestBuilder.request()
            .selectors(selectClass(fixture))
            .configurationParameter("junit.jupiter.extensions.autodetection.enabled", "false")
            .configurationParameter("junit.jupiter.execution.parallel.enabled", "true")
            .configurationParameter("junit.jupiter.execution.parallel.mode.default", "concurrent")
            .configurationParameter("junit.jupiter.execution.parallel.config.strategy", "fixed")
            .configurationParameter(
                "junit.jupiter.execution.parallel.config.fixed.parallelism", "2")
            .build();
    SummaryGeneratingListener summary = new SummaryGeneratingListener();
    var launcher =
        LauncherFactory.create(
            LauncherConfig.builder().enableTestExecutionListenerAutoRegistration(false).build());
    launcher.execute(
        request,
        summary,
        new TestExecutionListener() {
          @Override
          public void executionFinished(TestIdentifier test, TestExecutionResult result) {
            if (test.getUniqueId().equals(testId(UnfinishedFixture.class, "leavesWorkRunning"))
                || test.getUniqueId().equals(testId(QueuedFixture.class, "queuesWork"))) {
              scenario.releaseWorker.countDown();
              try {
                if (scenario.pending != null) scenario.pending.get(10, TimeUnit.SECONDS);
                for (Future<?> blocker : scenario.blockers) blocker.get(10, TimeUnit.SECONDS);
              } catch (Exception failure) {
                scenario.listenerFailure.set(failure);
              }
            }
          }
        });
    assertThat(scenario.listenerFailure.get()).as("late worker cleanup must complete").isNull();
    assertThat(scenario.proxy.getProxyConfig().getQueryListener().getListeners())
        .isEqualTo(scenario.queryListeners);
    assertThat(scenario.proxy.getProxyConfig().getMethodListener().getListeners())
        .isEqualTo(scenario.methodListeners);
    return summary.getSummary();
  }

  private static void assertSucceeded(TestExecutionSummary summary, int count) {
    assertThat(summary.getFailures()).isEmpty();
    assertThat(summary.getTestsSucceededCount()).isEqualTo(count);
  }

  private static void assertEvidence(
      Path output, Map<String, List<String>> expected, String outcome) throws Exception {
    List<QueryAuditReport> reports =
        HtmlReportAggregator.getInstance().getReports().stream()
            .filter(report -> expected.containsKey(report.getTestId()))
            .toList();
    assertThat(reports)
        .extracting(QueryAuditReport::getTestId)
        .containsExactlyInAnyOrderElementsOf(expected.keySet());
    for (QueryAuditReport report : reports) {
      assertThat(report.getAllQueries())
          .extracting(QueryRecord::sql)
          .containsExactlyElementsOf(expected.get(report.getTestId()));
      assertThat(report.getTotalQueryCount()).isEqualTo(expected.get(report.getTestId()).size());
    }
    Map<String, Object> canonical = readCanonical(output);
    assertThat(canonical.get("outcome")).isEqualTo(outcome);
    List<Map<String, Object>> canonicalReports =
        (List<Map<String, Object>>) canonical.get("reports");
    assertThat(canonicalReports)
        .extracting(report -> (String) report.get("testId"))
        .containsExactlyInAnyOrderElementsOf(expected.keySet());
    if (outcome.equals("PASS")) assertThat((List<?>) canonical.get("incompleteReasons")).isEmpty();
    String publicationJson = PUBLICATIONS.get(PUBLICATIONS.size() - 1).json();
    Map<String, Object> published = JsonPath.parse(publicationJson).json();
    assertThat(published.get("outcome")).isEqualTo(outcome);
    List<Map<String, Object>> publishedTests = (List<Map<String, Object>>) published.get("tests");
    Map<String, Integer> counts = new LinkedHashMap<>();
    expected.forEach(
        (id, queries) -> {
          counts.put(publishedId(id), queries.size());
          assertThat(publicationJson).doesNotContain(id);
        });
    assertThat(publishedTests)
        .extracting(test -> (String) test.get("testId"))
        .containsExactlyInAnyOrderElementsOf(counts.keySet());
    assertThat(publishedTests)
        .allSatisfy(
            test ->
                assertThat(((Number) test.get("totalQueries")).intValue())
                    .isEqualTo(counts.get(test.get("testId"))));
    assertThat(publicationJson)
        .doesNotContain(
            "SELECT ", "private-worker-name", "worker-private-marker", "analysis-private-marker");
  }

  private static void assertIncompleteReason(Path output, String testId, String safeReason)
      throws Exception {
    List<Map<String, Object>> reasons =
        (List<Map<String, Object>>) readCanonical(output).get("incompleteReasons");
    assertThat(reasons)
        .anySatisfy(
            reason -> {
              assertThat(reason.get("code")).isEqualTo("AUDIT_ANALYSIS_FAILED");
              assertThat(reason.get("detail"))
                  .isEqualTo("Query capture incomplete [" + safeReason + "] for " + testId);
            });
    assertThat(reasons)
        .allSatisfy(
            reason ->
                assertThat((String) reason.get("detail"))
                    .doesNotContain("SELECT ", "worker-private-marker"));
    Map<String, Object> published =
        JsonPath.parse(PUBLICATIONS.get(PUBLICATIONS.size() - 1).json()).json();
    assertThat((List<String>) published.get("incompleteReasonCodes"))
        .contains("AUDIT_ANALYSIS_FAILED");
  }

  private static Map<String, Object> readCanonical(Path output) throws Exception {
    return JsonPath.parse(Files.readString(output.resolve("report.json"))).json();
  }

  private static String testId(Class<?> fixture, String method) {
    return "[engine:junit-jupiter]/[class:" + fixture.getName() + "]/[method:" + method + "()]";
  }

  private static String publishedId(String rawId) {
    try {
      return "qa-published-test-v1:"
          + HexFormat.of()
              .formatHex(
                  MessageDigest.getInstance("SHA-256")
                      .digest(rawId.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException impossible) {
      throw new AssertionError(impossible);
    }
  }

  private static void execute(String sql) throws Exception {
    try (var connection = FixtureBase.dataSource.getConnection();
        var statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static void workerQuery(int value, boolean overlap) throws Exception {
    scenario.workerThreads.add(Thread.currentThread());
    if (overlap) scenario.workersOverlap.await(10, TimeUnit.SECONDS);
    execute("SELECT " + value);
    if (overlap) scenario.workersOverlap.await(10, TimeUnit.SECONDS);
  }

  private static void await(CountDownLatch latch, String reason) throws InterruptedException {
    assertThat(latch.await(10, TimeUnit.SECONDS)).as(reason).isTrue();
  }

  private static final class Scenario {
    final ExecutorService executor = Executors.newFixedThreadPool(2);
    final CyclicBarrier workersOverlap = new CyclicBarrier(2);
    final CyclicBarrier testsOverlap = new CyclicBarrier(2);
    final CountDownLatch workerStarted = new CountDownLatch(1);
    final CountDownLatch blockersStarted = new CountDownLatch(2);
    final CountDownLatch releaseWorker = new CountDownLatch(1);
    final CountDownLatch analysisQueryFinished = new CountDownLatch(1);
    final Set<Thread> workerThreads = ConcurrentHashMap.newKeySet();
    final Set<Thread> junitThreads = ConcurrentHashMap.newKeySet();
    final AtomicInteger lateQueriesExecuted = new AtomicInteger();
    final AtomicInteger analysisQueriesExecuted = new AtomicInteger();
    final AtomicReference<Throwable> listenerFailure = new AtomicReference<>();
    final List<Future<?>> blockers = new CopyOnWriteArrayList<>();
    final ProxyDataSource proxy;
    final List<?> queryListeners;
    final List<?> methodListeners;
    volatile Future<?> pending;
    boolean analysisProbeEnabled;

    Scenario() {
      JdbcDataSource source = new JdbcDataSource();
      source.setURL("jdbc:h2:mem:parallel-async-capture-acceptance");
      proxy = ProxyDataSourceBuilder.create(source).build();
      queryListeners = List.copyOf(proxy.getProxyConfig().getQueryListener().getListeners());
      methodListeners = List.copyOf(proxy.getProxyConfig().getMethodListener().getListeners());
    }
  }

  private static final class AnalysisSuppressionProbe implements AuditRule {
    @Override
    public RuleDescriptor descriptor() {
      return new RuleDescriptor(
          RuleId.of("test:analysis-probe"),
          "1",
          Set.of(FindingKindId.of("test:analysis-probe")),
          Map.of());
    }

    @Override
    public List<Finding> evaluate(RuleContext context) {
      if (scenario.analysisProbeEnabled
          && context.queries().stream().anyMatch(query -> query.sql().equals("SELECT 6101"))) {
        try {
          execute(PRIVATE_ANALYSIS_SQL);
          scenario.analysisQueriesExecuted.incrementAndGet();
        } catch (Exception failure) {
          throw new AssertionError(failure);
        } finally {
          scenario.analysisQueryFinished.countDown();
        }
      }
      return List.of();
    }
  }

  static class FixtureBase {
    static DataSource dataSource;
    @RegisterExtension static final QueryAuditExtension audit = new QueryAuditExtension(EXTENSIONS);
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  @Execution(ExecutionMode.CONCURRENT)
  static class WrappedFixture extends FixtureBase {
    @Test
    void callable() throws Exception {
      scenario.junitThreads.add(Thread.currentThread());
      execute("SELECT 1101");
      Future<Integer> future =
          scenario.executor.submit(
              QueryCaptureSession.wrap(
                  (Callable<Integer>)
                      () -> {
                        workerQuery(1102, true);
                        return 1102;
                      }));
      assertThat(future.get(10, TimeUnit.SECONDS)).isEqualTo(1102);
    }

    @Test
    void runnable() throws Exception {
      scenario.junitThreads.add(Thread.currentThread());
      execute("SELECT 2201");
      Future<?> future =
          scenario.executor.submit(
              QueryCaptureSession.wrap(
                  (Runnable)
                      () -> {
                        try {
                          workerQuery(2202, true);
                        } catch (Exception failure) {
                          throw new AssertionError(failure);
                        }
                      }));
      future.get(10, TimeUnit.SECONDS);
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class UnwrappedFixture extends FixtureBase {
    @Test
    @DisplayName("private-worker-name")
    void unwrapped() throws Exception {
      scenario.testsOverlap.await(10, TimeUnit.SECONDS);
      execute("SELECT 3301");
      scenario
          .executor
          .submit(
              (Callable<Void>)
                  () -> {
                    scenario.workerThreads.add(Thread.currentThread());
                    execute(PRIVATE_WORKER_SQL);
                    return null;
                  })
          .get(10, TimeUnit.SECONDS);
      scenario.testsOverlap.await(10, TimeUnit.SECONDS);
    }

    @Test
    void overlaps() throws Exception {
      scenario.testsOverlap.await(10, TimeUnit.SECONDS);
      execute("SELECT 3302");
      scenario.testsOverlap.await(10, TimeUnit.SECONDS);
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class SoleUnwrappedFixture extends FixtureBase {
    @Test
    void unwrapped() throws Exception {
      execute("SELECT 3401");
      scenario
          .executor
          .submit(
              (Callable<Void>)
                  () -> {
                    scenario.workerThreads.add(Thread.currentThread());
                    execute("SELECT 3402");
                    return null;
                  })
          .get(10, TimeUnit.SECONDS);
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class UnfinishedFixture extends FixtureBase {
    @Test
    void leavesWorkRunning() throws Exception {
      execute("SELECT 4401");
      scenario.pending =
          scenario.executor.submit(
              QueryCaptureSession.wrap(
                  (Callable<Void>)
                      () -> {
                        scenario.workerThreads.add(Thread.currentThread());
                        scenario.workerStarted.countDown();
                        await(
                            scenario.releaseWorker,
                            "the root listener must release the late worker");
                        execute("SELECT 4402");
                        scenario.lateQueriesExecuted.incrementAndGet();
                        return null;
                      }));
      await(scenario.workerStarted, "the worker must be running when this test ends");
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class FreshFixture extends FixtureBase {
    @Test
    void fresh() throws Exception {
      execute("SELECT 5501");
      scenario
          .executor
          .submit(
              QueryCaptureSession.wrap(
                  (Callable<Void>)
                      () -> {
                        workerQuery(5502, false);
                        return null;
                      }))
          .get(10, TimeUnit.SECONDS);
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class QueuedFixture extends FixtureBase {
    @Test
    void queuesWork() throws Exception {
      execute("SELECT 7701");
      for (int index = 0; index < 2; index++) {
        scenario.blockers.add(
            scenario.executor.submit(
                (Callable<Void>)
                    () -> {
                      scenario.blockersStarted.countDown();
                      await(
                          scenario.releaseWorker,
                          "the root listener must release occupied workers");
                      return null;
                    }));
      }
      await(scenario.blockersStarted, "both pool workers must be occupied before submission");
      scenario.pending =
          scenario.executor.submit(
              QueryCaptureSession.wrap(
                  (Callable<Void>)
                      () -> {
                        workerQuery(7702, false);
                        scenario.lateQueriesExecuted.incrementAndGet();
                        return null;
                      }));
      assertThat(scenario.pending.isDone()).isFalse();
      assertThat(scenario.workerThreads).as("the wrapped query must still be queued").isEmpty();
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  @Execution(ExecutionMode.CONCURRENT)
  static class AnalysisFixture extends FixtureBase {
    @Test
    void finishesFirst() throws Exception {
      execute("SELECT 6101");
      scenario.testsOverlap.await(10, TimeUnit.SECONDS);
    }

    @Test
    void staysActive() throws Exception {
      execute("SELECT 6201");
      scenario.testsOverlap.await(10, TimeUnit.SECONDS);
      await(
          scenario.analysisQueryFinished, "the sibling must execute SQL during afterEach analysis");
      execute("SELECT 6202");
    }
  }
}
