package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import com.jayway.jsonpath.JsonPath;
import io.queryaudit.core.extension.AuditExtensions;
import io.queryaudit.core.model.LifecyclePhase;
import io.queryaudit.core.model.QueryAuditReport;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import net.ttddyy.dsproxy.support.ProxyDataSource;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.core.LauncherConfig;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

/** Real Launcher acceptance tests: every overlap is established by a bounded barrier, not sleep. */
@Isolated("Owns fixture properties and the legacy report accumulator during nested Launcher runs")
class ParallelAuditCaptureLauncherTest {
  private static final String ENABLED = "queryaudit.test.parallelCaptureFixture";
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
          .reportSink("test:parallel-capture", true, PUBLICATIONS::add)
          .build();
  private static final ThreadLocal<String> TEST_ID = new ThreadLocal<>();
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
    FixtureBase.dataSource = null;
    scenario = null;
    TEST_ID.remove();
    HtmlReportAggregator.getInstance().reset();
    QueryAuditDataSourceStore.clear();
    PUBLICATIONS.clear();
  }

  @Test
  void concurrentMethodsHaveIndependentCaptureWindows(@TempDir Path output) throws Exception {
    useProxy(2);
    assertPassingRun(output, 2, 2, ConcurrentMethods.class);
  }

  @Test
  void perClassLifecycleSharesOnlyTheTestInstance(@TempDir Path output) throws Exception {
    useProxy(2);
    assertPassingRun(output, 2, 2, PerClassMethods.class);
    assertThat(scenario.instances).as("PER_CLASS must really reuse the instance").hasSize(1);
  }

  @Test
  void nestedSiblingsInheritConfigurationButNotCapture(@TempDir Path output) throws Exception {
    useProxy(2);
    assertPassingRun(output, 2, 2, NestedMethods.class);
    assertThat(scenario.expected.keySet())
        .anyMatch(id -> id.contains("[nested-class:First]"))
        .anyMatch(id -> id.contains("[nested-class:Second]"));
  }

  @Test
  void repeatedInvocationsKeepDistinctIdsDespiteTheSameDisplayName(@TempDir Path output)
      throws Exception {
    useProxy(2);
    assertPassingRun(output, 2, 2, RepeatedMethods.class);
    assertInvocationIdentities(2);
  }

  @Test
  void parameterizedInvocationsKeepDistinctIdsDespiteTheSameDisplayName(@TempDir Path output)
      throws Exception {
    useProxy(2);
    assertPassingRun(output, 2, 2, ParameterizedMethods.class);
    assertInvocationIdentities(2);
  }

  @RepeatedTest(10)
  void twoClassesAndTwoMethodsOverlapOnOneProxy(@TempDir Path output) throws Exception {
    useProxy(4);
    assertPassingRun(output, 4, 4, FirstConcurrentClass.class, SecondConcurrentClass.class);
  }

  @Test
  void concurrentClassesSharingAnInheritedRawStaticFieldRestoreItsOriginalValue(
      @TempDir Path output) throws Exception {
    scenario = new Scenario(4);
    DataSource original = rawSource();
    FixtureBase.dataSource = original;
    assertPassingRun(output, 4, 4, FirstConcurrentClass.class, SecondConcurrentClass.class);
    assertThat(FixtureBase.dataSource).isSameAs(original);
  }

  @Test
  void endingOneClassDoesNotDetachTheOtherClassCapture(@TempDir Path output) throws Exception {
    useProxy(2);
    scenario.finishSuffix = "[class:" + FastClass.class.getName() + "]";
    assertPassingRun(output, 2, 2, FastClass.class, SlowClass.class);
    assertThat(scenario.finished.getCount()).isZero();
    assertThat(scenario.expected.values())
        .anySatisfy(
            queries ->
                assertThat(queries)
                    .extracting(ExpectedQuery::sql)
                    .containsExactly("SELECT 801", "SELECT 802"));
  }

  @Test
  void testBodyFailureDoesNotStopItsSiblingOrLeakIntoTheNextLauncher(@TempDir Path output)
      throws Exception {
    useProxy(2);
    scenario.finishSuffix = "[method:fails()]";
    TestExecutionSummary summary = launch(output.resolve("failed"), 2, BodyFailure.class);
    assertExpectedFixtureFailure(summary);
    assertEvidence(output.resolve("failed"), 2, false);
    assertCleanListeners();

    // Keep both the public accumulator and publication collector across root executions.
    Scenario previous = scenario;
    scenario = new Scenario(1);
    scenario.proxy = previous.proxy;
    scenario.queryListeners = previous.queryListeners;
    scenario.methodListeners = previous.methodListeners;
    TestExecutionSummary next = launch(output.resolve("next"), 2, NextRoot.class);
    assertThat(next.getFailures()).isEmpty();
    assertThat(PUBLICATIONS).hasSize(2);
    assertEvidence(output.resolve("next"), 1, true);
    assertCleanListeners();
  }

  @Test
  void setupFailureIsIsolatedAndStillRetainsItsOwnSetupEvidence(@TempDir Path output)
      throws Exception {
    useProxy(2);
    scenario.finishSuffix = "[method:fails()]";
    TestExecutionSummary summary = launch(output, 2, SetupFailure.class);
    assertExpectedFixtureFailure(summary);
    assertThat(scenario.failedBodyEntered).isFalse();
    assertEvidence(output, 2, false);
    assertCleanListeners();
  }

  @Test
  void setupTestAndTeardownPhasesStayWithTheirOwnInvocation(@TempDir Path output) throws Exception {
    useProxy(2);
    assertPassingRun(output, 2, 2, LifecycleMethods.class);
    assertThat(scenario.expected.values())
        .allSatisfy(
            queries ->
                assertThat(queries)
                    .extracting(ExpectedQuery::phase)
                    .containsExactly(
                        LifecyclePhase.SETUP, LifecyclePhase.TEST, LifecyclePhase.TEARDOWN));
  }

  private void useProxy(int parties) {
    scenario = new Scenario(parties);
    ProxyDataSource proxy = ProxyDataSourceBuilder.create(rawSource()).build();
    FixtureBase.dataSource = proxy;
    scenario.proxy = proxy;
    scenario.queryListeners = List.copyOf(proxy.getProxyConfig().getQueryListener().getListeners());
    scenario.methodListeners =
        List.copyOf(proxy.getProxyConfig().getMethodListener().getListeners());
  }

  private static DataSource rawSource() {
    JdbcDataSource source = new JdbcDataSource();
    source.setURL("jdbc:h2:mem:parallel-capture-acceptance");
    return source;
  }

  private void assertPassingRun(Path output, int workers, int tests, Class<?>... fixtures)
      throws Exception {
    TestExecutionSummary summary = launch(output, workers, fixtures);
    assertThat(summary.getFailures()).isEmpty();
    assertThat(summary.getTestsSucceededCount()).isEqualTo(tests);
    assertThat(scenario.threads).as("fixture bodies must actually overlap").hasSize(workers);
    assertThat(PUBLICATIONS).hasSize(1);
    assertEvidence(output, tests, true);
    assertCleanListeners();
  }

  private static TestExecutionSummary launch(Path output, int workers, Class<?>... fixtures) {
    System.setProperty("queryAudit.reportOutputDir", output.toString());
    var request =
        LauncherDiscoveryRequestBuilder.request()
            .configurationParameter("junit.jupiter.extensions.autodetection.enabled", "false")
            .configurationParameter("junit.jupiter.execution.parallel.enabled", "true")
            .configurationParameter("junit.jupiter.execution.parallel.mode.default", "concurrent")
            .configurationParameter(
                "junit.jupiter.execution.parallel.mode.classes.default", "concurrent")
            .configurationParameter("junit.jupiter.execution.parallel.config.strategy", "fixed")
            .configurationParameter(
                "junit.jupiter.execution.parallel.config.fixed.parallelism",
                Integer.toString(workers));
    for (Class<?> fixture : fixtures) request.selectors(selectClass(fixture));
    SummaryGeneratingListener summary = new SummaryGeneratingListener();
    var launcher =
        LauncherFactory.create(
            LauncherConfig.builder().enableTestExecutionListenerAutoRegistration(false).build());
    launcher.execute(
        request.build(),
        summary,
        new TestExecutionListener() {
          @Override
          public void executionFinished(TestIdentifier test, TestExecutionResult result) {
            String suffix = scenario.finishSuffix;
            if (suffix != null && test.getUniqueId().endsWith(suffix))
              scenario.finished.countDown();
          }
        });
    return summary.getSummary();
  }

  private static void assertEvidence(Path output, int tests, boolean passing) throws Exception {
    assertThat(scenario.expected).hasSize(tests);
    List<QueryAuditReport> reports =
        HtmlReportAggregator.getInstance().getReports().stream()
            .filter(report -> scenario.expected.containsKey(report.getTestId()))
            .toList();
    assertThat(reports).hasSize(tests);
    assertThat(reports)
        .extracting(QueryAuditReport::getTestId)
        .containsExactlyInAnyOrderElementsOf(scenario.expected.keySet());
    for (QueryAuditReport report : reports) {
      List<ExpectedQuery> expected = scenario.expected.get(report.getTestId());
      assertThat(report.getAllQueries())
          .as(report.getTestId())
          .extracting(query -> new ExpectedQuery(query.sql(), query.phase()))
          .containsExactlyElementsOf(expected);
      assertThat(report.getTotalQueryCount()).isEqualTo(expected.size());
    }
    Map<String, Object> canonical =
        JsonPath.parse(Files.readString(output.resolve("report.json"))).json();
    List<Map<String, Object>> canonicalReports =
        (List<Map<String, Object>>) canonical.get("reports");
    assertThat(canonicalReports)
        .extracting(report -> (String) report.get("testId"))
        .containsExactlyInAnyOrderElementsOf(scenario.expected.keySet());
    String publicationJson = PUBLICATIONS.get(PUBLICATIONS.size() - 1).json();
    Map<String, Object> published = JsonPath.parse(publicationJson).json();
    List<Map<String, Object>> publishedTests = (List<Map<String, Object>>) published.get("tests");
    Map<String, Integer> expectedPublishedCounts = new LinkedHashMap<>();
    scenario.expected.forEach(
        (id, queries) -> {
          expectedPublishedCounts.put(publishedId(id), queries.size());
          assertThat(publicationJson).doesNotContain(id);
        });
    assertThat(publicationJson).doesNotContain("SELECT ", "same display name");
    assertThat(publishedTests)
        .extracting(test -> (String) test.get("testId"))
        .containsExactlyInAnyOrderElementsOf(expectedPublishedCounts.keySet());
    assertThat(publishedTests)
        .allSatisfy(
            test ->
                assertThat(((Number) test.get("totalQueries")).intValue())
                    .isEqualTo(expectedPublishedCounts.get(test.get("testId"))));
    long total = scenario.expected.values().stream().mapToLong(List::size).sum();
    assertThat(((Number) published.get("totalQueries")).longValue()).isEqualTo(total);
    if (passing) {
      assertThat(canonical.get("outcome")).isEqualTo("PASS");
      assertThat(published.get("outcome")).isEqualTo("PASS");
    }
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

  private static void assertCleanListeners() {
    if (scenario.proxy == null) return;
    assertThat(scenario.proxy.getProxyConfig().getQueryListener().getListeners())
        .isEqualTo(scenario.queryListeners);
    assertThat(scenario.proxy.getProxyConfig().getMethodListener().getListeners())
        .isEqualTo(scenario.methodListeners);
  }

  private static void assertInvocationIdentities(int count) {
    assertThat(scenario.expected.keySet())
        .hasSize(count)
        .allMatch(id -> id.contains("[test-template-invocation:#"));
    assertThat(HtmlReportAggregator.getInstance().getReports())
        .extracting(QueryAuditReport::getTestName)
        .containsOnly("same display name");
  }

  private static void assertExpectedFixtureFailure(TestExecutionSummary summary) {
    assertThat(summary.getTestsSucceededCount()).isEqualTo(1);
    assertThat(summary.getTestsFailedCount()).isEqualTo(1);
    assertThat(summary.getFailures())
        .singleElement()
        .satisfies(
            failure ->
                assertThat(failure.getException()).isInstanceOf(ExpectedFixtureFailure.class));
    assertThat(scenario.threads).hasSize(2);
    assertThat(PUBLICATIONS).hasSize(1);
  }

  private static void rendezvous() throws Exception {
    scenario.threads.add(Thread.currentThread());
    scenario.barrier.await(10, TimeUnit.SECONDS);
  }

  private static void awaitFinished() throws InterruptedException {
    assertThat(scenario.finished.await(10, TimeUnit.SECONDS))
        .as("the sibling must finish its complete JUnit lifecycle")
        .isTrue();
  }

  private static void query(int value) throws Exception {
    query(value, LifecyclePhase.TEST);
  }

  private static void query(int value, LifecyclePhase phase) throws Exception {
    String id = TEST_ID.get();
    assertThat(id).as("fixture identity callback must have run").isNotBlank();
    String sql = "SELECT " + value;
    scenario
        .expected
        .computeIfAbsent(id, ignored -> new CopyOnWriteArrayList<>())
        .add(new ExpectedQuery(sql, phase));
    try (var connection = FixtureBase.dataSource.getConnection();
        var statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static void queryTogether(int value) throws Exception {
    rendezvous();
    query(value);
    rendezvous();
  }

  private record ExpectedQuery(String sql, LifecyclePhase phase) {}

  private static final class Scenario {
    final CyclicBarrier barrier;
    final Map<String, List<ExpectedQuery>> expected = new ConcurrentHashMap<>();
    final Set<Thread> threads = ConcurrentHashMap.newKeySet();
    final Set<Object> instances = ConcurrentHashMap.newKeySet();
    final CountDownLatch finished = new CountDownLatch(1);
    final AtomicBoolean failedBodyEntered = new AtomicBoolean();
    String finishSuffix;
    ProxyDataSource proxy;
    List<?> queryListeners = List.of();
    List<?> methodListeners = List.of();

    Scenario(int parties) {
      barrier = new CyclicBarrier(parties);
    }
  }

  private static final class IdentityCallback implements BeforeEachCallback, AfterEachCallback {
    @Override
    public void beforeEach(ExtensionContext context) {
      TEST_ID.set(context.getUniqueId());
    }

    @Override
    public void afterEach(ExtensionContext context) {
      TEST_ID.remove();
    }
  }

  private static final class ExpectedFixtureFailure extends RuntimeException {}

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  @Execution(ExecutionMode.CONCURRENT)
  static class FixtureBase {
    static DataSource dataSource;

    @Order(-100)
    @RegisterExtension
    static final IdentityCallback identity = new IdentityCallback();

    @RegisterExtension static final QueryAuditExtension audit = new QueryAuditExtension(EXTENSIONS);
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class ConcurrentMethods extends FixtureBase {
    @Test
    void first() throws Exception {
      queryTogether(101);
    }

    @Test
    void second() throws Exception {
      queryTogether(102);
    }
  }

  @TestInstance(TestInstance.Lifecycle.PER_CLASS)
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class PerClassMethods extends FixtureBase {
    @Test
    void first() throws Exception {
      scenario.instances.add(this);
      queryTogether(201);
    }

    @Test
    void second() throws Exception {
      scenario.instances.add(this);
      queryTogether(202);
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class NestedMethods extends FixtureBase {
    @Nested
    @Execution(ExecutionMode.CONCURRENT)
    @EnabledIfSystemProperty(named = ENABLED, matches = "true")
    class First {
      @Test
      void ownQuery() throws Exception {
        queryTogether(301);
      }
    }

    @Nested
    @Execution(ExecutionMode.CONCURRENT)
    @EnabledIfSystemProperty(named = ENABLED, matches = "true")
    class Second {
      @Test
      void ownQuery() throws Exception {
        queryTogether(302);
      }
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class RepeatedMethods extends FixtureBase {
    @RepeatedTest(value = 2, name = "same display name")
    void repeated(RepetitionInfo repetition) throws Exception {
      queryTogether(400 + repetition.getCurrentRepetition());
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class ParameterizedMethods extends FixtureBase {
    @ParameterizedTest(name = "same display name")
    @ValueSource(ints = {501, 502})
    void parameterized(int value) throws Exception {
      queryTogether(value);
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class FirstConcurrentClass extends FixtureBase {
    @Test
    void first() throws Exception {
      queryTogether(601);
    }

    @Test
    void second() throws Exception {
      queryTogether(602);
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class SecondConcurrentClass extends FixtureBase {
    @Test
    void first() throws Exception {
      queryTogether(701);
    }

    @Test
    void second() throws Exception {
      queryTogether(702);
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class FastClass extends FixtureBase {
    @Test
    void finishesFirst() throws Exception {
      queryTogether(800);
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class SlowClass extends FixtureBase {
    @Test
    void staysActive() throws Exception {
      queryTogether(801);
      awaitFinished();
      query(802);
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class BodyFailure extends FixtureBase {
    @Test
    void fails() throws Exception {
      queryTogether(901);
      throw new ExpectedFixtureFailure();
    }

    @Test
    void survives() throws Exception {
      queryTogether(902);
      awaitFinished();
      query(903);
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class SetupFailure extends FixtureBase {
    @BeforeEach
    void setup(TestInfo test) throws Exception {
      boolean fails = test.getTestMethod().orElseThrow().getName().equals("fails");
      query(fails ? 1001 : 1002, LifecyclePhase.SETUP);
      rendezvous();
      if (fails) throw new ExpectedFixtureFailure();
    }

    @Test
    void fails() {
      scenario.failedBodyEntered.set(true);
    }

    @Test
    void survives() throws Exception {
      awaitFinished();
      query(1003);
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class NextRoot extends FixtureBase {
    @Test
    void freshCapture() throws Exception {
      queryTogether(1101);
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class LifecycleMethods extends FixtureBase {
    @BeforeEach
    void setup(TestInfo test) throws Exception {
      query(value(test) + 1, LifecyclePhase.SETUP);
      rendezvous();
    }

    @Test
    void first() throws Exception {
      queryTogether(1202);
    }

    @Test
    void second() throws Exception {
      queryTogether(1302);
    }

    @AfterEach
    void teardown(TestInfo test) throws Exception {
      query(value(test) + 3, LifecyclePhase.TEARDOWN);
      rendezvous();
    }

    private int value(TestInfo test) {
      return test.getTestMethod().orElseThrow().getName().equals("first") ? 1200 : 1300;
    }
  }
}
