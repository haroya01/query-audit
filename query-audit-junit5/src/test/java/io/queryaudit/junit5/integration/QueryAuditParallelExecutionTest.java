package io.queryaudit.junit5.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.junit5.BooleanOverride;
import io.queryaudit.junit5.QueryAudit;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
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

@DisplayName("QueryAudit real parallel method execution (issue #190)")
@Isolated("Owns fixture properties and the legacy report accumulator")
class QueryAuditParallelExecutionTest {
  private static final String ENABLED = "queryaudit.test.parallelMethodFixture";
  private static final List<String> PROPERTIES =
      List.of(
          ENABLED,
          "queryAudit.reportFormat",
          "queryAudit.reportOutputDir",
          "queryAudit.autoOpenReport",
          "queryAudit.coverageManifest");
  private static final Set<Thread> THREADS = ConcurrentHashMap.newKeySet();
  private static CyclicBarrier bothCapturing;
  private static CountDownLatch firstFinished;
  private final Map<String, String> originalProperties = new LinkedHashMap<>();

  @BeforeEach
  void prepare() {
    for (String key : PROPERTIES) {
      originalProperties.put(key, System.getProperty(key));
      System.clearProperty(key);
    }
    System.setProperty(ENABLED, "true");
    System.setProperty("queryAudit.reportFormat", "json");
    System.setProperty("queryAudit.autoOpenReport", "false");
    bothCapturing = new CyclicBarrier(2);
    firstFinished = new CountDownLatch(1);
    THREADS.clear();
    HtmlReportAggregator.getInstance().reset();
  }

  @AfterEach
  void restore() {
    originalProperties.forEach(
        (key, value) -> {
          if (value == null) System.clearProperty(key);
          else System.setProperty(key, value);
        });
    AuditedFixture.dataSource = null;
    THREADS.clear();
    HtmlReportAggregator.getInstance().reset();
  }

  @Test
  void endingOneConcurrentMethodDoesNotInterruptAnotherActiveCapture(@TempDir Path output) {
    JdbcDataSource source = new JdbcDataSource();
    source.setURL("jdbc:h2:mem:parallel-method-integration");
    var shared = ProxyDataSourceBuilder.create(source).build();
    AuditedFixture.dataSource = shared;
    List<?> listenersBefore =
        List.copyOf(shared.getProxyConfig().getQueryListener().getListeners());
    List<?> methodsBefore = List.copyOf(shared.getProxyConfig().getMethodListener().getListeners());
    System.setProperty("queryAudit.reportOutputDir", output.toString());
    var request =
        LauncherDiscoveryRequestBuilder.request()
            .selectors(selectClass(AuditedFixture.class))
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
            if (test.getUniqueId().endsWith("[method:firstMethod()]")) firstFinished.countDown();
          }
        });

    assertThat(summary.getSummary().getFailures()).isEmpty();
    assertThat(summary.getSummary().getTestsSucceededCount()).isEqualTo(2);
    assertThat(THREADS).hasSize(2);
    assertThat(firstFinished.getCount()).isZero();
    List<QueryAuditReport> reports = HtmlReportAggregator.getInstance().getReports();
    assertThat(reports).hasSize(2);
    for (QueryAuditReport report : reports) {
      List<String> expected =
          report.getTestId().endsWith("[method:firstMethod()]")
              ? List.of("SELECT 1")
              : List.of("SELECT 2", "SELECT 3");
      assertThat(report.getAllQueries())
          .extracting(QueryRecord::sql)
          .containsExactlyElementsOf(expected);
      assertThat(report.getTotalQueryCount()).isEqualTo(expected.size());
    }
    assertThat(shared.getProxyConfig().getQueryListener().getListeners())
        .isEqualTo(listenersBefore);
    assertThat(shared.getProxyConfig().getMethodListener().getListeners()).isEqualTo(methodsBefore);
    assertThat(output.resolve("report.json")).exists();
  }

  private static void query(String sql) throws Exception {
    try (var connection = AuditedFixture.dataSource.getConnection();
        var statement = connection.createStatement()) {
      statement.execute(sql);
    }
  }

  private static void overlap() throws Exception {
    THREADS.add(Thread.currentThread());
    bothCapturing.await(10, TimeUnit.SECONDS);
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  @Execution(ExecutionMode.CONCURRENT)
  @QueryAudit(failOnDetection = BooleanOverride.FALSE, autoOpenReport = BooleanOverride.FALSE)
  static class AuditedFixture {
    static DataSource dataSource;

    @Test
    void firstMethod() throws Exception {
      overlap();
      query("SELECT 1");
      overlap();
    }

    @Test
    void secondMethod() throws Exception {
      overlap();
      query("SELECT 2");
      overlap();
      assertThat(firstFinished.await(10, TimeUnit.SECONDS))
          .as("the first method must finish before the second capture continues")
          .isTrue();
      query("SELECT 3");
    }
  }
}
