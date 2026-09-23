package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import com.jayway.jsonpath.JsonPath;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.platform.launcher.core.LauncherConfig;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;

@Isolated("Owns properties and nested class-lifecycle fixtures")
class ParallelClassLifecycleCaptureTest {
  private static final String ENABLED = "queryaudit.test.classLifecycleCaptureFixture";
  private static final List<String> PROPERTIES =
      List.of(
          ENABLED,
          "queryAudit.reportFormat",
          "queryAudit.reportOutputDir",
          "queryAudit.reportRedaction",
          "queryAudit.autoOpenReport",
          "queryAudit.coverageManifest");
  private final Map<String, String> previous = new LinkedHashMap<>();
  private static CountDownLatch captureStarted;
  private static CountDownLatch classWorkFinished;

  @BeforeEach
  void prepare() {
    for (String key : PROPERTIES) {
      previous.put(key, System.getProperty(key));
      System.clearProperty(key);
    }
    System.setProperty(ENABLED, "true");
    System.setProperty("queryAudit.reportFormat", "json");
    System.setProperty("queryAudit.reportRedaction", "full");
    System.setProperty("queryAudit.autoOpenReport", "false");
    JdbcDataSource raw = new JdbcDataSource();
    raw.setURL("jdbc:h2:mem:class-lifecycle-capture;DB_CLOSE_DELAY=-1");
    SharedDataSource.dataSource = ProxyDataSourceBuilder.create(raw).build();
    captureStarted = new CountDownLatch(1);
    classWorkFinished = new CountDownLatch(1);
    HtmlReportAggregator.getInstance().reset();
  }

  @AfterEach
  void restore() {
    previous.forEach(
        (key, value) -> {
          if (value == null) System.clearProperty(key);
          else System.setProperty(key, value);
        });
    SharedDataSource.dataSource = null;
    HtmlReportAggregator.getInstance().reset();
    QueryAuditDataSourceStore.clear();
  }

  @Test
  void beforeAllSqlCannotContaminateAnotherClassActiveTest(@TempDir Path output) throws Exception {
    assertPassing(output, BeforeAllClass.class);
  }

  @Test
  void afterAllSqlCannotContaminateAnotherClassActiveTest(@TempDir Path output) throws Exception {
    assertPassing(output, AfterAllClass.class);
  }

  private static void assertPassing(Path output, Class<?> lifecycleClass) throws Exception {
    System.setProperty("queryAudit.reportOutputDir", output.toString());
    var request =
        LauncherDiscoveryRequestBuilder.request()
            .selectors(selectClass(ActiveClass.class), selectClass(lifecycleClass))
            .configurationParameter("junit.jupiter.extensions.autodetection.enabled", "true")
            .configurationParameter("junit.jupiter.execution.parallel.enabled", "true")
            .configurationParameter("junit.jupiter.execution.parallel.mode.default", "concurrent")
            .configurationParameter(
                "junit.jupiter.execution.parallel.mode.classes.default", "concurrent")
            .configurationParameter("junit.jupiter.execution.parallel.config.strategy", "fixed")
            .configurationParameter(
                "junit.jupiter.execution.parallel.config.fixed.parallelism", "2")
            .build();
    SummaryGeneratingListener summary = new SummaryGeneratingListener();
    LauncherFactory.create(
            LauncherConfig.builder().enableTestExecutionListenerAutoRegistration(false).build())
        .execute(request, summary);
    assertThat(summary.getSummary().getFailures()).isEmpty();
    assertThat(summary.getSummary().getTestsSucceededCount()).isEqualTo(2);
    String json = Files.readString(output.resolve("report.json"));
    assertThat(JsonPath.<String>read(json, "$.outcome")).isEqualTo("PASS");
    assertThat(HtmlReportAggregator.getInstance().getReports())
        .hasSize(2)
        .allSatisfy(
            report -> {
              String expected =
                  report.getTestId().contains(ActiveClass.class.getName())
                      ? "SELECT 101"
                      : "SELECT 202";
              assertThat(report.getAllQueries())
                  .extracting(QueryRecord::sql)
                  .containsExactly(expected);
            });
    assertThat(json).doesNotContain("UNATTRIBUTED_QUERY", "SELECT 900");
  }

  static class SharedDataSource {
    static DataSource dataSource;

    static void execute(String sql) throws Exception {
      try (var connection = dataSource.getConnection();
          var statement = connection.createStatement()) {
        statement.execute(sql);
      }
    }

    static void classLifecycleQuery() throws Exception {
      assertThat(captureStarted.await(10, TimeUnit.SECONDS)).isTrue();
      try {
        execute("SELECT 900");
      } finally {
        classWorkFinished.countDown();
      }
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  @QueryAudit(autoOpenReport = BooleanOverride.FALSE)
  static class ActiveClass extends SharedDataSource {
    @Test
    void audited() throws Exception {
      captureStarted.countDown();
      assertThat(classWorkFinished.await(10, TimeUnit.SECONDS)).isTrue();
      execute("SELECT 101");
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  @QueryAudit(autoOpenReport = BooleanOverride.FALSE)
  static class BeforeAllClass extends SharedDataSource {
    @BeforeAll
    static void setup() throws Exception {
      classLifecycleQuery();
    }

    @Test
    void audited() throws Exception {
      execute("SELECT 202");
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  @QueryAudit(autoOpenReport = BooleanOverride.FALSE)
  static class AfterAllClass extends SharedDataSource {
    @Test
    void audited() throws Exception {
      execute("SELECT 202");
    }

    @AfterAll
    static void teardown() throws Exception {
      classLifecycleQuery();
    }
  }
}
