package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import com.jayway.jsonpath.JsonPath;
import io.queryaudit.core.config.QueryAuditConfig;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.platform.launcher.core.LauncherConfig;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@Isolated("Owns properties and nested Launcher fixtures")
class ParallelExcludedCaptureTest {
  private static final String ENABLED = "queryaudit.test.excludedCaptureFixture";
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
  private static CountDownLatch ignoredFinished;

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
    raw.setURL("jdbc:h2:mem:excluded-capture;DB_CLOSE_DELAY=-1");
    SharedDataSource.dataSource = ProxyDataSourceBuilder.create(raw).build();
    captureStarted = new CountDownLatch(1);
    ignoredFinished = new CountDownLatch(1);
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
  void excludedMethodDoesNotContaminateAnotherAuditedClass(@TempDir Path output) throws Exception {
    assertPassing(output, ExcludedMethod.class, false);
  }

  @Test
  void excludedClassRemainsIgnoredWithExtensionAutoDetection(@TempDir Path output)
      throws Exception {
    assertPassing(output, ExcludedClass.class, true);
  }

  @Test
  void disabledSpringConfigurationDoesNotContaminateAnAuditedClass(@TempDir Path output)
      throws Exception {
    assertPassing(output, DisabledClass.class, true);
  }

  private static void assertPassing(Path output, Class<?> ignored, boolean autodetection)
      throws Exception {
    System.setProperty("queryAudit.reportOutputDir", output.toString());
    var request =
        LauncherDiscoveryRequestBuilder.request()
            .selectors(selectClass(AuditedClass.class), selectClass(ignored))
            .configurationParameter(
                "junit.jupiter.extensions.autodetection.enabled", Boolean.toString(autodetection))
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
        .singleElement()
        .satisfies(
            report ->
                assertThat(report.getAllQueries())
                    .extracting(QueryRecord::sql)
                    .containsExactly("SELECT 101"));
    assertThat(json).doesNotContain("UNATTRIBUTED_QUERY", "SELECT 999");
  }

  static class SharedDataSource {
    static DataSource dataSource;

    static void execute(String sql) throws Exception {
      try (var connection = dataSource.getConnection();
          var statement = connection.createStatement()) {
        statement.execute(sql);
      }
    }

    static void ignoredQuery() throws Exception {
      assertThat(captureStarted.await(10, TimeUnit.SECONDS)).isTrue();
      try {
        execute("SELECT 999");
      } finally {
        ignoredFinished.countDown();
      }
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  @QueryAudit(autoOpenReport = BooleanOverride.FALSE)
  static class AuditedClass extends SharedDataSource {
    @Test
    void audited() throws Exception {
      captureStarted.countDown();
      assertThat(ignoredFinished.await(10, TimeUnit.SECONDS)).isTrue();
      execute("SELECT 101");
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  @QueryAudit
  static class ExcludedMethod extends SharedDataSource {
    @Test
    @QueryAuditExclude
    void excluded() throws Exception {
      ignoredQuery();
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  @QueryAudit
  @QueryAuditExclude
  static class ExcludedClass extends SharedDataSource {
    @Test
    void excluded() throws Exception {
      ignoredQuery();
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  @QueryAudit
  @SpringJUnitConfig(DisabledConfig.class)
  static class DisabledClass extends SharedDataSource {
    @Test
    void disabled() throws Exception {
      ignoredQuery();
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class DisabledConfig {
    @Bean
    QueryAuditConfig queryAuditConfig() {
      return QueryAuditConfig.builder().enabled(false).build();
    }
  }
}
