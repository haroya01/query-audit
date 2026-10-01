package io.queryaudit.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import com.jayway.jsonpath.JsonPath;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.junit5.EnableQueryInspector;
import io.queryaudit.junit5.ExpectQueries;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class BackgroundWorkAttributionTest {
  private static final String ENABLED = "queryaudit.test.backgroundWork";

  @Test
  void aNamedPoolIsAwaitedAndItsSqlCountsTowardTheTest(@TempDir Path directory) throws Exception {
    SummaryGeneratingListener listener = launch(directory, CountedFixture.class);

    assertThat(listener.getSummary().getTestsSucceededCount()).isEqualTo(1);
    Map<String, Object> report = report(directory);
    assertThat(report.get("outcome")).isEqualTo("PASS");
    assertThat((List<?>) report.get("incompleteReasons")).isEmpty();
    assertThat(JsonPath.<Integer>read(report, "$.reports[0].summary.totalQueries")).isEqualTo(2);
  }

  @Test
  void backgroundSqlCountsAgainstTheTestBudget(@TempDir Path directory) {
    SummaryGeneratingListener listener = launch(directory, OverBudgetFixture.class);

    assertThat(listener.getSummary().getFailures())
        .singleElement()
        .satisfies(
            failure ->
                assertThat(failure.getException())
                    .hasMessageContaining("SELECT: executed 2, expected at most 1"));
  }

  @Test
  void withoutNamedPoolsBackgroundSqlKeepsTheRunInconclusive(@TempDir Path directory)
      throws Exception {
    SummaryGeneratingListener listener = launch(directory, UndeclaredFixture.class);

    assertThat(listener.getSummary().getTestsSucceededCount()).isEqualTo(1);
    Map<String, Object> report = report(directory);
    assertThat(report.get("outcome")).isEqualTo("INCONCLUSIVE");
    assertThat(JsonPath.<List<String>>read(report, "$.incompleteReasons[*].code"))
        .contains("AUDIT_ANALYSIS_FAILED");
  }

  private static Map<String, Object> report(Path directory) throws Exception {
    return JsonPath.parse(Files.readString(directory.resolve("report.json"))).json();
  }

  private static SummaryGeneratingListener launch(Path directory, Class<?> fixture) {
    Map<String, String> saved = new HashMap<>();
    for (String key : List.of(ENABLED, "queryAudit.report.outputDir", "queryAudit.report.format"))
      saved.put(key, System.getProperty(key));
    HtmlReportAggregator.getInstance().reset();
    try {
      System.setProperty(ENABLED, "true");
      System.setProperty("queryAudit.report.outputDir", directory.toString());
      System.setProperty("queryAudit.report.format", "json");
      SummaryGeneratingListener listener = new SummaryGeneratingListener();
      LauncherFactory.create()
          .execute(
              LauncherDiscoveryRequestBuilder.request()
                  .selectors(selectClass(fixture))
                  .configurationParameter("junit.jupiter.execution.parallel.enabled", "false")
                  .build(),
              listener);
      return listener;
    } finally {
      saved.forEach(
          (key, value) -> {
            if (value == null) System.clearProperty(key);
            else System.setProperty(key, value);
          });
      HtmlReportAggregator.getInstance().reset();
    }
  }

  abstract static class BackgroundFixture {
    @Autowired DataSource dataSource;
    @Autowired ThreadPoolTaskExecutor clickExecutor;

    void readHereAndLaterInTheBackground() {
      JdbcTemplate jdbc = new JdbcTemplate(dataSource);
      jdbc.queryForObject("SELECT 1", Integer.class);
      clickExecutor.execute(
          () -> {
            sleep(150);
            jdbc.queryForObject("SELECT 2", Integer.class);
          });
    }
  }

  @SpringJUnitConfig(Application.class)
  @TestPropertySource(properties = "query-audit.await-executors=clickExecutor")
  @EnableQueryInspector
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class CountedFixture extends BackgroundFixture {
    @Test
    @ExpectQueries(select = 2)
    void readsTwice() {
      readHereAndLaterInTheBackground();
    }
  }

  @SpringJUnitConfig(Application.class)
  @TestPropertySource(properties = "query-audit.await-executors=clickExecutor")
  @EnableQueryInspector
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class OverBudgetFixture extends BackgroundFixture {
    @Test
    @ExpectQueries(select = 1)
    void readsTwice() {
      readHereAndLaterInTheBackground();
    }
  }

  @SpringJUnitConfig(Application.class)
  @EnableQueryInspector
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class UndeclaredFixture {
    @Autowired DataSource dataSource;
    @Autowired ThreadPoolTaskExecutor clickExecutor;

    @Test
    void readsInTheBackground() throws Exception {
      JdbcTemplate jdbc = new JdbcTemplate(dataSource);
      clickExecutor
          .submit(() -> jdbc.queryForObject("SELECT 1", Integer.class))
          .get(5, TimeUnit.SECONDS);
    }
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(interrupted);
    }
  }

  @Configuration
  @Import(QueryAuditAutoConfiguration.class)
  static class Application {
    @Bean
    DataSource dataSource() {
      JdbcDataSource dataSource = new JdbcDataSource();
      dataSource.setURL("jdbc:h2:mem:background-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
      return dataSource;
    }

    @Bean
    ThreadPoolTaskExecutor clickExecutor() {
      ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
      executor.setCorePoolSize(1);
      executor.initialize();
      return executor;
    }
  }
}
