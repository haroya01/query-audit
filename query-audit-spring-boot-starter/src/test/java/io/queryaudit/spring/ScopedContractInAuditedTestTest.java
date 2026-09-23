package io.queryaudit.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectMethod;

import com.jayway.jsonpath.JsonPath;
import io.queryaudit.core.contract.QueryContractScope;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.junit5.BooleanOverride;
import io.queryaudit.junit5.EnableQueryInspector;
import io.queryaudit.junit5.QueryAudit;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class ScopedContractInAuditedTestTest {
  private static final String ENABLED = "queryaudit.test.scopedInAudited";
  static Path contracts;

  @Test
  void scopedBackgroundWorkKeepsTheMethodAuditComplete(@TempDir Path directory) throws Exception {
    contracts = Files.createDirectories(directory.resolve("contracts"));
    Files.write(
        contracts.resolve("api.contracts"), List.of("@junit | link-click | 1 | 0 | 0 | 0 | 1"));

    Map<String, Object> report = run(directory, selectClass(ObservedFixture.class));

    assertThat(report.get("outcome")).isEqualTo("PASS");
    assertThat((List<?>) report.get("incompleteReasons")).isEmpty();
  }

  @Test
  void methodContractFailuresNameTheFileInTheConfiguredDirectory(@TempDir Path directory)
      throws Exception {
    contracts = Files.createDirectories(directory.resolve("contracts"));
    String id =
        "[engine:junit-jupiter]/[class:"
            + EnforcedFixture.class.getName()
            + "]/[method:readsTwice()]";
    Files.write(
        contracts.resolve("methods.contracts"), List.of("@junit | " + id + " | 1 | 0 | 0 | 0 | 1"));

    SummaryGeneratingListener listener =
        launch(directory, selectMethod(EnforcedFixture.class, "readsTwice"));

    assertThat(listener.getSummary().getFailures())
        .singleElement()
        .satisfies(
            failure ->
                assertThat(failure.getException())
                    .hasMessageContaining("(" + contracts.resolve("methods.contracts") + ")")
                    .hasMessageContaining("SELECT: contract 1, executed 2 (+1)"));
  }

  private Map<String, Object> run(
      Path directory, org.junit.platform.engine.DiscoverySelector selector) throws Exception {
    SummaryGeneratingListener listener = launch(directory, selector);
    assertThat(listener.getSummary().getTestsFailedCount()).isZero();
    return JsonPath.parse(Files.readString(directory.resolve("report.json"))).json();
  }

  private SummaryGeneratingListener launch(
      Path directory, org.junit.platform.engine.DiscoverySelector selector) {
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
                  .selectors(selector)
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

  @SpringJUnitConfig(Application.class)
  @TestPropertySource(properties = "query-audit.contracts.await-executors=clickExecutor")
  @EnableQueryInspector
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class ObservedFixture {
    @Autowired QueryContractScope contracts;
    @Autowired DataSource dataSource;
    @Autowired ThreadPoolTaskExecutor clickExecutor;

    @DynamicPropertySource
    static void contracts(DynamicPropertyRegistry registry) {
      registry.add(
          "query-audit.contracts.path", () -> ScopedContractInAuditedTestTest.contracts.toString());
    }

    @Test
    void recordsAClickInTheBackground() throws Exception {
      JdbcTemplate jdbc = new JdbcTemplate(dataSource);
      contracts.verify(
          "link-click",
          () -> {
            clickExecutor.execute(() -> jdbc.queryForObject("SELECT 1", Integer.class));
            return null;
          });
    }
  }

  @SpringJUnitConfig(Application.class)
  @QueryAudit(autoOpenReport = BooleanOverride.FALSE)
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class EnforcedFixture {
    @Autowired DataSource dataSource;

    @DynamicPropertySource
    static void contracts(DynamicPropertyRegistry registry) {
      registry.add(
          "query-audit.contracts.path", () -> ScopedContractInAuditedTestTest.contracts.toString());
    }

    @Test
    void readsTwice() {
      JdbcTemplate jdbc = new JdbcTemplate(dataSource);
      jdbc.queryForObject("SELECT 1", Integer.class);
      jdbc.queryForObject("SELECT 2", Integer.class);
    }
  }

  @Configuration
  @Import(QueryAuditAutoConfiguration.class)
  static class Application {
    @Bean
    DataSource dataSource() {
      JdbcDataSource dataSource = new JdbcDataSource();
      dataSource.setURL("jdbc:h2:mem:audited-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
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
