package io.queryaudit.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.junit5.BooleanOverride;
import io.queryaudit.junit5.QueryAudit;
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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class MissingAutoConfigurationMessageTest {
  private static final String ENABLED = "queryaudit.test.missingAutoConfiguration";

  @Test
  void namesTheMissingAutoConfigurationWhenTheStarterIsPresent(@TempDir Path output) {
    Map<String, String> saved = new HashMap<>();
    for (String key : List.of(ENABLED, "queryAudit.reportOutputDir", "queryAudit.reportFormat"))
      saved.put(key, System.getProperty(key));
    HtmlReportAggregator.getInstance().reset();
    try {
      System.setProperty(ENABLED, "true");
      System.setProperty("queryAudit.reportOutputDir", output.toString());
      System.setProperty("queryAudit.reportFormat", "json");
      SummaryGeneratingListener listener = new SummaryGeneratingListener();
      LauncherFactory.create()
          .execute(
              LauncherDiscoveryRequestBuilder.request()
                  .selectors(selectClass(Fixture.class))
                  .configurationParameter("junit.jupiter.execution.parallel.enabled", "false")
                  .configurationParameter("junit.jupiter.extensions.autodetection.enabled", "false")
                  .build(),
              listener);

      assertThat(listener.getSummary().getFailures())
          .singleElement()
          .satisfies(
              failure ->
                  assertThat(failure.getException())
                      .hasMessageContaining("auto-configuration did not run in this test context")
                      .hasMessageContaining(
                          "@ImportAutoConfiguration(QueryAuditAutoConfiguration.class)")
                      .hasMessageNotContaining("Add the QueryAudit Spring Boot starter"));
    } finally {
      saved.forEach(
          (key, value) -> {
            if (value == null) System.clearProperty(key);
            else System.setProperty(key, value);
          });
      HtmlReportAggregator.getInstance().reset();
    }
  }

  @SpringJUnitConfig(PlainDataSourceConfiguration.class)
  @QueryAudit(autoOpenReport = BooleanOverride.FALSE)
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class Fixture {
    @Test
    void neverRuns() {}
  }

  @Configuration
  static class PlainDataSourceConfiguration {
    @Bean
    DataSource dataSource() {
      JdbcDataSource dataSource = new JdbcDataSource();
      dataSource.setURL("jdbc:h2:mem:plain-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
      return dataSource;
    }
  }
}
