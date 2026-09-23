package io.queryaudit.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.junit5.BooleanOverride;
import io.queryaudit.junit5.ExpectQueries;
import io.queryaudit.junit5.QueryAudit;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
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
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class ReplacedContextCaptureTest {
  private static final String ENABLED = "queryaudit.test.replacedContextCapture";

  @Test
  void queriesAfterContextReplacementAreCapturedAgainstTheNewDataSource(@TempDir Path output) {
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

      assertThat(listener.getSummary().getTestsFoundCount()).isEqualTo(3);
      assertThat(listener.getSummary().getTestsSucceededCount()).isEqualTo(2);
      assertThat(listener.getSummary().getTestsFailedCount()).isEqualTo(1);
      assertThat(listener.getSummary().getFailures())
          .singleElement()
          .satisfies(
              failure -> {
                assertThat(failure.getTestIdentifier().getDisplayName())
                    .startsWith("zeroBudgetAfterReplacement");
                assertThat(failure.getException())
                    .isInstanceOf(AssertionError.class)
                    .hasMessageContaining("SELECT");
              });
      assertThat(HtmlReportAggregator.getInstance().getReports())
          .extracting(report -> report.getTotalQueryCount())
          .containsExactly(1, 1, 1);
    } finally {
      saved.forEach(
          (key, value) -> {
            if (value == null) System.clearProperty(key);
            else System.setProperty(key, value);
          });
      HtmlReportAggregator.getInstance().reset();
    }
  }

  @SpringJUnitConfig(FixtureConfiguration.class)
  @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
  @QueryAudit(failOnDetection = BooleanOverride.FALSE, autoOpenReport = BooleanOverride.FALSE)
  @TestMethodOrder(MethodOrderer.OrderAnnotation.class)
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class Fixture {
    @Autowired DataSource dataSource;

    @Test
    @Order(1)
    @ExpectQueries(select = 1)
    void firstContext() throws Exception {
      selectOne();
    }

    @Test
    @Order(2)
    @ExpectQueries(select = 1)
    void secondContext() throws Exception {
      selectOne();
    }

    @Test
    @Order(3)
    @ExpectQueries(select = 0)
    void zeroBudgetAfterReplacement() throws Exception {
      selectOne();
    }

    private void selectOne() throws Exception {
      try (Connection connection = dataSource.getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("SELECT 1");
      }
    }
  }

  @Configuration
  @Import(QueryAuditAutoConfiguration.class)
  static class FixtureConfiguration {
    @Bean
    DataSource dataSource() {
      JdbcDataSource dataSource = new JdbcDataSource();
      dataSource.setURL("jdbc:h2:mem:replaced-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
      return dataSource;
    }
  }
}
