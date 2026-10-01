package io.queryaudit.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.extension.AuditRule;
import io.queryaudit.core.extension.FindingKindId;
import io.queryaudit.core.extension.RuleContext;
import io.queryaudit.core.extension.RuleDescriptor;
import io.queryaudit.core.extension.RuleId;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.junit5.BooleanOverride;
import io.queryaudit.junit5.QueryAudit;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class CustomFindingFailurePolicyIntegrationTest {
  private static final String ENABLED = "queryaudit.test.customFindingFailurePolicy";
  private static final AtomicInteger CALLS = new AtomicInteger();

  @Test
  void springRuleKindsCanBeSelectedWithoutEditingTheEnumAndUnselectedErrorsDoNotFail(
      @TempDir Path output) {
    Map<String, String> saved = new HashMap<>();
    for (String key : List.of(ENABLED, "queryAudit.reportOutputDir", "queryAudit.reportFormat"))
      saved.put(key, System.getProperty(key));
    CALLS.set(0);
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

      assertThat(listener.getSummary().getTestsFoundCount()).isEqualTo(4);
      assertThat(listener.getSummary().getTestsSucceededCount()).isEqualTo(1);
      assertThat(listener.getSummary().getTestsFailedCount()).isEqualTo(3);
      assertThat(listener.getSummary().getContainersFailedCount()).isZero();
      assertThat(listener.getSummary().getFailures())
          .allSatisfy(
              failure ->
                  assertThat(failure.getException())
                      .isInstanceOf(AssertionError.class)
                      .hasMessageContaining("shop:budget"));
      assertThat(CALLS).hasValue(4);
      assertThat(HtmlReportAggregator.getInstance().getReports())
          .hasSize(4)
          .allSatisfy(
              report -> {
                assertThat(report.getTotalQueryCount()).isEqualTo(1);
                assertThat(report.getFindings().errors())
                    .extracting(finding -> finding.kindId().value())
                    .contains("shop:budget");
              });
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
  @QueryAudit(failOnKinds = "shop:budget", autoOpenReport = BooleanOverride.FALSE)
  @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class Fixture {
    @Autowired DataSource dataSource;

    @Test
    void classSelectedCustomFails() throws Exception {
      query();
    }

    @Test
    @QueryAudit(failOnKinds = "shop:not-selected")
    void unselectedCustomErrorPassesWithoutHidingItsEvidence() throws Exception {
      query();
    }

    @Test
    @QueryAudit(failOn = IssueType.N_PLUS_ONE, failOnKinds = "shop:budget")
    void customAndLegacySelectionAreAUnion() throws Exception {
      query();
    }

    @Test
    @QueryAudit
    void defaultMethodPolicyStillIncludesCustomKinds() throws Exception {
      query();
    }

    private void query() throws Exception {
      try (Connection connection = dataSource.getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("SELECT 1");
      }
    }
  }

  @Configuration(proxyBeanMethods = false)
  @EnableAutoConfiguration
  static class FixtureConfiguration {
    @Bean
    DataSource dataSource() {
      JdbcDataSource source = new JdbcDataSource();
      source.setURL("jdbc:h2:mem:custom-failure-policy");
      return source;
    }

    @Bean
    QueryAuditConfig queryAuditConfig() {
      return QueryAuditConfig.builder().autoOpenReport(false).build();
    }

    @Bean
    AuditRule budgetRule() {
      return new AuditRule() {
        public RuleDescriptor descriptor() {
          return new RuleDescriptor(
              RuleId.of("shop:rule"), "1", Set.of(FindingKindId.of("shop:budget")), Map.of());
        }

        public List<Finding> evaluate(RuleContext context) {
          CALLS.incrementAndGet();
          return List.of(
              new Finding(
                  FindingKindId.of("shop:budget"),
                  Severity.ERROR,
                  context.queries().get(0).sql(),
                  null,
                  null,
                  "Custom budget exceeded",
                  "Improve the use case"));
        }
      };
    }
  }
}
