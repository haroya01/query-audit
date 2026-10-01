package io.queryaudit.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import io.queryaudit.core.analyzer.ExplainAnalyzer;
import io.queryaudit.core.analyzer.IndexMetadataProvider;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.RuleProfile;
import io.queryaudit.core.detector.DetectionRule;
import io.queryaudit.core.extension.AuditRule;
import io.queryaudit.core.extension.FindingKindId;
import io.queryaudit.core.extension.RuleContext;
import io.queryaudit.core.extension.RuleDescriptor;
import io.queryaudit.core.extension.RuleId;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.core.reporter.delivery.AuditReportSink;
import io.queryaudit.junit5.BooleanOverride;
import io.queryaudit.junit5.QueryAudit;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
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
class SpringAuditExtensionsIntegrationTest {

  private static final String FIXTURE_PROPERTY = "queryaudit.test.springExtensionsFixture";
  private static final String RULE_DETAIL = "finding from the dependency-injected Spring rule";
  private static final String EXPLAIN_DETAIL = "finding from the Spring EXPLAIN bean";
  private static final InvocationState STATE = new InvocationState();
  private String previousFixtureProperty;

  @BeforeEach
  void prepareFixture() {
    previousFixtureProperty = System.getProperty(FIXTURE_PROPERTY);
    System.setProperty(FIXTURE_PROPERTY, "true");
    STATE.reset();
    HtmlReportAggregator.getInstance().reset();
  }

  @AfterEach
  void restoreFixture() {
    if (previousFixtureProperty == null) {
      System.clearProperty(FIXTURE_PROPERTY);
    } else {
      System.setProperty(FIXTURE_PROPERTY, previousFixtureProperty);
    }
    HtmlReportAggregator.getInstance().reset();
  }

  @Test
  void springBeansReachTheJUnitAnalysisAndMethodPoliciesRemainPerTest() {
    var request =
        LauncherDiscoveryRequestBuilder.request()
            .selectors(selectClass(AuditedFixture.class))
            .configurationParameter("junit.jupiter.extensions.autodetection.enabled", "false")
            .configurationParameter("junit.jupiter.execution.parallel.enabled", "false")
            .build();
    SummaryGeneratingListener listener = new SummaryGeneratingListener();

    LauncherFactory.create().execute(request, listener);

    assertThat(listener.getSummary().getFailures())
        .extracting(failure -> failure.getException())
        .isEmpty();
    assertThat(listener.getSummary().getTestsSucceededCount()).isEqualTo(2);
    assertThat(STATE.ruleInvocations.get()).isEqualTo(2);
    assertThat(STATE.metadataInvocations.get()).isEqualTo(1);
    assertThat(STATE.explainInvocations.get()).isEqualTo(2);
    assertThat(STATE.openRuleInvocations.get()).isEqualTo(2);
    assertThat(STATE.sinkInvocations.get()).isEqualTo(1);
    assertThat(STATE.publishedJson)
        .contains("shop:custom", "sanitized-summary")
        .doesNotContain("injected sensitive diagnostic", "SELECT 1");
    assertThat(STATE.ruleCloseCalls.get()).as("only Spring closes its rule bean").isEqualTo(1);

    List<QueryAuditReport> reports = HtmlReportAggregator.getInstance().getReports();
    assertThat(reports).hasSize(2);
    QueryAuditReport standard = reportNamed(reports, "customRuleRuns()");
    QueryAuditReport suppressed = reportNamed(reports, "methodSuppressionRemainsLocal()");
    assertThat(standard.getConfirmedIssues()).anyMatch(issue -> RULE_DETAIL.equals(issue.detail()));
    assertThat(standard.getCustomConfirmedFindings())
        .extracting(finding -> finding.kindId().value())
        .containsExactly("shop:custom");
    assertThat(suppressed.getCustomConfirmedFindings()).isEmpty();
    assertThat(suppressed.getConfirmedIssues())
        .noneMatch(issue -> RULE_DETAIL.equals(issue.detail()));
    assertThat(reports)
        .allSatisfy(
            report -> {
              assertThat(report.getTotalQueryCount()).isEqualTo(1);
              assertThat(report.getInfoIssues())
                  .anyMatch(issue -> EXPLAIN_DETAIL.equals(issue.detail()));
            });
  }

  private static QueryAuditReport reportNamed(List<QueryAuditReport> reports, String name) {
    return reports.stream()
        .filter(report -> report.getTestName().equals(name))
        .findFirst()
        .orElseThrow();
  }

  @SpringJUnitConfig(FixtureConfiguration.class)
  @QueryAudit(failOnDetection = BooleanOverride.FALSE, autoOpenReport = BooleanOverride.FALSE)
  @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class AuditedFixture {
    @Autowired DataSource dataSource;

    @Test
    void customRuleRuns() throws Exception {
      executeQuery();
    }

    @Test
    @QueryAudit(
        failOnDetection = BooleanOverride.FALSE,
        suppress = {"select-all", "shop:custom"})
    void methodSuppressionRemainsLocal() throws Exception {
      executeQuery();
    }

    private void executeQuery() throws Exception {
      assertThat(STATE.ruleCloseCalls.get()).isZero();
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
      JdbcDataSource dataSource = new JdbcDataSource();
      dataSource.setURL("jdbc:h2:mem:spring-extension-fixture;DB_CLOSE_DELAY=-1");
      return dataSource;
    }

    @Bean
    QueryAuditConfig customConfig() {
      return QueryAuditConfig.builder()
          .ruleProfile(RuleProfile.STRICT)
          .autoOpenReport(false)
          .build();
    }

    @Bean
    InvocationState invocationState() {
      return STATE;
    }

    @Bean
    InjectedRule applicationRule(InvocationState state) {
      return new InjectedRule(state);
    }

    @Bean
    AuditRule openApplicationRule(InvocationState state) {
      return new AuditRule() {
        public RuleDescriptor descriptor() {
          return new RuleDescriptor(
              new RuleId("shop:rule"),
              "1",
              Set.of(FindingKindId.of("shop:custom")),
              Map.of("limit", "1"));
        }

        public List<Finding> evaluate(RuleContext context) {
          state.openRuleInvocations.incrementAndGet();
          return List.of(
              new Finding(
                  FindingKindId.of("shop:custom"),
                  Severity.WARNING,
                  context.queries().get(0).sql(),
                  null,
                  null,
                  "injected sensitive diagnostic",
                  "custom suggestion"));
        }
      };
    }

    @Bean
    AuditReportSink applicationSink(InvocationState state) {
      return run -> {
        state.sinkInvocations.incrementAndGet();
        state.publishedJson = run.json();
      };
    }

    @Bean
    IndexMetadataProvider applicationIndexMetadataProvider(InvocationState state) {
      return new IndexMetadataProvider() {
        @Override
        public String supportedDatabase() {
          return "h2";
        }

        @Override
        public IndexMetadata getIndexMetadata(Connection connection) {
          state.metadataInvocations.incrementAndGet();
          return new IndexMetadata(Map.of());
        }
      };
    }

    @Bean
    ExplainAnalyzer applicationExplainAnalyzer(InvocationState state) {
      return new ExplainAnalyzer() {
        @Override
        public String supportedDatabase() {
          return "h2";
        }

        @Override
        public List<Issue> analyze(Connection connection, List<QueryRecord> queries) {
          state.explainInvocations.incrementAndGet();
          return List.of(
              new Issue(
                  IssueType.FILESORT,
                  Severity.INFO,
                  queries.get(0).sql(),
                  null,
                  null,
                  EXPLAIN_DETAIL,
                  "Use the application EXPLAIN policy"));
        }
      };
    }
  }

  static final class InjectedRule implements DetectionRule, AutoCloseable {
    private final InvocationState state;

    InjectedRule(InvocationState state) {
      this.state = state;
    }

    @Override
    public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata metadata) {
      state.ruleInvocations.incrementAndGet();
      return List.of(
          new Issue(
              IssueType.SELECT_ALL,
              Severity.WARNING,
              queries.get(0).sql(),
              null,
              null,
              RULE_DETAIL,
              "Use the application query policy"));
    }

    @Override
    public void close() {
      state.ruleCloseCalls.incrementAndGet();
    }
  }

  static final class InvocationState {
    final AtomicInteger ruleInvocations = new AtomicInteger();
    final AtomicInteger metadataInvocations = new AtomicInteger();
    final AtomicInteger explainInvocations = new AtomicInteger();
    final AtomicInteger ruleCloseCalls = new AtomicInteger();
    final AtomicInteger openRuleInvocations = new AtomicInteger();
    final AtomicInteger sinkInvocations = new AtomicInteger();
    volatile String publishedJson;

    void reset() {
      ruleInvocations.set(0);
      metadataInvocations.set(0);
      explainInvocations.set(0);
      ruleCloseCalls.set(0);
      openRuleInvocations.set(0);
      sinkInvocations.set(0);
      publishedJson = null;
    }
  }
}
