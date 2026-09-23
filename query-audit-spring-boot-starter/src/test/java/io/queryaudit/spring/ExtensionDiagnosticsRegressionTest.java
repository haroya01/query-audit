package io.queryaudit.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import io.queryaudit.core.analyzer.ExplainAnalyzer;
import io.queryaudit.core.analyzer.IndexMetadataProvider;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.ReportFormat;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.core.reporter.delivery.AuditReportSink;
import io.queryaudit.core.reporter.delivery.PublishedAuditRun;
import io.queryaudit.core.reporter.delivery.ReportSinkRegistration;
import io.queryaudit.junit5.BooleanOverride;
import io.queryaudit.junit5.QueryAudit;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestContextManager;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/** Consumer-facing regressions, including the deliberately strict run-level sink contract. */
@Isolated(
    "Exercises nested launchers, Spring contexts, system properties, streams, and aggregation")
class ExtensionDiagnosticsRegressionTest {
  private static final String FIXTURE_PROPERTY = "queryaudit.test.extensionDiagnosticsFixture";
  private static final String SCENARIO_PROPERTY = "queryaudit.test.extensionDiagnosticsScenario";
  private static final String PRIVATE_FAILURE =
      "token=private-extension-token SELECT secret_value FROM private_customer";
  private static final FixtureState STATE = new FixtureState();
  private static final AuditReportSink SHARED_SINK = new RecordingSink("applicationSink", false);
  private static final List<Class<?>> FIXTURES = List.of(FirstContext.class, SecondContext.class);

  @TempDir Path output;
  private final Map<String, String> previousProperties = new LinkedHashMap<>();
  private List<QueryAuditReport> previousReports;
  private int previousReportLimit;
  private PrintStream previousOut;
  private PrintStream previousErr;

  @BeforeEach
  void isolateHostState() {
    previousOut = System.out;
    previousErr = System.err;
    var aggregator = HtmlReportAggregator.getInstance();
    previousReports = aggregator.getReports();
    previousReportLimit = aggregator.getMaxInMemoryReports();
    aggregator.reset();
    STATE.reset();
    STATE.output = output;
    setProperty(FIXTURE_PROPERTY, "true");
    setProperty(SCENARIO_PROPERTY, "separate-instances");
    setProperty("queryAudit.mode", "annotated");
    setProperty("queryAudit.reportOutputDir", output.toString());
    setProperty("queryaudit.autoOpenReport", "false");
  }

  @AfterEach
  void restoreHostState() {
    try {
      closeFixtureContexts();
    } finally {
      System.setOut(previousOut);
      System.setErr(previousErr);
      previousProperties.forEach(
          (key, value) -> {
            if (value == null) System.clearProperty(key);
            else System.setProperty(key, value);
          });
      var aggregator = HtmlReportAggregator.getInstance();
      aggregator.reset();
      aggregator.setMaxInMemoryReports(Integer.MAX_VALUE);
      previousReports.forEach(aggregator::addReport);
      aggregator.setMaxInMemoryReports(previousReportLimit);
      STATE.reset();
    }
  }

  @Test
  void contextLocalSinkInstancesFailFastAndPublishInconclusiveInsteadOfCleanPass() {
    Execution result = execute("separate-instances", FirstContext.class, SecondContext.class);

    assertSinkConfigurationConflict(result);
    assertThat(STATE.sinkBeans).hasValue(2);
    assertThat(STATE.closedContexts).hasValue(2);
  }

  @Test
  void theSameSharedSinkInstanceWorksAcrossTwoRealSpringContexts() {
    Execution result = execute("shared-instance", FirstContext.class, SecondContext.class);

    assertThat(result.summary().getFailures()).isEmpty();
    assertThat(result.summary().getTestsSucceededCount()).isEqualTo(2);
    assertThat(STATE.startedTests).hasValue(2);
    assertThat(STATE.sinkBeans).hasValue(2);
    assertThat(STATE.closedContexts).hasValue(2);
    assertThat(STATE.attemptedSinks).containsExactly("applicationSink");
    assertThat(STATE.publishedRuns)
        .singleElement()
        .satisfies(
            run -> {
              assertThat(run.outcome()).isEqualTo(AuditOutcome.PASS);
              assertThat(run.json()).contains("\"reportedTests\":2");
            });
  }

  @Test
  void changingRequiredPolicyStillFailsEvenWhenContextsShareTheExactSinkInstance() {
    Execution result = execute("conflicting-policy", FirstContext.class, SecondContext.class);

    assertSinkConfigurationConflict(result);
    assertThat(STATE.sinkBeans).hasValue(2);
    assertThat(STATE.closedContexts).hasValue(2);
  }

  @Test
  void ambiguousProviderDiagnosticsIdentifyBothRegistrationsAndTheConfigurationReason() {
    Execution result = execute("provider-conflict", FirstContext.class);

    assertThat(result.summary().getFailures()).isEmpty();
    assertThat(result.summary().getTestsSucceededCount()).isEqualTo(1);
    assertThat(STATE.metadataInvocations).hasValue(0);
    assertThat(STATE.publishedRuns)
        .singleElement()
        .satisfies(
            run -> {
              assertThat(run.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
              assertThat(run.json()).contains("CAPABILITY_INITIALIZATION_FAILED");
            });
    assertAll(
        () -> assertPrivateFailureIsNotExposed(result),
        () ->
            assertThat(result.output())
                .as("A user must be able to identify both conflicting provider registrations")
                .contains("index-metadata:applicationIndexes", "index-metadata:duplicateIndexes"),
        () ->
            assertThat(result.output().toLowerCase(Locale.ROOT))
                .as(
                    "Distinguish ambiguous provider configuration from a failed database"
                        + " connection")
                .containsAnyOf("ambiguous", "ambiguity", "multiple", "conflicting providers"));
  }

  @Test
  void optionalSinkDiagnosticsNameEveryFailedRegistrationWithoutExposingExceptionPayloads() {
    Execution result = execute("sink-failures", FirstContext.class);

    assertThat(result.summary().getFailures()).isEmpty();
    assertThat(result.summary().getTestsSucceededCount()).isEqualTo(1);
    assertThat(STATE.attemptedSinks)
        .containsExactly(
            "applicationSink", "brokenNotification", "brokenWebhook", "healthyArchive");
    assertThat(STATE.publishedRuns)
        .hasSize(4)
        .allSatisfy(run -> assertThat(run.outcome()).isEqualTo(AuditOutcome.PASS));
    assertAll(
        () -> assertPrivateFailureIsNotExposed(result),
        () -> assertFailedSinkIsNamed(result, "report-sink:brokenNotification"),
        () -> assertFailedSinkIsNamed(result, "report-sink:brokenWebhook"));
  }

  private static void assertSinkConfigurationConflict(Execution result) {
    assertThat(result.summary().getTestsSucceededCount()).isEqualTo(1);
    assertThat(result.summary().getTotalFailureCount()).isEqualTo(1);
    assertThat(result.summary().getFailures())
        .singleElement()
        .satisfies(
            failure ->
                assertThat(failure.getException())
                    .isInstanceOf(ExtensionConfigurationException.class)
                    .hasMessageContaining("conflicting report sink registrations"));
    assertThat(STATE.startedTests)
        .as("The incompatible context must fail before its test body")
        .hasValue(1);
    assertThat(STATE.attemptedSinks).containsExactly("applicationSink");
    assertThat(STATE.publishedRuns)
        .singleElement()
        .satisfies(
            run -> {
              assertThat(run.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
              assertThat(run.json()).contains("AUDIT_INITIALIZATION_FAILED");
            });
    assertThat(result.output()).contains("outcome: INCONCLUSIVE").doesNotContain("outcome: PASS");
  }

  private static void assertFailedSinkIsNamed(Execution result, String registrationId) {
    assertThat(result.output().lines().filter(line -> line.contains(registrationId)).toList())
        .as("A safe diagnostic must associate the failed status with %s", registrationId)
        .anyMatch(line -> line.toLowerCase(Locale.ROOT).contains("failed"));
  }

  private static void assertPrivateFailureIsNotExposed(Execution result) {
    assertThat(result.output()).doesNotContain(PRIVATE_FAILURE, "private-extension-token");
    assertThat(STATE.publishedRuns)
        .allSatisfy(
            run ->
                assertThat(run.json()).doesNotContain(PRIVATE_FAILURE, "private-extension-token"));
    assertThat(result.summary().getFailures()).isEmpty();
  }

  private Execution execute(String scenario, Class<?>... fixtures) {
    System.setProperty(SCENARIO_PROPERTY, scenario);
    var bytes = new ByteArrayOutputStream();
    var listener = new SummaryGeneratingListener();
    var request =
        LauncherDiscoveryRequestBuilder.request()
            .selectors(Arrays.stream(fixtures).map(type -> selectClass(type)).toList())
            .configurationParameter("junit.jupiter.extensions.autodetection.enabled", "false")
            .configurationParameter("junit.jupiter.execution.parallel.enabled", "false")
            .build();
    try (var capture = new PrintStream(bytes, true, StandardCharsets.UTF_8)) {
      System.setOut(capture);
      System.setErr(capture);
      try {
        LauncherFactory.create().execute(request, listener);
      } finally {
        try {
          closeFixtureContexts();
        } finally {
          System.setOut(previousOut);
          System.setErr(previousErr);
        }
      }
    }
    return new Execution(listener.getSummary(), bytes.toString(StandardCharsets.UTF_8));
  }

  private void setProperty(String key, String value) {
    previousProperties.put(key, System.getProperty(key));
    System.setProperty(key, value);
  }

  private static void closeFixtureContexts() {
    for (Class<?> fixture : FIXTURES) {
      new TestContextManager(fixture)
          .getTestContext()
          .markApplicationContextDirty(DirtiesContext.HierarchyMode.EXHAUSTIVE);
    }
  }

  private record Execution(TestExecutionSummary summary, String output) {}

  @SpringJUnitConfig(FixtureConfiguration.class)
  @QueryAudit(failOnDetection = BooleanOverride.FALSE, autoOpenReport = BooleanOverride.FALSE)
  @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  @ActiveProfiles("first")
  static class FirstContext {
    @Autowired DataSource dataSource;

    @Test
    void selects() throws Exception {
      executeQuery(dataSource);
    }
  }

  @SpringJUnitConfig(FixtureConfiguration.class)
  @QueryAudit(failOnDetection = BooleanOverride.FALSE, autoOpenReport = BooleanOverride.FALSE)
  @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  @ActiveProfiles("second")
  static class SecondContext {
    @Autowired DataSource dataSource;

    @Test
    void selects() throws Exception {
      executeQuery(dataSource);
    }
  }

  private static void executeQuery(DataSource dataSource) throws Exception {
    STATE.startedTests.incrementAndGet();
    try (var connection = dataSource.getConnection();
        var statement = connection.createStatement()) {
      statement.execute("SELECT 1");
    }
  }

  @Configuration(proxyBeanMethods = false)
  @EnableAutoConfiguration
  static class FixtureConfiguration {
    @Bean
    QueryAuditConfig auditConfig() {
      return QueryAuditConfig.builder()
          .reportFormat(ReportFormat.CONSOLE)
          .reportOutputDir(STATE.output.toString())
          .autoOpenReport(false)
          .failOnDetection(false)
          .build();
    }

    @Bean
    DataSource dataSource() {
      var source = new JdbcDataSource();
      source.setURL("jdbc:h2:mem:extension-diagnostics");
      return source;
    }

    @Bean
    AutoCloseable contextLifetime() {
      return () -> STATE.closedContexts.incrementAndGet();
    }

    @Bean
    AuditReportSink applicationSink() {
      STATE.sinkBeans.incrementAndGet();
      String scenario = System.getProperty(SCENARIO_PROPERTY);
      return scenario.equals("shared-instance") || scenario.equals("conflicting-policy")
          ? SHARED_SINK
          : new RecordingSink("applicationSink", false);
    }

    @Bean
    @ConditionalOnProperty(name = SCENARIO_PROPERTY, havingValue = "conflicting-policy")
    ReportSinkRegistration sinkPolicy(AuditReportSink applicationSink, Environment environment) {
      return new ReportSinkRegistration(
          "suite:shared",
          Arrays.asList(environment.getActiveProfiles()).contains("second"),
          applicationSink);
    }

    @Bean
    IndexMetadataProvider applicationIndexes() {
      return new IndexMetadataProvider() {
        @Override
        public String supportedDatabase() {
          return "h2";
        }

        @Override
        public IndexMetadata getIndexMetadata(Connection connection) {
          STATE.metadataInvocations.incrementAndGet();
          return new IndexMetadata(Map.of("audit_fixture", List.of()));
        }
      };
    }

    @Bean
    @ConditionalOnProperty(name = SCENARIO_PROPERTY, havingValue = "provider-conflict")
    IndexMetadataProvider duplicateIndexes() {
      return applicationIndexes();
    }

    @Bean
    ExplainAnalyzer plans() {
      return new ExplainAnalyzer() {
        @Override
        public String supportedDatabase() {
          return "h2";
        }

        @Override
        public List<Issue> analyze(Connection connection, List<QueryRecord> queries) {
          return List.of();
        }
      };
    }

    @Bean
    @ConditionalOnProperty(name = SCENARIO_PROPERTY, havingValue = "sink-failures")
    AuditReportSink brokenNotification() {
      return new RecordingSink("brokenNotification", true);
    }

    @Bean
    @ConditionalOnProperty(name = SCENARIO_PROPERTY, havingValue = "sink-failures")
    AuditReportSink brokenWebhook() {
      return new RecordingSink("brokenWebhook", true);
    }

    @Bean
    @ConditionalOnProperty(name = SCENARIO_PROPERTY, havingValue = "sink-failures")
    AuditReportSink healthyArchive() {
      return new RecordingSink("healthyArchive", false);
    }
  }

  private record RecordingSink(String name, boolean fails) implements AuditReportSink {
    @Override
    public void publish(PublishedAuditRun run) {
      STATE.attemptedSinks.add(name);
      STATE.publishedRuns.add(run);
      if (fails) throw new IllegalStateException(PRIVATE_FAILURE);
    }
  }

  private static final class FixtureState {
    private final AtomicInteger startedTests = new AtomicInteger();
    private final AtomicInteger sinkBeans = new AtomicInteger();
    private final AtomicInteger metadataInvocations = new AtomicInteger();
    private final AtomicInteger closedContexts = new AtomicInteger();
    private final List<String> attemptedSinks = new CopyOnWriteArrayList<>();
    private final List<PublishedAuditRun> publishedRuns = new CopyOnWriteArrayList<>();
    private Path output;

    private void reset() {
      startedTests.set(0);
      sinkBeans.set(0);
      metadataInvocations.set(0);
      closedContexts.set(0);
      attemptedSinks.clear();
      publishedRuns.clear();
      output = null;
    }
  }
}
