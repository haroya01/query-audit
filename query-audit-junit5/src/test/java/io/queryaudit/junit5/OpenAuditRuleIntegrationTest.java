package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import io.queryaudit.core.extension.AuditExtensions;
import io.queryaudit.core.extension.AuditRule;
import io.queryaudit.core.extension.FindingKindId;
import io.queryaudit.core.extension.RuleContext;
import io.queryaudit.core.extension.RuleDescriptor;
import io.queryaudit.core.extension.RuleId;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.core.reporter.delivery.PublishedAuditRun;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
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
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class OpenAuditRuleIntegrationTest {
  private static final String ENABLED = "queryaudit.test.openExtensions";
  private static final AtomicInteger RULE_CALLS = new AtomicInteger();
  private static final List<PublishedAuditRun> DELIVERIES = new ArrayList<>();
  private static boolean ruleFails;
  private static boolean emitFinding;
  private static boolean sinkFails;
  private static boolean executeQuery;
  private final Map<String, String> saved = new HashMap<>();
  private Path output;

  @BeforeEach
  void prepare(@TempDir Path output) {
    this.output = output;
    for (String key : List.of(ENABLED, "queryAudit.reportFormat", "queryAudit.reportOutputDir")) {
      saved.put(key, System.getProperty(key));
    }
    System.setProperty(ENABLED, "true");
    System.setProperty("queryAudit.reportFormat", "json");
    System.setProperty("queryAudit.reportOutputDir", output.toString());
    RULE_CALLS.set(0);
    DELIVERIES.clear();
    ruleFails = false;
    emitFinding = true;
    sinkFails = false;
    executeQuery = true;
    Fixture.audit = Fixture.createAudit(true);
    HtmlReportAggregator.getInstance().reset();
  }

  @AfterEach
  void restore() {
    saved.forEach(
        (key, value) -> {
          if (value == null) System.clearProperty(key);
          else System.setProperty(key, value);
        });
    HtmlReportAggregator.getInstance().reset();
    QueryAuditDataSourceStore.clear();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void openRuleRunsFailsTheTestAndReachesExactlyOneSanitizedSuiteDelivery(boolean autodetection)
      throws Exception {
    var summary = launch(autodetection);

    assertThat(summary.getTestsFailedCount()).isEqualTo(1);
    assertThat(summary.getFailures())
        .anySatisfy(
            failure ->
                assertThat(failure.getException()).hasMessageContaining("company:tenant-check"));
    assertThat(RULE_CALLS).hasValue(1);
    assertThat(DELIVERIES).hasSize(1);
    assertThat(DELIVERIES.get(0).outcome()).isEqualTo(AuditOutcome.FAIL);
    assertThat(DELIVERIES.get(0).json())
        .contains("company:tenant-check")
        .doesNotContain("private_marker");
    assertThat(Files.readString(output.resolve("report.json"))).contains("company:tenant-check");
  }

  @Test
  void failedRuleCannotPublishASuccessfulAnalysis() {
    ruleFails = true;

    var summary = launch(false);

    assertThat(summary.getTestsFailedCount()).isEqualTo(1);
    assertThat(summary.getFailures())
        .anySatisfy(
            failure ->
                assertThat(failure.getException())
                    .isInstanceOf(io.queryaudit.core.extension.AuditRuleException.class)
                    .hasMessageContaining("company:rule-registration")
                    .hasMessageContaining("EXECUTION_FAILED")
                    .hasMessageNotContaining("private_marker")
                    .hasNoCause());
    assertThat(DELIVERIES).hasSize(1);
    assertThat(DELIVERIES.get(0).outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
    assertThat(DELIVERIES.get(0).json()).doesNotContain("private_marker");
  }

  @Test
  void requiredSinkFailureFailsTheLauncherContainerButPreservesPassedAnalysis() throws Exception {
    emitFinding = false;
    sinkFails = true;

    var summary = launch(false);

    assertThat(summary.getTestsSucceededCount()).isEqualTo(1);
    assertThat(summary.getContainersFailedCount()).isEqualTo(1);
    assertThat(summary.getFailures())
        .anySatisfy(
            failure ->
                assertThat(failure.getException())
                    .rootCause()
                    .hasMessageContaining("required report publication failed")
                    .hasMessageNotContaining("private_marker"));
    assertThat(DELIVERIES).hasSize(1);
    assertThat(DELIVERIES.get(0).outcome()).isEqualTo(AuditOutcome.PASS);
    assertThat(Files.readString(output.resolve("report.json"))).contains("\"outcome\": \"PASS\"");
  }

  @Test
  void optionalSinkFailureDoesNotFailTheLauncher() {
    emitFinding = false;
    sinkFails = true;
    Fixture.audit = Fixture.createAudit(false);

    var summary = launch(false);

    assertThat(summary.getTestsSucceededCount()).isEqualTo(1);
    assertThat(summary.getFailures()).isEmpty();
    assertThat(DELIVERIES).hasSize(1);
    assertThat(DELIVERIES.get(0).outcome()).isEqualTo(AuditOutcome.PASS);
  }

  @Test
  void requiredPublicationAndAnalysisFailuresRemainSeparateLauncherFailures() throws Exception {
    sinkFails = true;

    var summary = launch(false);

    assertThat(summary.getTestsFailedCount()).isEqualTo(1);
    assertThat(summary.getContainersFailedCount()).isEqualTo(1);
    assertThat(DELIVERIES).hasSize(1);
    assertThat(DELIVERIES.get(0).outcome()).isEqualTo(AuditOutcome.FAIL);
    assertThat(Files.readString(output.resolve("report.json"))).contains("\"outcome\": \"FAIL\"");
  }

  @Test
  void anOpenRuleCanRejectAMissingQueryInAnOtherwiseEmptyAudit() {
    executeQuery = false;

    var summary = launch(false);

    assertThat(summary.getTestsFailedCount()).isEqualTo(1);
    assertThat(RULE_CALLS).hasValue(1);
    assertThat(DELIVERIES).hasSize(1);
    assertThat(DELIVERIES.get(0).outcome()).isEqualTo(AuditOutcome.FAIL);
    assertThat(DELIVERIES.get(0).json()).contains("\"totalQueries\":0", "company:tenant-check");
  }

  private static org.junit.platform.launcher.listeners.TestExecutionSummary launch(
      boolean autodetection) {
    var listener = new SummaryGeneratingListener();
    LauncherFactory.create()
        .execute(
            LauncherDiscoveryRequestBuilder.request()
                .selectors(selectClass(Fixture.class))
                .configurationParameter(
                    "junit.jupiter.extensions.autodetection.enabled",
                    Boolean.toString(autodetection))
                .build(),
            listener);
    return listener.getSummary();
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class Fixture {
    static DataSource dataSource = source();
    @RegisterExtension static QueryAuditExtension audit = createAudit(true);

    static QueryAuditExtension createAudit(boolean required) {
      return new QueryAuditExtension(
          AuditExtensions.builder()
              .auditRule("company:rule-registration", new TenantRule())
              .reportSink(
                  "company:sink",
                  required,
                  run -> {
                    DELIVERIES.add(run);
                    if (sinkFails) throw new IOException("private_marker sink failure");
                  })
              .build());
    }

    @Test
    void query() throws Exception {
      if (!executeQuery) return;
      try (Connection connection = dataSource.getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("SELECT 'private_marker'");
      }
    }

    private static DataSource source() {
      JdbcDataSource source = new JdbcDataSource();
      source.setURL("jdbc:h2:mem:open-audit-rule");
      return source;
    }
  }

  static final class TenantRule implements AuditRule {
    private static final FindingKindId KIND = FindingKindId.of("company:tenant-check");

    @Override
    public RuleDescriptor descriptor() {
      return new RuleDescriptor(RuleId.of("company:tenant-rule"), "1", Set.of(KIND), Map.of());
    }

    @Override
    public List<Finding> evaluate(RuleContext context) {
      RULE_CALLS.incrementAndGet();
      if (ruleFails) throw new IllegalStateException("private_marker rule failure");
      if (!emitFinding) return List.of();
      String sql = context.queries().isEmpty() ? null : context.queries().get(0).sql();
      return List.of(
          new Finding(
              KIND,
              Severity.WARNING,
              sql,
              null,
              null,
              "private_marker detail",
              "private_marker suggestion"));
    }
  }
}
