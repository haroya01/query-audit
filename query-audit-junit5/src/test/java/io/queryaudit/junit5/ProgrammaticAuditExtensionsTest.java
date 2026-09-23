package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import io.queryaudit.core.analyzer.ExplainAnalyzer;
import io.queryaudit.core.analyzer.IndexMetadataProvider;
import io.queryaudit.core.detector.DetectionRule;
import io.queryaudit.core.extension.AuditExtensions;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
class ProgrammaticAuditExtensionsTest {
  private static final String ENABLED = "queryaudit.test.programmaticExtensions";
  private static final AtomicInteger RULE_CALLS = new AtomicInteger();
  private static final AtomicInteger METADATA_CALLS = new AtomicInteger();
  private static final AtomicInteger EXPLAIN_CALLS = new AtomicInteger();
  private final Map<String, String> saved = new HashMap<>();

  @BeforeEach
  void prepare(@TempDir Path output) {
    for (String key :
        List.of(
            ENABLED,
            "queryAudit.reportFormat",
            "queryAudit.reportOutputDir",
            "queryAudit.autoOpenReport")) {
      saved.put(key, System.getProperty(key));
    }
    System.setProperty(ENABLED, "true");
    System.setProperty("queryAudit.reportFormat", "console");
    System.setProperty("queryAudit.reportOutputDir", output.toString());
    System.setProperty("queryAudit.autoOpenReport", "false");
    RULE_CALLS.set(0);
    METADATA_CALLS.set(0);
    EXPLAIN_CALLS.set(0);
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
  void aRegisteredExtensionOptsInAndRunsEachImplementationOnce(boolean autodetection) {
    DataSource original = Fixture.dataSource;
    var request =
        LauncherDiscoveryRequestBuilder.request()
            .selectors(selectClass(Fixture.class))
            .configurationParameter(
                "junit.jupiter.extensions.autodetection.enabled", Boolean.toString(autodetection))
            .build();
    var listener = new SummaryGeneratingListener();
    LauncherFactory.create().execute(request, listener);

    assertThat(listener.getSummary().getFailures()).isEmpty();
    assertThat(listener.getSummary().getTestsSucceededCount()).isEqualTo(1);
    assertThat(RULE_CALLS).hasValue(1);
    assertThat(METADATA_CALLS).hasValue(1);
    assertThat(EXPLAIN_CALLS).hasValue(1);
    assertThat(Fixture.dataSource).isSameAs(original);
  }

  @Test
  void instanceRegistrationIsRejectedRatherThanSilentlyIgnoringItsCatalog() {
    var request =
        LauncherDiscoveryRequestBuilder.request()
            .selectors(selectClass(InstanceRegistrationFixture.class))
            .configurationParameter("junit.jupiter.extensions.autodetection.enabled", "false")
            .build();
    var listener = new SummaryGeneratingListener();
    LauncherFactory.create().execute(request, listener);

    assertThat(listener.getSummary().getFailures())
        .anySatisfy(
            failure ->
                assertThat(failure.getException())
                    .hasMessageContaining("static @RegisterExtension"));
    assertThat(RULE_CALLS).hasValue(0);
  }

  @QueryAudit(autoOpenReport = BooleanOverride.FALSE)
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class InstanceRegistrationFixture {
    static DataSource dataSource = Fixture.dataSource();

    @RegisterExtension
    QueryAuditExtension audit =
        new QueryAuditExtension(
            AuditExtensions.builder()
                .rule("company:instance-registration", new RecordingRule())
                .build());

    @Test
    void query() {}
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class Fixture {
    static DataSource dataSource = dataSource();

    @RegisterExtension
    static QueryAuditExtension audit =
        new QueryAuditExtension(
            AuditExtensions.builder()
                .rule("company:query-observer", new RecordingRule())
                .indexMetadataProvider("company:h2-indexes", new RecordingMetadata())
                .explainAnalyzer("company:h2-plans", new RecordingExplain())
                .build());

    @Test
    void query() throws Exception {
      try (Connection connection = dataSource.getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("SELECT 1");
      }
    }

    private static DataSource dataSource() {
      JdbcDataSource dataSource = new JdbcDataSource();
      dataSource.setURL("jdbc:h2:mem:programmatic-extensions");
      return dataSource;
    }
  }

  static class RecordingRule implements DetectionRule {
    @Override
    public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexes) {
      assertThat(queries).hasSize(1);
      assertThat(indexes).isNotNull();
      RULE_CALLS.incrementAndGet();
      return List.of();
    }
  }

  static class RecordingMetadata implements IndexMetadataProvider {
    @Override
    public String supportedDatabase() {
      return "h2";
    }

    @Override
    public IndexMetadata getIndexMetadata(Connection connection) {
      METADATA_CALLS.incrementAndGet();
      return new IndexMetadata(Map.of());
    }
  }

  static class RecordingExplain implements ExplainAnalyzer {
    @Override
    public String supportedDatabase() {
      return "h2";
    }

    @Override
    public List<Issue> analyze(Connection connection, List<QueryRecord> queries) {
      EXPLAIN_CALLS.incrementAndGet();
      return List.of();
    }
  }
}
