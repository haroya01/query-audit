package io.queryaudit.junit5.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.junit5.BooleanOverride;
import io.queryaudit.junit5.EnableQueryInspector;
import io.queryaudit.junit5.ExpectQueries;
import io.queryaudit.junit5.QueryAudit;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import net.ttddyy.dsproxy.support.ProxyDataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class JudgmentWindowLauncherTest {

  private static final String ENABLED = "queryaudit.test.judgmentWindow";
  private static final String NOT_COUNTED =
      "Not counted: 4 setup or teardown queries. Set @QueryAudit(includeSetupQueries = true) to"
          + " count them.";
  private static final String CONTRACTED_ID =
      "[engine:junit-jupiter]/[class:"
          + ContractedWithSetup.class.getName()
          + "]/[method:readsOnce()]";

  @Test
  void budgetsJudgeOnlyTheTestBody(@TempDir Path directory) {
    TestExecutionSummary summary = run(directory, Map.of(), BodyBudget.class);

    assertThat(summary.getTestsFoundCount()).isEqualTo(2);
    assertThat(summary.getFailures())
        .singleElement()
        .satisfies(
            failure -> {
              assertThat(failure.getTestIdentifier().getDisplayName()).isEqualTo("readsTwice()");
              assertThat(failure.getException())
                  .hasMessageContaining("SELECT: executed 2, expected at most 1.")
                  .hasMessageNotContaining("INSERT:")
                  .hasMessageEndingWith(NOT_COUNTED);
            });
  }

  @Test
  void includeSetupQueriesCountsTheFixtureToo(@TempDir Path directory) {
    TestExecutionSummary summary = run(directory, Map.of(), IncludedSetup.class);

    assertThat(summary.getFailures())
        .singleElement()
        .satisfies(
            failure ->
                assertThat(failure.getException())
                    .hasMessageContaining("SELECT: executed 2, expected at most 1.")
                    .hasMessageContaining("INSERT: executed 3, expected at most 0.")
                    .hasMessageNotContaining("Not counted"));
  }

  @Test
  void aContractRecordedWithSetupQueriesAsksForOneReRecord(@TempDir Path directory)
      throws Exception {
    Path contracts = Files.createDirectories(directory.resolve("contracts"));
    Files.writeString(
        contracts.resolve(".query-audit-contracts"), "@junit | " + CONTRACTED_ID + " | 2 | 3 | 0 | 0 | 5\n");

    TestExecutionSummary summary =
        run(
            directory,
            Map.of("queryAudit.contracts.path", contracts.toString()),
            ContractedWithSetup.class);

    assertThat(summary.getFailures())
        .singleElement()
        .satisfies(
            failure ->
                assertThat(failure.getException())
                    .hasMessageContaining("SELECT: contract 2, executed 1 (-1)")
                    .hasMessageContaining("INSERT: contract 3, executed 0 (-3)")
                    .hasMessageContaining(
                        "The recorded counts include setup and teardown queries. QueryAudit 0.7"
                            + " and earlier counted them in method contracts; re-record once")
                    .hasMessageEndingWith(NOT_COUNTED));
  }

  @Test
  void recordingStoresTheTestBodyCounts(@TempDir Path directory) throws Exception {
    Path contracts = Files.createDirectories(directory.resolve("contracts"));
    Path file = contracts.resolve(".query-audit-contracts");
    Files.writeString(file, "");

    TestExecutionSummary summary =
        run(
            directory,
            Map.of(
                "queryAudit.contracts.path",
                contracts.toString(),
                "queryAudit.contracts.record",
                "true"),
            ContractedWithSetup.class);

    assertThat(summary.getTestsSucceededCount()).isEqualTo(1);
    assertThat(Files.readString(file)).contains(CONTRACTED_ID + " | 1 | 0 | 0 | 0 | 1");
  }

  private static TestExecutionSummary run(
      Path directory, Map<String, String> extra, Class<?> fixture) {
    Map<String, String> properties = new HashMap<>(extra);
    properties.put(ENABLED, "true");
    properties.put("queryAudit.report.format", "json");
    properties.put("queryAudit.report.outputDir", directory.toString());
    properties.put("queryAudit.autoOpenReport", "false");
    Map<String, String> saved = new HashMap<>();
    properties.keySet().forEach(key -> saved.put(key, System.getProperty(key)));
    HtmlReportAggregator.getInstance().reset();
    try {
      properties.forEach(System::setProperty);
      SummaryGeneratingListener listener = new SummaryGeneratingListener();
      LauncherFactory.create()
          .execute(
              LauncherDiscoveryRequestBuilder.request()
                  .selectors(selectClass(fixture))
                  .configurationParameter("junit.jupiter.execution.parallel.enabled", "false")
                  .build(),
              listener);
      return listener.getSummary();
    } finally {
      saved.forEach(
          (key, value) -> {
            if (value == null) System.clearProperty(key);
            else System.setProperty(key, value);
          });
      HtmlReportAggregator.getInstance().reset();
    }
  }

  abstract static class SeededFixture {
    static final DataSource DATA_SOURCE = dataSource();

    @BeforeEach
    void seed() throws SQLException {
      try (Connection connection = DATA_SOURCE.getConnection();
          Statement statement = connection.createStatement()) {
        for (int i = 0; i < 3; i++) {
          statement.executeUpdate("INSERT INTO seeded (id) VALUES (" + i + ")");
        }
        statement.executeQuery("SELECT COUNT(*) FROM seeded").close();
      }
    }

    static void select(int count) throws SQLException {
      try (Connection connection = DATA_SOURCE.getConnection();
          Statement statement = connection.createStatement()) {
        for (int i = 0; i < count; i++) {
          statement.executeQuery("SELECT id FROM seeded WHERE id = " + i).close();
        }
      }
    }

    private static DataSource dataSource() {
      JdbcDataSource raw = new JdbcDataSource();
      raw.setURL("jdbc:h2:mem:judgment-window;DB_CLOSE_DELAY=-1");
      try (Connection connection = raw.getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("CREATE TABLE IF NOT EXISTS seeded (id INT)");
      } catch (SQLException failure) {
        throw new IllegalStateException(failure);
      }
      return new ProxyDataSource(raw);
    }
  }

  @EnableQueryInspector
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class BodyBudget extends SeededFixture {
    @Test
    @ExpectQueries(select = 1, insert = 0)
    void readsOnce() throws SQLException {
      select(1);
    }

    @Test
    @ExpectQueries(select = 1, insert = 0)
    void readsTwice() throws SQLException {
      select(2);
    }
  }

  @QueryAudit(includeSetupQueries = true, autoOpenReport = BooleanOverride.FALSE)
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class IncludedSetup extends SeededFixture {
    @Test
    @ExpectQueries(select = 1, insert = 0)
    void readsOnce() throws SQLException {
      select(1);
    }
  }

  @EnableQueryInspector
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class ContractedWithSetup extends SeededFixture {
    @Test
    void readsOnce() throws SQLException {
      select(1);
    }
  }
}
