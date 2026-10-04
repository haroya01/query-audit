package io.queryaudit.junit5.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.junit5.EnableQueryInspector;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import net.ttddyy.dsproxy.support.ProxyDataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
@ResourceLock(Resources.SYSTEM_OUT)
class ConsoleOutputLauncherTest {

  private static final String ENABLED = "queryaudit.test.consoleOutput";

  @Test
  void aCleanTestPrintsOneLineAndATestWithFindingsPrintsItsReport(@TempDir Path directory) {
    String output = run(directory);

    assertThat(output)
        .contains("[QueryAudit] readsOnce(): no findings (profile: recommended)")
        .contains("[QueryAudit] Rule profile: recommended");
    assertThat(output.split("QUERYAUDIT REPORT", -1)).hasSize(2);
    assertThat(output).contains("QUERYAUDIT REPORT\n  Test: readsInALoop()");
  }

  private static String run(Path directory) {
    Map<String, String> properties =
        Map.of(
            ENABLED,
            "true",
            "queryAudit.report.format",
            "json",
            "queryAudit.report.outputDir",
            directory.toString(),
            "queryAudit.autoOpenReport",
            "false");
    Map<String, String> saved = new HashMap<>();
    properties.keySet().forEach(key -> saved.put(key, System.getProperty(key)));
    HtmlReportAggregator.getInstance().reset();
    ByteArrayOutputStream captured = new ByteArrayOutputStream();
    PrintStream originalOut = System.out;
    try (PrintStream console = new PrintStream(captured, true, StandardCharsets.UTF_8)) {
      properties.forEach(System::setProperty);
      System.setOut(console);
      LauncherFactory.create()
          .execute(
              LauncherDiscoveryRequestBuilder.request()
                  .selectors(selectClass(Fixture.class))
                  .configurationParameter("junit.jupiter.execution.parallel.enabled", "false")
                  .build(),
              new SummaryGeneratingListener());
    } finally {
      System.setOut(originalOut);
      saved.forEach(
          (key, value) -> {
            if (value == null) System.clearProperty(key);
            else System.setProperty(key, value);
          });
      HtmlReportAggregator.getInstance().reset();
    }
    return captured.toString(StandardCharsets.UTF_8);
  }

  @EnableQueryInspector
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  @TestMethodOrder(MethodOrderer.MethodName.class)
  static class Fixture {
    static final DataSource DATA_SOURCE = dataSource();

    @Test
    void readsInALoop() throws SQLException {
      try (Connection connection = DATA_SOURCE.getConnection();
          PreparedStatement statement =
              connection.prepareStatement("SELECT id FROM console_rows WHERE id = ?")) {
        for (int id = 1; id <= 3; id++) {
          statement.setInt(1, id);
          statement.executeQuery().close();
        }
      }
    }

    @Test
    void readsOnce() throws SQLException {
      try (Connection connection = DATA_SOURCE.getConnection();
          Statement statement = connection.createStatement()) {
        statement.executeQuery("SELECT COUNT(*) FROM console_rows").close();
      }
    }

    private static DataSource dataSource() {
      JdbcDataSource raw = new JdbcDataSource();
      raw.setURL("jdbc:h2:mem:console-output;DB_CLOSE_DELAY=-1");
      try (Connection connection = raw.getConnection();
          Statement statement = connection.createStatement()) {
        statement.execute("CREATE TABLE IF NOT EXISTS console_rows (id INT)");
      } catch (SQLException failure) {
        throw new IllegalStateException(failure);
      }
      return new ProxyDataSource(raw);
    }
  }
}
