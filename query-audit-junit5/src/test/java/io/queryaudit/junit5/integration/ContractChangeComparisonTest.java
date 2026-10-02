package io.queryaudit.junit5.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.core.reporter.ReportComparator;
import io.queryaudit.junit5.EnableQueryInspector;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;
import net.ttddyy.dsproxy.support.ProxyDataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class ContractChangeComparisonTest {
  private static final String ENABLED = "queryaudit.test.contractChange";
  private static final String SELECTS = "queryaudit.test.contractChangeSelects";
  private static final String TEST_ID =
      "[engine:junit-jupiter]/[class:" + ContractedRead.class.getName() + "]/[method:reads()]";

  @Test
  void aPullRequestThatReRecordsItsContractComparesAsPass(@TempDir Path directory)
      throws Exception {
    String base = run(directory.resolve("base"), 1, 1, false);
    String reRecorded = run(directory.resolve("pr"), 2, 2, false);

    var verdict = ReportComparator.compare(base, reRecorded);

    assertThat(verdict.comparisonComplete()).isTrue();
    assertThat(verdict.outcome()).isEqualTo(AuditOutcome.PASS);
  }

  @Test
  void aChangedQueryCountWithoutANewContractStillFails(@TempDir Path directory) throws Exception {
    String base = run(directory.resolve("base"), 1, 1, false);
    String unrecorded = run(directory.resolve("pr"), 2, 1, false);

    assertThat(ReportComparator.compare(base, unrecorded).outcome()).isEqualTo(AuditOutcome.FAIL);
  }

  @Test
  void aRunInRecordModeIsNotComparableWithAnEnforcedRun(@TempDir Path directory) throws Exception {
    String base = run(directory.resolve("base"), 1, 1, false);
    String recording = run(directory.resolve("pr"), 2, 1, true);

    var verdict = ReportComparator.compare(base, recording);

    assertThat(verdict.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
    assertThat(verdict.incompleteReasons())
        .extracting(reason -> reason.code())
        .contains(IncompleteReasonCode.INCOMPATIBLE_AUDIT_INPUTS);
  }

  private static String run(Path directory, int selects, int contractSelects, boolean record)
      throws Exception {
    Path contracts = Files.createDirectories(directory.resolve("contracts"));
    Files.writeString(
        contracts.resolve(".query-audit-contracts"),
        "@junit | "
            + TEST_ID
            + " | "
            + contractSelects
            + " | 0 | 0 | 0 | "
            + contractSelects
            + "\n");
    Map<String, String> properties =
        Map.of(
            ENABLED,
            "true",
            SELECTS,
            Integer.toString(selects),
            "queryAudit.contracts.path",
            contracts.toString(),
            "queryAudit.contracts.record",
            Boolean.toString(record),
            "queryAudit.report.format",
            "json",
            "queryAudit.report.outputDir",
            directory.toString(),
            "queryAudit.autoOpenReport",
            "false");
    Map<String, String> saved = new HashMap<>();
    properties.keySet().forEach(key -> saved.put(key, System.getProperty(key)));
    HtmlReportAggregator.getInstance().reset();
    try {
      properties.forEach(System::setProperty);
      SummaryGeneratingListener listener = new SummaryGeneratingListener();
      LauncherFactory.create()
          .execute(
              LauncherDiscoveryRequestBuilder.request()
                  .selectors(selectClass(ContractedRead.class))
                  .configurationParameter("junit.jupiter.execution.parallel.enabled", "false")
                  .build(),
              listener);
      assertThat(listener.getSummary().getTestsFoundCount()).isEqualTo(1);
    } finally {
      saved.forEach(
          (key, value) -> {
            if (value == null) System.clearProperty(key);
            else System.setProperty(key, value);
          });
      HtmlReportAggregator.getInstance().reset();
    }
    return Files.readString(directory.resolve("report.json"));
  }

  @EnableQueryInspector
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class ContractedRead {
    static final DataSource DATA_SOURCE = dataSource();

    @Test
    void reads() throws Exception {
      int selects = Integer.getInteger(SELECTS);
      try (Connection connection = DATA_SOURCE.getConnection();
          Statement statement = connection.createStatement()) {
        for (int i = 0; i < selects; i++) statement.executeQuery("SELECT " + (i + 1)).close();
      }
    }

    private static DataSource dataSource() {
      JdbcDataSource dataSource = new JdbcDataSource();
      dataSource.setURL("jdbc:h2:mem:contract-change-comparison;DB_CLOSE_DELAY=-1");
      return new ProxyDataSource(dataSource);
    }
  }
}
