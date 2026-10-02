package io.queryaudit.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import com.jayway.jsonpath.JsonPath;
import io.queryaudit.core.contract.QueryContractScope;
import io.queryaudit.core.contract.QueryContractViolation;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.junit5.EnableQueryInspector;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
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
import org.junit.platform.engine.DiscoverySelector;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class ScopedContractVerdictTest {
  private static final String ENABLED = "queryaudit.test.scopedVerdict";
  static final Path contracts = contractsDirectory();

  @Test
  void aScopedContractFailureInAnUnauditedTestFailsTheRun(@TempDir Path directory)
      throws Exception {
    contract(1);

    SummaryGeneratingListener listener =
        launch(directory, null, selectClass(AuditedRead.class), selectClass(UnauditedScope.class));

    assertThat(listener.getSummary().getFailures())
        .singleElement()
        .satisfies(
            failure ->
                assertThat(failure.getException())
                    .isInstanceOf(QueryContractViolation.class)
                    .hasMessageContaining("SELECT: contract 1, executed 2 (+1)"));
    Map<String, Object> report = report(directory);
    assertThat(report.get("outcome")).isEqualTo("FAIL");
    assertThat((List<?>) report.get("incompleteReasons")).isEmpty();
  }

  @Test
  void aMatchingScopedContractKeepsThePass(@TempDir Path directory) throws Exception {
    contract(2);

    SummaryGeneratingListener listener =
        launch(directory, null, selectClass(AuditedRead.class), selectClass(UnauditedScope.class));

    assertThat(listener.getSummary().getTestsFailedCount()).isZero();
    assertThat(report(directory).get("outcome")).isEqualTo("PASS");
  }

  @Test
  void aViolationTheTestExpectsDoesNotFailTheRun(@TempDir Path directory) throws Exception {
    contract(1);

    SummaryGeneratingListener listener =
        launch(
            directory, null, selectClass(AuditedRead.class), selectClass(ExpectedViolation.class));

    assertThat(listener.getSummary().getTestsFailedCount()).isZero();
    assertThat(report(directory).get("outcome")).isEqualTo("PASS");
  }

  @Test
  void aScopedContractFailureInsideAnAuditedTestFailsTheRun(@TempDir Path directory)
      throws Exception {
    contract(1);

    SummaryGeneratingListener listener = launch(directory, null, selectClass(AuditedScope.class));

    assertThat(listener.getSummary().getTestsFailedCount()).isEqualTo(1);
    assertThat(report(directory).get("outcome")).isEqualTo("FAIL");
  }

  @Test
  void anExpectedTestWhoseScopedContractFailsIsAFailNotMissingEvidence(@TempDir Path directory)
      throws Exception {
    contract(1);
    Path manifest = directory.resolve("expected-tests.txt");
    Files.writeString(
        manifest,
        "[engine:junit-jupiter]/[class:"
            + AuditedScope.class.getName()
            + "]/[method:readsTwiceInAScope()]\n");

    launch(directory, manifest, selectClass(AuditedScope.class));

    Map<String, Object> report = report(directory);
    assertThat(report.get("outcome")).isEqualTo("FAIL");
    assertThat((List<?>) report.get("incompleteReasons")).isEmpty();
    assertThat(JsonPath.<Object>read(report, "$.coverage.tests[0].gap")).isNull();
  }

  private static Path contractsDirectory() {
    try {
      return Files.createTempDirectory("scoped-verdict");
    } catch (IOException failure) {
      throw new UncheckedIOException(failure);
    }
  }

  private static void contract(int selects) throws Exception {
    Files.write(
        contracts.resolve("api.contracts"),
        List.of("@junit | scoped-read | " + selects + " | 0 | 0 | 0 | " + selects));
  }

  private static Map<String, Object> report(Path directory) throws Exception {
    return JsonPath.parse(Files.readString(directory.resolve("report.json"))).json();
  }

  private static SummaryGeneratingListener launch(
      Path directory, Path manifest, DiscoverySelector... selectors) {
    List<String> keys =
        List.of(
            ENABLED,
            "queryAudit.report.outputDir",
            "queryAudit.report.format",
            "queryAudit.coverage.manifest");
    Map<String, String> saved = new HashMap<>();
    for (String key : keys) saved.put(key, System.getProperty(key));
    HtmlReportAggregator.getInstance().reset();
    try {
      System.setProperty(ENABLED, "true");
      System.setProperty("queryAudit.report.outputDir", directory.toString());
      System.setProperty("queryAudit.report.format", "json");
      if (manifest != null) System.setProperty("queryAudit.coverage.manifest", manifest.toString());
      SummaryGeneratingListener listener = new SummaryGeneratingListener();
      LauncherFactory.create()
          .execute(
              LauncherDiscoveryRequestBuilder.request()
                  .selectors(selectors)
                  .configurationParameter("junit.jupiter.execution.parallel.enabled", "false")
                  .build(),
              listener);
      return listener;
    } finally {
      saved.forEach(
          (key, value) -> {
            if (value == null) System.clearProperty(key);
            else System.setProperty(key, value);
          });
      HtmlReportAggregator.getInstance().reset();
    }
  }

  abstract static class Fixture {
    @Autowired QueryContractScope scope;
    @Autowired DataSource dataSource;

    @DynamicPropertySource
    static void contracts(DynamicPropertyRegistry registry) {
      registry.add(
          "query-audit.contracts.path", () -> ScopedContractVerdictTest.contracts.toString());
    }

    Object readTwice() {
      JdbcTemplate jdbc = new JdbcTemplate(dataSource);
      jdbc.queryForObject("SELECT 1", Integer.class);
      return jdbc.queryForObject("SELECT 2", Integer.class);
    }
  }

  @SpringJUnitConfig(Application.class)
  @EnableQueryInspector
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class AuditedRead extends Fixture {
    @Test
    void readsOnce() {
      new JdbcTemplate(dataSource).queryForObject("SELECT 1", Integer.class);
    }
  }

  @SpringJUnitConfig(Application.class)
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class UnauditedScope extends Fixture {
    @Test
    void readsTwiceInAScope() throws Exception {
      scope.verify("scoped-read", this::readTwice);
    }
  }

  @SpringJUnitConfig(Application.class)
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class ExpectedViolation extends Fixture {
    @Test
    void assertsThatTheScopeRejectsTheExtraRead() {
      assertThatThrownBy(() -> scope.verify("scoped-read", this::readTwice))
          .isInstanceOf(QueryContractViolation.class);
    }
  }

  @SpringJUnitConfig(Application.class)
  @EnableQueryInspector
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class AuditedScope extends Fixture {
    @Test
    void readsTwiceInAScope() throws Exception {
      scope.verify("scoped-read", this::readTwice);
    }
  }

  @Configuration
  @Import(QueryAuditAutoConfiguration.class)
  static class Application {
    @Bean
    DataSource dataSource() {
      JdbcDataSource dataSource = new JdbcDataSource();
      dataSource.setURL("jdbc:h2:mem:scoped-verdict-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
      return dataSource;
    }
  }
}
