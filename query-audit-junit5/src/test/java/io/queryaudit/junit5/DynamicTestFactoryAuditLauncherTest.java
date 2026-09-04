package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.ReportFormat;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@DisplayName("Dynamic-test factory audit lifecycle")
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class DynamicTestFactoryAuditLauncherTest {

  private static final String FIXTURE_PROPERTY = "queryaudit.test.dynamicFactoryFixture";
  private static final String OUTPUT_PATH_PROPERTY = "queryaudit.test.dynamicFactoryPath";

  private String fixtureProperty;
  private String outputPathProperty;

  @BeforeEach
  void setUp() {
    fixtureProperty = System.getProperty(FIXTURE_PROPERTY);
    outputPathProperty = System.getProperty(OUTPUT_PATH_PROPERTY);
    DynamicTestFactoryFixture.FACTORY_INVOCATIONS.set(0);
    ParameterizedFixture.INVOCATIONS.set(0);
    HtmlReportAggregator.getInstance().reset();
    QueryAuditDataSourceStore.clear();
  }

  @AfterEach
  void tearDown() {
    restoreProperty(FIXTURE_PROPERTY, fixtureProperty);
    restoreProperty(OUTPUT_PATH_PROPERTY, outputPathProperty);
    HtmlReportAggregator.getInstance().reset();
    QueryAuditDataSourceStore.clear();
  }

  @Test
  @DisplayName("an active @TestFactory is rejected without a misleading PASS report")
  void activeFactoryIsInconclusive(@TempDir Path tempDir) throws IOException {
    Path outputDirectory = tempDir.resolve("dynamic-test-factory");

    TestExecutionSummary summary = runFixture(DynamicTestFactoryFixture.class, outputDirectory);

    assertThat(summary.getTotalFailureCount()).isGreaterThan(0);
    assertThat(summary.getFailures())
        .anySatisfy(
            failure ->
                assertThat(failure.getException())
                    .hasStackTraceContaining("cannot audit @TestFactory method")
                    .hasStackTraceContaining("separate audit boundary for each DynamicTest child")
                    .hasStackTraceContaining("@ParameterizedTest")
                    .hasStackTraceContaining("@QueryAuditExclude"));
    assertThat(DynamicTestFactoryFixture.FACTORY_INVOCATIONS).hasValue(0);

    Path reportPath = outputDirectory.resolve("report.json");
    assertThat(reportPath).exists();
    assertThat(Files.readString(reportPath))
        .contains("\"outcome\": \"INCONCLUSIVE\"")
        .contains("\"code\": \"AUDIT_INITIALIZATION_FAILED\"")
        .contains("Details omitted by report redaction")
        .doesNotContain("\"outcome\": \"PASS\"");
  }

  @Test
  @DisplayName("parameterized invocations remain supported audit boundaries")
  void parameterizedInvocationsRemainSupported(@TempDir Path tempDir) throws IOException {
    Path outputDirectory = tempDir.resolve("parameterized-test");

    TestExecutionSummary summary = runFixture(ParameterizedFixture.class, outputDirectory);

    assertThat(summary.getTotalFailureCount()).isZero();
    assertThat(summary.getTestsSucceededCount()).isEqualTo(2);
    assertThat(ParameterizedFixture.INVOCATIONS).hasValue(2);
    assertThat(Files.readString(outputDirectory.resolve("report.json")))
        .contains("\"outcome\": \"PASS\"")
        .contains("[test-template-invocation:#1]")
        .contains("[test-template-invocation:#2]");
  }

  private static TestExecutionSummary runFixture(Class<?> fixture, Path outputDirectory) {
    System.setProperty(FIXTURE_PROPERTY, "true");
    System.setProperty(OUTPUT_PATH_PROPERTY, outputDirectory.toString());
    LauncherDiscoveryRequest request =
        LauncherDiscoveryRequestBuilder.request()
            .selectors(selectClass(fixture))
            .configurationParameter("junit.jupiter.extensions.autodetection.enabled", "false")
            .build();
    SummaryGeneratingListener listener = new SummaryGeneratingListener();
    Launcher launcher = LauncherFactory.create();
    launcher.registerTestExecutionListeners(listener);
    launcher.execute(request);
    return listener.getSummary();
  }

  private static DataSource newDataSource(String name) {
    JdbcDataSource dataSource = new JdbcDataSource();
    dataSource.setURL("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1");
    return dataSource;
  }

  private static void restoreProperty(String key, String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class DynamicTestAuditConfiguration {

    @Bean
    QueryAuditConfig queryAuditConfig() {
      return QueryAuditConfig.builder()
          .reportFormat(ReportFormat.JSON)
          .reportOutputDir(System.getProperty(OUTPUT_PATH_PROPERTY))
          .build();
    }
  }

  @QueryAudit(autoOpenReport = BooleanOverride.FALSE)
  @SpringJUnitConfig(DynamicTestAuditConfiguration.class)
  @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class DynamicTestFactoryFixture {

    static final AtomicInteger FACTORY_INVOCATIONS = new AtomicInteger();
    static DataSource dataSource = newDataSource("dynamic-test-factory-audit");

    @TestFactory
    Stream<DynamicTest> auditedFactory() {
      FACTORY_INVOCATIONS.incrementAndGet();
      return Stream.of(dynamicTest("dynamic child", () -> {}));
    }
  }

  @QueryAudit(autoOpenReport = BooleanOverride.FALSE)
  @SpringJUnitConfig(DynamicTestAuditConfiguration.class)
  @DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class ParameterizedFixture {

    static final AtomicInteger INVOCATIONS = new AtomicInteger();
    static DataSource dataSource = newDataSource("parameterized-test-audit");

    @ParameterizedTest
    @ValueSource(strings = {"first", "second"})
    void auditedInvocation(String ignored) {
      INVOCATIONS.incrementAndGet();
    }
  }
}
