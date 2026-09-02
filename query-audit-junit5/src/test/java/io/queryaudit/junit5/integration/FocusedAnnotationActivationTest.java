package io.queryaudit.junit5.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.junit5.DetectNPlusOne;
import io.queryaudit.junit5.ExpectMaxQueryCount;
import io.queryaudit.junit5.ExpectQueries;
import io.queryaudit.junit5.QueryAuditExclude;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import javax.sql.DataSource;
import net.ttddyy.dsproxy.support.ProxyDataSource;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

@DisplayName("Issue #233: focused annotations activate QueryAudit")
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class FocusedAnnotationActivationTest {

  private static final String FIXTURE_PROPERTY = "queryaudit.test.focusedFixtures";

  private String auditMode;
  private String autoOpenReport;

  @BeforeEach
  void setUp() {
    auditMode = System.getProperty("queryAudit.mode");
    autoOpenReport = System.getProperty("queryaudit.autoOpenReport");
    System.setProperty("queryAudit.mode", "annotated");
    System.setProperty("queryaudit.autoOpenReport", "false");
    HtmlReportAggregator.getInstance().reset();
  }

  @AfterEach
  void tearDown() {
    restoreProperty("queryAudit.mode", auditMode);
    restoreProperty("queryaudit.autoOpenReport", autoOpenReport);
    HtmlReportAggregator.getInstance().reset();
  }

  @Test
  @DisplayName("query budget annotations enforce their contracts without a class annotation")
  void queryBudgetAnnotationsActivate() {
    TestExecutionSummary summary =
        runFixtures(ExpectQueriesFixture.class, ExpectMaxQueryCountFixture.class);

    assertThat(summary.getTestsFoundCount()).isEqualTo(2);
    assertThat(summary.getTestsFailedCount()).isEqualTo(2);
    assertThat(summary.getFailures())
        .extracting(failure -> failure.getException().getMessage())
        .anySatisfy(
            message -> assertThat(message).contains("SELECT: executed 1, expected at most 0"))
        .anySatisfy(
            message -> assertThat(message).contains("executed 1 queries, expected at most 0"));
    assertThat(HtmlReportAggregator.getInstance().getReports())
        .hasSize(2)
        .allSatisfy(report -> assertThat(report.getTotalQueryCount()).isEqualTo(1));
  }

  @Test
  @DisplayName("@DetectNPlusOne activates at method and class scope")
  void detectNPlusOneActivatesAtBothScopes() {
    TestExecutionSummary summary =
        runFixtures(MethodDetectNPlusOneFixture.class, ClassDetectNPlusOneFixture.class);

    assertThat(summary.getTestsFoundCount()).isEqualTo(2);
    assertThat(summary.getTestsFailedCount()).isZero();
    assertThat(HtmlReportAggregator.getInstance().getReports())
        .hasSize(2)
        .allSatisfy(
            report -> {
              assertThat(report.getTotalQueryCount()).isEqualTo(2);
              assertThat(report.getInfoIssues())
                  .anyMatch(issue -> issue.type() == IssueType.N_PLUS_ONE_SUSPECT);
            });
  }

  @Test
  @DisplayName("@QueryAuditExclude takes precedence over focused annotations")
  void exclusionsTakePrecedence() {
    TestExecutionSummary summary =
        runFixtures(MethodExcludedFixture.class, ClassExcludedFixture.class);

    assertThat(summary.getTestsFoundCount()).isEqualTo(2);
    assertThat(summary.getTestsSucceededCount()).isEqualTo(2);
    assertThat(HtmlReportAggregator.getInstance().getReports()).isEmpty();
  }

  private static TestExecutionSummary runFixtures(Class<?>... fixtures) {
    String previousValue = System.getProperty(FIXTURE_PROPERTY);
    System.setProperty(FIXTURE_PROPERTY, "true");
    try {
      LauncherDiscoveryRequest request =
          LauncherDiscoveryRequestBuilder.request()
              .selectors(Arrays.stream(fixtures).map(fixture -> selectClass(fixture)).toList())
              .configurationParameter("junit.jupiter.extensions.autodetection.enabled", "true")
              .build();
      SummaryGeneratingListener listener = new SummaryGeneratingListener();
      Launcher launcher = LauncherFactory.create();
      launcher.registerTestExecutionListeners(listener);
      launcher.execute(request);
      return listener.getSummary();
    } finally {
      restoreProperty(FIXTURE_PROPERTY, previousValue);
    }
  }

  private static void restoreProperty(String key, String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }

  abstract static class FocusedAnnotationDataSourceFixture {

    static final DataSource DATA_SOURCE = createDataSource();

    static void executeSelects(int count) throws SQLException {
      try (Connection connection = DATA_SOURCE.getConnection();
          Statement statement = connection.createStatement()) {
        for (int i = 0; i < count; i++) {
          statement.executeQuery("SELECT 1").close();
        }
      }
    }

    private static DataSource createDataSource() {
      JdbcDataSource dataSource = new JdbcDataSource();
      dataSource.setURL("jdbc:h2:mem:focused-annotation-activation;DB_CLOSE_DELAY=-1");
      return new ProxyDataSource(dataSource);
    }
  }

  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class ExpectQueriesFixture extends FocusedAnnotationDataSourceFixture {

    @Test
    @ExpectQueries(select = 0)
    void executesSelect() throws SQLException {
      executeSelects(1);
    }
  }

  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class ExpectMaxQueryCountFixture extends FocusedAnnotationDataSourceFixture {

    @Test
    @ExpectMaxQueryCount(0)
    void executesSelect() throws SQLException {
      executeSelects(1);
    }
  }

  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class MethodDetectNPlusOneFixture extends FocusedAnnotationDataSourceFixture {

    @Test
    @DetectNPlusOne(threshold = 2)
    void repeatsSelect() throws SQLException {
      executeSelects(2);
    }
  }

  @DetectNPlusOne(threshold = 2)
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class ClassDetectNPlusOneFixture extends FocusedAnnotationDataSourceFixture {

    @Test
    void repeatsSelect() throws SQLException {
      executeSelects(2);
    }
  }

  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class MethodExcludedFixture {

    @Test
    @QueryAuditExclude
    @ExpectQueries(select = 0)
    void excluded() {}
  }

  @QueryAuditExclude
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class ClassExcludedFixture {

    @Test
    @ExpectMaxQueryCount(0)
    void excluded() {}
  }
}
