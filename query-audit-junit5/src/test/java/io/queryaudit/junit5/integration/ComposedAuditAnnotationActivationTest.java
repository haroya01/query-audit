package io.queryaudit.junit5.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.junit5.EnableQueryInspector;
import io.queryaudit.junit5.ExpectQueries;
import io.queryaudit.junit5.QueryAudit;
import io.queryaudit.junit5.QueryAuditExclude;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
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

@DisplayName("Issue #290: composed and inherited audit annotations activate QueryAudit")
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class ComposedAuditAnnotationActivationTest {

  private static final String FIXTURE_PROPERTY = "queryaudit.test.composedFixtures";
  private static final String N_PLUS_ONE_MESSAGE = "N+1 Query detected";

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
  @DisplayName("direct, composed, and inherited @QueryAudit all fail the same N+1")
  void everyDeclarationFormEnforcesFindings() {
    TestExecutionSummary summary =
        runFixtures(
            DirectFixture.class,
            ComposedClassFixture.class,
            InheritedFixture.class,
            ComposedMethodFixture.class);

    assertThat(summary.getTestsFoundCount()).isEqualTo(4);
    assertThat(summary.getTestsFailedCount()).isEqualTo(4);
    assertThat(summary.getFailures())
        .allSatisfy(
            failure -> assertThat(failure.getException()).hasMessageContaining(N_PLUS_ONE_MESSAGE));
    assertThat(HtmlReportAggregator.getInstance().getReports())
        .hasSize(4)
        .allSatisfy(
            report ->
                assertThat(report.getConfirmedIssues())
                    .anyMatch(issue -> issue.type() == IssueType.N_PLUS_ONE));
  }

  @Test
  @DisplayName("a composed @EnableQueryInspector reports the N+1 without failing")
  void composedInspectorReportsWithoutFailing() {
    TestExecutionSummary summary = runFixtures(ComposedInspectorFixture.class);

    assertThat(summary.getTestsFoundCount()).isEqualTo(1);
    assertThat(summary.getTestsSucceededCount()).isEqualTo(1);
    assertThat(HtmlReportAggregator.getInstance().getReports())
        .singleElement()
        .satisfies(
            report ->
                assertThat(report.getConfirmedIssues())
                    .anyMatch(issue -> issue.type() == IssueType.N_PLUS_ONE));
  }

  @Test
  @DisplayName("a subclass annotation overrides the settings of an audited superclass")
  void nearestDeclarationWins() {
    TestExecutionSummary summary = runFixtures(OverridingSubclassFixture.class);

    assertThat(summary.getTestsFoundCount()).isEqualTo(1);
    assertThat(summary.getTestsSucceededCount()).isEqualTo(1);
    assertThat(HtmlReportAggregator.getInstance().getReports()).hasSize(1);
  }

  @Test
  @DisplayName("an inherited audit still enforces a method budget")
  void inheritedAuditEnforcesBudgets() {
    TestExecutionSummary summary = runFixtures(InheritedBudgetFixture.class);

    assertThat(summary.getTestsFailedCount()).isEqualTo(1);
    assertThat(summary.getFailures())
        .singleElement()
        .satisfies(
            failure ->
                assertThat(failure.getException())
                    .hasMessageContaining("SELECT: executed 1, expected at most 0"));
  }

  @Test
  @DisplayName("@QueryAuditExclude on a superclass excludes audited descendants")
  void excludedSuperclassExcludesDescendants() {
    TestExecutionSummary summary = runFixtures(DescendantOfExcludedFixture.class);

    assertThat(summary.getTestsFoundCount()).isEqualTo(1);
    assertThat(summary.getTestsSucceededCount()).isEqualTo(1);
    assertThat(HtmlReportAggregator.getInstance().getReports()).isEmpty();
  }

  @Test
  @DisplayName("a composed audit without a DataSource fails instead of passing unaudited")
  void composedAuditWithoutDataSourceFails() {
    TestExecutionSummary summary = runFixtures(ComposedWithoutDataSourceFixture.class);

    assertThat(summary.getTestsFoundCount()).isEqualTo(1);
    assertThat(summary.getTestsFailedCount()).isEqualTo(1);
    assertThat(summary.getFailures())
        .singleElement()
        .satisfies(
            failure ->
                assertThat(failure.getException()).hasMessageContaining("DataSource unavailable"));
  }

  private static TestExecutionSummary runFixtures(Class<?>... fixtures) {
    String previousValue = System.getProperty(FIXTURE_PROPERTY);
    System.setProperty(FIXTURE_PROPERTY, "true");
    try {
      LauncherDiscoveryRequest request =
          LauncherDiscoveryRequestBuilder.request()
              .selectors(Arrays.stream(fixtures).map(fixture -> selectClass(fixture)).toList())
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

  @Target({ElementType.TYPE, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  @QueryAudit
  @interface Audited {}

  @Target(ElementType.TYPE)
  @Retention(RetentionPolicy.RUNTIME)
  @EnableQueryInspector
  @interface Inspected {}

  abstract static class RepeatedSelectFixture {

    static final DataSource DATA_SOURCE = createDataSource();

    static void repeatSelect(int count) throws SQLException {
      try (Connection connection = DATA_SOURCE.getConnection();
          Statement statement = connection.createStatement()) {
        for (int i = 0; i < count; i++) {
          statement.executeQuery("SELECT " + (i + 1)).close();
        }
      }
    }

    private static DataSource createDataSource() {
      JdbcDataSource dataSource = new JdbcDataSource();
      dataSource.setURL("jdbc:h2:mem:composed-audit-annotations;DB_CLOSE_DELAY=-1");
      return new ProxyDataSource(dataSource);
    }
  }

  @QueryAudit
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class DirectFixture extends RepeatedSelectFixture {

    @Test
    void repeatsSelect() throws SQLException {
      repeatSelect(3);
    }
  }

  @Audited
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class ComposedClassFixture extends RepeatedSelectFixture {

    @Test
    void repeatsSelect() throws SQLException {
      repeatSelect(3);
    }
  }

  @QueryAudit
  abstract static class AuditedBase extends RepeatedSelectFixture {}

  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class InheritedFixture extends AuditedBase {

    @Test
    void repeatsSelect() throws SQLException {
      repeatSelect(3);
    }
  }

  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class ComposedMethodFixture extends RepeatedSelectFixture {

    @Test
    @Audited
    void repeatsSelect() throws SQLException {
      repeatSelect(3);
    }
  }

  @Inspected
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class ComposedInspectorFixture extends RepeatedSelectFixture {

    @Test
    void repeatsSelect() throws SQLException {
      repeatSelect(3);
    }
  }

  @QueryAudit(nPlusOneThreshold = 4)
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class OverridingSubclassFixture extends AuditedBase {

    @Test
    void repeatsSelectBelowItsOwnThreshold() throws SQLException {
      repeatSelect(3);
    }
  }

  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class InheritedBudgetFixture extends AuditedBase {

    @Test
    @ExpectQueries(select = 0)
    void exceedsBudget() throws SQLException {
      repeatSelect(1);
    }
  }

  @QueryAuditExclude
  abstract static class ExcludedBase extends RepeatedSelectFixture {}

  @QueryAudit
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class DescendantOfExcludedFixture extends ExcludedBase {

    @Test
    void repeatsSelect() throws SQLException {
      repeatSelect(3);
    }
  }

  @Audited
  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  static class ComposedWithoutDataSourceFixture {

    @Test
    void runs() {}
  }
}
