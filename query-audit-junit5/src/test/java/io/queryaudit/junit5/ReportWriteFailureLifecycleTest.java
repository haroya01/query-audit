package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.ReportFormat;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;

@DisplayName("Report write failure lifecycle")
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class ReportWriteFailureLifecycleTest {

  private static final String FIXTURE_PROPERTY = "queryaudit.test.reportWriteFailureFixture";
  private static final String OUTPUT_PATH_PROPERTY = "queryaudit.test.reportWriteFailurePath";

  private String fixtureProperty;
  private String outputPathProperty;

  @BeforeEach
  void setUp() {
    fixtureProperty = System.getProperty(FIXTURE_PROPERTY);
    outputPathProperty = System.getProperty(OUTPUT_PATH_PROPERTY);
    HtmlReportAggregator.getInstance().reset();
  }

  @AfterEach
  void tearDown() {
    restoreProperty(FIXTURE_PROPERTY, fixtureProperty);
    restoreProperty(OUTPUT_PATH_PROPERTY, outputPathProperty);
    HtmlReportAggregator.getInstance().reset();
  }

  @Test
  @DisplayName("a root finalizer failure is visible in the launcher result")
  void rootFinalizerFailureInvalidatesLauncherRun(@TempDir Path tempDir) throws IOException {
    Path blockedOutputDirectory = tempDir.resolve("not-a-directory");
    Files.writeString(blockedOutputDirectory, "occupied");
    System.setProperty(FIXTURE_PROPERTY, "true");
    System.setProperty(OUTPUT_PATH_PROPERTY, blockedOutputDirectory.toString());

    TestExecutionSummary summary = runFixture();

    assertThat(summary.getTestsFoundCount()).isEqualTo(1);
    assertThat(summary.getTestsSucceededCount()).isEqualTo(1);
    assertThat(summary.getTestsFailedCount()).isZero();
    assertThat(summary.getContainersFailedCount()).isEqualTo(1);
    assertThat(summary.getTotalFailureCount()).isEqualTo(1);
    assertThat(summary.getFailures())
        .singleElement()
        .satisfies(
            failure -> {
              assertThat(failure.getTestIdentifier().getUniqueId())
                  .isEqualTo("[engine:junit-jupiter]");
              assertThat(failure.getException())
                  .hasMessageContaining("Failed to close extension context")
                  .hasCauseInstanceOf(ReportWriteException.class)
                  .hasRootCauseInstanceOf(FileAlreadyExistsException.class);
              assertThat(failure.getException().getCause())
                  .hasMessageContaining("json report")
                  .hasMessageContaining(blockedOutputDirectory.resolve("report.json").toString())
                  .hasMessageContaining("audit run is incomplete");
            });
  }

  private static TestExecutionSummary runFixture() {
    LauncherDiscoveryRequest request =
        LauncherDiscoveryRequestBuilder.request()
            .selectors(selectClass(ReportFixture.class))
            .configurationParameter("junit.jupiter.extensions.autodetection.enabled", "false")
            .build();
    SummaryGeneratingListener listener = new SummaryGeneratingListener();
    Launcher launcher = LauncherFactory.create();
    launcher.registerTestExecutionListeners(listener);
    launcher.execute(request);
    return listener.getSummary();
  }

  private static void restoreProperty(String key, String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }

  @EnabledIfSystemProperty(named = FIXTURE_PROPERTY, matches = "true")
  @ExtendWith(RegisterFailingFinalizer.class)
  static class ReportFixture {

    @Test
    void passesBeforeSuiteFinalization() {
      QueryAuditReport report =
          new QueryAuditReport(
              ReportFixture.class.getName(),
              "passesBeforeSuiteFinalization",
              List.of(),
              List.of(),
              List.of(),
              List.of(),
              0,
              0,
              0);
      HtmlReportAggregator.getInstance().addReport(report);
    }
  }

  static class RegisterFailingFinalizer implements BeforeAllCallback {

    @Override
    public void beforeAll(ExtensionContext context) {
      Path outputDirectory = Path.of(System.getProperty(OUTPUT_PATH_PROPERTY));
      QueryAuditConfig config =
          QueryAuditConfig.builder()
              .reportFormat(ReportFormat.JSON)
              .reportOutputDir(outputDirectory.toString())
              .build();
      new QueryAuditExtension().registerReportFinalizer(context, config);
    }
  }
}
