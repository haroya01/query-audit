package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.ReportFormat;
import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class ReportRedactionConfigurationTest {

  @Test
  void systemPropertyControlsTheFinalJsonArtifact(@TempDir Path output) throws Exception {
    String previous = System.getProperty("queryAudit.reportRedaction");
    HtmlReportAggregator aggregator = HtmlReportAggregator.getInstance();
    try {
      aggregator.reset();
      aggregator.addReport(
          new QueryAuditReport(
              "AccountTest",
              "loadsAccount",
              List.of(),
              List.of(),
              List.of(new QueryRecord("SELECT 'fixture-secret'", 1, 0, null)),
              1,
              1,
              1));
      System.setProperty("queryAudit.reportRedaction", "full");
      QueryAuditConfig config = resolvedConfig();
      assertThat(config.getReportRedaction()).isEqualTo(ReportRedaction.FULL);
      new QueryAuditExtension.ReportFinalizer(
              new QueryAuditExtension(),
              output,
              ReportFormat.JSON,
              new QueryAuditExtension.AuditRunState(),
              config.getReportRedaction())
          .close();
      assertThat(Files.readString(output.resolve("report.json")))
          .contains("\"redaction\": \"FULL\"", "fixture-secret");

      System.clearProperty("queryAudit.reportRedaction");
      assertThat(resolvedConfig().getReportRedaction()).isEqualTo(ReportRedaction.REDACTED);
      new QueryAuditExtension.ReportFinalizer(new QueryAuditExtension(), output, ReportFormat.JSON)
          .close();
      assertThat(Files.readString(output.resolve("report.json")))
          .contains("\"redaction\": \"REDACTED\"")
          .doesNotContain("fixture-secret");
    } finally {
      if (previous == null) System.clearProperty("queryAudit.reportRedaction");
      else System.setProperty("queryAudit.reportRedaction", previous);
      aggregator.reset();
    }
  }

  @Test
  void aSuiteCannotMixRedactionModes(@TempDir Path output) {
    var finalizer =
        new QueryAuditExtension.ReportFinalizer(
            new QueryAuditExtension(), output, ReportFormat.JSON);
    assertThatThrownBy(
            () -> finalizer.requireConfiguration(output, ReportFormat.JSON, ReportRedaction.FULL))
        .isInstanceOf(ExtensionConfigurationException.class)
        .hasMessageContaining("conflicting report redaction modes");
  }

  private static QueryAuditConfig resolvedConfig() throws Exception {
    ExtensionContext context = mock(ExtensionContext.class);
    doReturn(ReportRedactionConfigurationTest.class).when(context).getRequiredTestClass();
    when(context.getStore(any())).thenReturn(mock(ExtensionContext.Store.class));
    Method buildConfig =
        QueryAuditExtension.class.getDeclaredMethod("buildConfig", ExtensionContext.class);
    buildConfig.setAccessible(true);
    return (QueryAuditConfig) buildConfig.invoke(new QueryAuditExtension(), context);
  }
}
