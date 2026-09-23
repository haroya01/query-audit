package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.config.ReportFormat;
import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.core.reporter.delivery.AuditReportSink;
import io.queryaudit.core.reporter.delivery.PublishedAuditRun;
import io.queryaudit.core.reporter.delivery.ReportSinkRegistration;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

@ResourceLock(Resources.SYSTEM_ERR)
class ReportSinkFinalizerTest {
  @BeforeEach
  void prepare() {
    HtmlReportAggregator.getInstance().reset();
    HtmlReportAggregator.getInstance().addReport(report());
  }

  @AfterEach
  void cleanup() {
    HtmlReportAggregator.getInstance().reset();
  }

  @Test
  void publicationIsOnceOnlyAndSanitizedEvenWhenLocalReportsUseFullDetail(@TempDir Path output) {
    List<PublishedAuditRun> delivered = new ArrayList<>();
    var finalizer = finalizer(new QueryAuditExtension(), output, ReportFormat.CONSOLE);
    finalizer.requireReportSinks(
        List.of(new ReportSinkRegistration("company:sink", true, delivered::add)));

    finalizer.close();
    finalizer.close();

    assertThat(delivered).hasSize(1);
    assertThat(delivered.get(0).json())
        .contains("sanitized-summary")
        .doesNotContain("private_marker", "private_test", "private_source");
    assertThat(finalizer.publicationResult().analysisOutcome()).isEqualTo(AuditOutcome.PASS);
  }

  @Test
  void requiredDeliveryFailureFailsTheSuiteWithoutRewritingTheAnalysisVerdict(@TempDir Path output)
      throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    var finalizer = finalizer(new QueryAuditExtension(), output, ReportFormat.JSON);
    finalizer.requireReportSinks(
        List.of(
            new ReportSinkRegistration(
                "company:sink",
                true,
                run -> {
                  attempts.incrementAndGet();
                  throw new IOException("private_marker secret");
                })));

    assertThatThrownBy(finalizer::close)
        .isInstanceOf(ExtensionConfigurationException.class)
        .hasMessageContaining("required report publication failed")
        .hasMessageNotContaining("private_marker")
        .hasNoCause();
    finalizer.close();

    assertThat(attempts).hasValue(1);
    assertThat(finalizer.publicationResult().hasRequiredFailures()).isTrue();
    assertThat(finalizer.publicationResult().analysisOutcome()).isEqualTo(AuditOutcome.PASS);
    assertThat(finalizer.result(List.of(report())).outcome()).isEqualTo(AuditOutcome.PASS);
    assertThat(Files.readString(output.resolve("report.json"))).contains("\"outcome\": \"PASS\"");
  }

  @Test
  void optionalFailureOnlyEmitsAFixedDiagnostic(@TempDir Path output) {
    var finalizer = finalizer(new QueryAuditExtension(), output, ReportFormat.CONSOLE);
    finalizer.requireReportSinks(
        List.of(
            new ReportSinkRegistration(
                "company:sink",
                false,
                run -> {
                  throw new IOException("private_marker secret");
                })));
    ByteArrayOutputStream diagnostic = new ByteArrayOutputStream();
    PrintStream previous = System.err;
    try (PrintStream captured = new PrintStream(diagnostic, true, StandardCharsets.UTF_8)) {
      System.setErr(captured);
      finalizer.close();
    } finally {
      System.setErr(previous);
    }

    assertThat(finalizer.publicationResult().hasOptionalFailures()).isTrue();
    assertThat(finalizer.publicationResult().hasRequiredFailures()).isFalse();
    assertThat(diagnostic.toString(StandardCharsets.UTF_8))
        .contains(
            "Optional report publication failed",
            "registration=company:sink",
            "status=FAILED",
            "required=false")
        .doesNotContain("private_marker", "secret");
  }

  @Test
  void rootRegistrationRequiresTheSameOrderedIdsPoliciesAndInstances(@TempDir Path output) {
    AuditReportSink sink = run -> {};
    var finalizer = finalizer(new QueryAuditExtension(), output, ReportFormat.CONSOLE);
    finalizer.requireReportSinks(List.of(new ReportSinkRegistration("company:sink", true, sink)));
    finalizer.requireReportSinks(List.of(new ReportSinkRegistration("company:sink", true, sink)));

    assertThatThrownBy(
            () ->
                finalizer.requireReportSinks(
                    List.of(new ReportSinkRegistration("company:sink", false, sink))))
        .isInstanceOf(ExtensionConfigurationException.class);
    assertThatThrownBy(
            () ->
                finalizer.requireReportSinks(
                    List.of(new ReportSinkRegistration("company:sink", true, run -> {}))))
        .isInstanceOf(ExtensionConfigurationException.class);
    assertThatThrownBy(() -> finalizer.requireReportSinks(List.of()))
        .isInstanceOf(ExtensionConfigurationException.class);
  }

  @Test
  void artifactFailureDoesNotSkipIndependentSinkDelivery(@TempDir Path output) {
    QueryAuditExtension brokenWriter =
        new QueryAuditExtension() {
          @Override
          void writeJsonReport(AuditRunResult result, Path target, ReportRedaction redaction)
              throws IOException {
            throw new IOException("unwritable artifact");
          }
        };
    List<PublishedAuditRun> delivered = new ArrayList<>();
    var finalizer = finalizer(brokenWriter, output, ReportFormat.JSON);
    finalizer.requireReportSinks(
        List.of(new ReportSinkRegistration("company:sink", true, delivered::add)));

    assertThatThrownBy(finalizer::close).isInstanceOf(ReportWriteException.class);

    assertThat(delivered).hasSize(1);
    assertThat(delivered.get(0).outcome()).isEqualTo(AuditOutcome.PASS);
    assertThat(finalizer.publicationResult().hasRequiredFailures()).isFalse();
  }

  private static QueryAuditExtension.ReportFinalizer finalizer(
      QueryAuditExtension extension, Path output, ReportFormat format) {
    var state = new QueryAuditExtension.AuditRunState();
    state.retainReport(report(), HtmlReportAggregator.DEFAULT_MAX_IN_MEMORY_REPORTS);
    return new QueryAuditExtension.ReportFinalizer(
        extension, output, format, state, ReportRedaction.FULL);
  }

  private static QueryAuditReport report() {
    return new QueryAuditReport(
        "private_test",
        "method",
        List.of(),
        List.of(),
        List.of(),
        List.of(new QueryRecord("SELECT 'private_marker'", 0, 0, "private_source")),
        1,
        1,
        0);
  }
}
