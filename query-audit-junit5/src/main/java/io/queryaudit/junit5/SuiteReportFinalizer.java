package io.queryaudit.junit5;

import io.queryaudit.core.config.ReportFormat;
import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.dedup.IssueFingerprintDeduplicator;
import io.queryaudit.core.model.AuditIncompleteReason;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.ranking.ImpactScorer;
import io.queryaudit.core.reporter.HtmlReporter;
import io.queryaudit.core.reporter.delivery.AuditReportPublisher;
import io.queryaudit.core.reporter.delivery.PublicationResult;
import io.queryaudit.core.reporter.delivery.ReportSinkRegistration;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Finalizes one root execution after test policies, keeping delivery failures separate from
 * analysis.
 */
class SuiteReportFinalizer implements ExtensionContext.Store.CloseableResource {

  private final AuditReportArtifacts.JsonWriter jsonWriter;
  private final java.util.function.Consumer<Path> browser;
  private final Path outputDirectory;
  private final ReportFormat reportFormat;
  private final ReportRedaction reportRedaction;
  private final SuiteAuditState runState;
  private AuditCoverageSession coverageSession;
  private volatile boolean autoOpen;
  private List<ReportSinkRegistration> reportSinks;
  private PublicationResult publicationResult;
  private boolean closed;

  SuiteReportFinalizer(
      AuditReportArtifacts.JsonWriter jsonWriter,
      java.util.function.Consumer<Path> browser,
      Path outputDirectory,
      ReportFormat reportFormat,
      SuiteAuditState runState,
      ReportRedaction reportRedaction) {
    this.jsonWriter = jsonWriter;
    this.browser = browser;
    this.outputDirectory = outputDirectory.toAbsolutePath().normalize();
    this.reportFormat = reportFormat;
    this.runState = runState;
    this.reportRedaction = reportRedaction;
  }

  void requireConfiguration(Path requestedDirectory, ReportFormat requestedFormat) {
    requireConfiguration(requestedDirectory, requestedFormat, ReportRedaction.REDACTED);
  }

  void requireConfiguration(
      Path requestedDirectory, ReportFormat requestedFormat, ReportRedaction requestedRedaction) {
    if (reportRedaction != requestedRedaction) {
      throw new ExtensionConfigurationException(
          "QueryAudit: conflicting report redaction modes in the same test run. "
              + "Use one query-audit.report.redaction value for all active test contexts.");
    }
    Path normalizedRequest = requestedDirectory.toAbsolutePath().normalize();
    if (!outputDirectory.equals(normalizedRequest)) {
      throw new ExtensionConfigurationException(
          "QueryAudit: conflicting report output directories in the same test run: '"
              + outputDirectory
              + "' and '"
              + normalizedRequest
              + "'. Use one query-audit.report.output-dir value for all active test contexts.");
    }
    if (reportFormat != requestedFormat) {
      throw new ExtensionConfigurationException(
          "QueryAudit: conflicting report formats in the same test run: '"
              + reportFormat.name().toLowerCase(Locale.ROOT)
              + "' and '"
              + requestedFormat.name().toLowerCase(Locale.ROOT)
              + "'. Use one query-audit.report.format value for all active test contexts.");
    }
  }

  synchronized void requireCoverageSession(AuditCoverageSession requestedSession) {
    if (coverageSession != null && coverageSession != requestedSession) {
      throw new ExtensionConfigurationException(
          "QueryAudit: audit coverage changed during the same test execution.");
    }
    coverageSession = requestedSession;
  }

  synchronized void requireReportSinks(List<ReportSinkRegistration> requested) {
    List<ReportSinkRegistration> snapshot = List.copyOf(requested);
    if (closed) {
      throw new ExtensionConfigurationException(
          "QueryAudit: suite publication has already completed.");
    }
    if (reportSinks == null) {
      reportSinks = snapshot;
      return;
    }
    boolean same = reportSinks.size() == snapshot.size();
    for (int index = 0; same && index < reportSinks.size(); index++) {
      ReportSinkRegistration existing = reportSinks.get(index);
      ReportSinkRegistration candidate = snapshot.get(index);
      same =
          existing.id().equals(candidate.id())
              && existing.required() == candidate.required()
              && existing.sink() == candidate.sink();
    }
    if (!same) {
      throw new ExtensionConfigurationException(
          "QueryAudit: conflicting report sink registrations in the same test run. "
              + "Use the same ordered registrations and sink instances for all active contexts.");
    }
  }

  PublicationResult publicationResult() {
    return publicationResult;
  }

  Path outputDirectory() {
    return outputDirectory;
  }

  ReportFormat reportFormat() {
    return reportFormat;
  }

  AuditRunResult result(List<QueryAuditReport> reports) {
    return runState.result(reports);
  }

  void enableAutoOpen() {
    this.autoOpen = true;
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    List<QueryAuditReport> reports =
        coverageSession == null ? runState.retainedReports() : coverageSession.reports();
    AuditRunResult runResult = runState.result(reports);
    if (coverageSession != null) {
      runResult = coverageSession.complete(runResult);
    }
    if (runResult.reports().isEmpty()
        && runResult.outcome() == AuditOutcome.PASS
        && (reportSinks == null || reportSinks.isEmpty())) {
      return;
    }

    Path reportPath = reportPath();
    ReportWriteException artifactFailure = null;
    try {
      if (coverageSession != null && reportFormat != ReportFormat.JSON) {
        jsonWriter.write(runResult, outputDirectory, reportRedaction);
      }
      switch (reportFormat) {
        case CONSOLE -> {
          // Per-test output and the suite summary are already on stdout.
        }
        case JSON -> jsonWriter.write(runResult, outputDirectory, reportRedaction);
        case HTML -> {
          new HtmlReporter()
              .writeToFile(
                  outputDirectory,
                  reports,
                  ImpactScorer.rank(
                      reports.stream()
                          .filter(report -> report.getConfirmedIssues() != null)
                          .flatMap(report -> report.getConfirmedIssues().stream())
                          .toList()),
                  IssueFingerprintDeduplicator.deduplicate(reports));
          System.out.println("[QueryAudit] file://" + reportPath.toAbsolutePath());
          if (autoOpen) {
            browser.accept(reportPath);
          }
        }
      }
    } catch (Exception e) {
      runState.markIncomplete(
          new AuditIncompleteReason(
              IncompleteReasonCode.REPORT_WRITE_FAILED,
              "Could not write the "
                  + reportFormat.name().toLowerCase(Locale.ROOT)
                  + " report to "
                  + reportPath.toAbsolutePath()));
      artifactFailure = new ReportWriteException(reportFormat, reportPath, e);
    }
    // Publication receives the completed analysis outcome, not the success of other deliveries.
    // A failed required sink fails the suite without rewriting report.json's analysis verdict.
    publicationResult =
        new AuditReportPublisher(reportSinks == null ? List.of() : reportSinks).publish(runResult);
    for (PublicationResult.Delivery delivery : publicationResult.deliveries()) {
      if (delivery.status() != PublicationResult.Status.DELIVERED) {
        System.err.println(
            "[QueryAudit] "
                + (delivery.required() ? "Required" : "Optional")
                + " report publication failed: registration="
                + delivery.registrationId()
                + ", status="
                + delivery.status()
                + ", required="
                + delivery.required()
                + "; analysis is unchanged.");
      }
    }
    printSummary(artifactFailure == null ? runResult : runState.result(runResult.reports()));
    ExtensionConfigurationException deliveryFailure =
        publicationResult.hasRequiredFailures()
            ? new ExtensionConfigurationException(
                "QueryAudit: required report publication failed. The analysis outcome is unchanged.")
            : null;
    if (artifactFailure != null) {
      if (deliveryFailure != null) artifactFailure.addSuppressed(deliveryFailure);
      throw artifactFailure;
    }
    if (deliveryFailure != null) throw deliveryFailure;
  }

  private Path reportPath() {
    return switch (reportFormat) {
      case CONSOLE -> outputDirectory;
      case JSON -> outputDirectory.resolve("report.json");
      case HTML -> outputDirectory.resolve("index.html");
    };
  }

  static void printSummary(AuditRunResult runResult) {
    List<QueryAuditReport> reports = runResult.reports();
    long totalErrors =
        reports.stream().mapToLong(report -> report.getFindings().errors().size()).sum();
    long totalWarnings =
        reports.stream().mapToLong(report -> report.getFindings().warnings().size()).sum();
    int totalQueries = reports.stream().mapToInt(QueryAuditReport::getTotalQueryCount).sum();

    String summary =
        "[QueryAudit] "
            + reports.size()
            + " tests, "
            + totalQueries
            + " queries"
            + (totalErrors > 0 ? ", " + totalErrors + " ERROR" + (totalErrors > 1 ? "S" : "") : "")
            + (totalWarnings > 0
                ? ", " + totalWarnings + " WARNING" + (totalWarnings > 1 ? "S" : "")
                : "")
            + (totalErrors == 0 && totalWarnings == 0 && runResult.outcome() == AuditOutcome.PASS
                ? " — all clean"
                : "");
    System.out.println();
    System.out.println(summary);
    System.out.println("[QueryAudit] outcome: " + runResult.outcome());
    for (AuditIncompleteReason reason : runResult.incompleteReasons()) {
      String detail = reason.detail() == null ? "" : ": " + reason.detail();
      System.out.println("[QueryAudit] incomplete: " + reason.code() + detail);
    }
    System.out.println();
  }
}
