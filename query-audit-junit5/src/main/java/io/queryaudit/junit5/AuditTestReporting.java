package io.queryaudit.junit5;

import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.reporter.ConsoleReporter;
import io.queryaudit.core.reporter.GitHubActionsReporter;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import java.util.Locale;

/**
 * Displays per-test diagnostics and retains the bounded presentation view for suite finalization.
 */
final class AuditTestReporting {
  void present(AuditScope scope, AnalyzedAudit result) {
    QueryAuditReport display = visible(result.report(), result.config().isShowInfo());
    System.out.println(
        "[QueryAudit] Rule profile: "
            + result.config().getRuleProfile().name().toLowerCase(Locale.ROOT));
    new ConsoleReporter(
            System.out, ConsoleReporter.detectColorSupport(), result.analyzer().getBaseline())
        .report(display);
    if ("true".equals(System.getenv("GITHUB_ACTIONS"))) {
      new GitHubActionsReporter(result.config().getReportRedaction()).report(display);
    }
    HtmlReportAggregator aggregator = HtmlReportAggregator.getInstance();
    scope.runState().retainReport(display, aggregator.getMaxInMemoryReports());
    // Keep the legacy public accumulator available, but never use it as another run's evidence.
    aggregator.addReport(display);
    AuditCoverageSession coverage = AuditCoverageListener.currentSession(scope.context());
    if (coverage != null) coverage.audited(display);
  }

  static QueryAuditReport visible(QueryAuditReport report, boolean showInfo) {
    return showInfo ? report : report.withoutInformationalFindings();
  }
}
