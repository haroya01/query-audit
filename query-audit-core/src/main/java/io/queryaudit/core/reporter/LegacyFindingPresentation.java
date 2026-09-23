package io.queryaudit.core.reporter;

import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.parser.SqlParser;
import io.queryaudit.core.ranking.ImpactScorer;
import io.queryaudit.core.ranking.RankedIssue;
import java.util.List;

/** The enum-only ranking and persisted HTML review keys retained by the Finding renderers. */
final class LegacyFindingPresentation {
  private LegacyFindingPresentation() {}

  static String description(Finding finding) {
    return finding
        .toIssue()
        .map(issue -> issue.type().getDescription())
        .orElse(finding.kindId().value());
  }

  static List<RankedIssue> rankBuiltIns(List<Finding> findings) {
    return ImpactScorer.rank(
        findings.stream().flatMap(finding -> finding.toIssue().stream()).toList());
  }

  static String htmlCheckKey(String testId, Finding finding) {
    if (!finding.kindId().isBuiltin()) return FindingId.of(testId, finding);
    String normalized = finding.query() != null ? SqlParser.normalize(finding.query()) : "";
    return finding.kindId().value() + "-" + Integer.toHexString(normalized.hashCode());
  }

  static String htmlReviewHash(List<QueryAuditReport> reports) {
    int hash = 0;
    for (QueryAuditReport report : reports) {
      var findings = report.getFindings();
      // The old ordering is persisted by report.js. Presentation unification must not invalidate
      // a user's saved checks: built-in errors/warnings/info, then custom confirmed/info.
      for (List<Finding> bucket :
          List.of(findings.errors(), findings.warnings(), findings.informational())) {
        for (Finding finding : bucket) {
          if (finding.kindId().isBuiltin()) {
            hash = 31 * hash + htmlCheckKey(report.getTestId(), finding).hashCode();
          }
        }
      }
      for (List<Finding> bucket : List.of(findings.confirmed(), findings.informational())) {
        for (Finding finding : bucket) {
          if (!finding.kindId().isBuiltin()) {
            hash = 31 * hash + htmlCheckKey(report.getTestId(), finding).hashCode();
          }
        }
      }
    }
    return Integer.toHexString(hash);
  }
}
