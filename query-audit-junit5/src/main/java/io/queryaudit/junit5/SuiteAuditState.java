package io.queryaudit.junit5;

import io.queryaudit.core.model.AuditIncompleteReason;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.provenance.ComparisonInputs;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Canonical analysis state and bounded report evidence owned by one root execution. */
class SuiteAuditState {
  private final Set<AuditIncompleteReason> incompleteReasons = new LinkedHashSet<>();
  private final Map<String, ComparisonInputs> comparisonInputs = new LinkedHashMap<>();
  private final Map<String, List<Finding>> informationalFindings = new LinkedHashMap<>();
  private final List<QueryAuditReport> retainedReports = new ArrayList<>();
  private boolean policyFailed;

  synchronized void markPolicyFailed() {
    policyFailed = true;
  }

  synchronized void markIncomplete(AuditIncompleteReason reason) {
    incompleteReasons.add(reason);
  }

  synchronized void recordInputs(String testId, ComparisonInputs inputs) {
    comparisonInputs.put(testId, inputs);
  }

  synchronized void recordReport(QueryAuditReport report) {
    informationalFindings.put(report.getTestId(), report.getFindings().informational());
  }

  synchronized void retainReport(QueryAuditReport display, int fullEvidenceLimit) {
    retainedReports.add(
        retainedReports.size() >= fullEvidenceLimit ? display.withoutQueryEvidence() : display);
  }

  synchronized List<QueryAuditReport> retainedReports() {
    return List.copyOf(retainedReports);
  }

  synchronized AuditRunResult result(List<QueryAuditReport> retainedReports) {
    List<QueryAuditReport> canonical =
        retainedReports.stream().map(this::restoreInformationalFindings).toList();
    return AuditRunResult.determine(canonical, policyFailed, incompleteReasons)
        .withComparisonInputs(comparisonInputs);
  }

  private QueryAuditReport restoreInformationalFindings(QueryAuditReport retained) {
    List<Finding> info = informationalFindings.get(retained.getTestId());
    return info == null || info.equals(retained.getFindings().informational())
        ? retained
        : retained.withInformationalFindings(info);
  }
}
