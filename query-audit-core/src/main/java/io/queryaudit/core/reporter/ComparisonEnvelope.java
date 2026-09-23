package io.queryaudit.core.reporter;

import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.model.AuditCoverage;
import io.queryaudit.core.model.AuditIncompleteReason;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.provenance.ComparisonInputs;
import io.queryaudit.core.reporter.ReportComparator.TestRef;
import java.util.List;
import java.util.Map;

/**
 * Validated comparison input. No parser-owned maps or presentation-only fields cross this boundary.
 */
record ComparisonEnvelope(
    AuditOutcome outcome,
    List<AuditIncompleteReason> incompleteReasons,
    List<TestReport> reports,
    ReportRedaction redaction,
    AuditCoverage coverage,
    Map<String, ComparisonInputs> comparisonInputs,
    SchemaVersion schemaVersion) {

  ComparisonEnvelope {
    incompleteReasons = List.copyOf(incompleteReasons);
    reports = List.copyOf(reports);
    comparisonInputs = Map.copyOf(comparisonInputs);
  }

  boolean hasFindingIds() {
    return schemaVersion.hasFindingIds();
  }

  record SchemaVersion(int major, int minor, String text) {
    boolean hasFindingIds() {
      return minor >= 7;
    }
  }

  record TestReport(
      String testId,
      TestRef ref,
      long totalQueries,
      long executionTimeMs,
      List<ReportedFinding> confirmed,
      List<ReportedFinding> info,
      List<ReportedFinding> acknowledged) {
    TestReport {
      confirmed = List.copyOf(confirmed);
      info = List.copyOf(info);
      acknowledged = List.copyOf(acknowledged);
    }

    boolean hasStableId() {
      return testId != null;
    }
  }

  record ReportedFinding(
      String findingId,
      String type,
      String query,
      String sourceLocation,
      String table,
      String column,
      String detail) {}
}
