package io.queryaudit.junit5;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.interceptor.QueryCaptureSnapshot;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import java.util.List;

/** Immutable handoff from analysis to presentation and direct test policy. */
record AnalyzedAudit(
    QueryAuditConfig config,
    QueryCaptureSnapshot capture,
    QueryAuditAnalyzer analyzer,
    QueryAuditReport report,
    JUnitTestIdentity identity,
    String testClass,
    String testName) {
  List<QueryRecord> queries() {
    return capture.queries();
  }
}
