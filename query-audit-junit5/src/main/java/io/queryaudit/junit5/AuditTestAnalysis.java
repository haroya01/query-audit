package io.queryaudit.junit5;

import io.queryaudit.core.baseline.Baseline;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.extension.AuditExtensions;
import io.queryaudit.core.interceptor.ConnectionUsageTracker;
import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.core.interceptor.QueryCaptureSnapshot;
import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Readable per-test analysis sequence; selection, file IO, and suite publication live elsewhere.
 */
final class AuditTestAnalysis {
  private final HibernateIntegration hibernate;
  private final QueryCountPolicies counts;
  private final ExplainAnalysis explain;
  private final ComparisonInputRecorder inputs;
  private final AuditSettingsResolver settings;

  AuditTestAnalysis(
      HibernateIntegration hibernate,
      QueryCountPolicies counts,
      ExplainAnalysis explain,
      ComparisonInputRecorder inputs,
      AuditSettingsResolver settings) {
    this.hibernate = hibernate;
    this.counts = counts;
    this.explain = explain;
    this.inputs = inputs;
    this.settings = settings;
  }

  AnalyzedAudit analyze(AuditScope scope, AuditExtensions extensions) {
    QueryInterceptor interceptor = scope.interceptor();
    QueryCaptureSnapshot capture = scope.stopCapture();
    if (capture.truncated()) {
      AuditDiagnostics.incomplete(
          scope,
          IncompleteReasonCode.QUERY_LIMIT_REACHED,
          scope.context().getDisplayName()
              + " retained "
              + capture.queries().size()
              + " queries and dropped "
              + capture.droppedCount());
    }
    List<QueryRecord> queries = capture.queries();
    QueryAuditConfig config = settings.buildConfig(scope.context(), scope.returnTypeResolver());
    Path baseline =
        Path.of(
            config.getBaselinePath() != null
                ? config.getBaselinePath()
                : Baseline.DEFAULT_FILE_NAME);
    QueryAuditAnalyzer analyzer = QueryAuditAnalyzer.withExtensions(config, baseline, extensions);
    Class<?> outermost = scope.context().getRequiredTestClass();
    while (outermost.getEnclosingClass() != null) outermost = outermost.getEnclosingClass();
    String testClass = outermost.getSimpleName();
    String testName = scope.context().getDisplayName();
    JUnitTestIdentity identity = JUnitTestIdentity.from(scope.context());
    QueryAuditReport report = analyzer.analyze(testClass, testName, queries, scope.metadata());

    LazyLoadTracker tracker = scope.tracker();
    if (tracker != null && !tracker.getRecords().isEmpty()) {
      report = hibernate.mergeNPlusOneIssues(report, tracker, analyzer);
    }
    if (tracker != null && !tracker.getExplicitLoads().isEmpty()) {
      report = hibernate.mergeFindByIdIssues(report, tracker, analyzer);
    }
    if (!capture.truncated()) {
      report =
          counts.detectRegression(
              scope, report, queries, identity.testId(), testClass, testName, analyzer);
    }
    report = explain.analyze(scope, extensions, report, queries, analyzer);
    report =
        mergeConnectionHeldIdleIssues(report, interceptor, analyzer)
            .withTestIdentity(identity.testId(), identity.selector())
            .withIndexMetadata(scope.metadata());

    scope.runState().recordReport(report);
    inputs.record(scope, analyzer, identity.testId(), testClass, testName);
    return new AnalyzedAudit(config, capture, analyzer, report, identity, testClass, testName);
  }

  QueryAuditReport mergeConnectionHeldIdleIssues(
      QueryAuditReport report, QueryInterceptor interceptor, QueryAuditAnalyzer analyzer) {
    QueryAuditConfig config = analyzer.getConfig();
    List<Issue> idleIssues = new ArrayList<>();
    for (ConnectionUsageTracker.ConnectionSession session :
        interceptor.getConnectionTracker().getCompletedSessions()) {
      long idleMillis = session.idleMillis();
      if (idleMillis < config.getConnectionHeldIdleThresholdMs()) {
        continue;
      }
      idleIssues.add(
          new Issue(
              IssueType.CONNECTION_HELD_IDLE,
              Severity.INFO,
              null,
              null,
              null,
              "Connection "
                  + session.connectionId()
                  + " held "
                  + session.heldMillis()
                  + "ms but executed database work for only "
                  + session.databaseWorkMillis()
                  + "ms ("
                  + idleMillis
                  + "ms idle"
                  + (session.released() ? "" : ", never released in the test window")
                  + ") — under load this shape exhausts the pool",
              "Release the connection before slow non-database work: move external calls (HTTP,"
                  + " push, file I/O) out of the transaction, or split the transaction around"
                  + " them",
              session.acquireCallSite()));
    }
    return analyzer.mergeDetectedIssues(report, idleIssues);
  }
}
