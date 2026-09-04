package io.queryaudit.junit5;

import io.queryaudit.core.model.AuditCoverage;
import io.queryaudit.core.model.AuditCoverage.Gap;
import io.queryaudit.core.model.AuditIncompleteReason;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.TestPlan;

/** State owned by a single platform execution, never inferred from the global report cache. */
final class AuditCoverageSession {

  private final Set<String> expected;
  private final List<AuditIncompleteReason> manifestFailures;
  private final Set<String> discovered = new HashSet<>();
  private final Set<String> executed = new HashSet<>();
  private final Set<String> skipped = new HashSet<>();
  private final Set<String> failedContainers = new HashSet<>();
  private final Map<String, TestExecutionResult.Status> results = new LinkedHashMap<>();
  private final Map<String, QueryAuditReport> reports = new LinkedHashMap<>();
  private boolean finalized;
  private Object extensionRoot;

  private AuditCoverageSession(Set<String> expected, List<AuditIncompleteReason> manifestFailures) {
    this.expected = expected;
    this.manifestFailures = manifestFailures;
  }

  static AuditCoverageSession open(TestPlan testPlan) {
    String configuredPath = System.getProperty(AuditCoverageManifest.PATH_PROPERTY);
    Path manifest;
    AuditCoverageSession session;
    try {
      manifest =
          Path.of(
              configuredPath == null ? AuditCoverageManifest.DEFAULT_FILE_NAME : configuredPath);
      if (configuredPath == null && Files.notExists(manifest)) {
        return null;
      }
      if (configuredPath != null && configuredPath.isBlank()) {
        throw new IllegalArgumentException("Blank coverage manifest path");
      }
      session = new AuditCoverageSession(AuditCoverageManifest.load(manifest), List.of());
    } catch (Exception failure) {
      // Keep private filesystem paths and parser diagnostics out of shared report artifacts.
      System.err.println("[QueryAudit] Cannot read the audit coverage manifest: " + failure);
      session =
          new AuditCoverageSession(
              Set.of(),
              List.of(
                  new AuditIncompleteReason(
                      IncompleteReasonCode.COVERAGE_MANIFEST_UNREADABLE,
                      "The expected-test manifest could not be loaded.")));
    }
    for (TestIdentifier root : testPlan.getRoots()) {
      session.discovered(root);
      for (TestIdentifier test : testPlan.getDescendants(root)) {
        session.discovered(test);
      }
    }
    return session;
  }

  synchronized void discovered(TestIdentifier test) {
    discovered.add(test.getUniqueId());
  }

  synchronized boolean bindRoot(Object root) {
    if (extensionRoot == null) {
      extensionRoot = root;
    }
    return extensionRoot == root;
  }

  synchronized void started(TestIdentifier test) {
    discovered(test);
    if (test.isTest()) {
      executed.add(test.getUniqueId());
    }
  }

  synchronized void skipped(TestIdentifier test) {
    skipped.add(test.getUniqueId());
  }

  synchronized void finished(TestIdentifier test, TestExecutionResult result) {
    TestExecutionResult.Status status = result.getStatus();
    if (test.isTest()) {
      if (status == TestExecutionResult.Status.FAILED
          && result.getThrowable().map(QueryAuditExtension::isAuditPolicyFailure).orElse(false)) {
        // A completed audit that rejects a policy is FAIL, not missing test evidence.
        status = TestExecutionResult.Status.SUCCESSFUL;
      }
      results.put(test.getUniqueId(), status);
    } else if (status != TestExecutionResult.Status.SUCCESSFUL) {
      failedContainers.add(test.getUniqueId());
    }
  }

  synchronized void audited(QueryAuditReport report) {
    int retentionLimit = HtmlReportAggregator.getInstance().getMaxInMemoryReports();
    QueryAuditReport retained =
        reports.size() >= retentionLimit ? report.withoutQueryEvidence() : report;
    reports.put(report.getTestId(), retained);
  }

  synchronized List<QueryAuditReport> reports() {
    return List.copyOf(reports.values());
  }

  synchronized AuditRunResult complete(AuditRunResult result) {
    finalized = true;
    List<AuditIncompleteReason> reasons = new ArrayList<>(result.incompleteReasons());
    reasons.addAll(manifestFailures);
    return AuditRunResult.determine(
            result.reports(),
            result.outcome() == io.queryaudit.core.model.AuditOutcome.FAIL,
            reasons)
        .withCoverage(coverage());
  }

  private AuditCoverage coverage() {
    Set<String> identities = new TreeSet<>(expected);
    identities.addAll(reports.keySet());
    List<AuditCoverage.Test> tests = new ArrayList<>();
    for (String identity : identities) {
      boolean wasExpected = expected.contains(identity);
      boolean wasAudited = reports.containsKey(identity);
      tests.add(
          new AuditCoverage.Test(
              identity,
              wasExpected,
              executed.contains(identity),
              wasAudited,
              wasExpected ? gap(identity, wasAudited) : null));
    }
    return new AuditCoverage(tests);
  }

  private Gap gap(String identity, boolean audited) {
    if (matchesParent(skipped, identity)) {
      return Gap.SKIPPED;
    }
    if (!executed.contains(identity)) {
      if (matchesParent(failedContainers, identity)) {
        return Gap.SETUP_FAILED;
      }
      return discovered.contains(identity) ? Gap.NOT_EXECUTED : Gap.NOT_DISCOVERED;
    }
    TestExecutionResult.Status result = results.get(identity);
    if (result == TestExecutionResult.Status.ABORTED) {
      return Gap.ABORTED;
    }
    if (result == TestExecutionResult.Status.FAILED) {
      return Gap.TEST_FAILED;
    }
    return audited ? null : Gap.AUDIT_MISSING;
  }

  private static boolean matchesParent(Set<String> parents, String identity) {
    return parents.stream()
        .anyMatch(parent -> identity.equals(parent) || identity.startsWith(parent + "/"));
  }

  synchronized void finishWithoutExtension() {
    if (finalized) {
      return;
    }
    AuditRunResult result = complete(AuditRunResult.pass(List.of()));
    Path outputDirectory = QueryAuditExtension.coverageReportOutputDirectory();
    try {
      new QueryAuditExtension().writeJsonReport(result, outputDirectory);
    } catch (IOException e) {
      throw new UncheckedIOException("Could not write the audit coverage report", e);
    }
    QueryAuditExtension.ReportFinalizer.printSummary(result);
  }
}
