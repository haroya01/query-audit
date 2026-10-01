package io.queryaudit.junit5;

import io.queryaudit.core.interceptor.QueryCaptureSnapshot;
import io.queryaudit.core.model.Finding;
import java.util.List;

/**
 * Applies direct assertions after evidence was retained, and records policy failures exactly here.
 */
final class AuditTestAssertions {
  private final AuditSettingsResolver settings;
  private final QueryCountPolicies counts;

  AuditTestAssertions(AuditSettingsResolver settings, QueryCountPolicies counts) {
    this.settings = settings;
    this.counts = counts;
  }

  void verify(AuditScope scope, AnalyzedAudit result) {
    if (result.capture().truncated()) {
      throw new AuditPolicyViolation(
          buildTruncatedCaptureMessage(
              result.testName(), scope.interceptor().getMaxQueries(), result.capture()));
    }
    try {
      var method = scope.context().getRequiredTestMethod();
      requireSatisfied(
          AuditAssertions.maxQueryCountFailure(
              method.getAnnotation(ExpectMaxQueryCount.class),
              result.queries(),
              result.testName()));
      ExpectQueries inlineBudget = method.getAnnotation(ExpectQueries.class);
      if (inlineBudget != null) {
        requireSatisfied(
            AuditAssertions.buildExpectQueriesFailureMessage(
                inlineBudget, result.queries(), result.testName()));
      }
      requireSatisfied(
          counts.contractFailure(
              scope,
              result.queries(),
              result.identity().testId(),
              result.testClass(),
              result.testName()));
      requireSatisfied(
          AuditAssertions.nPlusOneFailure(
              settings.findDetectNPlusOne(scope.context()), result.report(), result.testName()));
      if (result.config().isFailOnDetection() && result.report().getFindings().hasConfirmed()) {
        List<Finding> failing =
            AuditAssertions.failableFindings(
                result.report(), settings.findAnnotation(scope.context()));
        if (!failing.isEmpty())
          requireSatisfied(AuditAssertions.findingsFailureMessage(result.testName(), failing));
      }
    } catch (AuditPolicyViolation failure) {
      scope.runState().markPolicyFailed();
      throw failure;
    }
  }

  private static void requireSatisfied(String message) {
    if (message != null) throw new AuditPolicyViolation(message);
  }

  private static String buildTruncatedCaptureMessage(
      String testName, int maxQueries, QueryCaptureSnapshot capture) {
    return "QueryAudit: "
        + testName
        + " exceeded the query capture limit (maxQueries="
        + maxQueries
        + "). The audit is incomplete. Retained query count: "
        + capture.queries().size()
        + "; dropped query count: "
        + capture.droppedCount()
        + ". Increase query-audit.max-queries or reduce the test's query volume.";
  }
}
