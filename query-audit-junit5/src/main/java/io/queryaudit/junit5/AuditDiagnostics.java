package io.queryaudit.junit5;

import io.queryaudit.core.model.AuditIncompleteReason;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.provenance.AuditCapability;

/** Records incomplete analysis without conflating it with a completed policy rejection. */
final class AuditDiagnostics {
  private AuditDiagnostics() {}

  static void incomplete(AuditScope scope, IncompleteReasonCode code, String detail) {
    scope.runState().markIncomplete(new AuditIncompleteReason(code, detail));
  }

  static void initializationFailure(AuditScope scope, IncompleteReasonCode code, String detail) {
    scope.flag(AuditScope.Flag.INITIALIZATION_FAILED, true);
    incomplete(scope, code, detail);
  }

  static boolean initializationFailed(AuditScope scope) {
    return Boolean.TRUE.equals(scope.flag(AuditScope.Flag.INITIALIZATION_FAILED));
  }

  static void unexpectedInitializationFailure(AuditScope scope, Throwable failure) {
    if (!initializationFailed(scope)) {
      initializationFailure(
          scope,
          IncompleteReasonCode.AUDIT_INITIALIZATION_FAILED,
          "Could not initialize QueryAudit for " + scope.target() + ": " + failureMessage(failure));
    }
  }

  static void capabilityFailure(
      AuditScope scope, AuditCapability capability, IncompleteReasonCode code) {
    if (capability.state() == AuditCapability.State.FAILED) {
      incomplete(scope, code, "Capability failed: " + capability.source());
    }
  }

  static String failureMessage(Throwable failure) {
    String message = failure.getMessage();
    return message == null || message.isBlank() ? failure.getClass().getSimpleName() : message;
  }
}
