package io.queryaudit.junit5;

/**
 * A completed audit rejected a test policy; unlike infrastructure errors this is not missing
 * evidence.
 */
final class AuditPolicyViolation extends AssertionError {
  AuditPolicyViolation(String message) {
    super(message);
  }

  static boolean isPurePolicyFailure(Throwable failure) {
    return failure instanceof AuditPolicyViolation && failure.getSuppressed().length == 0;
  }
}
