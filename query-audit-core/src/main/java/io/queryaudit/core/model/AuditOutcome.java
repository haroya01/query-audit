package io.queryaudit.core.model;

/**
 * Run-level verdict for a QueryAudit suite.
 *
 * @since 0.6.0
 */
public enum AuditOutcome {
  /** The audit completed and every configured policy and contract passed. */
  PASS,

  /** The audit completed and a configured policy or contract failed. */
  FAIL,

  /** The audit did not collect enough trustworthy information to produce a verdict. */
  INCONCLUSIVE
}
