package io.queryaudit.core.model;

import java.util.Objects;

/**
 * One structured reason why a QueryAudit run could not produce a trustworthy verdict.
 *
 * @param code stable code for machine consumers
 * @param detail optional human-readable context
 * @since 0.6.0
 */
public record AuditIncompleteReason(IncompleteReasonCode code, String detail) {

  public AuditIncompleteReason {
    Objects.requireNonNull(code, "code");
    if (detail != null && detail.isBlank()) {
      detail = null;
    }
  }

  /** Creates a reason without additional context. */
  public static AuditIncompleteReason of(IncompleteReasonCode code) {
    return new AuditIncompleteReason(code, null);
  }
}
