package io.queryaudit.core.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Immutable run-level result written in the {@code report.json} suite envelope.
 *
 * <p>Incomplete reasons take precedence over policy failures: a run with both remains {@link
 * AuditOutcome#INCONCLUSIVE}. A completed run is {@link AuditOutcome#FAIL} when a configured check
 * failed and {@link AuditOutcome#PASS} otherwise.
 *
 * @since 0.6.0
 */
public record AuditRunResult(
    List<QueryAuditReport> reports,
    AuditOutcome outcome,
    List<AuditIncompleteReason> incompleteReasons) {

  public AuditRunResult {
    Objects.requireNonNull(reports, "reports");
    Objects.requireNonNull(outcome, "outcome");
    Objects.requireNonNull(incompleteReasons, "incompleteReasons");

    reports = List.copyOf(reports);
    List<AuditIncompleteReason> copiedReasons = new ArrayList<>();
    for (AuditIncompleteReason reason : incompleteReasons) {
      AuditIncompleteReason nonNullReason =
          Objects.requireNonNull(reason, "incompleteReasons must not contain null");
      if (!copiedReasons.contains(nonNullReason)) {
        copiedReasons.add(nonNullReason);
      }
    }
    incompleteReasons = List.copyOf(copiedReasons);

    if (outcome == AuditOutcome.INCONCLUSIVE && incompleteReasons.isEmpty()) {
      throw new IllegalArgumentException("INCONCLUSIVE requires at least one incomplete reason");
    }
    if (outcome != AuditOutcome.INCONCLUSIVE && !incompleteReasons.isEmpty()) {
      throw new IllegalArgumentException(outcome + " must not carry incomplete reasons");
    }
  }

  /** Returns a completed result with no policy or contract failures. */
  public static AuditRunResult pass(List<QueryAuditReport> reports) {
    return new AuditRunResult(reports, AuditOutcome.PASS, List.of());
  }

  /** Returns a completed result with at least one policy or contract failure. */
  public static AuditRunResult fail(List<QueryAuditReport> reports) {
    return new AuditRunResult(reports, AuditOutcome.FAIL, List.of());
  }

  /** Returns an incomplete result for one or more structured reasons. */
  public static AuditRunResult inconclusive(
      List<QueryAuditReport> reports,
      AuditIncompleteReason firstReason,
      AuditIncompleteReason... additionalReasons) {
    Objects.requireNonNull(firstReason, "firstReason");
    List<AuditIncompleteReason> reasons = new ArrayList<>();
    reasons.add(firstReason);
    Objects.requireNonNull(additionalReasons, "additionalReasons");
    for (AuditIncompleteReason reason : additionalReasons) {
      reasons.add(Objects.requireNonNull(reason, "additionalReasons must not contain null"));
    }
    return new AuditRunResult(reports, AuditOutcome.INCONCLUSIVE, reasons);
  }

  /**
   * Applies the suite precedence rule: incomplete reasons, then policy failure, then pass.
   *
   * @param policyFailed whether a configured policy or contract failed
   * @param incompleteReasons reasons that prevented a trustworthy verdict
   */
  public static AuditRunResult determine(
      List<QueryAuditReport> reports,
      boolean policyFailed,
      Collection<AuditIncompleteReason> incompleteReasons) {
    Objects.requireNonNull(incompleteReasons, "incompleteReasons");
    if (!incompleteReasons.isEmpty()) {
      return new AuditRunResult(
          reports, AuditOutcome.INCONCLUSIVE, new ArrayList<>(incompleteReasons));
    }
    return policyFailed ? fail(reports) : pass(reports);
  }

  /** Returns whether the audit completed with a trustworthy verdict. */
  public boolean isComplete() {
    return outcome != AuditOutcome.INCONCLUSIVE;
  }
}
