package io.queryaudit.core.reporter.delivery;

import io.queryaudit.core.model.AuditOutcome;
import java.util.List;
import java.util.Objects;

/**
 * Delivery status is independent of the verdict established by analysis. No exception data escapes.
 */
public record PublicationResult(AuditOutcome analysisOutcome, List<Delivery> deliveries) {
  public PublicationResult {
    Objects.requireNonNull(analysisOutcome, "analysisOutcome");
    deliveries = List.copyOf(deliveries);
  }

  /** Includes required deliveries not attempted after interruption. */
  public boolean hasRequiredFailures() {
    return deliveries.stream()
        .anyMatch(delivery -> delivery.required() && delivery.status() != Status.DELIVERED);
  }

  /** Optional diagnostics do not fail analysis or required publication. */
  public boolean hasOptionalFailures() {
    return deliveries.stream()
        .anyMatch(delivery -> !delivery.required() && delivery.status() != Status.DELIVERED);
  }

  public record Delivery(String registrationId, boolean required, Status status) {
    public Delivery {
      Objects.requireNonNull(registrationId, "registrationId");
      Objects.requireNonNull(status, "status");
    }
  }

  public enum Status {
    DELIVERED,
    FAILED,
    NOT_ATTEMPTED
  }
}
