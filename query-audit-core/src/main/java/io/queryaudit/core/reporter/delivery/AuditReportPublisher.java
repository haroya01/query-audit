package io.queryaudit.core.reporter.delivery;

import io.queryaudit.core.model.AuditRunResult;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Publishes in registration order, isolating ordinary sink failures from analysis and other sinks.
 */
public final class AuditReportPublisher {
  private final List<ReportSinkRegistration> registrations;

  public AuditReportPublisher(List<ReportSinkRegistration> registrations) {
    this.registrations = List.copyOf(registrations);
    Set<String> ids = new HashSet<>();
    for (ReportSinkRegistration registration : this.registrations) {
      if (!ids.add(registration.id())) {
        throw new IllegalArgumentException("Duplicate sink registration ID: " + registration.id());
      }
    }
  }

  public PublicationResult publish(AuditRunResult run) {
    Objects.requireNonNull(run, "run");
    List<PublicationResult.Delivery> deliveries = new ArrayList<>();
    if (registrations.isEmpty()) {
      return new PublicationResult(run.outcome(), deliveries);
    }
    PublishedAuditRun published = PublishedAuditRun.from(run);
    for (ReportSinkRegistration registration : registrations) {
      PublicationResult.Status status;
      if (Thread.currentThread().isInterrupted()) {
        status = PublicationResult.Status.NOT_ATTEMPTED;
      } else {
        try {
          registration.sink().publish(published);
          status = PublicationResult.Status.DELIVERED;
        } catch (InterruptedException failure) {
          Thread.currentThread().interrupt();
          status = PublicationResult.Status.FAILED;
        } catch (Exception failure) {
          // Sink messages and causes may contain credentials or SQL. Export only the status.
          status = PublicationResult.Status.FAILED;
        }
      }
      deliveries.add(
          new PublicationResult.Delivery(registration.id(), registration.required(), status));
    }
    return new PublicationResult(run.outcome(), deliveries);
  }
}
