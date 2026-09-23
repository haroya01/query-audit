package io.queryaudit.core.model;

import java.util.List;
import java.util.stream.Stream;

/**
 * The complete, immutable finding view for new report consumers. Every category includes built-in
 * and application-defined kinds; callers never need to choose between Issue and Finding lists.
 */
public record AuditFindings(
    List<Finding> confirmed, List<Finding> informational, List<Finding> acknowledged) {
  public AuditFindings {
    confirmed = List.copyOf(confirmed);
    informational = List.copyOf(informational);
    acknowledged = List.copyOf(acknowledged);
  }

  public static AuditFindings empty() {
    return new AuditFindings(List.of(), List.of(), List.of());
  }

  public List<Finding> errors() {
    return confirmed.stream().filter(finding -> finding.severity() == Severity.ERROR).toList();
  }

  public List<Finding> warnings() {
    return confirmed.stream().filter(finding -> finding.severity() == Severity.WARNING).toList();
  }

  public List<Finding> all() {
    return Stream.of(confirmed, informational, acknowledged).flatMap(List::stream).toList();
  }

  public boolean hasConfirmed() {
    return !confirmed.isEmpty();
  }

  public boolean isEmpty() {
    return confirmed.isEmpty() && informational.isEmpty() && acknowledged.isEmpty();
  }
}
