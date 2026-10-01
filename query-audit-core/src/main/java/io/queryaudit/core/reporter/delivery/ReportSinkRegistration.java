package io.queryaudit.core.reporter.delivery;

import java.util.Objects;

/** The host assigns a stable registration ID and decides whether publication is required. */
public record ReportSinkRegistration(String id, boolean required, AuditReportSink sink) {
  public ReportSinkRegistration {
    Objects.requireNonNull(id, "id");
    if (!id.matches("[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}")) {
      throw new IllegalArgumentException(
          "Sink registration ID must be a safe identifier of 1 to 200 characters");
    }
    Objects.requireNonNull(sink, "sink");
  }
}
