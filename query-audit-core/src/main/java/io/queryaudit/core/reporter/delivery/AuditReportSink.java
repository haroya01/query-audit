package io.queryaudit.core.reporter.delivery;

/** Receives one sanitized, immutable run summary. The host owns registration and failure policy. */
@FunctionalInterface
public interface AuditReportSink {
  /**
   * Publishes a run summary. Implementations must be safe for their host's execution lifecycle.
   * Throwing an exception records a delivery failure; it cannot change the analysis verdict.
   */
  void publish(PublishedAuditRun run) throws Exception;
}
