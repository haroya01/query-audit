package io.queryaudit.core.extension;

import io.queryaudit.core.model.Finding;
import java.util.List;

/**
 * Framework-independent detection extension. Implementations return findings, not audit verdicts.
 * The descriptor must remain stable throughout the instance's lifetime; evaluate must return a
 * non-null list containing only non-null findings of declared kinds. Hosts may reuse an instance
 * across tests and threads, so implementations must be thread-safe and avoid mutating shared state.
 * Exceptions fail the host analysis; they are never interpreted as an empty, successful result.
 * Enabled rules also receive empty evidence, so they can detect a missing expected query.
 */
public interface AuditRule {
  RuleDescriptor descriptor();

  List<Finding> evaluate(RuleContext context);
}
