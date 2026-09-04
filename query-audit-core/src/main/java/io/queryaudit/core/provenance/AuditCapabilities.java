package io.queryaudit.core.provenance;

import java.util.List;
import java.util.Objects;

/**
 * Optional inputs whose presence changes what the audit can detect.
 *
 * @since 0.6.0
 */
public record AuditCapabilities(
    AuditCapability indexMetadata,
    AuditCapability hibernateEvents,
    AuditCapability explain,
    AuditCapability repositoryReturnTypes) {
  public AuditCapabilities {
    Objects.requireNonNull(indexMetadata, "indexMetadata");
    Objects.requireNonNull(hibernateEvents, "hibernateEvents");
    Objects.requireNonNull(explain, "explain");
    Objects.requireNonNull(repositoryReturnTypes, "repositoryReturnTypes");
  }

  public boolean hasFailure() {
    return List.of(indexMetadata, hibernateEvents, explain, repositoryReturnTypes).stream()
        .anyMatch(capability -> capability.state() == AuditCapability.State.FAILED);
  }
}
