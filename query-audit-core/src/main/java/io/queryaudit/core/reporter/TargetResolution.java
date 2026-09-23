package io.queryaudit.core.reporter;

import java.util.Objects;

/** The result of requiring one recorded baseline finding to disappear from its audited test. */
public record TargetResolution(String findingId, String testId, Status status) {

  public TargetResolution {
    Objects.requireNonNull(findingId, "findingId");
    Objects.requireNonNull(status, "status");
  }

  public enum Status {
    RESOLVED,
    PERSISTING,
    INCOMPLETE
  }
}
