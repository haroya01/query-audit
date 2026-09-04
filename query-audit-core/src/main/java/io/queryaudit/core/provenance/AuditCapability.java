package io.queryaudit.core.provenance;

import java.util.Objects;

/**
 * The source and initialization result of one optional analysis capability.
 *
 * @since 0.6.0
 */
public record AuditCapability(State state, String source, boolean inputsComplete) {
  public enum State {
    AVAILABLE,
    ABSENT,
    FAILED
  }

  public AuditCapability {
    Objects.requireNonNull(state, "state");
    Objects.requireNonNull(source, "source");
    if (source.isBlank() || source.trim().equalsIgnoreCase("unknown")) {
      throw new IllegalArgumentException("Capability source must not be blank");
    }
    if (state == State.ABSENT && !source.equals("none")) {
      throw new IllegalArgumentException("An absent capability must use source 'none'");
    }
    if (state != State.ABSENT && source.equals("none")) {
      throw new IllegalArgumentException("An available or failed capability must name its source");
    }
  }

  public static AuditCapability available(String source) {
    return available(source, true);
  }

  public static AuditCapability available(String source, boolean inputsComplete) {
    return new AuditCapability(State.AVAILABLE, source, inputsComplete);
  }

  public static AuditCapability absent() {
    return new AuditCapability(State.ABSENT, "none", true);
  }

  public static AuditCapability failed(String source) {
    return new AuditCapability(State.FAILED, source, false);
  }
}
