package io.queryaudit.junit5;

/**
 * Tri-state enum for annotation boolean attributes that need to distinguish between "not specified"
 * (use inherited/default value) and "explicitly set".
 *
 * @since 0.2.0
 */
public enum BooleanOverride {
  /** Use the inherited value (from application.yml or hardcoded default). */
  INHERIT,
  /** Explicitly set to true. */
  TRUE,
  /** Explicitly set to false. */
  FALSE;

  /** Returns whether this override was explicitly specified. */
  public boolean isSpecified() {
    return this != INHERIT;
  }

  /** Returns the boolean value. Throws if called on INHERIT. */
  public boolean toBoolean() {
    if (this == INHERIT) {
      throw new IllegalStateException("Cannot convert INHERIT to boolean");
    }
    return this == TRUE;
  }
}
