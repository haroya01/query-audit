package io.queryaudit.core.provenance;

/**
 * One incompatible or unavailable input, without exposing underlying policy contents.
 *
 * @since 0.6.0
 */
public record ComparisonInputDifference(
    String testId, String field, String baseline, String candidate) {
  /** Why this difference blocks comparison, independently of JSON presentation. */
  public enum Kind {
    CHANGED,
    UNAVAILABLE
  }

  public Kind kind() {
    if ("comparisonInputs".equals(field)) {
      return Kind.UNAVAILABLE;
    }
    if ("detectorInputsComplete".equals(field) || field.endsWith(".inputsComplete")) {
      return !"true".equals(baseline) || !"true".equals(candidate)
          ? Kind.UNAVAILABLE
          : Kind.CHANGED;
    }
    return field.endsWith(".state") && ("FAILED".equals(baseline) || "FAILED".equals(candidate))
        ? Kind.UNAVAILABLE
        : Kind.CHANGED;
  }
}
