package io.queryaudit.core.extension;

/**
 * Stable, namespaced implementation identity, distinct from finding kinds and report finding IDs.
 */
public record RuleId(String value) {
  public RuleId {
    ExtensionIds.requireNamespaced(value, "Rule ID");
  }

  public static RuleId of(String value) {
    return new RuleId(value);
  }

  @Override
  public String toString() {
    return value;
  }
}
