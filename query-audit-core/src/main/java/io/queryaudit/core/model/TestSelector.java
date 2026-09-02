package io.queryaudit.core.model;

/**
 * A machine-readable selector that can reproduce one test execution.
 *
 * @since 0.6.0
 */
public record TestSelector(String type, String value) {

  public TestSelector {
    if (type == null || type.isBlank()) {
      throw new IllegalArgumentException("selector type must not be blank");
    }
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("selector value must not be blank");
    }
  }
}
