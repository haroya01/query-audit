package io.queryaudit.junit5;

import io.queryaudit.core.model.TestSelector;
import org.junit.jupiter.api.extension.ExtensionContext;

/** Stable identity and selector supplied by the JUnit Platform. */
record JUnitTestIdentity(String testId, TestSelector selector) {

  static JUnitTestIdentity from(ExtensionContext context) {
    String uniqueId = context.getUniqueId();
    return new JUnitTestIdentity(uniqueId, new TestSelector("junit-unique-id", uniqueId));
  }
}
