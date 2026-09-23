package io.queryaudit.core.extension;

import io.queryaudit.core.extension.internal.AuditRuleRuntime;
import io.queryaudit.core.model.Finding;
import java.util.List;

/**
 * Dependency-free contract checks for extension authors, usable from any test framework. These
 * checks exercise descriptor stability, declared kinds, and result validity. They do not prove SQL
 * correctness, thread safety, privacy, or that all hidden inputs have been declared.
 */
public final class AuditRuleTestKit {
  private AuditRuleTestKit() {}

  /** Evaluates once and returns an immutable result, or throws on a contract violation. */
  public static List<Finding> verify(AuditRule rule, RuleContext context) {
    RuleDescriptor descriptor = AuditRuleRuntime.validateDescriptor(rule);
    return AuditRuleRuntime.evaluate(rule, descriptor, context);
  }

  /** Evaluates twice against the same evidence and requires identical ordered findings. */
  public static List<Finding> verifyDeterministic(AuditRule rule, RuleContext context) {
    RuleDescriptor descriptor = AuditRuleRuntime.validateDescriptor(rule);
    List<Finding> first = AuditRuleRuntime.evaluate(rule, descriptor, context);
    List<Finding> second = AuditRuleRuntime.evaluate(rule, descriptor, context);
    if (!first.equals(second)) {
      throw new IllegalArgumentException(
          "Audit rule is not deterministic for the supplied evidence: " + descriptor.id());
    }
    return first;
  }
}
