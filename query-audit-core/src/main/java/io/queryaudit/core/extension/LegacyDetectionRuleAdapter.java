package io.queryaudit.core.extension;

import io.queryaudit.core.detector.DetectionRule;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.Issue;
import java.util.List;
import java.util.Objects;

/**
 * Adapts an existing detector to the open interface without changing its built-in policy codes. The
 * supplied descriptor must declare every built-in kind the detector may emit. It must not invent
 * custom kinds. Adaptation does not grant trust or replace the analyzer's built-in rules; do not
 * also register the delegate through another path.
 */
public final class LegacyDetectionRuleAdapter implements AuditRule {
  private final RuleDescriptor descriptor;
  private final DetectionRule delegate;

  public LegacyDetectionRuleAdapter(RuleDescriptor descriptor, DetectionRule delegate) {
    this.descriptor = Objects.requireNonNull(descriptor, "descriptor");
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    if (descriptor.findingKinds().stream().anyMatch(kind -> !kind.isBuiltin())) {
      throw new IllegalArgumentException(
          "A legacy adapter may declare only built-in finding kinds");
    }
  }

  @Override
  public RuleDescriptor descriptor() {
    return descriptor;
  }

  /** The existing implementation, exposed for host registration conflict diagnostics. */
  public DetectionRule delegate() {
    return delegate;
  }

  @Override
  public List<Finding> evaluate(RuleContext context) {
    List<Issue> issues =
        Objects.requireNonNull(
            delegate.evaluate(context.queries(), context.indexMetadata()),
            "Detection rule returned null");
    return issues.stream().map(Finding::fromIssue).toList();
  }
}
