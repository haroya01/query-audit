package example.audit;

import io.queryaudit.core.extension.AuditRule;
import io.queryaudit.core.extension.FindingKindId;
import io.queryaudit.core.extension.RuleContext;
import io.queryaudit.core.extension.RuleDescriptor;
import io.queryaudit.core.extension.RuleId;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.Severity;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/** Public-API-only example corresponding to the extension guide. */
public final class ApplicationBudgetRule implements AuditRule {
  public static final FindingKindId KIND = FindingKindId.of("shop:latency-budget");
  private final long thresholdNanos;
  private final RuleDescriptor descriptor;

  public ApplicationBudgetRule(long milliseconds) {
    if (milliseconds <= 0) throw new IllegalArgumentException("Budget must be positive");
    thresholdNanos = TimeUnit.MILLISECONDS.toNanos(milliseconds);
    descriptor =
        new RuleDescriptor(
            new RuleId("shop:budget-rule"),
            "1",
            Set.of(KIND),
            Map.of("milliseconds", Long.toString(milliseconds)));
  }

  @Override
  public RuleDescriptor descriptor() {
    return descriptor;
  }

  @Override
  public List<Finding> evaluate(RuleContext context) {
    return context.queries().stream()
        .filter(query -> query.executionTimeNanos() > thresholdNanos)
        .map(
            query ->
                new Finding(
                    KIND,
                    Severity.WARNING,
                    query.sql(),
                    null,
                    null,
                    "Application query budget exceeded",
                    "Review this use case's query budget"))
        .toList();
  }
}
