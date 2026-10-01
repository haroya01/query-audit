package io.queryaudit.core.detector;

import io.queryaudit.core.baseline.Baseline;
import io.queryaudit.core.baseline.BaselineEntry;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.Severity;
import java.util.List;

/** One policy path for legacy issues and open findings. Kept host-owned and non-extensible. */
final class FindingPolicy {
  enum Bucket {
    CONFIRMED,
    INFO,
    ACKNOWLEDGED
  }

  record Classified(Finding finding, Bucket bucket) {}

  private final QueryAuditConfig config;
  private final List<BaselineEntry> baseline;

  FindingPolicy(QueryAuditConfig config, List<BaselineEntry> baseline) {
    this.config = config;
    this.baseline = baseline;
  }

  Classified classify(Finding finding) {
    String code = finding.kindId().value();
    if (config.isRuleExcluded(code)
        || config.isSuppressed(code, finding.table(), finding.column())) {
      return null;
    }
    Finding effective = finding.withSeverity(config.getEffectiveSeverity(code, finding.severity()));
    Bucket bucket =
        Baseline.isFindingAcknowledged(baseline, effective)
            ? Bucket.ACKNOWLEDGED
            : effective.severity() == Severity.INFO ? Bucket.INFO : Bucket.CONFIRMED;
    return new Classified(effective, bucket);
  }
}
