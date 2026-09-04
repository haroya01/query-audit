package io.queryaudit.core.reporter;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.provenance.AuditCapabilities;
import io.queryaudit.core.provenance.AuditCapability;
import io.queryaudit.core.provenance.AuditInputFingerprints;
import io.queryaudit.core.provenance.AuditPolicyInputs;
import io.queryaudit.core.provenance.ComparisonInputs;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Explicit comparison metadata for fixtures that exercise unrelated report behavior. */
final class ComparisonInputFixtures {

  private ComparisonInputFixtures() {}

  static ComparisonInputs defaults() {
    AuditCapability absent = AuditCapability.absent();
    return new ComparisonInputs(
        "0.5.0",
        "recommended",
        "h2",
        "jsqlparser",
        "5.0",
        List.of("test-detector"),
        true,
        new AuditCapabilities(absent, absent, absent, absent),
        AuditInputFingerprints.create(
            QueryAuditConfig.defaults(), List.of(), AuditPolicyInputs.empty()));
  }

  static AuditRunResult withKnownInputs(AuditRunResult run) {
    Map<String, ComparisonInputs> inputs = new LinkedHashMap<>();
    run.reports().forEach(report -> inputs.put(report.getTestId(), defaults()));
    return run.withComparisonInputs(inputs);
  }

  static String json(AuditRunResult run) {
    return json(run, ReportRedaction.REDACTED);
  }

  static String json(AuditRunResult run, ReportRedaction redaction) {
    return JsonReporter.toRunEnvelopeJson(withKnownInputs(run), redaction);
  }
}
