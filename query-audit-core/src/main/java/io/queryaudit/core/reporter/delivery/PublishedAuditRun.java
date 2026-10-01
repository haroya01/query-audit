package io.queryaudit.core.reporter.delivery;

import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.reporter.FindingId;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;

/**
 * A deliberately restricted publication view, not the legacy report.json envelope.
 *
 * <p>Only host-generated identities, validated finding kinds, enums and counts are exported. SQL,
 * database identifiers, test display names, selectors, source locations, free-form finding text,
 * configuration and exception details are absent, even when local reports use FULL detail. Hashed
 * identities permit correlation; this is a data-minimization boundary, not an anonymity guarantee.
 * A sink never receives the source report or mutable analysis state.
 */
public final class PublishedAuditRun {
  public static final String SCHEMA_VERSION = "1.0.0";
  private final AuditOutcome outcome;
  private final String json;

  private PublishedAuditRun(AuditOutcome outcome, String json) {
    this.outcome = outcome;
    this.json = json;
  }

  /** Constructs the same restricted view regardless of the local artifact redaction setting. */
  public static PublishedAuditRun from(AuditRunResult run) {
    Objects.requireNonNull(run, "run");
    StringBuilder json = new StringBuilder();
    json.append("{\"schemaVersion\":\"")
        .append(SCHEMA_VERSION)
        .append("\",\"projection\":\"sanitized-summary\",\"outcome\":\"")
        .append(run.outcome())
        .append("\",\"reportedTests\":")
        .append(run.reports().size())
        .append(",\"totalQueries\":")
        .append(run.reports().stream().mapToLong(QueryAuditReport::getTotalQueryCount).sum())
        .append(",\"incompleteReasonCodes\":[");
    List<String> reasons =
        run.incompleteReasons().stream().map(reason -> reason.code().name()).distinct().toList();
    for (int index = 0; index < reasons.size(); index++) {
      if (index > 0) json.append(',');
      token(json, reasons.get(index));
    }
    json.append("],\"tests\":[");
    for (int index = 0; index < run.reports().size(); index++) {
      if (index > 0) json.append(',');
      QueryAuditReport report = run.reports().get(index);
      json.append("{\"testId\":");
      token(json, testIdentity(report.getTestId()));
      json.append(",\"totalQueries\":")
          .append(report.getTotalQueryCount())
          .append(",\"retainedQueries\":")
          .append(report.getRetainedQueryCount())
          .append(",\"omittedQueries\":")
          .append(report.getOmittedQueryCount());
      var findings = report.getFindings();
      findings(json, "confirmedFindings", report, findings.confirmed());
      findings(json, "informationalFindings", report, findings.informational());
      findings(json, "acknowledgedFindings", report, findings.acknowledged());
      json.append('}');
    }
    json.append("]}");
    return new PublishedAuditRun(run.outcome(), json.toString());
  }

  public AuditOutcome outcome() {
    return outcome;
  }

  /** The immutable JSON summary uses its own version, independent of report.json. */
  public String json() {
    return json;
  }

  private static void findings(
      StringBuilder json, String bucket, QueryAuditReport report, List<Finding> findings) {
    json.append(",\"").append(bucket).append("\":[");
    for (int index = 0; index < findings.size(); index++) {
      if (index > 0) json.append(',');
      Finding finding = findings.get(index);
      json.append("{\"findingId\":");
      token(json, FindingId.of(report.getTestId(), finding));
      json.append(",\"kindId\":");
      token(json, finding.kindId().value());
      json.append(",\"severity\":");
      token(json, finding.severity().name());
      json.append('}');
    }
    json.append(']');
  }

  private static void token(StringBuilder json, String value) {
    if (!value.matches("[A-Za-z0-9][A-Za-z0-9._:/-]*")) {
      throw new IllegalArgumentException(
          "Publication identities must use safe identifier characters");
    }
    json.append('"').append(value).append('"');
  }

  private static String testIdentity(String testId) {
    try {
      return "qa-published-test-v1:"
          + HexFormat.of()
              .formatHex(
                  MessageDigest.getInstance("SHA-256")
                      .digest(testId.getBytes(StandardCharsets.UTF_8)));
    } catch (NoSuchAlgorithmException failure) {
      throw new IllegalStateException("SHA-256 is unavailable", failure);
    }
  }
}
