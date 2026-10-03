package io.queryaudit.core.reporter;

import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.model.AuditCoverage;
import io.queryaudit.core.model.AuditIncompleteReason;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.provenance.ComparisonInputs;
import io.queryaudit.core.reporter.ComparisonEnvelope.ReportedFinding;
import io.queryaudit.core.reporter.ComparisonEnvelope.SchemaVersion;
import io.queryaudit.core.reporter.ComparisonEnvelope.TestReport;
import io.queryaudit.core.reporter.ReportComparator.TestRef;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Parses and validates report schema evolution once, then discards the untyped JSON tree. */
final class ComparisonEnvelopeReader {
  private static final int SUPPORTED_SCHEMA_MAJOR = 1;
  private static final int FIRST_OUTCOME_SCHEMA_MINOR = 1;
  private static final int FIRST_STABLE_IDENTITY_SCHEMA_MINOR = 2;
  private static final Pattern SCHEMA_VERSION_PATTERN = Pattern.compile("^(\\d+)\\.(\\d+)\\.\\d+$");
  private static final Pattern FINDING_ID_PATTERN =
      Pattern.compile(Pattern.quote(FindingId.PREFIX) + "[0-9a-f]{64}");

  private ComparisonEnvelopeReader() {}

  private record LegacyRef(String testClass, String testName) {}

  static ComparisonEnvelope read(String envelopeJson) {
    Object root = MiniJsonParser.parse(envelopeJson);
    if (!(root instanceof Map<?, ?> envelope)) {
      throw invalidEnvelope("expected a JSON object");
    }
    SchemaVersion schemaVersion = requireSupportedSchemaVersion(envelope);
    ReportRedaction redaction = requireRedaction(envelope, schemaVersion);
    boolean stableIdentityRequired = schemaVersion.minor() >= FIRST_STABLE_IDENTITY_SCHEMA_MINOR;
    if (!(envelope.get("reports") instanceof List<?> entries)) {
      throw invalidEnvelope("reports must be an array");
    }

    List<TestReport> reports = new ArrayList<>(entries.size());
    for (int i = 0; i < entries.size(); i++) {
      Object entry = entries.get(i);
      if (!(entry instanceof Map<?, ?> report)) {
        throw invalidEnvelope("reports[" + i + "] must be an object");
      }
      reports.add(readReport(report, i, stableIdentityRequired, schemaVersion.hasFindingIds()));
    }
    validateUniqueIdentities(reports);
    AuditCoverage coverage = CoverageJson.read(envelope, schemaVersion.minor() >= 5);
    Map<String, ComparisonInputs> comparisonInputs =
        ComparisonInputsJson.read(envelope, schemaVersion.minor() >= 6);

    if (schemaVersion.minor() < FIRST_OUTCOME_SCHEMA_MINOR) {
      return new ComparisonEnvelope(
          AuditOutcome.INCONCLUSIVE,
          List.of(
              new AuditIncompleteReason(
                  IncompleteReasonCode.UNSUPPORTED_SCHEMA,
                  "schemaVersion " + schemaVersion.text() + " does not declare a run outcome")),
          reports,
          redaction,
          coverage,
          comparisonInputs,
          schemaVersion);
    }

    AuditOutcome outcome;
    List<AuditIncompleteReason> incompleteReasons;
    try {
      outcome = requireOutcome(envelope);
      incompleteReasons = requireIncompleteReasons(envelope);
    } catch (UnsupportedReportSchemaException e) {
      return new ComparisonEnvelope(
          AuditOutcome.INCONCLUSIVE,
          List.of(
              new AuditIncompleteReason(IncompleteReasonCode.UNSUPPORTED_SCHEMA, e.getMessage())),
          reports,
          redaction,
          coverage,
          comparisonInputs,
          schemaVersion);
    }
    try {
      AuditRunResult validated =
          new AuditRunResult(List.of(), outcome, incompleteReasons, null, comparisonInputs, List.of());
      if (coverage != null
          && coverage.failedToAudit() > 0
          && outcome != AuditOutcome.INCONCLUSIVE) {
        throw invalidEnvelope("coverage gaps require an INCONCLUSIVE outcome");
      }
      return new ComparisonEnvelope(
          validated.outcome(),
          validated.incompleteReasons(),
          reports,
          redaction,
          coverage,
          comparisonInputs,
          schemaVersion);
    } catch (IllegalArgumentException e) {
      throw invalidEnvelope("outcome and incompleteReasons are inconsistent: " + e.getMessage());
    }
  }

  private static ReportRedaction requireRedaction(Map<?, ?> envelope, SchemaVersion version) {
    if (!envelope.containsKey("redaction") && version.minor() < 4) {
      return ReportRedaction.FULL;
    }
    Object value = requireField(envelope, "redaction", "envelope");
    if (!(value instanceof String mode)) {
      throw invalidEnvelope("envelope.redaction must be a string");
    }
    try {
      return ReportRedaction.valueOf(mode);
    } catch (IllegalArgumentException e) {
      throw unsupportedEvolution("unknown report redaction mode");
    }
  }

  private static AuditOutcome requireOutcome(Map<?, ?> envelope) {
    Object value = requireField(envelope, "outcome", "envelope");
    if (!(value instanceof String outcome)) {
      throw invalidEnvelope("envelope.outcome must be a string");
    }
    try {
      return AuditOutcome.valueOf(outcome);
    } catch (IllegalArgumentException e) {
      throw unsupportedEvolution("unknown outcome '" + outcome + "'");
    }
  }

  private static List<AuditIncompleteReason> requireIncompleteReasons(Map<?, ?> envelope) {
    List<?> entries = requireArray(envelope, "incompleteReasons", "envelope");
    List<AuditIncompleteReason> reasons = new ArrayList<>(entries.size());
    for (int i = 0; i < entries.size(); i++) {
      String path = "envelope.incompleteReasons[" + i + "]";
      if (!(entries.get(i) instanceof Map<?, ?> reason)) {
        throw invalidEnvelope(path + " must be an object");
      }
      Object codeValue = requireField(reason, "code", path);
      if (!(codeValue instanceof String code)) {
        throw invalidEnvelope(path + ".code must be a string");
      }
      IncompleteReasonCode reasonCode;
      try {
        reasonCode = IncompleteReasonCode.valueOf(code);
      } catch (IllegalArgumentException e) {
        throw unsupportedEvolution("unknown incomplete reason '" + code + "'");
      }
      Object detailValue = requireField(reason, "detail", path);
      if (detailValue != null && !(detailValue instanceof String)) {
        throw invalidEnvelope(path + ".detail must be a string or null");
      }
      reasons.add(new AuditIncompleteReason(reasonCode, (String) detailValue));
    }
    return reasons;
  }

  private static TestReport readReport(
      Map<?, ?> report,
      int reportIndex,
      boolean stableIdentityRequired,
      boolean findingIdsRequired) {
    String path = "reports[" + reportIndex + "]";
    if (stableIdentityRequired) {
      requireNonBlankString(report, "testId", path);
      requireTestSelector(report, path);
    } else if (report.containsKey("testId")) {
      requireNonBlankString(report, "testId", path);
      if (report.containsKey("testSelector")) {
        requireTestSelector(report, path);
      }
    }
    requireNullableString(report, "testClass", path);
    requireString(report, "testName", path);
    Map<?, ?> summary = requireObject(report, "summary", path);
    requireInteger(summary, "totalQueries", path + ".summary");
    requireInteger(summary, "executionTimeMs", path + ".summary");
    Set<String> findingIds = new LinkedHashSet<>();
    List<ReportedFinding> confirmed =
        readFindings(report, "confirmedIssues", path, findingIdsRequired, findingIds);
    List<ReportedFinding> info =
        readFindings(report, "infoIssues", path, findingIdsRequired, findingIds);
    List<ReportedFinding> acknowledged =
        readFindings(report, "acknowledgedIssues", path, findingIdsRequired, findingIds);
    String testId = optionalString(report, "testId");
    return new TestReport(
        testId,
        new TestRef(testId, (String) report.get("testClass"), (String) report.get("testName")),
        (Long) summary.get("totalQueries"),
        (Long) summary.get("executionTimeMs"),
        confirmed,
        info,
        acknowledged);
  }

  private static List<ReportedFinding> readFindings(
      Map<?, ?> report,
      String field,
      String path,
      boolean findingIdsRequired,
      Set<String> findingIds) {
    if (!findingIdsRequired && !field.equals("confirmedIssues") && !report.containsKey(field)) {
      return List.of();
    }
    List<?> issues = requireArray(report, field, path);
    List<ReportedFinding> findings = new ArrayList<>(issues.size());
    for (int index = 0; index < issues.size(); index++) {
      String findingPath = path + "." + field + "[" + index + "]";
      if (!(issues.get(index) instanceof Map<?, ?> finding)) {
        throw invalidEnvelope(findingPath + " must be an object");
      }
      requireString(finding, "type", findingPath);
      requireNullableString(finding, "query", findingPath);
      requireNullableString(finding, "sourceLocation", findingPath);
      requireNullableString(finding, "table", findingPath);
      requireNullableString(finding, "detail", findingPath);
      if (findingIdsRequired || finding.containsKey("column")) {
        requireNullableString(finding, "column", findingPath);
      }
      String findingId = findingIdsRequired ? requireFindingId(finding, findingPath) : null;
      if (findingIdsRequired && !findingIds.add(findingId)) {
        throw invalidEnvelope(findingPath + ".findingId duplicates another finding in this test");
      }
      findings.add(
          new ReportedFinding(
              findingId,
              (String) finding.get("type"),
              (String) finding.get("query"),
              (String) finding.get("sourceLocation"),
              (String) finding.get("table"),
              (String) finding.get("column"),
              (String) finding.get("detail")));
    }
    return List.copyOf(findings);
  }

  private static String requireFindingId(Map<?, ?> finding, String path) {
    requireString(finding, "findingId", path);
    String findingId = (String) finding.get("findingId");
    if (FINDING_ID_PATTERN.matcher(findingId).matches()) {
      return findingId;
    }
    if (!findingId.startsWith(FindingId.PREFIX) && findingId.matches("qa-finding-v[0-9]+:.*")) {
      throw unsupportedEvolution("unsupported findingId algorithm");
    }
    throw invalidEnvelope(
        path + ".findingId must use qa-finding-v1: followed by 64 lowercase hex digits");
  }

  private static Map<?, ?> requireObject(Map<?, ?> object, String field, String path) {
    Object value = requireField(object, field, path);
    if (value instanceof Map<?, ?> map) {
      return map;
    }
    throw invalidEnvelope(path + "." + field + " must be an object");
  }

  private static List<?> requireArray(Map<?, ?> object, String field, String path) {
    Object value = requireField(object, field, path);
    if (value instanceof List<?> list) {
      return list;
    }
    throw invalidEnvelope(path + "." + field + " must be an array");
  }

  private static void requireString(Map<?, ?> object, String field, String path) {
    Object value = requireField(object, field, path);
    if (!(value instanceof String)) {
      throw invalidEnvelope(path + "." + field + " must be a string");
    }
  }

  private static void requireNonBlankString(Map<?, ?> object, String field, String path) {
    Object value = requireField(object, field, path);
    if (!(value instanceof String text) || text.isBlank()) {
      throw invalidEnvelope(path + "." + field + " must be a non-blank string");
    }
  }

  private static void requireTestSelector(Map<?, ?> report, String path) {
    Object value = requireField(report, "testSelector", path);
    if (value == null) {
      return;
    }
    if (!(value instanceof Map<?, ?> selector)) {
      throw invalidEnvelope(path + ".testSelector must be an object or null");
    }
    requireNonBlankString(selector, "type", path + ".testSelector");
    requireNonBlankString(selector, "value", path + ".testSelector");
  }

  private static String optionalString(Map<?, ?> object, String field) {
    Object value = object.get(field);
    return value instanceof String text ? text : null;
  }

  private static void requireNullableString(Map<?, ?> object, String field, String path) {
    Object value = requireField(object, field, path);
    if (value != null && !(value instanceof String)) {
      throw invalidEnvelope(path + "." + field + " must be a string or null");
    }
  }

  private static void requireInteger(Map<?, ?> object, String field, String path) {
    Object value = requireField(object, field, path);
    if (!(value instanceof Long)) {
      throw invalidEnvelope(path + "." + field + " must be an integer");
    }
  }

  private static Object requireField(Map<?, ?> object, String field, String path) {
    if (!object.containsKey(field)) {
      throw invalidEnvelope(path + "." + field + " is required");
    }
    return object.get(field);
  }

  private static SchemaVersion requireSupportedSchemaVersion(Map<?, ?> envelope) {
    Object value = envelope.get("schemaVersion");
    if (!(value instanceof String schemaVersion)) {
      throw invalidEnvelope("schemaVersion is required and must be a string");
    }

    Matcher version = SCHEMA_VERSION_PATTERN.matcher(schemaVersion);
    if (!version.matches()) {
      throw invalidEnvelope("invalid schemaVersion; expected major.minor.patch");
    }
    int major = Integer.parseInt(version.group(1));
    int minor = Integer.parseInt(version.group(2));
    if (major != SUPPORTED_SCHEMA_MAJOR) {
      throw new UnsupportedReportSchemaException(
          "unsupported schemaVersion "
              + schemaVersion
              + "; this comparator supports "
              + SUPPORTED_SCHEMA_MAJOR
              + ".x");
    }
    return new SchemaVersion(major, minor, schemaVersion);
  }

  private static IllegalArgumentException invalidEnvelope(String reason) {
    return new IllegalArgumentException(
        "not a supported report.json envelope — " + reason + " (schema 1.0+ envelope)");
  }

  private static UnsupportedReportSchemaException unsupportedEvolution(String reason) {
    return new UnsupportedReportSchemaException(
        reason + "; update QueryAudit to read this report schema safely");
  }

  static final class UnsupportedReportSchemaException extends IllegalArgumentException {

    UnsupportedReportSchemaException(String message) {
      super(message);
    }
  }

  private static void validateUniqueIdentities(List<TestReport> reports) {
    Set<String> stableIds = new LinkedHashSet<>();
    Set<LegacyRef> legacyIds = new LinkedHashSet<>();
    for (TestReport report : reports) {
      if (report.hasStableId()) {
        if (!stableIds.add(report.testId())) {
          throw invalidEnvelope("duplicate testId " + report.testId());
        }
      } else if (!legacyIds.add(new LegacyRef(report.ref().testClass(), report.ref().testName()))) {
        throw invalidEnvelope(
            "legacy test identity is ambiguous for "
                + report.ref().testClass()
                + "."
                + report.ref().testName()
                + "; regenerate the 0.5 report with QueryAudit 0.6+");
      }
    }
  }
}
