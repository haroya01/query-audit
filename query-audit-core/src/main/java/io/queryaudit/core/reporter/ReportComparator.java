package io.queryaudit.core.reporter;

import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.model.AuditCoverage;
import io.queryaudit.core.model.AuditIncompleteReason;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.IncompleteReasonCode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Compares two {@code report.json} runs into a machine-readable resolution verdict (issue #167).
 *
 * <p>Every fix loop — human or automated — ends with the same question: <em>did my change resolve
 * the finding without introducing new ones?</em> The verdict answers it from the two reports alone:
 * which confirmed findings were resolved, which are new, which persist, and how the query profile
 * moved.
 *
 * <p><strong>Matching key:</strong> {@code testId|type|query|sourceMethod}. The query field holds
 * the normalized statement pattern, and source line numbers are removed before matching, so
 * findings survive display-name and line-only changes while still distinguishing different tests
 * and call sites. Only <em>confirmed</em> findings participate — INFO advisories don't gate fix
 * loops.
 *
 * <p><strong>Exit contract</strong> (CLI): {@code 0} for {@link AuditOutcome#PASS}, {@code 1} for
 * {@link AuditOutcome#FAIL}, and {@code 2} for {@link AuditOutcome#INCONCLUSIVE} or usage/parse
 * errors.
 *
 * @author haroya
 * @since 0.5.0
 */
public final class ReportComparator {

  private static final int SUPPORTED_SCHEMA_MAJOR = 1;
  private static final int FIRST_OUTCOME_SCHEMA_MINOR = 1;
  private static final int FIRST_STABLE_IDENTITY_SCHEMA_MINOR = 2;
  private static final Pattern SCHEMA_VERSION_PATTERN = Pattern.compile("^(\\d+)\\.(\\d+)\\.\\d+$");
  private static final Pattern SOURCE_LINE_NUMBER = Pattern.compile("(?m):-?\\d+$");

  private ReportComparator() {
    // static entry points only
  }

  /** One confirmed finding, reduced to its stable identity, matching key, and display fields. */
  public record Finding(
      String testId,
      String testClass,
      String testName,
      String type,
      String table,
      String detail,
      String key) {

    /** Retains the 0.5 constructor signature for ordinary constructor calls. */
    public Finding(
        String testClass, String testName, String type, String table, String detail, String key) {
      this(null, testClass, testName, type, table, detail, key);
    }
  }

  /** Identifies an audited test using the fields available in the schema 1.x report envelope. */
  public record TestRef(String testId, String testClass, String testName) {

    /** Retains the 0.5 constructor signature for ordinary constructor calls. */
    public TestRef(String testClass, String testName) {
      this(null, testClass, testName);
    }
  }

  private record LegacyRef(String testClass, String testName) {}

  private record ParsedReport(Map<?, ?> value, String testId, TestRef ref) {
    boolean hasStableId() {
      return testId != null;
    }
  }

  private record ComparedFinding(Finding finding, String testIdentity) {}

  private record Envelope(
      AuditOutcome outcome,
      List<AuditIncompleteReason> incompleteReasons,
      List<ParsedReport> reports,
      ReportRedaction redaction,
      AuditCoverage coverage) {}

  /** The comparison result; incomplete comparisons cannot produce a trustworthy success signal. */
  public record Verdict(
      List<Finding> resolved,
      List<Finding> newFindings,
      List<Finding> persisting,
      long queriesBefore,
      long queriesAfter,
      long executionTimeMsBefore,
      long executionTimeMsAfter,
      List<TestRef> missingTests,
      AuditOutcome outcome,
      List<AuditIncompleteReason> incompleteReasons,
      List<TestRef> unexpectedTests) {

    public Verdict {
      resolved = List.copyOf(resolved);
      newFindings = List.copyOf(newFindings);
      persisting = List.copyOf(persisting);
      missingTests = List.copyOf(missingTests);
      unexpectedTests = List.copyOf(unexpectedTests);
      AuditRunResult validated = new AuditRunResult(List.of(), outcome, incompleteReasons);
      incompleteReasons = validated.incompleteReasons();
    }

    /** Retains the outcome-aware constructor introduced before coverage manifests. */
    public Verdict(
        List<Finding> resolved,
        List<Finding> newFindings,
        List<Finding> persisting,
        long queriesBefore,
        long queriesAfter,
        long executionTimeMsBefore,
        long executionTimeMsAfter,
        List<TestRef> missingTests,
        AuditOutcome outcome,
        List<AuditIncompleteReason> incompleteReasons) {
      this(
          resolved,
          newFindings,
          persisting,
          queriesBefore,
          queriesAfter,
          executionTimeMsBefore,
          executionTimeMsAfter,
          missingTests,
          outcome,
          incompleteReasons,
          List.of());
    }

    /** Retains the original constructor for callers compiled against the 0.5.0 API. */
    public Verdict(
        List<Finding> resolved,
        List<Finding> newFindings,
        List<Finding> persisting,
        long queriesBefore,
        long queriesAfter,
        long executionTimeMsBefore,
        long executionTimeMsAfter) {
      this(
          resolved,
          newFindings,
          persisting,
          queriesBefore,
          queriesAfter,
          executionTimeMsBefore,
          executionTimeMsAfter,
          List.of(),
          newFindings.isEmpty() ? AuditOutcome.PASS : AuditOutcome.FAIL,
          List.of());
    }

    /** Retains the original complete/missing-tests constructor from the 0.5.x API. */
    public Verdict(
        List<Finding> resolved,
        List<Finding> newFindings,
        List<Finding> persisting,
        long queriesBefore,
        long queriesAfter,
        long executionTimeMsBefore,
        long executionTimeMsAfter,
        List<TestRef> missingTests) {
      this(
          resolved,
          newFindings,
          persisting,
          queriesBefore,
          queriesAfter,
          executionTimeMsBefore,
          executionTimeMsAfter,
          missingTests,
          missingTests.isEmpty()
              ? (newFindings.isEmpty() ? AuditOutcome.PASS : AuditOutcome.FAIL)
              : AuditOutcome.INCONCLUSIVE,
          missingTests.isEmpty()
              ? List.of()
              : List.of(AuditIncompleteReason.of(IncompleteReasonCode.EXPECTED_TEST_MISSING)));
    }

    /** Returns whether both report inputs support a trustworthy comparison. */
    public boolean complete() {
      return outcome != AuditOutcome.INCONCLUSIVE;
    }
  }

  /** Compares two envelope documents (the string content of two {@code report.json} files). */
  public static Verdict compare(String beforeJson, String afterJson) {
    Envelope beforeEnvelope;
    Envelope afterEnvelope;
    try {
      beforeEnvelope = readEnvelope(beforeJson);
      afterEnvelope = readEnvelope(afterJson);
    } catch (UnsupportedReportSchemaException e) {
      return inconclusiveVerdict(
          new AuditIncompleteReason(IncompleteReasonCode.UNSUPPORTED_SCHEMA, e.getMessage()));
    }

    if (beforeEnvelope.redaction() != afterEnvelope.redaction()) {
      return inconclusiveVerdict(
          new AuditIncompleteReason(
              IncompleteReasonCode.REPORT_REDACTION_MISMATCH,
              "Reports use different redaction modes; regenerate both reports with the same mode"));
    }

    List<ParsedReport> beforeReports = beforeEnvelope.reports();
    List<ParsedReport> afterReports = afterEnvelope.reports();
    Map<ParsedReport, String> beforeIdentities = comparisonIdentities(beforeReports, afterReports);
    Map<ParsedReport, String> afterIdentities = comparisonIdentities(afterReports, beforeReports);
    List<ComparedFinding> before = confirmedFindings(beforeReports, beforeIdentities);
    List<ComparedFinding> after = confirmedFindings(afterReports, afterIdentities);

    Set<String> beforeKeys = new LinkedHashSet<>();
    before.forEach(f -> beforeKeys.add(f.finding().key()));
    Set<String> afterKeys = new LinkedHashSet<>();
    after.forEach(f -> afterKeys.add(f.finding().key()));

    Map<String, TestRef> beforeTests = auditedTests(beforeReports, beforeIdentities);
    Map<String, TestRef> afterTests = auditedTests(afterReports, afterIdentities);
    List<TestRef> missingTests = missingTests(beforeTests, afterTests, afterEnvelope.coverage());
    Set<String> unexpectedIds = unexpectedIds(afterEnvelope.coverage());
    List<TestRef> unexpectedTests =
        afterTests.entrySet().stream()
            .filter(
                test ->
                    !beforeTests.containsKey(test.getKey())
                        || unexpectedIds.contains(test.getValue().testId()))
            .map(Map.Entry::getValue)
            .toList();
    Set<String> incompleteIds = coverageGapIds(beforeEnvelope.coverage());
    incompleteIds.addAll(coverageGapIds(afterEnvelope.coverage()));
    boolean sameManifest =
        Objects.equals(
            expectedIds(beforeEnvelope.coverage()), expectedIds(afterEnvelope.coverage()));

    List<Finding> resolved =
        before.stream()
            .filter(f -> sameManifest && !incompleteIds.contains(f.finding().testId()))
            .filter(f -> afterTests.containsKey(f.testIdentity()))
            .filter(f -> !afterKeys.contains(f.finding().key()))
            .map(ComparedFinding::finding)
            .toList();
    List<Finding> fresh =
        after.stream()
            .filter(f -> !beforeKeys.contains(f.finding().key()))
            .map(ComparedFinding::finding)
            .toList();
    List<Finding> persisting =
        after.stream()
            .filter(f -> beforeKeys.contains(f.finding().key()))
            .map(ComparedFinding::finding)
            .toList();

    List<AuditIncompleteReason> incompleteReasons = new ArrayList<>();
    incompleteReasons.addAll(beforeEnvelope.incompleteReasons());
    incompleteReasons.addAll(afterEnvelope.incompleteReasons());
    if (!sameManifest) {
      incompleteReasons.add(
          new AuditIncompleteReason(
              IncompleteReasonCode.COVERAGE_MANIFEST_MISMATCH,
              "Reports do not declare the same expected-test manifest."));
    }
    if (!missingTests.isEmpty()) {
      incompleteReasons.add(AuditIncompleteReason.of(IncompleteReasonCode.EXPECTED_TEST_MISSING));
    }
    AuditRunResult comparisonResult =
        AuditRunResult.determine(
            List.of(),
            afterEnvelope.outcome() == AuditOutcome.FAIL || !fresh.isEmpty(),
            incompleteReasons);

    return new Verdict(
        resolved,
        fresh,
        persisting,
        sumSummary(beforeReports, "totalQueries"),
        sumSummary(afterReports, "totalQueries"),
        sumSummary(beforeReports, "executionTimeMs"),
        sumSummary(afterReports, "executionTimeMs"),
        missingTests,
        comparisonResult.outcome(),
        comparisonResult.incompleteReasons(),
        unexpectedTests);
  }

  private static Verdict inconclusiveVerdict(AuditIncompleteReason reason) {
    return new Verdict(
        List.of(),
        List.of(),
        List.of(),
        0,
        0,
        0,
        0,
        List.of(),
        AuditOutcome.INCONCLUSIVE,
        List.of(reason));
  }

  /** Renders the verdict as JSON (the {@code verdict.json} contract). */
  public static String toJson(Verdict verdict) {
    return toJson(verdict, ReportRedaction.REDACTED);
  }

  /** Full diagnostic details require an explicit opt-in, including for legacy input reports. */
  public static String toJson(Verdict verdict, ReportRedaction redaction) {
    ReportRedactor redactor = new ReportRedactor(redaction);
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("  \"redaction\": \"").append(redaction).append("\",\n");
    sb.append("  \"outcome\": \"").append(verdict.outcome()).append("\",\n");
    sb.append("  \"incompleteReasons\": ");
    appendIncompleteReasons(sb, verdict.incompleteReasons(), redactor);
    sb.append(",\n  \"newFindings\": ");
    appendFindings(sb, verdict.newFindings(), redactor);
    sb.append(",\n  \"resolved\": ");
    appendFindings(sb, verdict.resolved(), redactor);
    sb.append(",\n  \"persisting\": ");
    appendFindings(sb, verdict.persisting(), redactor);
    sb.append(",\n  \"complete\": ").append(verdict.complete());
    sb.append(",\n  \"missingTests\": ");
    appendTests(sb, verdict.missingTests());
    sb.append(",\n  \"unexpectedTests\": ");
    appendTests(sb, verdict.unexpectedTests());
    sb.append(",\n  \"queryCountDelta\": {\"before\": ")
        .append(verdict.queriesBefore())
        .append(", \"after\": ")
        .append(verdict.queriesAfter())
        .append("},\n");
    sb.append("  \"executionTimeMsDelta\": {\"before\": ")
        .append(verdict.executionTimeMsBefore())
        .append(", \"after\": ")
        .append(verdict.executionTimeMsAfter())
        .append("}\n");
    sb.append("}");
    return sb.toString();
  }

  /** One-screen console summary. */
  public static String toSummary(Verdict verdict) {
    StringBuilder sb = new StringBuilder();
    sb.append("[QueryAudit] compare: ")
        .append(verdict.outcome())
        .append("; ")
        .append(verdict.newFindings().size())
        .append(" new, ")
        .append(verdict.resolved().size())
        .append(" resolved, ")
        .append(verdict.persisting().size())
        .append(" persisting; queries ")
        .append(verdict.queriesBefore())
        .append(" -> ")
        .append(verdict.queriesAfter());
    if (!verdict.missingTests().isEmpty()) {
      sb.append("; INCOMPLETE: ")
          .append(verdict.missingTests().size())
          .append(" baseline ")
          .append(verdict.missingTests().size() == 1 ? "test" : "tests")
          .append(" missing");
      for (TestRef test : verdict.missingTests()) {
        sb.append("\n  MISSING  ").append(describe(test));
      }
    } else if (!verdict.incompleteReasons().isEmpty()) {
      sb.append("; INCONCLUSIVE: ");
      for (int i = 0; i < verdict.incompleteReasons().size(); i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(verdict.incompleteReasons().get(i).code());
      }
    }
    for (Finding f : verdict.newFindings()) {
      sb.append("\n  NEW      ").append(describe(f));
    }
    for (Finding f : verdict.resolved()) {
      sb.append("\n  RESOLVED ").append(describe(f));
    }
    return sb.toString();
  }

  /**
   * CLI entry point: {@code ReportComparator <before.json> <after.json> [verdict.json]}. Prints the
   * summary; writes {@code verdict.json} when the third argument is given.
   */
  public static void main(String[] args) throws Exception {
    System.exit(run(args));
  }

  static int run(String[] args) {
    if (args.length < 2 || args.length > 3) {
      System.err.println(
          "usage: java io.queryaudit.core.reporter.ReportComparator"
              + " <before.json> <after.json> [verdict.json]");
      return 2;
    }
    Verdict verdict;
    try {
      verdict =
          compare(
              Files.readString(Path.of(args[0]), StandardCharsets.UTF_8),
              Files.readString(Path.of(args[1]), StandardCharsets.UTF_8));
    } catch (Exception e) {
      System.err.println("[QueryAudit] compare failed: " + e.getMessage());
      return 2;
    }
    System.out.println(toSummary(verdict));
    if (args.length == 3) {
      try {
        Path verdictPath = Path.of(args[2]);
        Files.writeString(verdictPath, toJson(verdict), StandardCharsets.UTF_8);
        System.out.println("[QueryAudit] verdict: " + verdictPath.toAbsolutePath());
      } catch (Exception e) {
        System.err.println(
            "[QueryAudit] could not write verdict to '" + args[2] + "': " + e.getMessage());
        return 2;
      }
    }
    return exitCode(verdict);
  }

  static int exitCode(Verdict verdict) {
    return switch (verdict.outcome()) {
      case PASS -> 0;
      case FAIL -> 1;
      case INCONCLUSIVE -> 2;
    };
  }

  // ── Envelope reading ───────────────────────────────────────────────

  private static Envelope readEnvelope(String envelopeJson) {
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

    List<ParsedReport> reports = new ArrayList<>(entries.size());
    for (int i = 0; i < entries.size(); i++) {
      Object entry = entries.get(i);
      if (!(entry instanceof Map<?, ?> report)) {
        throw invalidEnvelope("reports[" + i + "] must be an object");
      }
      validateReport(report, i, stableIdentityRequired);
      String testId = optionalString(report, "testId");
      reports.add(
          new ParsedReport(
              report,
              testId,
              new TestRef(
                  testId, (String) report.get("testClass"), (String) report.get("testName"))));
    }
    AuditCoverage coverage = CoverageJson.read(envelope, schemaVersion.minor() >= 5);

    if (schemaVersion.minor() < FIRST_OUTCOME_SCHEMA_MINOR) {
      return new Envelope(
          AuditOutcome.INCONCLUSIVE,
          List.of(
              new AuditIncompleteReason(
                  IncompleteReasonCode.UNSUPPORTED_SCHEMA,
                  "schemaVersion " + schemaVersion.text() + " does not declare a run outcome")),
          reports,
          redaction,
          coverage);
    }

    AuditOutcome outcome;
    List<AuditIncompleteReason> incompleteReasons;
    try {
      outcome = requireOutcome(envelope);
      incompleteReasons = requireIncompleteReasons(envelope);
    } catch (UnsupportedReportSchemaException e) {
      return new Envelope(
          AuditOutcome.INCONCLUSIVE,
          List.of(
              new AuditIncompleteReason(IncompleteReasonCode.UNSUPPORTED_SCHEMA, e.getMessage())),
          reports,
          redaction,
          coverage);
    }
    try {
      AuditRunResult validated = new AuditRunResult(List.of(), outcome, incompleteReasons);
      if (coverage != null
          && coverage.failedToAudit() > 0
          && outcome != AuditOutcome.INCONCLUSIVE) {
        throw invalidEnvelope("coverage gaps require an INCONCLUSIVE outcome");
      }
      return new Envelope(
          validated.outcome(), validated.incompleteReasons(), reports, redaction, coverage);
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

  private static void validateReport(
      Map<?, ?> report, int reportIndex, boolean stableIdentityRequired) {
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

    List<?> confirmedIssues = requireArray(report, "confirmedIssues", path);
    for (int i = 0; i < confirmedIssues.size(); i++) {
      Object entry = confirmedIssues.get(i);
      String findingPath = path + ".confirmedIssues[" + i + "]";
      if (!(entry instanceof Map<?, ?> finding)) {
        throw invalidEnvelope(findingPath + " must be an object");
      }
      requireString(finding, "type", findingPath);
      requireNullableString(finding, "query", findingPath);
      requireNullableString(finding, "sourceLocation", findingPath);
      requireNullableString(finding, "table", findingPath);
      requireNullableString(finding, "detail", findingPath);
    }
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

  private record SchemaVersion(int major, int minor, String text) {}

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

  private static final class UnsupportedReportSchemaException extends IllegalArgumentException {

    UnsupportedReportSchemaException(String message) {
      super(message);
    }
  }

  private static Map<ParsedReport, String> comparisonIdentities(
      List<ParsedReport> reports, List<ParsedReport> otherReports) {
    validateUniqueIdentities(reports);
    validateLegacyMatches(reports, otherReports);

    Set<String> otherStableIds = new LinkedHashSet<>();
    Set<LegacyRef> otherLegacyIds = new LinkedHashSet<>();
    for (ParsedReport other : otherReports) {
      if (other.hasStableId()) {
        otherStableIds.add(other.testId());
      } else {
        otherLegacyIds.add(legacyRef(other.ref()));
      }
    }

    Map<ParsedReport, String> identities = new IdentityHashMap<>();
    for (ParsedReport report : reports) {
      if (!report.hasStableId()) {
        identities.put(report, legacyIdentity(report.ref()));
        continue;
      }

      boolean stableMatch = otherStableIds.contains(report.testId());
      boolean legacyMatch = !stableMatch && otherLegacyIds.contains(legacyRef(report.ref()));
      identities.put(
          report, legacyMatch ? legacyIdentity(report.ref()) : stableIdentity(report.testId()));
    }
    return identities;
  }

  private static void validateUniqueIdentities(List<ParsedReport> reports) {
    Set<String> stableIds = new LinkedHashSet<>();
    Set<LegacyRef> legacyIds = new LinkedHashSet<>();
    for (ParsedReport report : reports) {
      if (report.hasStableId()) {
        if (!stableIds.add(report.testId())) {
          throw invalidEnvelope("duplicate testId " + report.testId());
        }
      } else if (!legacyIds.add(legacyRef(report.ref()))) {
        throw ambiguousLegacyIdentity(report.ref());
      }
    }
  }

  private static void validateLegacyMatches(
      List<ParsedReport> reports, List<ParsedReport> otherReports) {
    Map<LegacyRef, Integer> otherIdentityCounts = new LinkedHashMap<>();
    for (ParsedReport other : otherReports) {
      otherIdentityCounts.merge(legacyRef(other.ref()), 1, Integer::sum);
    }
    for (ParsedReport report : reports) {
      if (report.hasStableId()) {
        continue;
      }
      if (otherIdentityCounts.getOrDefault(legacyRef(report.ref()), 0) > 1) {
        throw ambiguousLegacyIdentity(report.ref());
      }
    }
  }

  private static IllegalArgumentException ambiguousLegacyIdentity(TestRef test) {
    return invalidEnvelope(
        "legacy test identity is ambiguous for "
            + describe(test)
            + "; regenerate the 0.5 report with QueryAudit 0.6+");
  }

  private static String stableIdentity(String testId) {
    return "stable:" + testId;
  }

  private static String legacyIdentity(TestRef ref) {
    return "legacy:" + lengthPrefixed(ref.testClass()) + lengthPrefixed(ref.testName());
  }

  private static LegacyRef legacyRef(TestRef ref) {
    return new LegacyRef(ref.testClass(), ref.testName());
  }

  private static String lengthPrefixed(String value) {
    return value == null ? "-:" : value.length() + ":" + value;
  }

  private static List<ComparedFinding> confirmedFindings(
      List<ParsedReport> reports, Map<ParsedReport, String> identities) {
    List<ComparedFinding> findings = new ArrayList<>();
    for (ParsedReport parsedReport : reports) {
      Map<?, ?> report = parsedReport.value();
      String testClass = parsedReport.ref().testClass();
      String testName = parsedReport.ref().testName();
      String testIdentity = identities.get(parsedReport);
      List<?> confirmed = (List<?>) report.get("confirmedIssues");
      for (Object entry : confirmed) {
        Map<?, ?> issue = (Map<?, ?>) entry;
        String type = (String) issue.get("type");
        String query = (String) issue.get("query");
        String sourceLocation = stableSourceLocation((String) issue.get("sourceLocation"));
        String key = testIdentity + "|" + type + "|" + query + "|" + sourceLocation;
        findings.add(
            new ComparedFinding(
                new Finding(
                    parsedReport.testId(),
                    testClass,
                    testName,
                    type,
                    (String) issue.get("table"),
                    (String) issue.get("detail"),
                    key),
                testIdentity));
      }
    }
    return findings;
  }

  private static String stableSourceLocation(String sourceLocation) {
    if (sourceLocation == null) {
      return null;
    }
    return SOURCE_LINE_NUMBER.matcher(sourceLocation).replaceAll("");
  }

  private static Map<String, TestRef> auditedTests(
      List<ParsedReport> reports, Map<ParsedReport, String> identities) {
    Map<String, TestRef> tests = new LinkedHashMap<>();
    for (ParsedReport report : reports) {
      tests.put(identities.get(report), report.ref());
    }
    return tests;
  }

  private static List<TestRef> missingTests(
      Map<String, TestRef> before, Map<String, TestRef> after, AuditCoverage coverage) {
    Map<String, TestRef> missing = new LinkedHashMap<>();
    before.forEach(
        (id, test) -> {
          if (!after.containsKey(id)) {
            missing.put(id, test);
          }
        });
    for (String id : coverageGapIds(coverage)) {
      String key = stableIdentity(id);
      TestRef test = before.getOrDefault(key, after.getOrDefault(key, new TestRef(id, null, id)));
      missing.putIfAbsent(key, test);
    }
    return List.copyOf(missing.values());
  }

  private static Set<String> coverageGapIds(AuditCoverage coverage) {
    Set<String> ids = new LinkedHashSet<>();
    if (coverage != null) {
      coverage.tests().stream()
          .filter(test -> test.gap() != null)
          .forEach(test -> ids.add(test.testId()));
    }
    return ids;
  }

  private static Set<String> unexpectedIds(AuditCoverage coverage) {
    Set<String> ids = new LinkedHashSet<>();
    if (coverage != null) {
      coverage.tests().stream()
          .filter(test -> !test.expected())
          .forEach(test -> ids.add(test.testId()));
    }
    return ids;
  }

  private static Set<String> expectedIds(AuditCoverage coverage) {
    if (coverage == null) {
      return null;
    }
    Set<String> ids = new LinkedHashSet<>();
    coverage.tests().stream()
        .filter(AuditCoverage.Test::expected)
        .forEach(test -> ids.add(test.testId()));
    return ids;
  }

  private static long sumSummary(List<ParsedReport> reports, String field) {
    long sum = 0;
    for (ParsedReport report : reports) {
      Map<?, ?> summary = (Map<?, ?>) report.value().get("summary");
      sum += (Long) summary.get(field);
    }
    return sum;
  }

  // ── Rendering helpers ──────────────────────────────────────────────

  private static void appendIncompleteReasons(
      StringBuilder sb, List<AuditIncompleteReason> reasons, ReportRedactor redactor) {
    if (reasons.isEmpty()) {
      sb.append("[]");
      return;
    }
    sb.append("[\n");
    for (int i = 0; i < reasons.size(); i++) {
      AuditIncompleteReason reason = reasons.get(i);
      sb.append("    {\"code\": \"").append(reason.code()).append("\", \"detail\": ");
      appendString(sb, redactor.diagnostic(reason.detail()));
      sb.append("}");
      if (i < reasons.size() - 1) {
        sb.append(",");
      }
      sb.append("\n");
    }
    sb.append("  ]");
  }

  private static void appendFindings(
      StringBuilder sb, List<Finding> findings, ReportRedactor redactor) {
    if (findings.isEmpty()) {
      sb.append("[]");
      return;
    }
    sb.append("[\n");
    for (int i = 0; i < findings.size(); i++) {
      Finding f = findings.get(i);
      sb.append("    {\"testId\": ");
      appendString(sb, f.testId());
      sb.append(", \"test\": \"")
          .append(JsonReporter.escapeJson(f.testClass() + "." + f.testName()))
          .append("\", \"type\": \"")
          .append(JsonReporter.escapeJson(f.type()))
          .append("\"");
      if (f.table() != null) {
        sb.append(", \"table\": \"")
            .append(JsonReporter.escapeJson(redactor.sql(f.table())))
            .append("\"");
      }
      if (f.detail() != null) {
        sb.append(", \"detail\": \"")
            .append(JsonReporter.escapeJson(redactor.diagnostic(f.detail())))
            .append("\"");
      }
      sb.append("}");
      if (i < findings.size() - 1) {
        sb.append(",");
      }
      sb.append("\n");
    }
    sb.append("  ]");
  }

  private static void appendTests(StringBuilder sb, List<TestRef> tests) {
    if (tests.isEmpty()) {
      sb.append("[]");
      return;
    }
    sb.append("[\n");
    for (int i = 0; i < tests.size(); i++) {
      TestRef test = tests.get(i);
      sb.append("    {\"testId\": ");
      appendString(sb, test.testId());
      sb.append(", \"testClass\": ");
      appendString(sb, test.testClass());
      sb.append(", \"testName\": ");
      appendString(sb, test.testName());
      sb.append("}");
      if (i < tests.size() - 1) {
        sb.append(",");
      }
      sb.append("\n");
    }
    sb.append("  ]");
  }

  private static void appendString(StringBuilder sb, String value) {
    if (value == null) {
      sb.append("null");
      return;
    }
    sb.append("\"").append(JsonReporter.escapeJson(value)).append("\"");
  }

  private static String describe(Finding f) {
    return f.type()
        + (f.table() != null ? " (table: " + f.table() + ")" : "")
        + " in "
        + f.testClass()
        + "."
        + f.testName();
  }

  private static String describe(TestRef test) {
    return test.testClass() + "." + test.testName();
  }
}
