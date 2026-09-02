package io.queryaudit.core.reporter;

import io.queryaudit.core.model.AuditIncompleteReason;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.IncompleteReasonCode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
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
 * <p><strong>Matching key:</strong> {@code testClass|testName|type|query|sourceMethod}. The query
 * field holds the normalized statement pattern, and source line numbers are removed before
 * matching, so findings survive line-only refactors while still distinguishing different call
 * sites. Only <em>confirmed</em> findings participate — INFO advisories don't gate fix loops.
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
  private static final Pattern SCHEMA_VERSION_PATTERN = Pattern.compile("^(\\d+)\\.(\\d+)\\.\\d+$");
  private static final Pattern SOURCE_LINE_NUMBER = Pattern.compile("(?m):-?\\d+$");

  private ReportComparator() {
    // static entry points only
  }

  /** One confirmed finding, reduced to its matching key plus display fields. */
  public record Finding(
      String testClass, String testName, String type, String table, String detail, String key) {}

  /** Identifies an audited test using the fields available in the schema 1.x report envelope. */
  public record TestRef(String testClass, String testName) {}

  private record Envelope(
      AuditOutcome outcome,
      List<AuditIncompleteReason> incompleteReasons,
      List<Map<?, ?>> reports) {}

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
      List<AuditIncompleteReason> incompleteReasons) {

    public Verdict {
      resolved = List.copyOf(resolved);
      newFindings = List.copyOf(newFindings);
      persisting = List.copyOf(persisting);
      missingTests = List.copyOf(missingTests);
      AuditRunResult validated = new AuditRunResult(List.of(), outcome, incompleteReasons);
      incompleteReasons = validated.incompleteReasons();
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

    List<Map<?, ?>> beforeReports = beforeEnvelope.reports();
    List<Map<?, ?>> afterReports = afterEnvelope.reports();
    List<Finding> before = confirmedFindings(beforeReports);
    List<Finding> after = confirmedFindings(afterReports);

    Set<String> beforeKeys = new LinkedHashSet<>();
    before.forEach(f -> beforeKeys.add(f.key()));
    Set<String> afterKeys = new LinkedHashSet<>();
    after.forEach(f -> afterKeys.add(f.key()));

    Set<TestRef> beforeTests = auditedTests(beforeReports);
    Set<TestRef> afterTests = auditedTests(afterReports);
    List<TestRef> missingTests =
        beforeTests.stream().filter(test -> !afterTests.contains(test)).toList();

    List<Finding> resolved =
        before.stream()
            .filter(f -> afterTests.contains(new TestRef(f.testClass(), f.testName())))
            .filter(f -> !afterKeys.contains(f.key()))
            .toList();
    List<Finding> fresh = after.stream().filter(f -> !beforeKeys.contains(f.key())).toList();
    List<Finding> persisting = after.stream().filter(f -> beforeKeys.contains(f.key())).toList();

    List<AuditIncompleteReason> incompleteReasons = new ArrayList<>();
    incompleteReasons.addAll(beforeEnvelope.incompleteReasons());
    incompleteReasons.addAll(afterEnvelope.incompleteReasons());
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
        comparisonResult.incompleteReasons());
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
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("  \"outcome\": \"").append(verdict.outcome()).append("\",\n");
    sb.append("  \"incompleteReasons\": ");
    appendIncompleteReasons(sb, verdict.incompleteReasons());
    sb.append(",\n  \"newFindings\": ");
    appendFindings(sb, verdict.newFindings());
    sb.append(",\n  \"resolved\": ");
    appendFindings(sb, verdict.resolved());
    sb.append(",\n  \"persisting\": ");
    appendFindings(sb, verdict.persisting());
    sb.append(",\n  \"complete\": ").append(verdict.complete());
    sb.append(",\n  \"missingTests\": ");
    appendTests(sb, verdict.missingTests());
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
    if (!(envelope.get("reports") instanceof List<?> entries)) {
      throw invalidEnvelope("reports must be an array");
    }

    List<Map<?, ?>> reports = new ArrayList<>(entries.size());
    for (int i = 0; i < entries.size(); i++) {
      Object entry = entries.get(i);
      if (!(entry instanceof Map<?, ?> report)) {
        throw invalidEnvelope("reports[" + i + "] must be an object");
      }
      validateReport(report, i);
      reports.add(report);
    }

    if (schemaVersion.minor() < FIRST_OUTCOME_SCHEMA_MINOR) {
      return new Envelope(
          AuditOutcome.INCONCLUSIVE,
          List.of(
              new AuditIncompleteReason(
                  IncompleteReasonCode.UNSUPPORTED_SCHEMA,
                  "schemaVersion " + schemaVersion.text() + " does not declare a run outcome")),
          reports);
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
          reports);
    }
    try {
      AuditRunResult validated = new AuditRunResult(List.of(), outcome, incompleteReasons);
      return new Envelope(validated.outcome(), validated.incompleteReasons(), reports);
    } catch (IllegalArgumentException e) {
      throw invalidEnvelope("outcome and incompleteReasons are inconsistent: " + e.getMessage());
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

  private static void validateReport(Map<?, ?> report, int reportIndex) {
    String path = "reports[" + reportIndex + "]";
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

  private static List<Finding> confirmedFindings(List<Map<?, ?>> reports) {
    List<Finding> findings = new ArrayList<>();
    for (Map<?, ?> report : reports) {
      String testClass = (String) report.get("testClass");
      String testName = (String) report.get("testName");
      List<?> confirmed = (List<?>) report.get("confirmedIssues");
      for (Object entry : confirmed) {
        Map<?, ?> issue = (Map<?, ?>) entry;
        String type = (String) issue.get("type");
        String query = (String) issue.get("query");
        String sourceLocation = stableSourceLocation((String) issue.get("sourceLocation"));
        String key = testClass + "|" + testName + "|" + type + "|" + query + "|" + sourceLocation;
        findings.add(
            new Finding(
                testClass,
                testName,
                type,
                (String) issue.get("table"),
                (String) issue.get("detail"),
                key));
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

  private static Set<TestRef> auditedTests(List<Map<?, ?>> reports) {
    Set<TestRef> tests = new LinkedHashSet<>();
    for (Map<?, ?> report : reports) {
      tests.add(new TestRef((String) report.get("testClass"), (String) report.get("testName")));
    }
    return tests;
  }

  private static long sumSummary(List<Map<?, ?>> reports, String field) {
    long sum = 0;
    for (Map<?, ?> report : reports) {
      Map<?, ?> summary = (Map<?, ?>) report.get("summary");
      sum += (Long) summary.get(field);
    }
    return sum;
  }

  // ── Rendering helpers ──────────────────────────────────────────────

  private static void appendIncompleteReasons(
      StringBuilder sb, List<AuditIncompleteReason> reasons) {
    if (reasons.isEmpty()) {
      sb.append("[]");
      return;
    }
    sb.append("[\n");
    for (int i = 0; i < reasons.size(); i++) {
      AuditIncompleteReason reason = reasons.get(i);
      sb.append("    {\"code\": \"").append(reason.code()).append("\", \"detail\": ");
      appendString(sb, reason.detail());
      sb.append("}");
      if (i < reasons.size() - 1) {
        sb.append(",");
      }
      sb.append("\n");
    }
    sb.append("  ]");
  }

  private static void appendFindings(StringBuilder sb, List<Finding> findings) {
    if (findings.isEmpty()) {
      sb.append("[]");
      return;
    }
    sb.append("[\n");
    for (int i = 0; i < findings.size(); i++) {
      Finding f = findings.get(i);
      sb.append("    {\"test\": \"")
          .append(JsonReporter.escapeJson(f.testClass() + "." + f.testName()))
          .append("\", \"type\": \"")
          .append(JsonReporter.escapeJson(f.type()))
          .append("\"");
      if (f.table() != null) {
        sb.append(", \"table\": \"").append(JsonReporter.escapeJson(f.table())).append("\"");
      }
      if (f.detail() != null) {
        sb.append(", \"detail\": \"").append(JsonReporter.escapeJson(f.detail())).append("\"");
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
      sb.append("    {\"testClass\": ");
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
