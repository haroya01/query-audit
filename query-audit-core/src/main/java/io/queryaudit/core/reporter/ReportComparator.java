package io.queryaudit.core.reporter;

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
 * <p><strong>Matching key:</strong> {@code testClass|testName|type|query|sourceLocation}. The query
 * field holds the normalized statement pattern, so findings survive unrelated refactors as long as
 * the statement shape and call site are stable. Only <em>confirmed</em> findings participate — INFO
 * advisories don't gate fix loops.
 *
 * <p><strong>Exit contract</strong> (CLI): {@code 0} when a complete comparison has no new
 * findings, {@code 1} when a complete comparison has new findings, and {@code 2} when the
 * comparison is incomplete or on usage/parse errors.
 *
 * @author haroya
 * @since 0.5.0
 */
public final class ReportComparator {

  private static final String SUPPORTED_SCHEMA_MAJOR = "1";
  private static final Pattern SCHEMA_VERSION_PATTERN = Pattern.compile("^(\\d+)\\.\\d+\\.\\d+$");

  private ReportComparator() {
    // static entry points only
  }

  /** One confirmed finding, reduced to its matching key plus display fields. */
  public record Finding(
      String testClass, String testName, String type, String table, String detail, String key) {}

  /** Identifies an audited test using the fields available in the schema 1.x report envelope. */
  public record TestRef(String testClass, String testName) {}

  /** The comparison result; incomplete comparisons cannot produce a trustworthy success signal. */
  public record Verdict(
      List<Finding> resolved,
      List<Finding> newFindings,
      List<Finding> persisting,
      long queriesBefore,
      long queriesAfter,
      long executionTimeMsBefore,
      long executionTimeMsAfter,
      List<TestRef> missingTests) {

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
          List.of());
    }

    /** Returns whether every test audited in the baseline also appears in the candidate report. */
    public boolean complete() {
      return missingTests.isEmpty();
    }
  }

  /** Compares two envelope documents (the string content of two {@code report.json} files). */
  public static Verdict compare(String beforeJson, String afterJson) {
    List<Map<String, Object>> beforeReports = reports(beforeJson);
    List<Map<String, Object>> afterReports = reports(afterJson);
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

    return new Verdict(
        resolved,
        fresh,
        persisting,
        sumSummary(beforeReports, "totalQueries"),
        sumSummary(afterReports, "totalQueries"),
        sumSummary(beforeReports, "executionTimeMs"),
        sumSummary(afterReports, "executionTimeMs"),
        missingTests);
  }

  /** Renders the verdict as JSON (the {@code verdict.json} contract). */
  public static String toJson(Verdict verdict) {
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("  \"newFindings\": ");
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
        .append(verdict.newFindings().size())
        .append(" new, ")
        .append(verdict.resolved().size())
        .append(" resolved, ")
        .append(verdict.persisting().size())
        .append(" persisting; queries ")
        .append(verdict.queriesBefore())
        .append(" -> ")
        .append(verdict.queriesAfter());
    if (!verdict.complete()) {
      sb.append("; INCOMPLETE: ")
          .append(verdict.missingTests().size())
          .append(" baseline ")
          .append(verdict.missingTests().size() == 1 ? "test" : "tests")
          .append(" missing");
      for (TestRef test : verdict.missingTests()) {
        sb.append("\n  MISSING  ").append(describe(test));
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
    if (args.length < 2 || args.length > 3) {
      System.err.println(
          "usage: java io.queryaudit.core.reporter.ReportComparator"
              + " <before.json> <after.json> [verdict.json]");
      System.exit(2);
      return;
    }
    Verdict verdict;
    try {
      verdict =
          compare(
              Files.readString(Path.of(args[0]), StandardCharsets.UTF_8),
              Files.readString(Path.of(args[1]), StandardCharsets.UTF_8));
    } catch (Exception e) {
      System.err.println("[QueryAudit] compare failed: " + e.getMessage());
      System.exit(2);
      return;
    }
    System.out.println(toSummary(verdict));
    if (args.length == 3) {
      Files.writeString(Path.of(args[2]), toJson(verdict), StandardCharsets.UTF_8);
      System.out.println("[QueryAudit] verdict: " + Path.of(args[2]).toAbsolutePath());
    }
    System.exit(exitCode(verdict));
  }

  static int exitCode(Verdict verdict) {
    if (!verdict.complete()) {
      return 2;
    }
    return verdict.newFindings().isEmpty() ? 0 : 1;
  }

  // ── Envelope reading ───────────────────────────────────────────────

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> reports(String envelopeJson) {
    Object root = MiniJsonParser.parse(envelopeJson);
    if (!(root instanceof Map<?, ?> envelope)) {
      throw invalidEnvelope("expected a JSON object");
    }
    requireSupportedSchemaVersion(envelope);
    if (!(envelope.get("reports") instanceof List<?>)) {
      throw invalidEnvelope("reports must be an array");
    }
    return (List<Map<String, Object>>) envelope.get("reports");
  }

  private static void requireSupportedSchemaVersion(Map<?, ?> envelope) {
    Object value = envelope.get("schemaVersion");
    if (!(value instanceof String schemaVersion)) {
      throw invalidEnvelope("schemaVersion is required and must be a string");
    }

    Matcher version = SCHEMA_VERSION_PATTERN.matcher(schemaVersion);
    if (!version.matches()) {
      throw invalidEnvelope("invalid schemaVersion; expected major.minor.patch");
    }
    if (!SUPPORTED_SCHEMA_MAJOR.equals(version.group(1))) {
      throw invalidEnvelope(
          "unsupported schemaVersion; this comparator supports " + SUPPORTED_SCHEMA_MAJOR + ".x");
    }
  }

  private static IllegalArgumentException invalidEnvelope(String reason) {
    return new IllegalArgumentException(
        "not a supported report.json envelope — " + reason + " (QueryAudit 0.5.0+)");
  }

  @SuppressWarnings("unchecked")
  private static List<Finding> confirmedFindings(List<Map<String, Object>> reports) {
    List<Finding> findings = new ArrayList<>();
    for (Map<String, Object> report : reports) {
      String testClass = (String) report.get("testClass");
      String testName = (String) report.get("testName");
      Object confirmed = report.get("confirmedIssues");
      if (!(confirmed instanceof List<?> list)) {
        continue;
      }
      for (Object entry : list) {
        Map<String, Object> issue = (Map<String, Object>) entry;
        String type = (String) issue.get("type");
        String query = (String) issue.get("query");
        String sourceLocation = (String) issue.get("sourceLocation");
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

  private static Set<TestRef> auditedTests(List<Map<String, Object>> reports) {
    Set<TestRef> tests = new LinkedHashSet<>();
    for (Map<String, Object> report : reports) {
      tests.add(new TestRef((String) report.get("testClass"), (String) report.get("testName")));
    }
    return tests;
  }

  private static long sumSummary(List<Map<String, Object>> reports, String field) {
    long sum = 0;
    for (Map<String, Object> report : reports) {
      if (report.get("summary") instanceof Map<?, ?> summary
          && summary.get(field) instanceof Long value) {
        sum += value;
      }
    }
    return sum;
  }

  // ── Rendering helpers ──────────────────────────────────────────────

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
