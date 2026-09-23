package io.queryaudit.core.reporter;

import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.model.AuditIncompleteReason;
import io.queryaudit.core.provenance.ComparisonInputDifference;
import io.queryaudit.core.reporter.ReportComparator.Finding;
import io.queryaudit.core.reporter.ReportComparator.TestRef;
import io.queryaudit.core.reporter.ReportComparator.Verdict;
import java.util.List;
import java.util.Set;

/** Presents an already-decided verdict; output redaction cannot affect comparison decisions. */
final class ReportVerdictWriter {
  private ReportVerdictWriter() {}

  /** Full diagnostic details require an explicit opt-in, including for legacy input reports. */
  static String toJson(Verdict verdict, ReportRedaction redaction) {
    ReportRedactor redactor = new ReportRedactor(redaction);
    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    sb.append("  \"redaction\": \"").append(redaction).append("\",\n");
    sb.append("  \"outcome\": \"").append(verdict.outcome()).append("\",\n");
    sb.append("  \"allTargetsResolved\": ").append(verdict.allTargetsResolved()).append(",\n");
    sb.append("  \"noNewRegressions\": ").append(verdict.noNewRegressions()).append(",\n");
    sb.append("  \"comparisonComplete\": ").append(verdict.comparisonComplete()).append(",\n");
    sb.append("  \"targetResolutions\": ");
    appendTargetResolutions(sb, verdict.targetResolutions());
    sb.append(",\n");
    sb.append("  \"findingIdentity\": {\"mode\": ");
    appendString(sb, verdict.findingIdentity().mode());
    sb.append(", \"baselineSchemaVersion\": ");
    appendString(sb, verdict.findingIdentity().baselineSchemaVersion());
    sb.append(", \"candidateSchemaVersion\": ");
    appendString(sb, verdict.findingIdentity().candidateSchemaVersion());
    sb.append("},\n");
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
    sb.append(",\n  \"inputDifferences\": ");
    appendInputDifferences(sb, verdict.inputDifferences());
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

  private static void appendTargetResolutions(StringBuilder json, List<TargetResolution> targets) {
    json.append('[');
    for (int i = 0; i < targets.size(); i++) {
      if (i > 0) {
        json.append(',');
      }
      TargetResolution target = targets.get(i);
      json.append("{\"findingId\": ");
      appendString(json, target.findingId());
      json.append(", \"testId\": ");
      appendString(json, target.testId());
      json.append(", \"status\": ");
      appendString(json, target.status().name());
      json.append('}');
    }
    json.append(']');
  }

  private static void appendInputDifferences(
      StringBuilder json, List<ComparisonInputDifference> differences) {
    json.append('[');
    for (int index = 0; index < differences.size(); index++) {
      if (index > 0) {
        json.append(',');
      }
      ComparisonInputDifference difference = differences.get(index);
      json.append("\n    {\"testId\":");
      ComparisonInputsJson.string(json, difference.testId());
      json.append(",\"field\":");
      ComparisonInputsJson.string(json, difference.field());
      json.append(",\"baseline\":");
      ComparisonInputsJson.string(
          json, safeDifferenceValue(difference.field(), difference.baseline()));
      json.append(",\"candidate\":");
      ComparisonInputsJson.string(
          json, safeDifferenceValue(difference.field(), difference.candidate()));
      json.append('}');
    }
    if (!differences.isEmpty()) {
      json.append("\n  ");
    }
    json.append(']');
  }

  private static String safeDifferenceValue(String field, String value) {
    if (value == null || value.matches("(?:sha256:)?[0-9a-f]{64}")) {
      return value;
    }
    boolean availability = field.equals("comparisonInputs") && value.equals("available");
    boolean state =
        field.endsWith(".state") && Set.of("AVAILABLE", "ABSENT", "FAILED").contains(value);
    boolean completeness =
        (field.equals("detectorInputsComplete") || field.endsWith(".inputsComplete"))
            && Set.of("true", "false").contains(value);
    boolean version =
        (field.equals("queryAuditVersion") || field.equals("parser.version"))
            && value.matches("[0-9]+(?:[.][0-9]+)+(?:[-+][A-Za-z0-9.-]+)?");
    boolean profile =
        field.equals("profile") && Set.of("strict", "recommended", "minimal").contains(value);
    boolean dialect =
        field.equals("databaseDialect")
            && Set.of("h2", "mysql", "postgresql", "mariadb", "oracle", "microsoft sql server")
                .contains(value);
    boolean parser =
        field.equals("parser.name") && Set.of("JSqlParser", "jsqlparser").contains(value);
    if (availability || state || completeness || version || profile || dialect || parser) {
      return value;
    }
    return ComparisonInputsJson.fingerprint(value);
  }

  /** One-screen console summary. */
  static String toSummary(Verdict verdict) {
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
    for (TargetResolution target : verdict.targetResolutions()) {
      sb.append("\n  TARGET ").append(target.status()).append("  ").append(target.findingId());
    }
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
      sb.append("    {\"findingId\": ");
      appendString(sb, f.findingId());
      sb.append(", \"testId\": ");
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
      sb.append(", \"column\": ");
      appendString(sb, redactor.sql(f.column()));
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
