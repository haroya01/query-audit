package io.queryaudit.core.reporter;

import io.queryaudit.core.model.AuditCoverage;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Encoding and validation for the manifest coverage section of a suite report. */
final class CoverageJson {

  private CoverageJson() {}

  static void append(StringBuilder json, AuditCoverage coverage) {
    if (coverage == null) {
      json.append("null");
      return;
    }
    json.append("{\n");
    json.append("    \"expected\": ").append(coverage.expected()).append(",\n");
    json.append("    \"executed\": ").append(coverage.executed()).append(",\n");
    json.append("    \"skipped\": ").append(coverage.skipped()).append(",\n");
    json.append("    \"audited\": ").append(coverage.audited()).append(",\n");
    json.append("    \"failedToAudit\": ").append(coverage.failedToAudit()).append(",\n");
    json.append("    \"tests\": [");
    for (int i = 0; i < coverage.tests().size(); i++) {
      AuditCoverage.Test test = coverage.tests().get(i);
      if (i > 0) {
        json.append(',');
      }
      json.append("\n      {\"testId\": \"")
          .append(JsonReporter.escapeJson(test.testId()))
          .append("\", \"expected\": ")
          .append(test.expected())
          .append(", \"executed\": ")
          .append(test.executed())
          .append(", \"audited\": ")
          .append(test.audited())
          .append(", \"gap\": ");
      if (test.gap() == null) {
        json.append("null");
      } else {
        json.append('"').append(test.gap()).append('"');
      }
      json.append('}');
    }
    json.append("\n    ]\n  }");
  }

  static AuditCoverage read(Map<?, ?> envelope, boolean required) {
    if (!envelope.containsKey("coverage")) {
      if (required) {
        throw invalid("coverage is required");
      }
      return null;
    }
    if (envelope.get("coverage") == null) {
      return null;
    }
    if (!(envelope.get("coverage") instanceof Map<?, ?> value)
        || !(value.get("tests") instanceof List<?> entries)) {
      throw invalid("coverage must be null or an object containing tests");
    }
    List<AuditCoverage.Test> tests = new ArrayList<>();
    for (Object entry : entries) {
      if (!(entry instanceof Map<?, ?> test)
          || !(test.get("testId") instanceof String testId)
          || !(test.get("expected") instanceof Boolean expected)
          || !(test.get("executed") instanceof Boolean executed)
          || !(test.get("audited") instanceof Boolean audited)
          || !test.containsKey("gap")) {
        throw invalid("coverage test requires testId, expected, executed, audited, and gap");
      }
      AuditCoverage.Gap gap = null;
      if (test.get("gap") != null) {
        if (!(test.get("gap") instanceof String code)) {
          throw invalid("coverage gap must be a string or null");
        }
        try {
          gap = AuditCoverage.Gap.valueOf(code);
        } catch (IllegalArgumentException e) {
          throw invalid("unknown coverage gap: " + code);
        }
      }
      tests.add(new AuditCoverage.Test(testId, expected, executed, audited, gap));
    }
    AuditCoverage coverage = new AuditCoverage(tests);
    requireTotal(value, "expected", coverage.expected());
    requireTotal(value, "executed", coverage.executed());
    requireTotal(value, "skipped", coverage.skipped());
    requireTotal(value, "audited", coverage.audited());
    requireTotal(value, "failedToAudit", coverage.failedToAudit());
    Set<String> auditedIds =
        tests.stream()
            .filter(AuditCoverage.Test::audited)
            .map(AuditCoverage.Test::testId)
            .collect(Collectors.toSet());
    List<?> reports = (List<?>) envelope.get("reports");
    Set<String> reportIds =
        reports.stream()
            .map(report -> (String) ((Map<?, ?>) report).get("testId"))
            .collect(Collectors.toSet());
    if (reportIds.size() != reports.size() || !auditedIds.equals(reportIds)) {
      throw invalid("audited coverage IDs must match the per-test reports");
    }
    return coverage;
  }

  private static void requireTotal(Map<?, ?> value, String name, long expected) {
    if (!(value.get(name) instanceof Long actual) || actual != expected) {
      throw invalid("coverage." + name + " does not match its test entries");
    }
  }

  private static IllegalArgumentException invalid(String message) {
    return new IllegalArgumentException("Invalid report coverage: " + message);
  }
}
