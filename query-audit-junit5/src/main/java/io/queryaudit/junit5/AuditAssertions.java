package io.queryaudit.junit5;

import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.parser.SqlParser;
import io.queryaudit.core.regression.QueryContracts;
import io.queryaudit.core.regression.QueryCounts;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Direct test policy, separate from detection and report publication. A null message means the
 * assertion passed; AuditTestAssertions records the run outcome and raises its policy error.
 */
final class AuditAssertions {
  private AuditAssertions() {}

  static String maxQueryCountFailure(
      ExpectMaxQueryCount annotation, List<QueryRecord> queries, String testName) {
    if (annotation == null) return null;

    int max = annotation.value();
    int actual = queries.size();
    if (actual > max) {
      return String.format(
          "QueryAudit: %s executed %d queries, expected at most %d.\n"
              + "Tip: Check the Query Patterns section in the report above to identify which"
              + " queries to optimize.",
          testName, actual, max);
    }
    return null;
  }

  static String buildExpectQueriesFailureMessage(
      ExpectQueries annotation, List<QueryRecord> queries, String testName) {
    StringBuilder violations = new StringBuilder();
    appendBudgetViolation(
        violations, "SELECT", annotation.select(), queries, SqlParser::isSelectQuery);
    appendBudgetViolation(
        violations, "INSERT", annotation.insert(), queries, SqlParser::isInsertQuery);
    appendBudgetViolation(
        violations, "UPDATE", annotation.update(), queries, SqlParser::isUpdateQuery);
    appendBudgetViolation(
        violations, "DELETE", annotation.delete(), queries, SqlParser::isDeleteQuery);
    appendBudgetViolation(violations, "TOTAL", annotation.total(), queries, sql -> true);

    if (violations.length() == 0) {
      return null;
    }
    return "QueryAudit: " + testName + " exceeded its query budget.\n" + violations;
  }

  private static void appendBudgetViolation(
      StringBuilder sb,
      String type,
      int max,
      List<QueryRecord> queries,
      Predicate<String> typeMatcher) {
    if (max < 0) {
      return;
    }
    List<QueryRecord> matched = queries.stream().filter(q -> typeMatcher.test(q.sql())).toList();
    if (matched.size() <= max) {
      return;
    }

    sb.append(String.format("%s: executed %d, expected at most %d.\n", type, matched.size(), max));
    for (QueryRecord query : matched) {
      String sql = query.sql();
      sb.append("  ").append(sql.length() > 100 ? sql.substring(0, 100) + "..." : sql);
      String callSite = firstStackFrame(query.stackTrace());
      if (callSite != null) {
        sb.append("\n    at ").append(callSite);
      }
      sb.append('\n');
    }
  }

  private static String firstStackFrame(String stackTrace) {
    if (stackTrace == null || stackTrace.isEmpty()) {
      return null;
    }
    int newline = stackTrace.indexOf('\n');
    return newline < 0 ? stackTrace : stackTrace.substring(0, newline);
  }

  static String nPlusOneFailure(
      DetectNPlusOne annotation, QueryAuditReport report, String testName) {
    if (annotation == null) return null;

    List<Finding> nPlusOneIssues =
        report.getFindings().confirmed().stream()
            .filter(finding -> finding.kindId().value().equals(IssueType.N_PLUS_ONE.getCode()))
            .toList();

    if (!nPlusOneIssues.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append("QueryAudit: N+1 detected in ").append(testName).append("!\n\n");
      for (Finding issue : nPlusOneIssues) {
        sb.append("  ").append(issue.detail());
        if (issue.query() != null) {
          String sql = issue.query();
          sb.append("\n  Query: ").append(sql.length() > 100 ? sql.substring(0, 100) + "..." : sql);
        }
        if (issue.sourceLocation() != null) {
          sb.append("\n  Source: ").append(issue.sourceLocation());
        }
        sb.append("\n  Fix: ").append(issue.suggestion()).append("\n\n");
      }
      return sb.toString();
    }
    return null;
  }

  static List<Issue> failableIssues(QueryAuditReport report, QueryAudit annotation) {
    List<Issue> confirmed = report.getConfirmedIssues();
    if (confirmed == null || confirmed.isEmpty()) {
      return List.of();
    }

    if (annotation != null && annotation.failOn().length > 0) {
      Set<IssueType> failOnTypes = Set.of(annotation.failOn());
      return confirmed.stream().filter(issue -> failOnTypes.contains(issue.type())).toList();
    }

    return confirmed;
  }

  static String failureMessage(String testName, List<Issue> issues) {
    return findingsFailureMessage(testName, issues.stream().map(Finding::fromIssue).toList());
  }

  static List<Finding> failableFindings(QueryAuditReport report, QueryAudit annotation) {
    FindingFailurePolicy policy = FindingFailurePolicy.from(annotation);
    return report.getFindings().confirmed().stream()
        .filter(finding -> policy.includes(finding.kindId().value()))
        .toList();
  }

  static String findingsFailureMessage(String testName, List<Finding> issues) {
    StringBuilder sb = new StringBuilder();
    sb.append("QueryAudit detected ")
        .append(issues.size())
        .append(" issue(s) in ")
        .append(testName)
        .append(":\n\n");

    for (Finding issue : issues) {
      String description =
          issue
              .toIssue()
              .map(value -> value.type().getDescription())
              .orElse(issue.kindId().value());
      sb.append("  [").append(issue.severity()).append("] ").append(description);
      if (issue.table() != null) {
        sb.append(" (table: ").append(issue.table()).append(")");
      }
      if (issue.detail() != null) {
        sb.append("\n    Detail: ").append(issue.detail());
      }
      if (issue.suggestion() != null) {
        sb.append("\n    Suggestion: ").append(issue.suggestion());
      }
      if (issue.sourceLocation() != null && !issue.sourceLocation().isBlank()) {
        sb.append("\n    Call stack:");
        issue
            .sourceLocation()
            .lines()
            .filter(AuditAssertions::isApplicationFrame)
            .limit(5)
            .forEach(frame -> sb.append("\n      at ").append(frame));
      }
      sb.append("\n");
    }
    return sb.toString();
  }

  private static boolean isApplicationFrame(String frame) {
    return !frame.startsWith("java.")
        && !frame.startsWith("jdk.")
        && !frame.startsWith("sun.")
        && !frame.startsWith("worker.org.gradle.");
  }

  static String contractFailure(
      String testId,
      String testClass,
      String testName,
      List<QueryRecord> queries,
      Map<String, QueryCounts> contracts,
      String source) {
    return QueryContracts.verify(
        testId, testClass, testName, QueryCounts.from(queries), contracts, queries, source);
  }
}
