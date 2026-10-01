package io.queryaudit.core.model;

import io.queryaudit.core.extension.FindingKindId;
import java.util.Objects;
import java.util.Optional;

/**
 * Open finding payload. The host assigns occurrence identity and applies policy and redaction.
 * Built-in {@link Issue} remains unchanged and can be viewed through {@link #fromIssue(Issue)}.
 */
public record Finding(
    FindingKindId kindId,
    Severity severity,
    String query,
    String table,
    String column,
    String detail,
    String suggestion,
    String sourceLocation) {
  public Finding {
    Objects.requireNonNull(kindId, "kindId");
    Objects.requireNonNull(severity, "severity");
  }

  public Finding(
      FindingKindId kindId,
      Severity severity,
      String query,
      String table,
      String column,
      String detail,
      String suggestion) {
    this(kindId, severity, query, table, column, detail, suggestion, null);
  }

  public static Finding fromIssue(Issue issue) {
    Objects.requireNonNull(issue, "issue");
    return new Finding(
        FindingKindId.builtin(issue.type()),
        issue.severity(),
        issue.query(),
        issue.table(),
        issue.column(),
        issue.detail(),
        issue.suggestion(),
        issue.sourceLocation());
  }

  public Finding withSeverity(Severity effectiveSeverity) {
    return severity == effectiveSeverity
        ? this
        : new Finding(
            kindId, effectiveSeverity, query, table, column, detail, suggestion, sourceLocation);
  }

  /**
   * Returns the exact legacy representation for a built-in kind; custom kinds have no enum value.
   */
  public Optional<Issue> toIssue() {
    for (IssueType type : IssueType.values()) {
      if (type.getCode().equals(kindId.value())) {
        return Optional.of(
            new Issue(type, severity, query, table, column, detail, suggestion, sourceLocation));
      }
    }
    return Optional.empty();
  }
}
