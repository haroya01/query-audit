package io.queryaudit.core.analyzer;

import io.queryaudit.core.model.Issue;
import java.util.List;
import java.util.Objects;

/**
 * Signals an incomplete EXPLAIN pass while retaining findings from plans already read.
 *
 * <p>The exception message contains no SQL or connection details. The original cause remains
 * available for local diagnosis and must not be copied into a public report without redaction.
 *
 * @since 0.6.0
 */
public final class ExplainAnalysisException extends RuntimeException {
  public enum Reason {
    EXECUTION_FAILED("EXPLAIN analysis did not complete"),
    UNSUPPORTED_PARAMETERS(
        "EXPLAIN does not analyze SQL containing '?'; captured bind values and types are unavailable");

    private final String message;

    Reason(String message) {
      this.message = message;
    }
  }

  private final Reason reason;
  private final List<Issue> completedIssues;

  public ExplainAnalysisException(List<Issue> completedIssues, Throwable cause) {
    this(Reason.EXECUTION_FAILED, completedIssues, cause);
  }

  public ExplainAnalysisException(Reason reason, List<Issue> completedIssues, Throwable cause) {
    super(Objects.requireNonNull(reason, "reason").message, Objects.requireNonNull(cause, "cause"));
    this.reason = reason;
    this.completedIssues = List.copyOf(completedIssues);
  }

  public Reason getReason() {
    return reason;
  }

  public List<Issue> getCompletedIssues() {
    return completedIssues;
  }
}
