package io.queryaudit.core.reporter;

import io.queryaudit.core.model.Issue;
import java.util.List;

/**
 * Maps high-precision issue types to machine-readable remediation hints for the JSON report.
 *
 * <p>The human-readable {@code suggestion} string stays the primary channel; the structured hint
 * exists so automated consumers (CI bots, remediation tooling) can act on a finding without parsing
 * prose. Only rules whose fix shape is unambiguous get a hint — everything else returns {@code
 * null} and the JSON field is omitted.
 *
 * @author haroya
 * @since 0.5.0
 */
public final class RemediationHints {

  private RemediationHints() {
    // utility class
  }

  /**
   * A machine-readable fix description: what to do ({@code kind}), where ({@code table}), and on
   * which columns (may be empty when the kind is not column-scoped).
   */
  public record Remediation(String kind, String table, List<String> columns) {}

  /** Returns the structured hint for the issue, or {@code null} when none is defined. */
  public static Remediation forIssue(Issue issue) {
    return switch (issue.type()) {
      case MISSING_WHERE_INDEX,
              MISSING_JOIN_INDEX,
              MISSING_ORDER_BY_INDEX,
              MISSING_GROUP_BY_INDEX ->
          new Remediation(
              "add-index",
              issue.table(),
              issue.column() != null ? List.of(issue.column()) : List.of());
      case N_PLUS_ONE, FIND_BY_ID_FOR_ASSOCIATION ->
          new Remediation("batch-fetch", issue.table(), List.of());
      case REPEATED_SINGLE_INSERT -> new Remediation("batch-insert", issue.table(), List.of());
      case REPEATED_SINGLE_UPDATE -> new Remediation("batch-update", issue.table(), List.of());
      case UPDATE_WITHOUT_WHERE -> new Remediation("add-where-clause", issue.table(), List.of());
      case UNBOUNDED_RESULT_SET -> new Remediation("add-limit", issue.table(), List.of());
      case SELECT_ALL -> new Remediation("select-explicit-columns", issue.table(), List.of());
      default -> null;
    };
  }
}
