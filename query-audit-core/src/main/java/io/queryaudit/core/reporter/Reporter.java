package io.queryaudit.core.reporter;

import io.queryaudit.core.model.QueryAuditReport;

/**
 * Strategy interface for presenting a {@link QueryAuditReport} to the user. Implementations format
 * and output the analysis report in various formats such as console, HTML, or JSON.
 *
 * @author haroya
 * @since 0.2.0
 */
public interface Reporter {

  /**
   * Outputs the given report in an implementation-specific format.
   *
   * @param report the analysis report to present
   */
  void report(QueryAuditReport report);
}
