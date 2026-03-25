package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryRecord;
import java.util.List;

/**
 * A rule that evaluates a list of query records and returns detected issues.
 * Implementations of this interface define specific SQL anti-pattern detection logic
 * and are registered with the query analyzer to inspect captured queries during tests.
 *
 * @author haroya
 * @since 0.2.0
 */
public interface DetectionRule {
  List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata);

  /**
   * Returns the rule code for this detector, used for accurate disable matching.
   * Built-in detectors may override this to return their exact issue-type code.
   * Returns {@code null} by default, which causes the analyzer to fall back to
   * the class-name heuristic.
   *
   * @return the rule code string, or {@code null} if not explicitly declared
   * @since 0.2.0
   */
  default String getRuleCode() {
    return null;
  }
}
