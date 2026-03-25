package io.queryaudit.core.regression;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.Severity;
import java.util.ArrayList;
import java.util.List;

/**
 * Compares current test query counts against a stored baseline and flags significant increases as
 * regressions.
 *
 * <p>A regression is reported when <b>both</b> conditions are met:
 *
 * <ul>
 *   <li>The increase ratio exceeds {@value #INCREASE_RATIO_THRESHOLD} (50%)
 *   <li>The absolute increase is at least {@value #ABSOLUTE_INCREASE_THRESHOLD} queries
 * </ul>
 *
 * <p>This dual-threshold approach avoids false positives on small tests (e.g., 2 -> 4 queries is a
 * 2x ratio but only +2, not worth flagging).
 *
 * @author haroya
 * @since 0.2.0
 */
public class QueryCountRegressionDetector {

  /** Minimum ratio (current / baseline) to consider a regression. */
  static final double INCREASE_RATIO_THRESHOLD = 1.5;

  /** Minimum absolute increase in query count to consider a regression. */
  static final int ABSOLUTE_INCREASE_THRESHOLD = 5;

  /**
   * Detects query count regressions by comparing current counts against the baseline.
   *
   * @param testClass the test class name
   * @param testMethod the test method name
   * @param current the query counts from the current test run
   * @param baseline the query counts from the stored baseline, or {@code null} if no baseline
   *     exists
   * @return a list of regression issues (empty if no regression detected)
   */
  public List<Issue> detect(
      String testClass, String testMethod, QueryCounts current, QueryCounts baseline) {
    List<Issue> issues = new ArrayList<>();

    if (baseline == null) {
      // No baseline yet -- first run, nothing to compare against
      return issues;
    }

    // Check total count regression
    if (current.totalCount() > baseline.totalCount()) {
      int increase = current.totalCount() - baseline.totalCount();
      double ratio = (double) current.totalCount() / Math.max(baseline.totalCount(), 1);

      if (ratio >= INCREASE_RATIO_THRESHOLD && increase >= ABSOLUTE_INCREASE_THRESHOLD) {
        Severity severity;
        if (ratio >= 3.0) {
          severity = Severity.ERROR; // 3x+ increase = serious regression
        } else if (ratio >= 2.0) {
          severity = Severity.WARNING; // 2x increase
        } else {
          severity = Severity.WARNING; // 1.5x increase
        }

        issues.add(
            new Issue(
                IssueType.QUERY_COUNT_REGRESSION,
                severity,
                null,
                null,
                null,
                String.format(
                    "Query count regression: %d -> %d queries (+%d, %.0f%% increase). "
                        + "Baseline: %d SELECT, %d INSERT, %d UPDATE, %d DELETE. "
                        + "Current: %d SELECT, %d INSERT, %d UPDATE, %d DELETE.",
                    baseline.totalCount(),
                    current.totalCount(),
                    increase,
                    (ratio - 1) * 100,
                    baseline.selectCount(),
                    baseline.insertCount(),
                    baseline.updateCount(),
                    baseline.deleteCount(),
                    current.selectCount(),
                    current.insertCount(),
                    current.updateCount(),
                    current.deleteCount()),
                "Query count increased significantly since last baseline. "
                    + "Check for newly introduced N+1, removed caching, or added queries. "
                    + "If this change is intentional, update the baseline with: ./gradlew test -DqueryGuard.updateBaseline=true"));
      }
    }

    // Also check SELECT-specific regression (most common indicator of N+1)
    if (current.selectCount() > baseline.selectCount()) {
      int selectIncrease = current.selectCount() - baseline.selectCount();
      double selectRatio = (double) current.selectCount() / Math.max(baseline.selectCount(), 1);

      if (selectRatio >= 2.0 && selectIncrease >= ABSOLUTE_INCREASE_THRESHOLD) {
        // Only add if not already covered by total count regression
        if (issues.isEmpty()) {
          issues.add(
              new Issue(
                  IssueType.QUERY_COUNT_REGRESSION,
                  Severity.WARNING,
                  null,
                  null,
                  null,
                  String.format(
                      "SELECT count regression: %d -> %d SELECTs (+%d). Possible N+1 introduced.",
                      baseline.selectCount(), current.selectCount(), selectIncrease),
                  "SELECT query count doubled. This often indicates a newly introduced N+1 pattern."));
        }
      }
    }

    return issues;
  }
}
