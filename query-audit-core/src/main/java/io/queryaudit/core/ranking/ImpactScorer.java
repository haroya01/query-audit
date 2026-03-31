package io.queryaudit.core.ranking;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.Severity;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Ranks issues by impact score using a formula inspired by pt-query-digest (Percona) and SolarWinds
 * DPA: {@code impactScore = frequencyScore + severityScore + patternScore}.
 *
 * <p>Issues are grouped by a unique fingerprint (issueType + table + column) so that duplicate
 * occurrences across multiple tests are counted as frequency rather than appearing as separate
 * entries.
 *
 * @author haroya
 * @since 0.2.0
 */
public final class ImpactScorer {

  private ImpactScorer() {}

  /**
   * Computes impact scores for the given issues and returns them ranked by score descending.
   *
   * @param issues list of issues (may span multiple test methods)
   * @return ranked list sorted by impact score descending, with 1-based rank assigned
   */
  public static List<RankedIssue> rank(List<Issue> issues) {
    if (issues == null || issues.isEmpty()) {
      return List.of();
    }

    // Group by fingerprint, keeping the first issue as representative
    Map<String, List<Issue>> grouped = new LinkedHashMap<>();
    for (Issue issue : issues) {
      String fingerprint = fingerprint(issue);
      grouped.computeIfAbsent(fingerprint, k -> new ArrayList<>()).add(issue);
    }

    // Score each unique issue group
    List<RankedIssue> ranked = new ArrayList<>(grouped.size());
    for (Map.Entry<String, List<Issue>> entry : grouped.entrySet()) {
      List<Issue> group = entry.getValue();
      Issue representative = group.get(0);
      int frequency = group.size();
      int score = computeScore(representative, frequency);
      // rank will be assigned after sorting
      ranked.add(new RankedIssue(representative, score, frequency, 0));
    }

    // Sort by score descending, then by issue type name for stable ordering
    ranked.sort(
        Comparator.comparingInt(RankedIssue::impactScore)
            .reversed()
            .thenComparing(r -> r.issue().type().name()));

    // Assign 1-based ranks
    List<RankedIssue> result = new ArrayList<>(ranked.size());
    for (int i = 0; i < ranked.size(); i++) {
      RankedIssue r = ranked.get(i);
      result.add(new RankedIssue(r.issue(), r.impactScore(), r.frequency(), i + 1));
    }

    return List.copyOf(result);
  }

  // -------------------------------------------------------------------------
  // Scoring
  // -------------------------------------------------------------------------

  static int computeScore(Issue issue, int frequency) {
    return frequencyScore(frequency) + severityScore(issue.severity()) + patternScore(issue.type());
  }

  static int frequencyScore(int frequency) {
    // N occurrences contribute N * 10, plus M test methods contribute M * 5.
    // Since each Issue instance comes from one test method invocation,
    // frequency == number of test methods that hit this pattern.
    return frequency * 10 + frequency * 5;
  }

  static int severityScore(Severity severity) {
    return switch (severity) {
      case ERROR -> 100;
      case WARNING -> 30;
      case INFO -> 5;
    };
  }

  static int patternScore(IssueType type) {
    return switch (type) {
      case N_PLUS_ONE, N_PLUS_ONE_SUSPECT -> 50;
      case MISSING_JOIN_INDEX -> 40;
      case FOR_UPDATE_WITHOUT_INDEX -> 40;
      case MISSING_WHERE_INDEX -> 30;
      case CORRELATED_SUBQUERY -> 30;
      case WHERE_FUNCTION, NON_SARGABLE_EXPRESSION -> 25;
      case CARTESIAN_JOIN -> 100;
      case REDUNDANT_INDEX -> 15;
      case REDUNDANT_FILTER -> 10;
      case SELECT_ALL -> 10;
      case OFFSET_PAGINATION -> 20;
      case OR_ABUSE -> 15;
      default -> 5;
    };
  }

  // -------------------------------------------------------------------------
  // Fingerprint
  // -------------------------------------------------------------------------

  /** Produces a unique key for deduplication: issueType + table + column. */
  static String fingerprint(Issue issue) {
    String table = issue.table() != null ? issue.table() : "";
    String column = issue.column() != null ? issue.column() : "";
    return issue.type().name() + "|" + table + "|" + column;
  }
}
