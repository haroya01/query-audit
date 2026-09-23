package io.queryaudit.core.detector;

import io.queryaudit.core.identity.FindingIdentity;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.Severity;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Keeps all observations of one open finding in the same host-policy bucket. */
final class OpenFindingClassifier {
  private OpenFindingClassifier() {}

  static List<FindingPolicy.Classified> classify(List<Finding> findings, FindingPolicy policy) {
    Map<String, List<FindingPolicy.Classified>> groups = new LinkedHashMap<>();
    for (Finding finding : findings) {
      FindingPolicy.Classified classified = policy.classify(finding);
      if (classified != null) {
        // Only equality within this one test matters; persisted IDs use the real framework test ID.
        String identity = FindingIdentity.of("query-audit:current-test", finding);
        groups.computeIfAbsent(identity, ignored -> new ArrayList<>()).add(classified);
      }
    }

    List<FindingPolicy.Classified> result = new ArrayList<>();
    for (List<FindingPolicy.Classified> observations : groups.values()) {
      Severity strongest =
          observations.stream()
              .map(value -> value.finding().severity())
              .min(Severity::compareTo)
              .orElseThrow();
      boolean allAcknowledged =
          observations.stream()
              .allMatch(value -> value.bucket() == FindingPolicy.Bucket.ACKNOWLEDGED);
      FindingPolicy.Bucket bucket =
          allAcknowledged
              ? FindingPolicy.Bucket.ACKNOWLEDGED
              : strongest == Severity.INFO
                  ? FindingPolicy.Bucket.INFO
                  : FindingPolicy.Bucket.CONFIRMED;
      // Different SQL representations can share a finding ID but not a legacy baseline pattern.
      // A partly acknowledged group must not hide any unacknowledged observation.
      for (FindingPolicy.Classified observation : observations) {
        result.add(
            new FindingPolicy.Classified(observation.finding().withSeverity(strongest), bucket));
      }
    }
    return List.copyOf(result);
  }
}
