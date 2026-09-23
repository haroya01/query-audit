package io.queryaudit.core.reporter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/** Validates explicit targets and resolves them within their baseline test, never across tests. */
final class ComparisonTargets {
  private static final Pattern ID =
      Pattern.compile(Pattern.quote(FindingId.PREFIX) + "[0-9a-f]{64}");

  private ComparisonTargets() {}

  static List<String> requested(Collection<String> findingIds) {
    Objects.requireNonNull(findingIds, "requiredFindingIds");
    Set<String> unique = new LinkedHashSet<>();
    for (String id : findingIds) {
      if (id == null || !ID.matcher(id).matches()) {
        throw new IllegalArgumentException(
            "--require-resolved expects a qa-finding-v1: ID with 64 lowercase hexadecimal characters");
      }
      unique.add(id);
    }
    return List.copyOf(unique);
  }

  static List<TargetResolution> unavailable(List<String> requiredIds) {
    return requiredIds.stream()
        .map(id -> new TargetResolution(id, null, TargetResolution.Status.INCOMPLETE))
        .toList();
  }

  static List<TargetResolution> evaluate(
      List<String> requiredIds,
      Map<String, Set<String>> baseline,
      Map<String, Set<String>> candidate,
      boolean comparisonComplete) {
    List<TargetResolution> results = new ArrayList<>();
    for (String id : requiredIds) {
      List<String> matchingTests =
          baseline.entrySet().stream()
              .filter(test -> test.getValue().contains(id))
              .map(Map.Entry::getKey)
              .toList();
      if (matchingTests.isEmpty()) {
        throw new IllegalArgumentException(
            "Required finding ID is not present in the baseline: " + id);
      }
      if (matchingTests.size() > 1) {
        throw new IllegalArgumentException(
            "Required finding ID is ambiguous across baseline tests: " + id);
      }
      String testId = matchingTests.get(0);
      TargetResolution.Status status;
      if (!comparisonComplete || !candidate.containsKey(testId)) {
        status = TargetResolution.Status.INCOMPLETE;
      } else if (candidate.get(testId).contains(id)) {
        status = TargetResolution.Status.PERSISTING;
      } else {
        status = TargetResolution.Status.RESOLVED;
      }
      results.add(new TargetResolution(id, testId, status));
    }
    return List.copyOf(results);
  }
}
