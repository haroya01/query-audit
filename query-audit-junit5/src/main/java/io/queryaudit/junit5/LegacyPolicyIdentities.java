package io.queryaudit.junit5;

import io.queryaudit.core.regression.QueryCountBaseline;
import io.queryaudit.core.regression.QueryCounts;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;

/** The compatibility boundary for ambiguous pre-stable-ID policy entries. */
final class LegacyPolicyIdentities {
  private LegacyPolicyIdentities() {}

  static void track(
      AuditScope scope,
      String policy,
      Map<String, QueryCounts> entries,
      String testId,
      String testClass,
      String testName,
      boolean legacyFallbackEnabled) {
    if (!QueryCountBaseline.hasLegacyIdentity(entries, testClass, testName)) {
      return;
    }
    boolean usesLegacyIdentity =
        legacyFallbackEnabled
            && QueryCountBaseline.usesLegacyIdentity(entries, testId, testClass, testName);
    String warningKey = policy + "|" + testClass + "|" + testName;
    QueryAuditExtension.LegacyIdentityRegistry registry =
        claim(scope, warningKey, testId, usesLegacyIdentity, testClass, testName);
    if (!usesLegacyIdentity) {
      return;
    }
    if (registry.markWarning(warningKey)) {
      System.err.println(
          "[QueryAudit] Legacy 0.5 "
              + policy
              + " entry matched "
              + testClass
              + "."
              + testName
              + " by display name. Re-record the file to migrate this test to its stable JUnit"
              + " ID; legacy entries cannot distinguish packages or duplicate display names.");
    }
  }

  static QueryAuditExtension.LegacyIdentityRegistry claim(
      AuditScope scope,
      String claimKey,
      String testId,
      boolean usesLegacyIdentity,
      String testClass,
      String testName) {
    QueryAuditExtension.LegacyIdentityRegistry registry = scope.identityClaims();
    List<String> conflictingIds = registry.register(claimKey, testId, usesLegacyIdentity);
    if (!conflictingIds.isEmpty()) {
      throw new ExtensionConfigurationException(
          "QueryAudit: ambiguous 0.5 identity for "
              + testClass
              + "."
              + testName
              + ". Stable JUnit IDs "
              + String.join(", ", conflictingIds)
              + " match the same legacy entry while at least one test still depends on that"
              + " fallback. Re-record the policy file with QueryAudit"
              + " 0.6+.");
    }
    return registry;
  }

  static class Registry {

    private final Map<String, Map<String, Boolean>> claims = new HashMap<>();
    private final Set<String> warnings = new HashSet<>();

    synchronized List<String> register(String claimKey, String testId, boolean usesLegacyIdentity) {
      Map<String, Boolean> claimsForEntry =
          claims.computeIfAbsent(claimKey, ignored -> new LinkedHashMap<>());
      claimsForEntry.merge(testId, usesLegacyIdentity, Boolean::logicalOr);
      boolean fallbackIsAmbiguous =
          claimsForEntry.size() > 1 && claimsForEntry.containsValue(Boolean.TRUE);
      return fallbackIsAmbiguous ? List.copyOf(claimsForEntry.keySet()) : List.of();
    }

    synchronized boolean markWarning(String warningKey) {
      return warnings.add(warningKey);
    }
  }
}
