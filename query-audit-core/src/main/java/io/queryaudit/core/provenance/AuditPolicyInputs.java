package io.queryaudit.core.provenance;

import io.queryaudit.core.regression.QueryCounts;
import java.util.Map;
import java.util.Objects;

/**
 * Loaded policy values, independent of the files and machines they came from.
 *
 * @since 0.6.0
 */
public record AuditPolicyInputs(
    Map<String, QueryCounts> queryContracts,
    Map<String, QueryCounts> countBaseline,
    Map<String, Integer> inlineLimits,
    boolean recordContracts,
    boolean recordCountBaseline) {
  public AuditPolicyInputs {
    queryContracts = Map.copyOf(Objects.requireNonNull(queryContracts, "queryContracts"));
    countBaseline = Map.copyOf(Objects.requireNonNull(countBaseline, "countBaseline"));
    inlineLimits = Map.copyOf(Objects.requireNonNull(inlineLimits, "inlineLimits"));
  }

  public static AuditPolicyInputs empty() {
    return new AuditPolicyInputs(Map.of(), Map.of(), Map.of(), false, false);
  }
}
