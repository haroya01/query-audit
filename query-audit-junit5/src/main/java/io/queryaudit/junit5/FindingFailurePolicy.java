package io.queryaudit.junit5;

import io.queryaudit.core.extension.FindingKindId;
import io.queryaudit.core.model.IssueType;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;

/** One validated selection for direct assertions and the effective-policy fingerprint. */
record FindingFailurePolicy(Set<String> kinds) {
  FindingFailurePolicy {
    kinds = Collections.unmodifiableSet(new TreeSet<>(kinds));
  }

  static FindingFailurePolicy from(QueryAudit annotation) {
    Set<String> kinds = new TreeSet<>();
    if (annotation != null) {
      for (IssueType type : annotation.failOn()) kinds.add(type.getCode());
      for (String kind : annotation.failOnKinds()) {
        try {
          kinds.add(FindingKindId.of(kind).value());
        } catch (IllegalArgumentException failure) {
          throw new ExtensionConfigurationException(
              "QueryAudit failOnKinds requires built-in codes or lowercase namespaced finding IDs");
        }
      }
    }
    return new FindingFailurePolicy(kinds);
  }

  boolean includes(String kind) {
    return kinds.isEmpty() || kinds.contains(kind);
  }

  void recordInto(Map<String, Integer> limits) {
    if (kinds.isEmpty()) limits.put("failOnAll", 1);
    else kinds.forEach(kind -> limits.put("failOn." + kind, 1));
  }
}
