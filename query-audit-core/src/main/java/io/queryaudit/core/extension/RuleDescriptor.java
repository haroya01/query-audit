package io.queryaudit.core.extension;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * Immutable declarations about an implementation and every finding kind it can emit. Effective
 * settings must describe non-secret inputs, not credentials or captured SQL. They support
 * diagnostics and host fingerprinting; declarations alone never grant comparison trust.
 */
public record RuleDescriptor(
    RuleId id,
    String version,
    Set<FindingKindId> findingKinds,
    Map<String, String> effectiveSettings) {
  public RuleDescriptor {
    Objects.requireNonNull(id, "id");
    if (version == null || version.isBlank()) {
      throw new IllegalArgumentException("Rule version must not be blank");
    }
    Objects.requireNonNull(findingKinds, "findingKinds");
    if (findingKinds.isEmpty() || findingKinds.stream().anyMatch(Objects::isNull)) {
      throw new IllegalArgumentException("A rule must declare at least one non-null finding kind");
    }
    findingKinds = Collections.unmodifiableSet(new LinkedHashSet<>(findingKinds));
    Objects.requireNonNull(effectiveSettings, "effectiveSettings");
    effectiveSettings.forEach(
        (key, value) -> {
          if (key == null || key.isBlank() || value == null) {
            throw new IllegalArgumentException(
                "Effective settings require nonblank keys and non-null values");
          }
        });
    effectiveSettings = Collections.unmodifiableMap(new TreeMap<>(effectiveSettings));
  }
}
