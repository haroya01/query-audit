package io.queryaudit.core.provenance;

import io.queryaudit.core.config.RuleProfile;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * Effective inputs for one stable test identity in a report envelope.
 *
 * @since 0.6.0
 */
public record ComparisonInputs(
    String queryAuditVersion,
    String profile,
    String databaseDialect,
    String parserName,
    String parserVersion,
    List<String> detectorCapabilities,
    boolean detectorInputsComplete,
    AuditCapabilities capabilities,
    AuditInputFingerprints fingerprints) {
  public ComparisonInputs {
    requireKnownValue(queryAuditVersion, "queryAuditVersion");
    requireKnownValue(profile, "profile");
    profile = RuleProfile.parse(profile).name().toLowerCase(Locale.ROOT);
    requireKnownValue(databaseDialect, "databaseDialect");
    requireKnownValue(parserName, "parserName");
    requireKnownValue(parserVersion, "parserVersion");
    detectorCapabilities =
        Objects.requireNonNull(detectorCapabilities, "detectorCapabilities").stream()
            .sorted()
            .toList();
    detectorCapabilities.forEach(value -> requireKnownValue(value, "detectorCapabilities"));
    Objects.requireNonNull(capabilities, "capabilities");
    Objects.requireNonNull(fingerprints, "fingerprints");
  }

  private static void requireKnownValue(String value, String field) {
    Objects.requireNonNull(value, field);
    if (value.isBlank() || value.trim().equalsIgnoreCase("unknown")) {
      throw new IllegalArgumentException(field + " must identify the actual audit input");
    }
  }
}
