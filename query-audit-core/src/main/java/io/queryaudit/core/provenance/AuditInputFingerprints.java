package io.queryaudit.core.provenance;

import io.queryaudit.core.baseline.BaselineEntry;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.parser.SqlParser;
import io.queryaudit.core.regression.QueryCounts;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * SHA-256 digests of effective settings; no raw policy text or file paths are reported.
 *
 * @since 0.6.0
 */
public record AuditInputFingerprints(
    String ruleSettings,
    String thresholds,
    String suppressions,
    String queryContracts,
    String findingBaseline) {
  public AuditInputFingerprints {
    for (String value :
        List.of(ruleSettings, thresholds, suppressions, queryContracts, findingBaseline)) {
      if (!value.matches("[0-9a-f]{64}")) {
        throw new IllegalArgumentException("Fingerprint must be a lowercase SHA-256 digest");
      }
    }
  }

  public static AuditInputFingerprints create(
      QueryAuditConfig config, Collection<BaselineEntry> baseline, AuditPolicyInputs policy) {
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(baseline, "baseline");
    Objects.requireNonNull(policy, "policy");
    return new AuditInputFingerprints(
        CanonicalFingerprint.of(ruleSettings(config)),
        CanonicalFingerprint.of(thresholds(config)),
        CanonicalFingerprint.of(
            Map.of(
                "patterns", sorted(config.getSuppressPatterns()),
                "queries", sorted(config.getSuppressQueries()))),
        CanonicalFingerprint.of(
            Map.of(
                "contracts", counts(policy.queryContracts()),
                "countBaseline", counts(policy.countBaseline()),
                "inlineLimits", policy.inlineLimits(),
                "recordContracts", policy.recordContracts(),
                "recordCountBaseline", policy.recordCountBaseline())),
        CanonicalFingerprint.of(
            baseline.stream()
                .map(
                    entry ->
                        CanonicalFingerprint.of(
                            Arrays.asList(
                                entry.issueCode(),
                                lower(entry.table()),
                                lower(entry.column()),
                                SqlParser.normalize(entry.queryPattern()))))
                .sorted()
                .distinct()
                .toList()));
  }

  private static Map<String, Object> ruleSettings(QueryAuditConfig config) {
    Map<String, Object> values = new TreeMap<>();
    values.put("enabled", config.isEnabled());
    values.put("failOnDetection", config.isFailOnDetection());
    values.put("showInfo", config.isShowInfo());
    values.put("auditMode", config.getAuditMode().name());
    values.put("includeSetupQueries", config.isIncludeSetupQueries());
    values.put("maxQueries", config.getMaxQueries());
    values.put("countInsteadOfExists", config.isCountInsteadOfExistsEnabled());
    values.put("disabledRules", sorted(config.getDisabledRules()));
    values.put("enabledRules", sorted(config.getEnabledRules()));
    Map<String, String> severities = new TreeMap<>();
    config
        .getSeverityOverrides()
        .forEach((code, severity) -> severities.put(code, severity.name()));
    values.put("severityOverrides", severities);
    // Some SQL normalization and suppression checks use the JVM's default locale.
    values.put("normalizationLocale", Locale.getDefault().toLanguageTag());
    return values;
  }

  private static Map<String, Object> thresholds(QueryAuditConfig config) {
    Map<String, Object> values = new TreeMap<>();
    values.put("nPlusOne", config.getNPlusOneThreshold());
    values.put("offsetPagination", config.getOffsetPaginationThreshold());
    values.put("orClause", config.getOrClauseThreshold());
    values.put("largeInList", config.getLargeInListThreshold());
    values.put("tooManyJoins", config.getTooManyJoinsThreshold());
    values.put("excessiveColumn", config.getExcessiveColumnThreshold());
    values.put("repeatedInsert", config.getRepeatedInsertThreshold());
    values.put("repeatedInsertExcludeTables", sorted(config.getRepeatedInsertExcludeTables()));
    values.put("repeatedUpdate", config.getRepeatedUpdateThreshold());
    values.put("repeatedUpdateExcludeTables", sorted(config.getRepeatedUpdateExcludeTables()));
    values.put("writeAmplification", config.getWriteAmplificationThreshold());
    values.put("slowQueryWarningMs", config.getSlowQueryWarningMs());
    values.put("slowQueryErrorMs", config.getSlowQueryErrorMs());
    values.put("connectionHeldIdleMs", config.getConnectionHeldIdleThresholdMs());
    return values;
  }

  private static Map<String, Object> counts(Map<String, QueryCounts> counts) {
    Map<String, Object> values = new TreeMap<>();
    counts.forEach(
        (test, value) ->
            values.put(
                test,
                List.of(
                    value.selectCount(),
                    value.insertCount(),
                    value.updateCount(),
                    value.deleteCount(),
                    value.totalCount())));
    return values;
  }

  private static List<String> sorted(Collection<String> values) {
    return values.stream().sorted(Comparator.nullsFirst(Comparator.naturalOrder())).toList();
  }

  private static String lower(String value) {
    return value == null ? null : value.toLowerCase(Locale.ROOT);
  }
}
