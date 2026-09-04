package io.queryaudit.core.provenance;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.baseline.BaselineEntry;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.regression.QueryCounts;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AuditInputFingerprintsTest {
  @Test
  void collectionAndMapOrderingDoesNotChangeTheContract() {
    QueryAuditConfig first =
        QueryAuditConfig.builder()
            .suppressPatterns(new LinkedHashSet<>(List.of("select-all", "force-index-hint")))
            .build();
    QueryAuditConfig second =
        QueryAuditConfig.builder()
            .suppressPatterns(new LinkedHashSet<>(List.of("force-index-hint", "select-all")))
            .build();
    BaselineEntry one =
        new BaselineEntry("select-all", "users", null, "select * from users", "one", "reason");
    BaselineEntry two =
        new BaselineEntry("select-all", "orders", null, "select * from orders", "two", "reason");
    assertThat(AuditInputFingerprints.create(first, List.of(one, two), AuditPolicyInputs.empty()))
        .isEqualTo(
            AuditInputFingerprints.create(second, List.of(two, one), AuditPolicyInputs.empty()));

    Map<String, Object> ordered = new LinkedHashMap<>();
    ordered.put("z", 2);
    ordered.put("a", 1);
    assertThat(CanonicalFingerprint.of(ordered))
        .isEqualTo(CanonicalFingerprint.of(Map.of("a", 1, "z", 2)));
  }

  @Test
  void nullEmptyAndDelimiterContainingValuesRemainDistinct() {
    assertThat(CanonicalFingerprint.of(Arrays.asList("a|b", null)))
        .isNotEqualTo(CanonicalFingerprint.of(List.of("a", "b|")));
    assertThat(CanonicalFingerprint.of((Object) null)).isNotEqualTo(CanonicalFingerprint.of(""));
    assertThat(CanonicalFingerprint.of("1")).isNotEqualTo(CanonicalFingerprint.of(1));
  }

  @Test
  void changingPolicyAffectsOnlyItsFingerprint() {
    AuditInputFingerprints original =
        fingerprints(QueryAuditConfig.defaults(), AuditPolicyInputs.empty());
    AuditInputFingerprints threshold =
        fingerprints(
            QueryAuditConfig.builder().nPlusOneThreshold(20).build(), AuditPolicyInputs.empty());
    assertThat(threshold.thresholds()).isNotEqualTo(original.thresholds());
    assertThat(threshold.ruleSettings()).isEqualTo(original.ruleSettings());
    AuditInputFingerprints suppression =
        fingerprints(
            QueryAuditConfig.builder().addSuppressPattern("n-plus-one").build(),
            AuditPolicyInputs.empty());
    assertThat(suppression.suppressions()).isNotEqualTo(original.suppressions());
    AuditPolicyInputs policy =
        new AuditPolicyInputs(
            Map.of("id", new QueryCounts(1, 0, 0, 0, 1)), Map.of(), Map.of(), false, false);
    assertThat(fingerprints(QueryAuditConfig.defaults(), policy).queryContracts())
        .isNotEqualTo(original.queryContracts());
  }

  @Test
  void outputPathsAndBaselineCommentsDoNotAffectComparison() {
    QueryAuditConfig first =
        QueryAuditConfig.builder()
            .baselinePath("/one/private/file")
            .reportOutputDir("/one/reports")
            .build();
    QueryAuditConfig second =
        QueryAuditConfig.builder()
            .baselinePath("/two/private/file")
            .reportOutputDir("/two/reports")
            .build();
    BaselineEntry one =
        new BaselineEntry(
            "select-all", "USERS", null, "select * from users", "one", "private rationale");
    BaselineEntry two =
        new BaselineEntry(
            "select-all", "users", null, "select * from users", "two", "updated rationale");
    assertThat(AuditInputFingerprints.create(first, List.of(one), AuditPolicyInputs.empty()))
        .isEqualTo(AuditInputFingerprints.create(second, List.of(two), AuditPolicyInputs.empty()));
  }

  @Test
  void policyRecordModesAndInlineBudgetsCannotDisappearInComparison() {
    AuditInputFingerprints original =
        fingerprints(QueryAuditConfig.defaults(), AuditPolicyInputs.empty());
    for (AuditPolicyInputs policy :
        List.of(
            new AuditPolicyInputs(Map.of(), Map.of(), Map.of(), true, false),
            new AuditPolicyInputs(Map.of(), Map.of(), Map.of(), false, true),
            new AuditPolicyInputs(Map.of(), Map.of(), Map.of("maxQueries", 3), false, false))) {
      assertThat(fingerprints(QueryAuditConfig.defaults(), policy).queryContracts())
          .isNotEqualTo(original.queryContracts());
    }
  }

  private static AuditInputFingerprints fingerprints(
      QueryAuditConfig config, AuditPolicyInputs policy) {
    return AuditInputFingerprints.create(config, List.of(), policy);
  }
}
