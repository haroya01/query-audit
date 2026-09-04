package io.queryaudit.core.provenance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.config.QueryAuditConfig;
import java.util.List;
import org.junit.jupiter.api.Test;

class ComparisonInputCompatibilityTest {
  private static final AuditCapabilities ABSENT =
      new AuditCapabilities(
          AuditCapability.absent(),
          AuditCapability.absent(),
          AuditCapability.absent(),
          AuditCapability.absent());

  @Test
  void identicalKnownInputsAreComparableEvenWhenOptionalCapabilitiesAreAbsent() {
    ComparisonInputs inputs = inputs("strict", ABSENT);
    assertThat(ComparisonInputCompatibility.compare("test", inputs, inputs)).isEmpty();
  }

  @Test
  void twoMissingContractsAreNeverAssumedEquivalent() {
    assertThat(ComparisonInputCompatibility.compare("test", null, null))
        .extracting(ComparisonInputDifference::field)
        .containsExactly("comparisonInputs");
    assertThatThrownBy(
            () ->
                new ComparisonInputs(
                    "unknown",
                    "strict",
                    "h2",
                    "JSqlParser",
                    "5.3",
                    List.of(),
                    true,
                    ABSENT,
                    hashes()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void aWeakerProfileAndMissingMetadataCannotResolveAFinding() {
    AuditCapabilities available =
        new AuditCapabilities(
            AuditCapability.available("jdbc-provider"),
            AuditCapability.absent(),
            AuditCapability.absent(),
            AuditCapability.absent());
    assertThat(
            ComparisonInputCompatibility.compare(
                "test", inputs("strict", available), inputs("minimal", ABSENT)))
        .extracting(ComparisonInputDifference::field)
        .contains(
            "profile", "capabilities.indexMetadata.state", "capabilities.indexMetadata.source");
  }

  @Test
  void matchingFailuresAreStillIncompatible() {
    AuditCapabilities failed =
        new AuditCapabilities(
            AuditCapability.failed("jdbc-provider"),
            AuditCapability.absent(),
            AuditCapability.absent(),
            AuditCapability.absent());
    ComparisonInputs inputs = inputs("strict", failed);
    assertThat(ComparisonInputCompatibility.compare("test", inputs, inputs))
        .extracting(ComparisonInputDifference::field)
        .containsExactly(
            "capabilities.indexMetadata.inputsComplete", "capabilities.indexMetadata.state");
  }

  @Test
  void customDetectorStateWithoutAFingerprintRemainsUnverified() {
    ComparisonInputs inputs =
        new ComparisonInputs(
            "0.6.0",
            "strict",
            "h2",
            "JSqlParser",
            "5.3",
            List.of("custom-detector@hash"),
            false,
            ABSENT,
            hashes());
    assertThat(ComparisonInputCompatibility.compare("test", inputs, inputs))
        .extracting(ComparisonInputDifference::field)
        .containsExactly("detectorInputsComplete");
    assertThat(
            io.queryaudit.core.model.AuditRunResult.pass(List.of())
                .withComparisonInputs(java.util.Map.of("test", inputs))
                .outcome())
        .isEqualTo(io.queryaudit.core.model.AuditOutcome.PASS);
  }

  @Test
  void runtimeIdentitiesComeFromTheArtifact() {
    assertThat(AuditRuntimeIdentity.queryAuditVersion()).isNotBlank().doesNotContain("${");
    assertThat(AuditRuntimeIdentity.implementation(getClass()))
        .matches(java.util.regex.Pattern.quote(getClass().getName()) + "@[0-9a-f]{64}")
        .isNotEqualTo(AuditRuntimeIdentity.implementation(ComparisonInputs.class));
  }

  private static ComparisonInputs inputs(String profile, AuditCapabilities capabilities) {
    return new ComparisonInputs(
        "0.6.0",
        profile,
        "h2",
        "JSqlParser",
        "5.3",
        List.of("detector@hash"),
        true,
        capabilities,
        hashes());
  }

  private static AuditInputFingerprints hashes() {
    return AuditInputFingerprints.create(
        QueryAuditConfig.defaults(), List.of(), AuditPolicyInputs.empty());
  }
}
