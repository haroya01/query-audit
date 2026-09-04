package io.queryaudit.core.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;

class AuditCoverageTest {

  @Test
  void missingExpectedEvidenceOverridesBothPassAndFail() {
    AuditCoverage coverage =
        new AuditCoverage(
            List.of(
                new AuditCoverage.Test("missing", true, false, false, AuditCoverage.Gap.SKIPPED)));

    assertThat(AuditRunResult.pass(List.of()).withCoverage(coverage).outcome())
        .isEqualTo(AuditOutcome.INCONCLUSIVE);
    assertThat(AuditRunResult.fail(List.of()).withCoverage(coverage).outcome())
        .isEqualTo(AuditOutcome.INCONCLUSIVE);
    assertThat(coverage.incompleteReasons())
        .containsExactly(
            new AuditIncompleteReason(
                IncompleteReasonCode.EXPECTED_TEST_MISSING, "missing: SKIPPED"));
  }

  @Test
  void coverageCannotInventAnAuditReport() {
    AuditCoverage coverage =
        new AuditCoverage(List.of(new AuditCoverage.Test("invented", true, true, true, null)));

    assertThatThrownBy(() -> AuditRunResult.pass(List.of()).withCoverage(coverage))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("match the per-test reports");
  }

  @Test
  void contradictoryExecutionStatesAndDuplicateIdsAreRejected() {
    assertThatThrownBy(() -> new AuditCoverage.Test("a", true, false, true, null))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> new AuditCoverage.Test("a", true, true, false, AuditCoverage.Gap.SKIPPED))
        .isInstanceOf(IllegalArgumentException.class);
    AuditCoverage.Test missing =
        new AuditCoverage.Test("a", true, false, false, AuditCoverage.Gap.NOT_DISCOVERED);
    assertThatThrownBy(() -> new AuditCoverage(List.of(missing, missing)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Duplicate");
  }

  @Test
  void legacyConstructorsDoNotClaimManifestVerification() {
    assertThat(AuditRunResult.pass(List.of()).coverage()).isNull();
  }
}
