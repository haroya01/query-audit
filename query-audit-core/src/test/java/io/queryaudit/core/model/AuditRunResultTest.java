package io.queryaudit.core.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.provenance.AuditCapabilities;
import io.queryaudit.core.provenance.AuditCapability;
import io.queryaudit.core.provenance.AuditInputFingerprints;
import io.queryaudit.core.provenance.AuditPolicyInputs;
import io.queryaudit.core.provenance.ComparisonInputs;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AuditRunResultTest {

  @Test
  void incompleteReasonsTakePrecedenceOverPolicyFailures() {
    AuditRunResult result =
        AuditRunResult.determine(
            List.of(),
            true,
            List.of(
                AuditIncompleteReason.of(IncompleteReasonCode.DATASOURCE_UNAVAILABLE),
                AuditIncompleteReason.of(IncompleteReasonCode.QUERY_LIMIT_REACHED)));

    assertThat(result.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
    assertThat(result.incompleteReasons())
        .containsExactly(
            AuditIncompleteReason.of(IncompleteReasonCode.DATASOURCE_UNAVAILABLE),
            AuditIncompleteReason.of(IncompleteReasonCode.QUERY_LIMIT_REACHED));
    assertThat(result.isComplete()).isFalse();
  }

  @Test
  void completedRunsFailOnlyWhenAPolicyFailed() {
    assertThat(AuditRunResult.determine(List.of(), false, List.of()))
        .isEqualTo(AuditRunResult.pass(List.of()));
    assertThat(AuditRunResult.determine(List.of(), true, List.of()))
        .isEqualTo(AuditRunResult.fail(List.of()));
    assertThat(AuditRunResult.pass(List.of()).isComplete()).isTrue();
    assertThat(AuditRunResult.fail(List.of()).isComplete()).isTrue();
  }

  @Test
  void aFailedCapabilityCannotProduceACompleteResult() {
    Map<String, ComparisonInputs> inputs =
        Map.of("test", inputs(AuditCapability.failed("provider")));

    AuditRunResult result = AuditRunResult.pass(List.of()).withComparisonInputs(inputs);

    assertThat(result.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
    assertThat(result.incompleteReasons())
        .containsExactly(
            AuditIncompleteReason.of(IncompleteReasonCode.CAPABILITY_EXECUTION_FAILED));
    assertThat(AuditRunResult.fail(List.of()).withComparisonInputs(inputs).outcome())
        .isEqualTo(AuditOutcome.INCONCLUSIVE);

    AuditIncompleteReason initialization =
        AuditIncompleteReason.of(IncompleteReasonCode.CAPABILITY_INITIALIZATION_FAILED);
    assertThat(
            AuditRunResult.inconclusive(List.of(), initialization)
                .withComparisonInputs(inputs)
                .incompleteReasons())
        .containsExactly(initialization);
  }

  @Test
  void coverageAndComparisonInputCopiesPreserveEachOther() {
    Map<String, ComparisonInputs> inputs = Map.of("missing", inputs(AuditCapability.absent()));
    AuditCoverage coverage =
        new AuditCoverage(
            List.of(
                new AuditCoverage.Test(
                    "missing", true, false, false, AuditCoverage.Gap.NOT_DISCOVERED)));

    AuditRunResult first =
        AuditRunResult.pass(List.of()).withComparisonInputs(inputs).withCoverage(coverage);
    AuditRunResult second =
        AuditRunResult.pass(List.of()).withCoverage(coverage).withComparisonInputs(inputs);

    assertThat(first).isEqualTo(second);
    assertThat(first.comparisonInputs()).isEqualTo(inputs);
    assertThat(first.coverage()).isEqualTo(coverage);
    assertThat(first.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
  }

  private static ComparisonInputs inputs(AuditCapability metadata) {
    AuditCapability absent = AuditCapability.absent();
    return new ComparisonInputs(
        "0.6.0",
        "recommended",
        "h2",
        "JSqlParser",
        "5.3",
        List.of(),
        true,
        new AuditCapabilities(metadata, absent, absent, absent),
        AuditInputFingerprints.create(
            QueryAuditConfig.defaults(), List.of(), AuditPolicyInputs.empty()));
  }

  @Test
  void incompleteReasonsAreDeduplicatedAndImmutable() {
    AuditRunResult result =
        new AuditRunResult(
            List.of(),
            AuditOutcome.INCONCLUSIVE,
            List.of(
                AuditIncompleteReason.of(IncompleteReasonCode.REPORT_WRITE_FAILED),
                AuditIncompleteReason.of(IncompleteReasonCode.REPORT_WRITE_FAILED)));

    assertThat(result.incompleteReasons())
        .containsExactly(AuditIncompleteReason.of(IncompleteReasonCode.REPORT_WRITE_FAILED));
    assertThatThrownBy(
            () ->
                result
                    .incompleteReasons()
                    .add(AuditIncompleteReason.of(IncompleteReasonCode.EXPECTED_TEST_MISSING)))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void rejectsContradictoryOutcomeAndReasonCombinations() {
    assertThatThrownBy(() -> new AuditRunResult(List.of(), AuditOutcome.INCONCLUSIVE, List.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("requires at least one");
    assertThatThrownBy(
            () ->
                new AuditRunResult(
                    List.of(),
                    AuditOutcome.PASS,
                    List.of(AuditIncompleteReason.of(IncompleteReasonCode.QUERY_LIMIT_REACHED))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not carry");
    assertThatThrownBy(
            () ->
                new AuditRunResult(
                    List.of(),
                    AuditOutcome.FAIL,
                    List.of(AuditIncompleteReason.of(IncompleteReasonCode.CONTRACT_UNREADABLE))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must not carry");
  }
}
