package io.queryaudit.core.model;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
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
