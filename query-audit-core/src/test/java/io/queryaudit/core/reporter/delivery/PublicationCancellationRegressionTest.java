package io.queryaudit.core.reporter.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.AuditIncompleteReason;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.IncompleteReasonCode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

/** Cancellation contracts: these assert the desired behavior, not the known broken behavior. */
class PublicationCancellationRegressionTest {
  private enum SinkAction {
    RETURN,
    CHECKED_FAILURE,
    RUNTIME_FAILURE,
    INTERRUPTED_EXCEPTION,
    FLAG_AND_RETURN,
    FLAG_AND_CHECKED_FAILURE,
    FLAG_AND_RUNTIME_FAILURE;

    boolean cancels() {
      return this == INTERRUPTED_EXCEPTION || name().startsWith("FLAG_");
    }

    PublicationResult.Status status() {
      return this == RETURN || this == FLAG_AND_RETURN
          ? PublicationResult.Status.DELIVERED
          : PublicationResult.Status.FAILED;
    }

    void execute() throws Exception {
      if (name().startsWith("FLAG_")) Thread.currentThread().interrupt();
      switch (this) {
        case RETURN, FLAG_AND_RETURN -> {}
        case CHECKED_FAILURE, FLAG_AND_CHECKED_FAILURE -> throw new IOException("private-token");
        case RUNTIME_FAILURE, FLAG_AND_RUNTIME_FAILURE ->
            throw new IllegalStateException("private-token");
        case INTERRUPTED_EXCEPTION -> throw new InterruptedException("private-token");
      }
    }
  }

  static Stream<Arguments> outcomesAndActions() {
    return Stream.of(AuditOutcome.values())
        .flatMap(
            outcome -> Stream.of(SinkAction.values()).map(action -> Arguments.of(outcome, action)));
  }

  @ParameterizedTest(name = "analysis={0}, sink={1}")
  @MethodSource("outcomesAndActions")
  void cancellationStopsLaterSinksWhileOrdinaryFailuresAllowThem(
      AuditOutcome outcome, SinkAction action) {
    boolean previouslyInterrupted = Thread.interrupted();
    List<String> calls = new ArrayList<>();
    var publisher =
        new AuditReportPublisher(
            List.of(
                new ReportSinkRegistration(
                    "test:first",
                    false,
                    run -> {
                      calls.add("first");
                      action.execute();
                    }),
                new ReportSinkRegistration("test:required", true, run -> calls.add("required")),
                new ReportSinkRegistration("test:optional", false, run -> calls.add("optional"))));
    try {
      PublicationResult result = publisher.publish(run(outcome));

      assertThat(result.analysisOutcome()).isEqualTo(outcome);
      assertThat(result.toString())
          .doesNotContain("private-token", "IOException", "IllegalStateException");
      assertThat(Thread.currentThread().isInterrupted()).isEqualTo(action.cancels());
      assertThat(calls)
          .containsExactlyElementsOf(
              action.cancels() ? List.of("first") : List.of("first", "required", "optional"));
      var later =
          action.cancels()
              ? PublicationResult.Status.NOT_ATTEMPTED
              : PublicationResult.Status.DELIVERED;
      assertThat(result.deliveries())
          .containsExactly(
              new PublicationResult.Delivery("test:first", false, action.status()),
              new PublicationResult.Delivery("test:required", true, later),
              new PublicationResult.Delivery("test:optional", false, later));
      assertThat(result.hasRequiredFailures()).isEqualTo(action.cancels());
      assertThat(result.hasOptionalFailures())
          .isEqualTo(action.cancels() || action.status() == PublicationResult.Status.FAILED);
    } finally {
      Thread.interrupted();
      if (previouslyInterrupted) Thread.currentThread().interrupt();
    }
  }

  @ParameterizedTest
  @EnumSource(AuditOutcome.class)
  void anAlreadyInterruptedCallerDoesNotStartAnySink(AuditOutcome outcome) {
    boolean previouslyInterrupted = Thread.interrupted();
    List<String> calls = new ArrayList<>();
    var publisher =
        new AuditReportPublisher(
            List.of(
                new ReportSinkRegistration("test:required", true, run -> calls.add("required")),
                new ReportSinkRegistration("test:optional", false, run -> calls.add("optional"))));
    try {
      Thread.currentThread().interrupt();
      PublicationResult result = publisher.publish(run(outcome));

      assertThat(calls).isEmpty();
      assertThat(result.analysisOutcome()).isEqualTo(outcome);
      assertThat(result.deliveries())
          .extracting(PublicationResult.Delivery::status)
          .containsExactly(
              PublicationResult.Status.NOT_ATTEMPTED, PublicationResult.Status.NOT_ATTEMPTED);
      assertThat(result.hasRequiredFailures()).isTrue();
      assertThat(result.hasOptionalFailures()).isTrue();
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
      if (previouslyInterrupted) Thread.currentThread().interrupt();
    }
  }

  @Test
  void cancellationFromAnotherThreadIsObservedBeforeStartingTheNextDelivery() throws Exception {
    CountDownLatch entered = new CountDownLatch(1);
    AtomicReference<PublicationResult> result = new AtomicReference<>();
    AtomicReference<Throwable> failure = new AtomicReference<>();
    List<String> calls = new ArrayList<>();
    var publisher =
        new AuditReportPublisher(
            List.of(
                new ReportSinkRegistration(
                    "test:in-flight",
                    false,
                    run -> {
                      calls.add("in-flight");
                      entered.countDown();
                      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
                      while (!Thread.currentThread().isInterrupted()
                          && System.nanoTime() < deadline) {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
                      }
                      if (!Thread.currentThread().isInterrupted())
                        throw new AssertionError("Cancellation never arrived");
                    }),
                new ReportSinkRegistration("test:later", true, run -> calls.add("later"))));
    Thread worker =
        new Thread(
            () -> {
              try {
                result.set(publisher.publish(run(AuditOutcome.FAIL)));
              } catch (Throwable problem) {
                failure.set(problem);
              }
            },
            "publication-cancellation-regression");
    worker.setDaemon(true);
    try {
      worker.start();
      assertThat(entered.await(10, TimeUnit.SECONDS)).as("First sink must be in flight").isTrue();
      worker.interrupt();
      worker.join(TimeUnit.SECONDS.toMillis(10));

      assertThat(worker.isAlive()).isFalse();
      assertThat(failure.get()).isNull();
      assertThat(result.get()).isNotNull();
      assertThat(result.get().analysisOutcome()).isEqualTo(AuditOutcome.FAIL);
      assertThat(calls).containsExactly("in-flight");
      assertThat(result.get().deliveries().get(1).status())
          .isEqualTo(PublicationResult.Status.NOT_ATTEMPTED);
    } finally {
      worker.interrupt();
      worker.join(TimeUnit.SECONDS.toMillis(10));
    }
  }

  private static AuditRunResult run(AuditOutcome outcome) {
    return switch (outcome) {
      case PASS -> AuditRunResult.pass(List.of());
      case FAIL -> AuditRunResult.fail(List.of());
      case INCONCLUSIVE ->
          AuditRunResult.inconclusive(
              List.of(), AuditIncompleteReason.of(IncompleteReasonCode.AUDIT_ANALYSIS_FAILED));
    };
  }
}
