package io.queryaudit.core.reporter.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class AuditReportPublisherTest {
  @Test
  void separatesRequiredAndOptionalFailuresFromTheOriginalAnalysisVerdict() {
    List<PublishedAuditRun> received = new ArrayList<>();
    AuditReportPublisher publisher =
        new AuditReportPublisher(
            List.of(
                new ReportSinkRegistration(
                    "optional:broken",
                    false,
                    run -> {
                      throw new SQLException("SELECT 'secret'");
                    }),
                new ReportSinkRegistration(
                    "required:broken",
                    true,
                    run -> {
                      received.add(run);
                      throw new SQLException("password=secret");
                    }),
                new ReportSinkRegistration("required:healthy", true, received::add)));

    PublicationResult result = publisher.publish(AuditRunResult.pass(List.of()));

    assertThat(result.analysisOutcome()).isEqualTo(AuditOutcome.PASS);
    assertThat(result.hasRequiredFailures()).isTrue();
    assertThat(result.hasOptionalFailures()).isTrue();
    assertThat(result.deliveries())
        .extracting(PublicationResult.Delivery::status)
        .containsExactly(
            PublicationResult.Status.FAILED,
            PublicationResult.Status.FAILED,
            PublicationResult.Status.DELIVERED);
    assertThat(received).hasSize(2);
    assertThat(received.get(0)).isSameAs(received.get(1));
    assertThat(result.toString()).doesNotContain("SQLException", "SELECT", "secret", "password");
    assertThatThrownBy(() -> result.deliveries().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void optionalFailureDoesNotTurnARequiredPublicationIntoFailure() {
    PublicationResult result =
        new AuditReportPublisher(
                List.of(
                    new ReportSinkRegistration(
                        "optional",
                        false,
                        run -> {
                          throw new IllegalStateException("private detail");
                        })))
            .publish(AuditRunResult.fail(List.of()));

    assertThat(result.analysisOutcome()).isEqualTo(AuditOutcome.FAIL);
    assertThat(result.hasRequiredFailures()).isFalse();
    assertThat(result.hasOptionalFailures()).isTrue();
  }

  @Test
  void restoresInterruptionAndDoesNotStartFurtherPublications() {
    AtomicInteger calls = new AtomicInteger();
    AuditReportPublisher publisher =
        new AuditReportPublisher(
            List.of(
                new ReportSinkRegistration(
                    "optional:interrupt",
                    false,
                    run -> {
                      throw new InterruptedException("private detail");
                    }),
                new ReportSinkRegistration(
                    "required:later", true, run -> calls.incrementAndGet())));
    try {
      PublicationResult result = publisher.publish(AuditRunResult.pass(List.of()));

      assertThat(Thread.currentThread().isInterrupted()).isTrue();
      assertThat(calls).hasValue(0);
      assertThat(result.analysisOutcome()).isEqualTo(AuditOutcome.PASS);
      assertThat(result.hasRequiredFailures()).isTrue();
      assertThat(result.deliveries().get(1).status())
          .isEqualTo(PublicationResult.Status.NOT_ATTEMPTED);
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void doesNotSwallowVirtualMachineOrAssertionErrors() {
    AssertionError error = new AssertionError("sink bug");
    AuditReportPublisher publisher =
        new AuditReportPublisher(
            List.of(
                new ReportSinkRegistration(
                    "broken",
                    true,
                    run -> {
                      throw error;
                    })));

    assertThatThrownBy(() -> publisher.publish(AuditRunResult.pass(List.of()))).isSameAs(error);
  }

  @Test
  void snapshotsRegistrationsAndRejectsAmbiguousOrUnsafeIds() {
    List<ReportSinkRegistration> registrations = new ArrayList<>();
    registrations.add(new ReportSinkRegistration("company:report", true, run -> {}));
    AuditReportPublisher publisher = new AuditReportPublisher(registrations);
    registrations.clear();

    assertThat(publisher.publish(AuditRunResult.pass(List.of())).deliveries()).hasSize(1);
    assertThatThrownBy(() -> new ReportSinkRegistration("secret SELECT", true, run -> {}))
        .isInstanceOf(IllegalArgumentException.class);
    ReportSinkRegistration duplicate = new ReportSinkRegistration("duplicate", true, run -> {});
    assertThatThrownBy(() -> new AuditReportPublisher(List.of(duplicate, duplicate)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
