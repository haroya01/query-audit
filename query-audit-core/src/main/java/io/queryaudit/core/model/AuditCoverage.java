package io.queryaudit.core.model;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** The expected test manifest reconciled with one JUnit execution. */
public record AuditCoverage(List<Test> tests) {

  public AuditCoverage {
    tests = List.copyOf(tests);
    Set<String> identities = new HashSet<>();
    for (Test test : tests) {
      if (!identities.add(test.testId())) {
        throw new IllegalArgumentException("Duplicate coverage test ID: " + test.testId());
      }
    }
  }

  /** Why an expected test did not provide complete audit evidence. */
  public enum Gap {
    NOT_DISCOVERED,
    NOT_EXECUTED,
    SETUP_FAILED,
    SKIPPED,
    ABORTED,
    TEST_FAILED,
    AUDIT_MISSING
  }

  /** A stable test identity, its observed execution, and any missing audit evidence. */
  public record Test(String testId, boolean expected, boolean executed, boolean audited, Gap gap) {

    public Test {
      Objects.requireNonNull(testId, "testId");
      if (testId.isBlank()) {
        throw new IllegalArgumentException("testId must not be blank");
      }
      if (!expected && gap != null) {
        throw new IllegalArgumentException("Only expected tests can have a coverage gap");
      }
      if (audited && !executed) {
        throw new IllegalArgumentException("An audited test must have executed");
      }
      if (expected && gap == null && !audited) {
        throw new IllegalArgumentException("An expected test without an audit requires a gap");
      }
      if (gap == Gap.AUDIT_MISSING && audited) {
        throw new IllegalArgumentException("AUDIT_MISSING cannot have an audit");
      }
      if (gap != null) {
        boolean executionRequired =
            gap == Gap.ABORTED || gap == Gap.TEST_FAILED || gap == Gap.AUDIT_MISSING;
        if (executed != executionRequired) {
          throw new IllegalArgumentException("Execution state does not match coverage gap " + gap);
        }
      }
    }
  }

  public long expected() {
    return tests.stream().filter(Test::expected).count();
  }

  public long executed() {
    return tests.stream().filter(Test::executed).count();
  }

  public long skipped() {
    return tests.stream().filter(test -> test.gap() == Gap.SKIPPED).count();
  }

  /** Number of per-test audit reports, including reports from aborted or failed tests. */
  public long audited() {
    return tests.stream().filter(Test::audited).count();
  }

  /** Expected tests that did not supply complete, usable audit evidence. */
  public long failedToAudit() {
    return tests.stream().filter(test -> test.gap() != null).count();
  }

  public List<AuditIncompleteReason> incompleteReasons() {
    return tests.stream()
        .filter(test -> test.gap() != null)
        .map(
            test ->
                new AuditIncompleteReason(
                    IncompleteReasonCode.EXPECTED_TEST_MISSING, test.testId() + ": " + test.gap()))
        .toList();
  }
}
