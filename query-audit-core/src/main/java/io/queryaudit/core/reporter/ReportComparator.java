package io.queryaudit.core.reporter;

import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.model.AuditIncompleteReason;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.provenance.ComparisonInputDifference;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Compares two {@code report.json} runs into a machine-readable resolution verdict (issue #167).
 *
 * <p>Every fix loop — human or automated — ends with the same question: <em>did my change resolve
 * the finding without introducing new ones?</em> The verdict answers it from the two reports alone:
 * which confirmed findings were resolved, which are new, which persist, and how the query profile
 * moved.
 *
 * <p>Reports from schema 1.7 onward match recorded finding IDs within each test. Earlier reports
 * use a legacy key from the available query, source method, rule, table, and column fields. The
 * verdict identifies which matching mode was used. Recorded IDs are never reconstructed from
 * redacted evidence and do not authenticate the report. Only <em>confirmed</em> findings gate fix
 * loops.
 *
 * <p><strong>Exit contract</strong> (CLI): {@code 0} for {@link AuditOutcome#PASS}, {@code 1} for
 * {@link AuditOutcome#FAIL}, and {@code 2} for {@link AuditOutcome#INCONCLUSIVE} or usage/parse
 * errors.
 *
 * @author haroya
 * @since 0.5.0
 */
public final class ReportComparator {

  private ReportComparator() {
    // static entry points only
  }

  /** One confirmed finding, reduced to its stable identity, matching key, and display fields. */
  public record Finding(
      String testId,
      String testClass,
      String testName,
      String type,
      String table,
      String detail,
      String key,
      String findingId,
      String column) {

    /** Retains the constructor introduced with stable test identities. */
    public Finding(
        String testId,
        String testClass,
        String testName,
        String type,
        String table,
        String detail,
        String key) {
      this(testId, testClass, testName, type, table, detail, key, null, null);
    }

    /** Retains the 0.5 constructor signature for ordinary constructor calls. */
    public Finding(
        String testClass, String testName, String type, String table, String detail, String key) {
      this(null, testClass, testName, type, table, detail, key);
    }
  }

  /** Identifies the finding-matching contract without claiming that report contents are trusted. */
  public record FindingIdentity(
      String mode, String baselineSchemaVersion, String candidateSchemaVersion) {

    public FindingIdentity {
      if (!Set.of("RECORDED", "LEGACY", "UNAVAILABLE").contains(mode)) {
        throw new IllegalArgumentException("Unknown finding identity mode");
      }
    }

    private static FindingIdentity unavailable() {
      return new FindingIdentity("UNAVAILABLE", null, null);
    }
  }

  /** Identifies an audited test using the fields available in the schema 1.x report envelope. */
  public record TestRef(String testId, String testClass, String testName) {

    /** Retains the 0.5 constructor signature for ordinary constructor calls. */
    public TestRef(String testClass, String testName) {
      this(null, testClass, testName);
    }
  }

  /** The comparison result; incomplete comparisons cannot produce a trustworthy success signal. */
  public record Verdict(
      List<Finding> resolved,
      List<Finding> newFindings,
      List<Finding> persisting,
      long queriesBefore,
      long queriesAfter,
      long executionTimeMsBefore,
      long executionTimeMsAfter,
      List<TestRef> missingTests,
      AuditOutcome outcome,
      List<AuditIncompleteReason> incompleteReasons,
      List<TestRef> unexpectedTests,
      List<ComparisonInputDifference> inputDifferences,
      FindingIdentity findingIdentity,
      List<TargetResolution> targetResolutions) {

    public Verdict {
      resolved = List.copyOf(resolved);
      newFindings = List.copyOf(newFindings);
      persisting = List.copyOf(persisting);
      missingTests = List.copyOf(missingTests);
      unexpectedTests = List.copyOf(unexpectedTests);
      inputDifferences = List.copyOf(inputDifferences);
      targetResolutions = List.copyOf(targetResolutions);
      Objects.requireNonNull(findingIdentity, "findingIdentity");
      AuditRunResult validated = new AuditRunResult(List.of(), outcome, incompleteReasons);
      incompleteReasons = validated.incompleteReasons();
    }

    /** Retains the finding-identity-aware constructor introduced before explicit targets. */
    public Verdict(
        List<Finding> resolved,
        List<Finding> newFindings,
        List<Finding> persisting,
        long queriesBefore,
        long queriesAfter,
        long executionTimeMsBefore,
        long executionTimeMsAfter,
        List<TestRef> missingTests,
        AuditOutcome outcome,
        List<AuditIncompleteReason> incompleteReasons,
        List<TestRef> unexpectedTests,
        List<ComparisonInputDifference> inputDifferences,
        FindingIdentity findingIdentity) {
      this(
          resolved,
          newFindings,
          persisting,
          queriesBefore,
          queriesAfter,
          executionTimeMsBefore,
          executionTimeMsAfter,
          missingTests,
          outcome,
          incompleteReasons,
          unexpectedTests,
          inputDifferences,
          findingIdentity,
          List.of());
    }

    /** Retains the comparison-input-aware constructor introduced before finding identities. */
    public Verdict(
        List<Finding> resolved,
        List<Finding> newFindings,
        List<Finding> persisting,
        long queriesBefore,
        long queriesAfter,
        long executionTimeMsBefore,
        long executionTimeMsAfter,
        List<TestRef> missingTests,
        AuditOutcome outcome,
        List<AuditIncompleteReason> incompleteReasons,
        List<TestRef> unexpectedTests,
        List<ComparisonInputDifference> inputDifferences) {
      this(
          resolved,
          newFindings,
          persisting,
          queriesBefore,
          queriesAfter,
          executionTimeMsBefore,
          executionTimeMsAfter,
          missingTests,
          outcome,
          incompleteReasons,
          unexpectedTests,
          inputDifferences,
          FindingIdentity.unavailable());
    }

    /** Retains the coverage-aware constructor introduced before comparison input checks. */
    public Verdict(
        List<Finding> resolved,
        List<Finding> newFindings,
        List<Finding> persisting,
        long queriesBefore,
        long queriesAfter,
        long executionTimeMsBefore,
        long executionTimeMsAfter,
        List<TestRef> missingTests,
        AuditOutcome outcome,
        List<AuditIncompleteReason> incompleteReasons,
        List<TestRef> unexpectedTests) {
      this(
          resolved,
          newFindings,
          persisting,
          queriesBefore,
          queriesAfter,
          executionTimeMsBefore,
          executionTimeMsAfter,
          missingTests,
          outcome,
          incompleteReasons,
          unexpectedTests,
          List.of());
    }

    /** Retains the outcome-aware constructor introduced before coverage manifests. */
    public Verdict(
        List<Finding> resolved,
        List<Finding> newFindings,
        List<Finding> persisting,
        long queriesBefore,
        long queriesAfter,
        long executionTimeMsBefore,
        long executionTimeMsAfter,
        List<TestRef> missingTests,
        AuditOutcome outcome,
        List<AuditIncompleteReason> incompleteReasons) {
      this(
          resolved,
          newFindings,
          persisting,
          queriesBefore,
          queriesAfter,
          executionTimeMsBefore,
          executionTimeMsAfter,
          missingTests,
          outcome,
          incompleteReasons,
          List.of());
    }

    /** Retains the original constructor for callers compiled against the 0.5.0 API. */
    public Verdict(
        List<Finding> resolved,
        List<Finding> newFindings,
        List<Finding> persisting,
        long queriesBefore,
        long queriesAfter,
        long executionTimeMsBefore,
        long executionTimeMsAfter) {
      this(
          resolved,
          newFindings,
          persisting,
          queriesBefore,
          queriesAfter,
          executionTimeMsBefore,
          executionTimeMsAfter,
          List.of(),
          newFindings.isEmpty() ? AuditOutcome.PASS : AuditOutcome.FAIL,
          List.of());
    }

    /** Retains the original complete/missing-tests constructor from the 0.5.x API. */
    public Verdict(
        List<Finding> resolved,
        List<Finding> newFindings,
        List<Finding> persisting,
        long queriesBefore,
        long queriesAfter,
        long executionTimeMsBefore,
        long executionTimeMsAfter,
        List<TestRef> missingTests) {
      this(
          resolved,
          newFindings,
          persisting,
          queriesBefore,
          queriesAfter,
          executionTimeMsBefore,
          executionTimeMsAfter,
          missingTests,
          missingTests.isEmpty()
              ? (newFindings.isEmpty() ? AuditOutcome.PASS : AuditOutcome.FAIL)
              : AuditOutcome.INCONCLUSIVE,
          missingTests.isEmpty()
              ? List.of()
              : List.of(AuditIncompleteReason.of(IncompleteReasonCode.EXPECTED_TEST_MISSING)));
    }

    /** Returns whether both report inputs support a trustworthy comparison. */
    public boolean complete() {
      return outcome != AuditOutcome.INCONCLUSIVE;
    }

    /** Explicit alias for the completeness gate, independent of target and regression results. */
    public boolean comparisonComplete() {
      return complete();
    }

    /** Whether the comparison observed no new confirmed findings; not itself a success verdict. */
    public boolean noNewRegressions() {
      return newFindings.isEmpty();
    }

    /** True when every requested target resolved, or no targets were requested. */
    public boolean allTargetsResolved() {
      return targetResolutions.stream()
          .allMatch(target -> target.status() == TargetResolution.Status.RESOLVED);
    }
  }

  /** Compares two envelope documents (the string content of two {@code report.json} files). */
  public static Verdict compare(String beforeJson, String afterJson) {
    return compare(beforeJson, afterJson, List.of());
  }

  /**
   * Also requires the selected baseline finding IDs to disappear from all categories of their
   * audited tests. Both inputs must record native finding IDs; legacy matching cannot prove this.
   * Unknown or ambiguous baseline targets are rejected as invalid requests.
   */
  public static Verdict compare(
      String beforeJson, String afterJson, Collection<String> requiredFindingIds) {
    List<String> requiredIds = ComparisonTargets.requested(requiredFindingIds);
    try {
      ComparisonEnvelope before = ComparisonEnvelopeReader.read(beforeJson);
      ComparisonEnvelope after = ComparisonEnvelopeReader.read(afterJson);
      return ReportComparisonEngine.compare(before, after, requiredIds);
    } catch (ComparisonEnvelopeReader.UnsupportedReportSchemaException e) {
      return ReportComparisonEngine.inconclusiveVerdict(
          new AuditIncompleteReason(IncompleteReasonCode.UNSUPPORTED_SCHEMA, e.getMessage()),
          requiredIds);
    }
  }

  /** Renders the verdict as JSON (the {@code verdict.json} contract). */
  public static String toJson(Verdict verdict) {
    return toJson(verdict, ReportRedaction.REDACTED);
  }

  /** Full diagnostic details require an explicit opt-in, including for legacy input reports. */
  public static String toJson(Verdict verdict, ReportRedaction redaction) {
    return ReportVerdictWriter.toJson(verdict, redaction);
  }

  /** One-screen console summary. */
  public static String toSummary(Verdict verdict) {
    return ReportVerdictWriter.toSummary(verdict);
  }

  /**
   * CLI entry point: {@code ReportComparator <before.json> <after.json> [verdict.json]
   * [--require-resolved <findingId>]...}. Prints the summary and optionally writes a verdict.
   */
  public static void main(String[] args) throws Exception {
    System.exit(run(args));
  }

  static int run(String[] args) {
    return ReportComparisonCommand.run(args);
  }

  static int exitCode(Verdict verdict) {
    return switch (verdict.outcome()) {
      case PASS -> 0;
      case FAIL -> 1;
      case INCONCLUSIVE -> 2;
    };
  }
}
