package io.queryaudit.core.reporter;

import io.queryaudit.core.model.AuditCoverage;
import io.queryaudit.core.model.AuditIncompleteReason;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.provenance.ComparisonInputCompatibility;
import io.queryaudit.core.provenance.ComparisonInputDifference;
import io.queryaudit.core.provenance.ComparisonInputs;
import io.queryaudit.core.reporter.ComparisonEnvelope.ReportedFinding;
import io.queryaudit.core.reporter.ComparisonEnvelope.TestReport;
import io.queryaudit.core.reporter.ReportComparator.Finding;
import io.queryaudit.core.reporter.ReportComparator.FindingIdentity;
import io.queryaudit.core.reporter.ReportComparator.TestRef;
import io.queryaudit.core.reporter.ReportComparator.Verdict;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Compares validated report data without JSON parsing, schema-field lookups, or output I/O. */
final class ReportComparisonEngine {
  private ReportComparisonEngine() {}

  private record LegacyRef(String testClass, String testName) {}

  private record ComparedFinding(Finding finding, String testIdentity) {}

  static Verdict compare(
      ComparisonEnvelope beforeEnvelope,
      ComparisonEnvelope afterEnvelope,
      List<String> requiredIds) {
    if (beforeEnvelope.redaction() != afterEnvelope.redaction()) {
      return inconclusiveVerdict(
          new AuditIncompleteReason(
              IncompleteReasonCode.REPORT_REDACTION_MISMATCH,
              "Reports use different redaction modes; regenerate both reports with the same mode"),
          requiredIds);
    }

    if (!requiredIds.isEmpty()
        && (!beforeEnvelope.hasFindingIds() || !afterEnvelope.hasFindingIds())) {
      return inconclusiveVerdict(
          new AuditIncompleteReason(
              IncompleteReasonCode.UNSUPPORTED_SCHEMA,
              "Target resolution requires recorded finding IDs in both reports (schema 1.7 or later)"),
          requiredIds);
    }

    List<TestReport> beforeReports = beforeEnvelope.reports();
    List<TestReport> afterReports = afterEnvelope.reports();
    Map<TestReport, String> beforeIdentities = comparisonIdentities(beforeReports, afterReports);
    Map<TestReport, String> afterIdentities = comparisonIdentities(afterReports, beforeReports);
    boolean recordedIdentity = beforeEnvelope.hasFindingIds() && afterEnvelope.hasFindingIds();
    List<ComparedFinding> before =
        confirmedFindings(beforeEnvelope, beforeIdentities, recordedIdentity);
    List<ComparedFinding> after =
        confirmedFindings(afterEnvelope, afterIdentities, recordedIdentity);

    Set<String> beforeKeys = new LinkedHashSet<>();
    before.forEach(f -> beforeKeys.add(f.finding().key()));
    Set<String> afterKeys = new LinkedHashSet<>();
    after.forEach(f -> afterKeys.add(f.finding().key()));

    Map<String, TestRef> beforeTests = auditedTests(beforeReports, beforeIdentities);
    Map<String, TestRef> afterTests = auditedTests(afterReports, afterIdentities);
    List<TestRef> missingTests = missingTests(beforeTests, afterTests, afterEnvelope.coverage());
    Set<String> unexpectedIds = unexpectedIds(afterEnvelope.coverage());
    List<TestRef> unexpectedTests =
        afterTests.entrySet().stream()
            .filter(
                test ->
                    !beforeTests.containsKey(test.getKey())
                        || unexpectedIds.contains(test.getValue().testId()))
            .map(Map.Entry::getValue)
            .toList();
    Set<String> incompleteIds = coverageGapIds(beforeEnvelope.coverage());
    incompleteIds.addAll(coverageGapIds(afterEnvelope.coverage()));
    boolean sameManifest =
        Objects.equals(
            expectedIds(beforeEnvelope.coverage()), expectedIds(afterEnvelope.coverage()));
    List<ComparisonInputDifference> inputDifferences =
        compareInputs(beforeEnvelope, afterEnvelope, beforeTests, afterTests);

    List<Finding> resolved =
        before.stream()
            .filter(f -> inputDifferences.isEmpty())
            .filter(f -> sameManifest && !incompleteIds.contains(f.finding().testId()))
            .filter(f -> afterTests.containsKey(f.testIdentity()))
            .filter(f -> !afterKeys.contains(f.finding().key()))
            .map(ComparedFinding::finding)
            .toList();
    List<Finding> fresh =
        after.stream()
            .filter(f -> !beforeKeys.contains(f.finding().key()))
            .map(ComparedFinding::finding)
            .toList();
    List<Finding> persisting =
        after.stream()
            .filter(f -> beforeKeys.contains(f.finding().key()))
            .map(ComparedFinding::finding)
            .toList();

    List<AuditIncompleteReason> incompleteReasons = new ArrayList<>();
    incompleteReasons.addAll(beforeEnvelope.incompleteReasons());
    incompleteReasons.addAll(afterEnvelope.incompleteReasons());
    if (inputDifferences.stream().anyMatch(ReportComparisonEngine::unavailableInput)) {
      incompleteReasons.add(
          AuditIncompleteReason.of(IncompleteReasonCode.COMPARISON_INPUTS_UNAVAILABLE));
    }
    if (inputDifferences.stream().anyMatch(difference -> !unavailableInput(difference))) {
      incompleteReasons.add(
          AuditIncompleteReason.of(IncompleteReasonCode.INCOMPATIBLE_AUDIT_INPUTS));
    }
    if (!sameManifest) {
      incompleteReasons.add(
          new AuditIncompleteReason(
              IncompleteReasonCode.COVERAGE_MANIFEST_MISMATCH,
              "Reports do not declare the same expected-test manifest."));
    }
    if (!missingTests.isEmpty()) {
      incompleteReasons.add(AuditIncompleteReason.of(IncompleteReasonCode.EXPECTED_TEST_MISSING));
    }
    List<TargetResolution> targets =
        requiredIds.isEmpty()
            ? List.of()
            : ComparisonTargets.evaluate(
                requiredIds,
                findingIdsByTest(beforeReports),
                findingIdsByTest(afterReports),
                incompleteReasons.isEmpty());
    boolean unresolvedTarget =
        targets.stream().anyMatch(target -> target.status() != TargetResolution.Status.RESOLVED);
    AuditRunResult comparisonResult =
        AuditRunResult.determine(
            List.of(),
            afterEnvelope.outcome() == AuditOutcome.FAIL || !fresh.isEmpty() || unresolvedTarget,
            incompleteReasons);

    return new Verdict(
        resolved,
        fresh,
        persisting,
        beforeReports.stream().mapToLong(TestReport::totalQueries).sum(),
        afterReports.stream().mapToLong(TestReport::totalQueries).sum(),
        beforeReports.stream().mapToLong(TestReport::executionTimeMs).sum(),
        afterReports.stream().mapToLong(TestReport::executionTimeMs).sum(),
        missingTests,
        comparisonResult.outcome(),
        comparisonResult.incompleteReasons(),
        unexpectedTests,
        inputDifferences,
        new FindingIdentity(
            recordedIdentity ? "RECORDED" : "LEGACY",
            beforeEnvelope.schemaVersion().text(),
            afterEnvelope.schemaVersion().text()),
        targets);
  }

  private static Map<String, Set<String>> findingIdsByTest(List<TestReport> reports) {
    Map<String, Set<String>> findings = new LinkedHashMap<>();
    for (TestReport report : reports) {
      Set<String> ids = new LinkedHashSet<>();
      for (List<ReportedFinding> category :
          List.of(report.confirmed(), report.info(), report.acknowledged())) {
        for (ReportedFinding finding : category) {
          ids.add(finding.findingId());
        }
      }
      findings.put(report.testId(), ids);
    }
    return findings;
  }

  private static List<ComparisonInputDifference> compareInputs(
      ComparisonEnvelope baseline,
      ComparisonEnvelope candidate,
      Map<String, TestRef> beforeTests,
      Map<String, TestRef> afterTests) {
    Set<String> identities = new LinkedHashSet<>(beforeTests.keySet());
    identities.addAll(afterTests.keySet());
    if (identities.isEmpty()) {
      return List.of(new ComparisonInputDifference(null, "comparisonInputs", null, null));
    }
    List<ComparisonInputDifference> differences = new ArrayList<>();
    for (String identity : identities) {
      TestRef before = beforeTests.get(identity);
      TestRef after = afterTests.get(identity);
      ComparisonInputs previous =
          before == null || before.testId() == null
              ? null
              : baseline.comparisonInputs().get(before.testId());
      ComparisonInputs current =
          after == null || after.testId() == null
              ? null
              : candidate.comparisonInputs().get(after.testId());
      String testId = after != null ? after.testId() : before.testId();
      boolean presentInBothRuns = before != null && after != null;
      if (presentInBothRuns) {
        differences.addAll(ComparisonInputCompatibility.compare(testId, previous, current));
      } else {
        ComparisonInputs present = before != null ? previous : current;
        differences.addAll(ComparisonInputCompatibility.compare(testId, present, present));
      }
    }
    return List.copyOf(differences);
  }

  private static boolean unavailableInput(ComparisonInputDifference difference) {
    return difference.kind() == ComparisonInputDifference.Kind.UNAVAILABLE;
  }

  static Verdict inconclusiveVerdict(AuditIncompleteReason reason, List<String> requiredIds) {
    return new Verdict(
        List.of(),
        List.of(),
        List.of(),
        0,
        0,
        0,
        0,
        List.of(),
        AuditOutcome.INCONCLUSIVE,
        List.of(reason),
        List.of(),
        List.of(),
        new FindingIdentity("UNAVAILABLE", null, null),
        ComparisonTargets.unavailable(requiredIds));
  }

  private static Map<TestReport, String> comparisonIdentities(
      List<TestReport> reports, List<TestReport> otherReports) {
    validateLegacyMatches(reports, otherReports);

    Set<String> otherStableIds = new LinkedHashSet<>();
    Set<LegacyRef> otherLegacyIds = new LinkedHashSet<>();
    for (TestReport other : otherReports) {
      if (other.hasStableId()) {
        otherStableIds.add(other.testId());
      } else {
        otherLegacyIds.add(legacyRef(other.ref()));
      }
    }

    Map<TestReport, String> identities = new IdentityHashMap<>();
    for (TestReport report : reports) {
      if (!report.hasStableId()) {
        identities.put(report, legacyIdentity(report.ref()));
        continue;
      }

      boolean stableMatch = otherStableIds.contains(report.testId());
      boolean legacyMatch = !stableMatch && otherLegacyIds.contains(legacyRef(report.ref()));
      identities.put(
          report, legacyMatch ? legacyIdentity(report.ref()) : stableIdentity(report.testId()));
    }
    return identities;
  }

  private static void validateLegacyMatches(
      List<TestReport> reports, List<TestReport> otherReports) {
    Map<LegacyRef, Integer> otherIdentityCounts = new LinkedHashMap<>();
    for (TestReport other : otherReports) {
      otherIdentityCounts.merge(legacyRef(other.ref()), 1, Integer::sum);
    }
    for (TestReport report : reports) {
      if (report.hasStableId()) {
        continue;
      }
      if (otherIdentityCounts.getOrDefault(legacyRef(report.ref()), 0) > 1) {
        throw ambiguousLegacyIdentity(report.ref());
      }
    }
  }

  private static IllegalArgumentException ambiguousLegacyIdentity(TestRef test) {
    return invalidEnvelope(
        "legacy test identity is ambiguous for "
            + test.testClass()
            + "."
            + test.testName()
            + "; regenerate the 0.5 report with QueryAudit 0.6+");
  }

  private static String stableIdentity(String testId) {
    return "stable:" + testId;
  }

  private static String legacyIdentity(TestRef ref) {
    return "legacy:" + lengthPrefixed(ref.testClass()) + lengthPrefixed(ref.testName());
  }

  private static LegacyRef legacyRef(TestRef ref) {
    return new LegacyRef(ref.testClass(), ref.testName());
  }

  private static String lengthPrefixed(String value) {
    return value == null ? "-:" : value.length() + ":" + value;
  }

  private static List<ComparedFinding> confirmedFindings(
      ComparisonEnvelope envelope, Map<TestReport, String> identities, boolean recordedIdentity) {
    List<ComparedFinding> findings = new ArrayList<>();
    for (TestReport parsedReport : envelope.reports()) {
      String testClass = parsedReport.ref().testClass();
      String testName = parsedReport.ref().testName();
      String testIdentity = identities.get(parsedReport);
      Set<String> legacyKeys = new LinkedHashSet<>();
      for (ReportedFinding issue : parsedReport.confirmed()) {
        String type = issue.type();
        String findingId = envelope.hasFindingIds() ? issue.findingId() : null;
        String column = issue.column();
        String key =
            recordedIdentity
                ? "recorded:" + lengthPrefixed(testIdentity) + findingId
                : FindingId.legacyKey(
                    testIdentity,
                    type,
                    issue.query(),
                    issue.sourceLocation(),
                    issue.table(),
                    column);
        if (envelope.hasFindingIds() && !recordedIdentity && !legacyKeys.add(key)) {
          throw invalidEnvelope(
              "legacy finding identity is ambiguous; regenerate both reports with schema 1.7 or later");
        }
        findings.add(
            new ComparedFinding(
                new Finding(
                    parsedReport.testId(),
                    testClass,
                    testName,
                    type,
                    issue.table(),
                    issue.detail(),
                    key,
                    findingId,
                    column),
                testIdentity));
      }
    }
    return findings;
  }

  private static Map<String, TestRef> auditedTests(
      List<TestReport> reports, Map<TestReport, String> identities) {
    Map<String, TestRef> tests = new LinkedHashMap<>();
    for (TestReport report : reports) {
      tests.put(identities.get(report), report.ref());
    }
    return tests;
  }

  private static List<TestRef> missingTests(
      Map<String, TestRef> before, Map<String, TestRef> after, AuditCoverage coverage) {
    Map<String, TestRef> missing = new LinkedHashMap<>();
    before.forEach(
        (id, test) -> {
          if (!after.containsKey(id)) {
            missing.put(id, test);
          }
        });
    for (String id : coverageGapIds(coverage)) {
      String key = stableIdentity(id);
      TestRef test = before.getOrDefault(key, after.getOrDefault(key, new TestRef(id, null, id)));
      missing.putIfAbsent(key, test);
    }
    return List.copyOf(missing.values());
  }

  private static Set<String> coverageGapIds(AuditCoverage coverage) {
    Set<String> ids = new LinkedHashSet<>();
    if (coverage != null) {
      coverage.tests().stream()
          .filter(test -> test.gap() != null)
          .forEach(test -> ids.add(test.testId()));
    }
    return ids;
  }

  private static Set<String> unexpectedIds(AuditCoverage coverage) {
    Set<String> ids = new LinkedHashSet<>();
    if (coverage != null) {
      coverage.tests().stream()
          .filter(test -> !test.expected())
          .forEach(test -> ids.add(test.testId()));
    }
    return ids;
  }

  private static Set<String> expectedIds(AuditCoverage coverage) {
    if (coverage == null) {
      return null;
    }
    Set<String> ids = new LinkedHashSet<>();
    coverage.tests().stream()
        .filter(AuditCoverage.Test::expected)
        .forEach(test -> ids.add(test.testId()));
    return ids;
  }

  private static IllegalArgumentException invalidEnvelope(String reason) {
    return new IllegalArgumentException(
        "not a supported report.json envelope — " + reason + " (schema 1.0+ envelope)");
  }
}
