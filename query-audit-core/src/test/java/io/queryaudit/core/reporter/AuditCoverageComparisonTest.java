package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.queryaudit.core.model.AuditCoverage;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.Severity;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AuditCoverageComparisonTest {

  @Test
  void completeCoverageAllowsAResolvedFinding() {
    String before = covered(List.of(report("a", true)), completed("a"));
    String after = covered(List.of(report("a", false)), completed("a"));

    ReportComparator.Verdict verdict = ReportComparator.compare(before, after);

    assertThat(verdict.outcome()).isEqualTo(AuditOutcome.PASS);
    assertThat(verdict.resolved()).hasSize(1);
    assertThat(verdict.missingTests()).isEmpty();
    assertThat(verdict.unexpectedTests()).isEmpty();
  }

  @Test
  void skippedExpectedTestIsMissingRatherThanResolved(@TempDir Path directory) throws Exception {
    String before = covered(List.of(report("a", true)), completed("a"));
    String after =
        covered(
            List.of(), new AuditCoverage.Test("a", true, false, false, AuditCoverage.Gap.SKIPPED));

    ReportComparator.Verdict verdict = ReportComparator.compare(before, after);

    assertThat(verdict.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
    assertThat(verdict.resolved()).isEmpty();
    assertThat(verdict.missingTests())
        .extracting(ReportComparator.TestRef::testId)
        .containsExactly("a");
    Path beforeFile = directory.resolve("before.json");
    Path afterFile = directory.resolve("after.json");
    Files.writeString(beforeFile, before);
    Files.writeString(afterFile, after);
    assertThat(ReportComparator.run(new String[] {beforeFile.toString(), afterFile.toString()}))
        .isEqualTo(2);
  }

  @Test
  void aPartialReportFromAnAbortedTestDoesNotResolveFindings() {
    String before = covered(List.of(report("a", true)), completed("a"));
    String after =
        covered(
            List.of(report("a", false)),
            new AuditCoverage.Test("a", true, true, true, AuditCoverage.Gap.ABORTED));

    ReportComparator.Verdict verdict = ReportComparator.compare(before, after);

    assertThat(verdict.resolved()).isEmpty();
    assertThat(verdict.missingTests())
        .extracting(ReportComparator.TestRef::testId)
        .containsExactly("a");
    assertThat(verdict.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
  }

  @Test
  void additionalAuditsAreListedSeparatelyFromNewFindings() {
    String before = covered(List.of(report("a", false)), completed("a"));
    String after =
        covered(
            List.of(report("a", false), report("extra", false)),
            completed("a"),
            new AuditCoverage.Test("extra", false, true, true, null));

    ReportComparator.Verdict verdict = ReportComparator.compare(before, after);

    assertThat(verdict.outcome()).isEqualTo(AuditOutcome.PASS);
    assertThat(verdict.newFindings()).isEmpty();
    assertThat(verdict.unexpectedTests())
        .extracting(ReportComparator.TestRef::testId)
        .containsExactly("extra");
    assertThat(ReportComparator.toJson(verdict)).contains("\"unexpectedTests\": [");
  }

  @Test
  void changingOrRemovingTheManifestIsNotAnEquivalentRun() {
    String before = covered(List.of(report("a", true)), completed("a"));
    String changed = covered(List.of(report("b", false)), completed("b"));
    String unverified =
        JsonReporter.toRunEnvelopeJson(AuditRunResult.pass(List.of(report("a", false))));

    for (String after : List.of(changed, unverified)) {
      ReportComparator.Verdict verdict = ReportComparator.compare(before, after);
      assertThat(verdict.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
      assertThat(verdict.resolved()).isEmpty();
      assertThat(verdict.incompleteReasons())
          .extracting(reason -> reason.code())
          .contains(IncompleteReasonCode.COVERAGE_MANIFEST_MISMATCH);
    }
  }

  @Test
  void rejectsTotalsThatContradictTheCoverageEntries() {
    String valid = covered(List.of(report("a", false)), completed("a"));
    String inconsistent = valid.replace("\"expected\": 1,", "\"expected\": 0,");

    assertThatThrownBy(() -> ReportComparator.compare(valid, inconsistent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("coverage.expected");
  }

  @Test
  void rejectsPassWithCoverageGapsEvenWhenReasonsHaveBeenRemoved() throws Exception {
    String incomplete =
        covered(
            List.of(), new AuditCoverage.Test("a", true, false, false, AuditCoverage.Gap.SKIPPED));
    ObjectNode edited = (ObjectNode) new ObjectMapper().readTree(incomplete);
    edited.put("outcome", "PASS");
    edited.putArray("incompleteReasons");

    assertThatThrownBy(() -> ReportComparator.compare(incomplete, edited.toString()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("coverage gaps");
  }

  @Test
  void rejectsCoverageThatInventsTheReportedTestId() throws Exception {
    String valid = covered(List.of(report("a", false)), completed("a"));
    ObjectNode edited = (ObjectNode) new ObjectMapper().readTree(valid);
    ((ObjectNode) edited.path("coverage").path("tests").get(0)).put("testId", "invented");

    assertThatThrownBy(() -> ReportComparator.compare(valid, edited.toString()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("must match");
  }

  private static AuditCoverage.Test completed(String id) {
    return new AuditCoverage.Test(id, true, true, true, null);
  }

  private static String covered(List<QueryAuditReport> reports, AuditCoverage.Test... tests) {
    return JsonReporter.toRunEnvelopeJson(
        AuditRunResult.pass(reports).withCoverage(new AuditCoverage(List.of(tests))));
  }

  private static QueryAuditReport report(String id, boolean finding) {
    List<Issue> issues =
        finding
            ? List.of(
                new Issue(
                    IssueType.SELECT_ALL,
                    Severity.WARNING,
                    "SELECT * FROM orders",
                    "orders",
                    null,
                    "Explicit columns are easier to maintain",
                    "List required columns"))
            : List.of();
    return new QueryAuditReport("OrderTest", id, issues, List.of(), List.of(), 0, 0, 0)
        .withTestIdentity(id, null);
  }
}
