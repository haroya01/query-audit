package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.model.AuditCoverage;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.model.TestSelector;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class TargetResolutionComparisonTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final List<String> CATEGORIES =
      List.of("confirmedIssues", "infoIssues", "acknowledgedIssues");
  @TempDir Path directory;

  @Test
  void anExistingFindingPassesByDefaultButFailsWhenExplicitlyRequired() throws Exception {
    ObjectNode before = envelope(report("load", "orders"));
    String target = id(before, 0, 0);

    var defaultVerdict = ReportComparator.compare(before.toString(), before.toString());
    var targeted = compare(before, before, target);

    assertThat(defaultVerdict.outcome()).isEqualTo(AuditOutcome.PASS);
    assertThat(defaultVerdict.targetResolutions()).isEmpty();
    assertThat(defaultVerdict.allTargetsResolved()).isTrue();
    assertThat(targeted.outcome()).isEqualTo(AuditOutcome.FAIL);
    assertThat(targeted.noNewRegressions()).isTrue();
    assertThat(targeted.comparisonComplete()).isTrue();
    assertThat(targeted.allTargetsResolved()).isFalse();
    assertThat(targeted.targetResolutions())
        .containsExactly(
            new TargetResolution(target, "test:load", TargetResolution.Status.PERSISTING));
    assertThat(run(before, before, target)).isEqualTo(1);
    assertThat(ReportComparator.toSummary(targeted)).contains("TARGET PERSISTING", target);
  }

  @Test
  void resolvedTargetPassesEvenWhenAnUnselectedFindingPersists() throws Exception {
    ObjectNode before = envelope(report("load", "orders", "payments"));
    ObjectNode after = envelope(report("load", "payments"));
    String target = id(before, 0, 0);

    var verdict = compare(before, after, target);

    assertThat(verdict.outcome()).isEqualTo(AuditOutcome.PASS);
    assertThat(verdict.allTargetsResolved()).isTrue();
    assertThat(verdict.persisting()).hasSize(1);
    assertThat(run(before, after, target)).isZero();
    JsonNode document = JSON.readTree(Files.readString(directory.resolve("verdict.json")));
    assertThat(document.path("allTargetsResolved").asBoolean()).isTrue();
    assertThat(document.path("noNewRegressions").asBoolean()).isTrue();
    assertThat(document.path("comparisonComplete").asBoolean()).isTrue();
    assertThat(document.path("complete").asBoolean()).isTrue();
    assertThat(document.path("targetResolutions").get(0).path("findingId").asText())
        .isEqualTo(target);
    assertThat(document.path("targetResolutions").get(0).path("status").asText())
        .isEqualTo("RESOLVED");
  }

  @Test
  void allRequestedTargetsMustResolveAndRepeatedIdsAreDeduplicated() throws Exception {
    ObjectNode before = envelope(report("load", "orders", "payments"));
    ObjectNode after = envelope(report("load", "payments"));
    String orders = id(before, 0, 0);
    String payments = id(before, 0, 1);

    var verdict = compare(before, after, orders, payments, orders);

    assertThat(verdict.outcome()).isEqualTo(AuditOutcome.FAIL);
    assertThat(verdict.targetResolutions())
        .extracting(TargetResolution::status)
        .containsExactly(TargetResolution.Status.RESOLVED, TargetResolution.Status.PERSISTING);
    assertThat(run(before, after, orders, payments, orders)).isEqualTo(1);
    assertThat(run(before, envelope(report("load")), orders, payments)).isZero();
  }

  @Test
  void resolvedTargetDoesNotHideANewConfirmedRegression() throws Exception {
    ObjectNode before = envelope(report("load", "orders"));
    ObjectNode after = envelope(report("load", "payments"));

    var verdict = compare(before, after, id(before, 0, 0));

    assertThat(verdict.allTargetsResolved()).isTrue();
    assertThat(verdict.noNewRegressions()).isFalse();
    assertThat(verdict.comparisonComplete()).isTrue();
    assertThat(verdict.outcome()).isEqualTo(AuditOutcome.FAIL);
    assertThat(run(before, after, id(before, 0, 0))).isEqualTo(1);
  }

  @Test
  void candidatePolicyFailureIsPreservedEvenWithNoNewFindings() throws Exception {
    ObjectNode before = envelope(report("load", "orders"));
    ObjectNode after = envelope(report("load"));
    after.put("outcome", "FAIL");

    var verdict = compare(before, after, id(before, 0, 0));

    assertThat(verdict.allTargetsResolved()).isTrue();
    assertThat(verdict.noNewRegressions()).isTrue();
    assertThat(verdict.outcome()).isEqualTo(AuditOutcome.FAIL);
    assertThat(run(before, after, id(before, 0, 0))).isEqualTo(1);
  }

  @Test
  void aMissingTargetTestIsIncompleteNotResolved() throws Exception {
    ObjectNode before = envelope(report("load", "orders"), report("other"));
    ObjectNode after = envelope(report("other"));

    assertIncomplete(before, after, id(before, 0, 0), IncompleteReasonCode.EXPECTED_TEST_MISSING);
  }

  @ParameterizedTest
  @EnumSource(AuditCoverage.Gap.class)
  void skippedFailedAbortedAndOmittedTargetTestsAreIncomplete(AuditCoverage.Gap gap)
      throws Exception {
    ObjectNode before = covered(List.of(report("load", "orders")), completed("test:load"));
    boolean executed =
        gap == AuditCoverage.Gap.ABORTED
            || gap == AuditCoverage.Gap.TEST_FAILED
            || gap == AuditCoverage.Gap.AUDIT_MISSING;
    boolean partialAudit = gap == AuditCoverage.Gap.ABORTED || gap == AuditCoverage.Gap.TEST_FAILED;
    ObjectNode after =
        covered(
            partialAudit ? List.of(report("load")) : List.of(),
            new AuditCoverage.Test("test:load", true, executed, partialAudit, gap));

    assertIncomplete(before, after, id(before, 0, 0), IncompleteReasonCode.EXPECTED_TEST_MISSING);
  }

  @Test
  void weakerCandidateInputsCannotMakeATargetResolved() throws Exception {
    ObjectNode before = envelope(report("load", "orders"));
    ObjectNode after = envelope(report("load"));
    ObjectNode inputs = (ObjectNode) after.path("comparisonInputs").path("test:load");
    inputs.put("profile", "minimal");

    assertIncomplete(
        before, after, id(before, 0, 0), IncompleteReasonCode.INCOMPATIBLE_AUDIT_INPUTS);
  }

  @Test
  void redactionMismatchDoesNotLoseRequestedTargets() throws Exception {
    ObjectNode before = envelope(report("load", "orders"));
    ObjectNode after =
        (ObjectNode)
            JSON.readTree(
                ComparisonInputFixtures.json(
                    AuditRunResult.pass(List.of(report("load"))), ReportRedaction.FULL));

    assertIncomplete(
        before, after, id(before, 0, 0), IncompleteReasonCode.REPORT_REDACTION_MISMATCH);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void targetsRequireNativeIdsOnBothSides(boolean legacyBaseline) throws Exception {
    ObjectNode before = envelope(report("load", "orders"));
    String target = id(before, 0, 0);
    ObjectNode after = envelope(report("load"));
    ObjectNode legacy = legacyBaseline ? before : after;
    legacy.put("schemaVersion", "1.6.0");
    for (JsonNode report : legacy.path("reports")) {
      for (String category : CATEGORIES) {
        for (JsonNode finding : report.path(category)) {
          ((ObjectNode) finding).remove("findingId");
        }
      }
    }

    assertIncomplete(before, after, target, IncompleteReasonCode.UNSUPPORTED_SCHEMA);
    assertThat(ReportComparator.compare(before.toString(), after.toString()).outcome())
        .isEqualTo(AuditOutcome.PASS);
  }

  @Test
  void unsupportedSchemasReturnIncompleteTargetStatuses() throws Exception {
    ObjectNode before = envelope(report("load", "orders"));
    ObjectNode after = envelope(report("load"));
    after.put("schemaVersion", "2.0.0");
    assertIncomplete(before, after, id(before, 0, 0), IncompleteReasonCode.UNSUPPORTED_SCHEMA);
  }

  @ParameterizedTest
  @ValueSource(strings = {"infoIssues", "acknowledgedIssues"})
  void movingATargetToANonGatingCategoryIsNotAResolution(String category) throws Exception {
    ObjectNode before = envelope(report("load", "orders"));
    ObjectNode after = before.deepCopy();
    JsonNode finding = ((ArrayNode) after.path("reports").get(0).path("confirmedIssues")).remove(0);
    ((ArrayNode) after.path("reports").get(0).path(category)).add(finding);

    var verdict = compare(before, after, id(before, 0, 0));

    assertThat(verdict.noNewRegressions()).isTrue();
    assertThat(verdict.allTargetsResolved()).isFalse();
    assertThat(verdict.outcome()).isEqualTo(AuditOutcome.FAIL);
    assertThat(verdict.targetResolutions().get(0).status())
        .isEqualTo(TargetResolution.Status.PERSISTING);
  }

  @ParameterizedTest
  @ValueSource(strings = {"infoIssues", "acknowledgedIssues"})
  void nonConfirmedBaselineFindingsCanBeExplicitlyTargeted(String category) throws Exception {
    ObjectNode before = envelope(report("load", "orders"));
    String target = id(before, 0, 0);
    JsonNode finding =
        ((ArrayNode) before.path("reports").get(0).path("confirmedIssues")).remove(0);
    ((ArrayNode) before.path("reports").get(0).path(category)).add(finding);

    assertThat(compare(before, before, target).outcome()).isEqualTo(AuditOutcome.FAIL);
    assertThat(compare(before, envelope(report("load")), target).outcome())
        .isEqualTo(AuditOutcome.PASS);
  }

  @Test
  void aRecordedIdInAnotherTestCannotSatisfyOrBlockTheOriginalTarget() throws Exception {
    ObjectNode before = envelope(report("load", "orders"), report("other"));
    String target = id(before, 0, 0);
    ObjectNode after = envelope(report("load"), report("other", "orders"));
    ((ObjectNode) after.path("reports").get(1).path("confirmedIssues").get(0))
        .put("findingId", target);

    var verdict = compare(before, after, target);

    assertThat(verdict.allTargetsResolved()).isTrue();
    assertThat(verdict.noNewRegressions()).isFalse();
    assertThat(verdict.outcome()).isEqualTo(AuditOutcome.FAIL);
  }

  @Test
  void ambiguousBaselineIdsAreNotAcceptedAsOneTarget() throws Exception {
    ObjectNode before = envelope(report("load", "orders"), report("other", "orders"));
    String target = id(before, 0, 0);
    ((ObjectNode) before.path("reports").get(1).path("confirmedIssues").get(0))
        .put("findingId", target);

    assertThatThrownBy(() -> compare(before, before, target)).hasMessageContaining("ambiguous");
    assertThat(run(before, before, target)).isEqualTo(2);
    assertThat(directory.resolve("verdict.json")).doesNotExist();
  }

  @Test
  void unknownBaselineIdsAreUsageErrorsRatherThanAlreadyResolved() throws Exception {
    ObjectNode before = envelope(report("load", "orders"));
    String target = "qa-finding-v1:" + "a".repeat(64);

    assertThatThrownBy(() -> compare(before, before, target))
        .hasMessageContaining("not present in the baseline");
    assertThat(run(before, before, target)).isEqualTo(2);
    assertThat(directory.resolve("verdict.json")).doesNotExist();
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "not-an-id", "qa-finding-v2:abc", "qa-finding-v1:abc"})
  void malformedTargetsDoNotBecomeSuccessfulComparisons(String invalidId) throws Exception {
    ObjectNode before = envelope(report("load", "orders"));
    assertThatThrownBy(() -> compare(before, before, invalidId))
        .hasMessageContaining("--require-resolved expects");
    assertThat(run(before, before, invalidId)).isEqualTo(2);
  }

  @Test
  void cliSupportsOptionsBeforePathsAndEqualsSyntaxWithoutAnOutputFile() throws Exception {
    ObjectNode before = envelope(report("load", "orders"));
    Path baseline = directory.resolve("before.json");
    Path candidate = directory.resolve("after.json");
    Files.writeString(baseline, before.toString());
    Files.writeString(candidate, envelope(report("load")).toString());
    assertThat(
            ReportComparator.run(
                new String[] {
                  "--require-resolved=" + id(before, 0, 0),
                  baseline.toString(),
                  candidate.toString()
                }))
        .isZero();
    assertThat(
            ReportComparator.run(
                new String[] {baseline.toString(), candidate.toString(), "--require-resolved"}))
        .isEqualTo(2);
    assertThat(
            ReportComparator.run(
                new String[] {baseline.toString(), candidate.toString(), "--unknown"}))
        .isEqualTo(2);
  }

  private void assertIncomplete(
      ObjectNode before, ObjectNode after, String target, IncompleteReasonCode code)
      throws Exception {
    var verdict = compare(before, after, target);
    assertThat(verdict.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
    assertThat(verdict.comparisonComplete()).isFalse();
    assertThat(verdict.allTargetsResolved()).isFalse();
    assertThat(verdict.targetResolutions())
        .singleElement()
        .satisfies(
            result -> {
              assertThat(result.findingId()).isEqualTo(target);
              assertThat(result.status()).isEqualTo(TargetResolution.Status.INCOMPLETE);
            });
    assertThat(verdict.incompleteReasons()).extracting(reason -> reason.code()).contains(code);
    assertThat(run(before, after, target)).isEqualTo(2);
  }

  private int run(ObjectNode before, ObjectNode after, String... targets) throws Exception {
    Path baseline = directory.resolve("before.json");
    Path candidate = directory.resolve("after.json");
    Path output = directory.resolve("verdict.json");
    Files.writeString(baseline, before.toString());
    Files.writeString(candidate, after.toString());
    List<String> arguments =
        new ArrayList<>(List.of(baseline.toString(), candidate.toString(), output.toString()));
    for (String target : targets) {
      arguments.add("--require-resolved");
      arguments.add(target);
    }
    return ReportComparator.run(arguments.toArray(String[]::new));
  }

  private static ReportComparator.Verdict compare(
      ObjectNode before, ObjectNode after, String... targets) {
    return ReportComparator.compare(before.toString(), after.toString(), List.of(targets));
  }

  private static ObjectNode envelope(QueryAuditReport... reports) throws Exception {
    return (ObjectNode)
        JSON.readTree(ComparisonInputFixtures.json(AuditRunResult.pass(List.of(reports))));
  }

  private static ObjectNode covered(List<QueryAuditReport> reports, AuditCoverage.Test... tests)
      throws Exception {
    return (ObjectNode)
        JSON.readTree(
            ComparisonInputFixtures.json(
                AuditRunResult.pass(reports).withCoverage(new AuditCoverage(List.of(tests)))));
  }

  private static AuditCoverage.Test completed(String id) {
    return new AuditCoverage.Test(id, true, true, true, null);
  }

  private static QueryAuditReport report(String name, String... tables) {
    List<Issue> issues = new ArrayList<>();
    for (String table : tables) {
      issues.add(
          new Issue(
              IssueType.N_PLUS_ONE,
              Severity.ERROR,
              "select * from " + table + " where customer_id = ?",
              table,
              "customer_id",
              "Repeated query",
              "Batch it",
              "OrderService.load:10"));
    }
    return new QueryAuditReport("OrderTest", name, issues, List.of(), List.of(), List.of(), 1, 3, 0)
        .withTestIdentity("test:" + name, new TestSelector("junit-unique-id", "test:" + name));
  }

  private static String id(ObjectNode envelope, int report, int finding) {
    return envelope
        .path("reports")
        .get(report)
        .path("confirmedIssues")
        .get(finding)
        .path("findingId")
        .asText();
  }
}
