package io.queryaudit.core.extension;

import static io.queryaudit.core.extension.OpenFindingContractTest.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.config.RuleProfile;
import io.queryaudit.core.detector.DetectionRule;
import io.queryaudit.core.detector.NPlusOneDetector;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.LifecyclePhase;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.reporter.FindingId;
import io.queryaudit.core.reporter.JsonReporter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class OpenAuditRuleAnalyzerTest {
  @TempDir Path tempDir;

  @Test
  void openRulesAreAdditiveAndTheirFindingsParticipateInTheReportVerdict() {
    AuditRule rule = budgetRule();
    QueryAuditAnalyzer analyzer = analyzer(config().build(), rule);
    QueryAuditReport report = analyze(analyzer);

    assertThat(analyzer.getRules()).anyMatch(NPlusOneDetector.class::isInstance);
    assertThat(analyzer.getAuditRules()).containsExactly(rule);
    assertThat(analyzer.getRuleDescriptors()).containsExactly(rule.descriptor());
    assertThat(report.getCustomConfirmedFindings()).containsExactly(finding(KIND));
    assertThat(report.getConfirmedFindings()).contains(finding(KIND));
    assertThat(report.getConfirmedIssues()).noneMatch(issue -> issue.detail().equals("detail"));
    assertThat(report.hasConfirmedIssues()).isTrue();
    assertThat(analyzer.hasCompleteRuleInputs()).isFalse();
    assertThatThrownBy(() -> analyzer.getAuditRules().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> analyzer.getRuleDescriptors().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void disabledKindsAndProfilesPreventExecutionAndDoNotPoisonCompleteness() {
    AtomicInteger calls = new AtomicInteger();
    AuditRule rule =
        rule(
            descriptor("acme:budget", Set.of(KIND)),
            ignored -> {
              calls.incrementAndGet();
              return List.of(finding(KIND));
            });
    for (QueryAuditConfig excluded :
        List.of(
            config().addDisabledRule(KIND.value()).build(),
            config().ruleProfile(RuleProfile.MINIMAL).build(),
            config().addEnabledRule(KIND.value()).addDisabledRule(KIND.value()).build())) {
      QueryAuditAnalyzer analyzer = analyzer(excluded, rule);
      assertThat(analyze(analyzer).getCustomConfirmedFindings()).isEmpty();
      assertThat(analyzer.hasCompleteRuleInputs()).isTrue();
      assertThat(analyzer.getAuditRules()).isEmpty();
    }
    assertThat(calls).hasValue(0);
    QueryAuditAnalyzer enabled =
        analyzer(
            config().ruleProfile(RuleProfile.MINIMAL).addEnabledRule(KIND.value()).build(), rule);
    assertThat(analyze(enabled).getCustomConfirmedFindings()).hasSize(1);
    assertThat(calls).hasValue(1);
    assertThat(enabled.hasCompleteRuleInputs()).isFalse();
  }

  @Test
  void selectionUsesFindingKindsNotRuleIdentityOrAssemblyIds() {
    QueryAuditAnalyzer analyzer =
        analyzer(
            config().addDisabledRule("acme:budget").addDisabledRule("assembly").build(),
            budgetRule());
    assertThat(analyze(analyzer).getCustomConfirmedFindings()).hasSize(1);
  }

  @Test
  void multiKindRulesCannotReturnDisabledKindsThroughAnEnabledSibling() {
    FindingKindId second = FindingKindId.of("acme:secondary");
    AuditRule multi =
        rule(
            descriptor("acme:multi", Set.of(KIND, second)),
            ignored -> List.of(finding(KIND), finding(second)));
    QueryAuditAnalyzer analyzer = analyzer(config().addDisabledRule(KIND.value()).build(), multi);
    assertThat(analyze(analyzer).getCustomConfirmedFindings()).containsExactly(finding(second));
  }

  @Test
  void severitySuppressionAndExactSqlBaselineUseTheSameHostPolicy() throws Exception {
    QueryAuditReport info =
        analyze(
            analyzer(
                config().addSeverityOverride(KIND.value(), Severity.INFO).build(), budgetRule()));
    assertThat(info.getCustomInfoFindings())
        .containsExactly(finding(KIND).withSeverity(Severity.INFO));
    assertThat(info.getCustomConfirmedFindings()).isEmpty();
    QueryAuditReport suppressed =
        analyze(analyzer(config().addSuppressPattern(KIND.value()).build(), budgetRule()));
    assertThat(suppressed.getCustomConfirmedFindings()).isEmpty();
    assertThat(suppressed.getCustomInfoFindings()).isEmpty();

    Path baseline = tempDir.resolve("baseline");
    Files.writeString(baseline, KIND.value() + " | users | id | team | accepted | " + SQL);
    QueryAuditAnalyzer acknowledged =
        QueryAuditAnalyzer.withExtensions(
            config().addSeverityOverride(KIND.value(), Severity.ERROR).build(),
            baseline,
            AuditExtensions.builder().auditRule("assembly", budgetRule()).build());
    QueryAuditReport report = analyze(acknowledged);
    assertThat(report.getCustomAcknowledgedFindings())
        .containsExactly(finding(KIND).withSeverity(Severity.ERROR));
    assertThat(report.getCustomConfirmedFindings()).isEmpty();
    assertThat(report.getAcknowledgedCount()).isZero(); // legacy count contract remains intact
    assertThat(report.getAcknowledgedFindings())
        .contains(finding(KIND).withSeverity(Severity.ERROR));

    Files.writeString(
        baseline, KIND.value() + " | users | id | team | accepted | SELECT name FROM users");
    assertThat(
            analyze(
                    QueryAuditAnalyzer.withExtensions(
                        config().build(),
                        baseline,
                        AuditExtensions.builder().auditRule("assembly", budgetRule()).build()))
                .getCustomConfirmedFindings())
        .hasSize(1);
  }

  @Test
  void evidenceIsHostScopedAndInputListsCannotBeChangedByRules() {
    AtomicReference<RuleContext> received = new AtomicReference<>();
    AuditRule rule =
        rule(
            descriptor("acme:budget", Set.of(KIND)),
            context -> {
              received.set(context);
              return List.of();
            });
    QueryAuditAnalyzer analyzer = analyzer(config().addSuppressQuery("SELECT 42").build(), rule);
    QueryRecord setup =
        new QueryRecord(
            "SELECT id FROM fixtures",
            "select id from fixtures",
            1,
            1,
            null,
            0,
            LifecyclePhase.SETUP);
    List<QueryRecord> input =
        new ArrayList<>(List.of(setup, new QueryRecord("SELECT 42", 1, 1, null), QUERY));
    analyzer.analyze("scope", input, new IndexMetadata(Map.of()));
    assertThat(received.get().queries()).containsExactly(QUERY);
    assertThatThrownBy(() -> received.get().queries().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThat(input).hasSize(3);
    analyzer(config().includeSetupQueries(true).build(), rule)
        .analyze("scope", List.of(setup, QUERY), null);
    assertThat(received.get().queries()).containsExactly(setup, QUERY);
  }

  @Test
  void openRulesReceiveEmptyEvidenceButDisabledAnalyzerAndLegacyRulesDoNotExecute() {
    AtomicInteger calls = new AtomicInteger();
    AuditRule rule =
        rule(
            descriptor("acme:budget", Set.of(KIND)),
            ignored -> {
              calls.incrementAndGet();
              return List.of();
            });
    QueryAuditAnalyzer analyzer = analyzer(config().build(), rule);
    analyzer.analyze("empty", List.of(), null);
    analyzer.analyze("null", null, null);
    analyzer(config().enabled(false).build(), rule).analyze("disabled", List.of(QUERY), null);
    assertThat(calls).hasValue(2);
    AtomicInteger legacyCalls = new AtomicInteger();
    DetectionRule legacy =
        (queries, indexes) -> {
          legacyCalls.incrementAndGet();
          return List.of();
        };
    new QueryAuditAnalyzer(config().build(), List.of(), List.of(legacy))
        .analyze("empty", List.of(), null);
    assertThat(legacyCalls).hasValue(0);
    // Existing detectors still receive an empty scoped list when only suppressed/setup input
    // exists.
    analyzer(config().addSuppressQuery(SQL).build(), rule)
        .analyze("filtered", List.of(QUERY), null);
    assertThat(calls).hasValue(3);
  }

  @Test
  void zeroQueryViolationsAndExceptionsCannotBecomeAnEmptySuccessfulReport() {
    AuditRule missing =
        rule(
            descriptor("acme:budget", Set.of(KIND)),
            context -> {
              assertThat(context.queries()).isEmpty();
              return List.of(
                  new Finding(
                      KIND,
                      Severity.ERROR,
                      null,
                      null,
                      null,
                      "Expected a query",
                      "Execute the operation"));
            });
    QueryAuditReport report =
        analyzer(config().build(), missing).analyze("Example", "zero", List.of(), null);
    assertThat(report.hasConfirmedIssues()).isTrue();
    assertThat(report.getCustomConfirmedFindings()).hasSize(1);
    assertThat(report.getTotalQueryCount()).isZero();
    assertThat(report.getTestClass()).isEqualTo("Example");
    AuditRule failure =
        rule(
            descriptor("acme:budget", Set.of(KIND)),
            ignored -> {
              throw new IllegalStateException("zero evidence failed");
            });
    assertThatThrownBy(() -> analyzer(config().build(), failure).analyze("zero", List.of(), null))
        .isInstanceOf(AuditRuleException.class)
        .hasMessageContaining("EXECUTION_FAILED")
        .hasMessageNotContaining("zero evidence failed")
        .hasNoCause();
  }

  @Test
  void repeatedLogicalFindingsUseTheStrongestEffectiveSeverityAndPreserveEveryObservation() {
    Finding info =
        new Finding(
            KIND, Severity.INFO, SQL, "users", "id", "first observation", "first advice", "source");
    Finding error =
        new Finding(
            KIND,
            Severity.ERROR,
            SQL,
            "users",
            "id",
            "second observation",
            "second advice",
            "source");
    AuditRule repeated =
        rule(descriptor("acme:budget", Set.of(KIND)), ignored -> List.of(info, error));
    QueryAuditReport report = analyze(analyzer(config().build(), repeated));
    assertThat(report.getCustomInfoFindings()).isEmpty();
    assertThat(report.getCustomConfirmedFindings())
        .containsExactly(info.withSeverity(Severity.ERROR), error);
    assertThat(JsonReporter.toJson(report, ReportRedaction.FULL))
        .contains("first observation", "second observation", "occurrences");
    assertThat(JsonReporter.toJson(report))
        .contains("occurrences")
        .doesNotContain("first observation", "second observation");
    QueryAuditReport overridden =
        analyze(
            analyzer(config().addSeverityOverride(KIND.value(), Severity.INFO).build(), repeated));
    assertThat(overridden.getCustomConfirmedFindings()).isEmpty();
    assertThat(overridden.getCustomInfoFindings())
        .containsExactly(info, error.withSeverity(Severity.INFO));
  }

  @Test
  void partlyBaselinedRepresentationsOfOneFindingCannotHideAnUnacknowledgedObservation()
      throws Exception {
    Finding spaced = finding(KIND);
    Finding compact =
        new Finding(
            KIND,
            Severity.WARNING,
            SQL.replace("id = 1", "id=2"),
            "users",
            "id",
            "another observation",
            "suggestion",
            "source");
    assertThat(FindingId.of("same-test", spaced)).isEqualTo(FindingId.of("same-test", compact));
    Path baseline = tempDir.resolve("partial-baseline");
    Files.writeString(baseline, KIND.value() + " | users | id | team | accepted | " + SQL);
    AuditRule repeated =
        rule(descriptor("acme:budget", Set.of(KIND)), ignored -> List.of(spaced, compact));
    QueryAuditAnalyzer analyzer =
        QueryAuditAnalyzer.withExtensions(
            config().build(),
            baseline,
            AuditExtensions.builder().auditRule("repeated", repeated).build());
    QueryAuditReport report = analyze(analyzer);
    assertThat(report.getCustomAcknowledgedFindings()).isEmpty();
    assertThat(report.getCustomConfirmedFindings()).containsExactly(spaced, compact);
    assertThat(JsonReporter.toJson(report, ReportRedaction.FULL)).contains("another observation");
    assertThat(JsonReporter.toJson(report))
        .contains("occurrences")
        .doesNotContain("another observation");
  }

  @Test
  void customBucketsSurviveClassIdentityMetadataEvidenceAndLegacyMergeCopies() {
    QueryAuditAnalyzer analyzer = analyzer(config().build(), budgetRule());
    QueryAuditReport original = analyzer.analyze("example.Test", "budget", List.of(QUERY), null);
    QueryAuditReport copy =
        original
            .withTestIdentity("framework-id", null)
            .withIndexMetadata(new IndexMetadata(Map.of("users", List.of())))
            .withoutQueryEvidence();
    Issue extra =
        new Issue(IssueType.SELECT_ALL, Severity.INFO, SQL, "users", null, "external", null);
    QueryAuditReport merged = analyzer.mergeDetectedIssues(copy, List.of(extra));
    assertThat(merged.getCustomConfirmedFindings()).containsExactly(finding(KIND));
    assertThat(merged.getTestClass()).isEqualTo("example.Test");
    assertThat(merged.getTestId()).isEqualTo("framework-id");
    assertThat(merged.getAllQueries()).isEmpty();
    assertThat(merged.getIndexMetadata().hasTable("users")).isTrue();
    assertThat(merged.getInfoFindings()).contains(Finding.fromIssue(extra));
    assertThat(merged.hasConfirmedIssues()).isTrue();
  }

  @Test
  void legacyAdapterRetainsLegacyBucketsAndCannotRunTheSameDelegateTwice() {
    DetectionRule delegate =
        (queries, indexes) ->
            List.of(
                new Issue(
                    IssueType.SELECT_ALL, Severity.INFO, SQL, "users", null, "adapted", null));
    LegacyDetectionRuleAdapter adapter =
        new LegacyDetectionRuleAdapter(
            descriptor("acme:legacy", Set.of(FindingKindId.builtin(IssueType.SELECT_ALL))),
            delegate);
    QueryAuditAnalyzer analyzer = analyzer(config().build(), adapter);
    QueryAuditReport report = analyze(analyzer);
    assertThat(report.getInfoIssues()).anyMatch(issue -> "adapted".equals(issue.detail()));
    assertThat(report.getCustomInfoFindings()).isEmpty();
    assertThatThrownBy(
            () ->
                QueryAuditAnalyzer.withExtensions(
                    config().build(),
                    tempDir.resolve("missing"),
                    AuditExtensions.builder()
                        .rule("old", delegate)
                        .auditRule("new", adapter)
                        .build()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("both directly");
  }

  @Test
  void invalidResultsAndImplementationFailureNeverBecomeAReport() {
    AuditRule undeclared =
        rule(
            descriptor("acme:budget", Set.of(KIND)),
            ignored -> List.of(finding(FindingKindId.of("acme:undeclared"))));
    assertThatThrownBy(() -> analyze(analyzer(config().build(), undeclared)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("undeclared");
    AuditRule failure =
        rule(
            descriptor("acme:budget", Set.of(KIND)),
            ignored -> {
              throw new IllegalStateException("extension failed");
            });
    assertThatThrownBy(() -> analyze(analyzer(config().build(), failure)))
        .isInstanceOf(AuditRuleException.class)
        .hasMessageContaining("EXECUTION_FAILED")
        .hasMessageNotContaining("extension failed")
        .hasNoCause();
  }

  private AuditRule budgetRule() {
    return rule(descriptor("acme:budget", Set.of(KIND)), ignored -> List.of(finding(KIND)));
  }

  private QueryAuditAnalyzer analyzer(QueryAuditConfig config, AuditRule rule) {
    return QueryAuditAnalyzer.withExtensions(
        config,
        tempDir.resolve("missing"),
        AuditExtensions.builder().auditRule("assembly", rule).build());
  }

  private static QueryAuditConfig.Builder config() {
    return QueryAuditConfig.builder().addDisabledRule("service-loader-detection-rule");
  }

  private static QueryAuditReport analyze(QueryAuditAnalyzer analyzer) {
    return analyzer.analyze("budget", List.of(QUERY), new IndexMetadata(Map.of()));
  }
}
