package io.queryaudit.core.extension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.baseline.BaselineEntry;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.RuleProfile;
import io.queryaudit.core.detector.DetectionRule;
import io.queryaudit.core.detector.NPlusOneDetector;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.LifecyclePhase;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AuditExtensionsAnalyzerTest {
  private static final IndexMetadata EMPTY_INDEXES = new IndexMetadata(Map.of());
  private static final String SQL = "SELECT id FROM users LIMIT 1";

  @Test
  void explicitCatalogRulesRunAlongsideTheBuiltInRules() {
    CatalogRule rule = new CatalogRule();
    QueryAuditAnalyzer analyzer = analyzer(config().build(), List.of(), rule);

    QueryAuditReport report = analyze(analyzer);

    assertThat(analyzer.getRules()).anyMatch(NPlusOneDetector.class::isInstance);
    assertThat(analyzer.getRules()).contains(rule);
    assertThat(report.getInfoIssues()).anyMatch(issue -> issue.detail().equals("catalog rule"));
    assertThat(rule.evaluations).isEqualTo(1);
    assertThat(analyzer.hasCompleteRuleInputs()).isFalse();
    assertThatThrownBy(() -> analyzer.getRules().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void catalogRegistrationIdDoesNotReplaceTheRulesPolicyCode() {
    CatalogRule rule = new CatalogRule();
    QueryAuditAnalyzer analyzer =
        analyzer(config().addDisabledRule("select-all").build(), List.of(), rule);

    analyze(analyzer);

    assertThat(rule.evaluations).isZero();
    assertThat(analyzer.getRules()).doesNotContain(rule);
    assertThat(analyzer.hasCompleteRuleInputs()).isTrue();
  }

  @Test
  void profileSelectionAndExplicitEnablementApplyBeforeCustomRuleExecution() {
    CatalogRule excluded = new CatalogRule();
    QueryAuditAnalyzer minimal =
        analyzer(config().ruleProfile(RuleProfile.MINIMAL).build(), List.of(), excluded);
    analyze(minimal);
    assertThat(excluded.evaluations).isZero();

    CatalogRule enabled = new CatalogRule();
    QueryAuditAnalyzer optedIn =
        analyzer(
            config().ruleProfile(RuleProfile.MINIMAL).addEnabledRule("select-all").build(),
            List.of(),
            enabled);
    analyze(optedIn);
    assertThat(enabled.evaluations).isEqualTo(1);
    assertThat(optedIn.hasCompleteRuleInputs()).isFalse();

    CatalogRule disabled = new CatalogRule();
    QueryAuditAnalyzer optedOut =
        analyzer(
            config().addEnabledRule("select-all").addDisabledRule("select-all").build(),
            List.of(),
            disabled);
    analyze(optedOut);
    assertThat(disabled.evaluations).isZero();
  }

  @Test
  void hostStillControlsSeveritySuppressionAndBaselineAcknowledgement() {
    QueryAuditReport overridden =
        analyze(
            analyzer(
                config().addSeverityOverride("select-all", Severity.ERROR).build(),
                List.of(),
                new CatalogRule()));
    assertThat(overridden.getConfirmedIssues())
        .singleElement()
        .satisfies(issue -> assertThat(issue.severity()).isEqualTo(Severity.ERROR));

    QueryAuditReport suppressed =
        analyze(
            analyzer(
                config().addSuppressPattern("select-all").build(), List.of(), new CatalogRule()));
    assertThat(suppressed.getConfirmedIssues()).isEmpty();
    assertThat(suppressed.getInfoIssues()).isEmpty();

    BaselineEntry baseline =
        new BaselineEntry("select-all", "users", null, SQL, "team", "accepted custom finding");
    QueryAuditReport acknowledged =
        analyze(analyzer(config().build(), List.of(baseline), new CatalogRule()));
    assertThat(acknowledged.getAcknowledgedIssues())
        .singleElement()
        .satisfies(issue -> assertThat(issue.detail()).isEqualTo("catalog rule"));
    assertThat(acknowledged.getInfoIssues()).isEmpty();
  }

  @Test
  void customRulesReceiveOnlyTheHostsImmutableInScopeQueryList() {
    CatalogRule rule = new CatalogRule();
    QueryAuditAnalyzer analyzer =
        analyzer(config().addSuppressQuery("SELECT 42").build(), List.of(), rule);
    QueryRecord included = query(SQL, LifecyclePhase.TEST);
    List<QueryRecord> captured =
        new ArrayList<>(
            List.of(
                query("SELECT id FROM fixtures LIMIT 1", LifecyclePhase.SETUP),
                query("SELECT 42", LifecyclePhase.TEST),
                included));

    analyzer.analyze("catalogScope", captured, EMPTY_INDEXES);

    assertThat(rule.lastQueries).containsExactly(included);
    assertThatThrownBy(() -> rule.lastQueries.clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThat(captured).hasSize(3);
  }

  @Test
  void implementationFailureIsNotConvertedToAnEmptyFindingList() {
    DetectionRule failure =
        (queries, indexes) -> {
          throw new IllegalStateException("custom rule failed");
        };

    assertThatThrownBy(() -> analyze(analyzer(config().build(), List.of(), failure)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("custom rule failed");
  }

  private static QueryAuditConfig.Builder config() {
    return QueryAuditConfig.builder().addDisabledRule("service-loader-detection-rule");
  }

  private static QueryAuditAnalyzer analyzer(
      QueryAuditConfig config, List<BaselineEntry> baseline, DetectionRule rule) {
    AuditExtensions extensions =
        AuditExtensions.builder().rule("company:catalog-rule", rule).build();
    return new QueryAuditAnalyzer(config, baseline, extensions.rules());
  }

  private static QueryAuditReport analyze(QueryAuditAnalyzer analyzer) {
    return analyzer.analyze("catalogTest", List.of(query(SQL, LifecyclePhase.TEST)), EMPTY_INDEXES);
  }

  private static QueryRecord query(String sql, LifecyclePhase phase) {
    return new QueryRecord(sql, SqlParser.normalize(sql), 0L, 0L, "", 0, phase);
  }

  private static final class CatalogRule implements DetectionRule {
    private int evaluations;
    private List<QueryRecord> lastQueries = List.of();

    @Override
    public String getRuleCode() {
      return "select-all";
    }

    @Override
    public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
      evaluations++;
      lastQueries = queries;
      return queries.stream()
          .map(
              query ->
                  new Issue(
                      IssueType.SELECT_ALL,
                      Severity.INFO,
                      query.sql(),
                      "users",
                      null,
                      "catalog rule",
                      "company suggestion"))
          .toList();
    }
  }
}
