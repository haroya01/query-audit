package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.RuleProfile;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class DetectionRuleRegistryTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  @Test
  void registersBuiltInDiscoveredAndAdditionalRulesInOrder() {
    DetectionRule firstAdditionalRule = new NoOpRule();
    DetectionRule secondAdditionalRule = new NoOpRule();

    List<DetectionRule> rules =
        new DetectionRuleRegistry(QueryAuditConfig.defaults())
            .createRules(List.of(firstAdditionalRule, secondAdditionalRule));

    int discoveredRuleIndex = indexOf(rules, TestServiceLoaderDetectionRule.class);
    assertThat(discoveredRuleIndex).isPositive();
    List<DetectionRule> builtInRules = rules.subList(0, discoveredRuleIndex);

    assertThat(builtInRules).hasSize(58);
    assertThat(rules.get(0)).isInstanceOf(NPlusOneDetector.class);
    assertThat(builtInRules.get(builtInRules.size() - 1))
        .isInstanceOf(ForceIndexHintDetector.class);
    assertThat(builtInRules).extracting(DetectionRule::getClass).doesNotHaveDuplicates();
    assertThat(rules).endsWith(firstAdditionalRule, secondAdditionalRule);
  }

  @Test
  void filtersRegisteredRulesBeforeAppendingAdditionalRules() {
    QueryAuditConfig config =
        QueryAuditConfig.builder()
            .addDisabledRule("select-all")
            .addDisabledRule("service-loader-detection-rule")
            .build();
    DisabledSelectAllRule additionalRule = new DisabledSelectAllRule();

    List<DetectionRule> rules =
        new DetectionRuleRegistry(config).createRules(List.of(additionalRule));

    assertThat(rules).noneMatch(SelectAllDetector.class::isInstance);
    assertThat(rules).noneMatch(TestServiceLoaderDetectionRule.class::isInstance);
    assertThat(rules.get(rules.size() - 1)).isSameAs(additionalRule);

    QueryAuditAnalyzer analyzer =
        new QueryAuditAnalyzer(config, List.of(), List.of(additionalRule));
    QueryAuditReport report =
        analyzer.analyze(
            "additionalRuleFiltering", List.of(query("SELECT id FROM users", 1)), EMPTY_INDEX);

    assertThat(report.getConfirmedIssues())
        .noneMatch(issue -> issue.type() == IssueType.SELECT_ALL);
    assertThat(report.getInfoIssues()).noneMatch(issue -> issue.type() == IssueType.SELECT_ALL);
  }

  @Test
  void appliesProfilesToRulesThatDeclareTheirCodes() {
    QueryAuditConfig minimal = QueryAuditConfig.builder().ruleProfile(RuleProfile.MINIMAL).build();
    QueryAuditConfig reEnabled =
        QueryAuditConfig.builder()
            .ruleProfile(RuleProfile.MINIMAL)
            .addEnabledRule("repeated-single-update")
            .build();

    List<DetectionRule> minimalRules = new DetectionRuleRegistry(minimal).createRules();
    List<DetectionRule> reEnabledRules = new DetectionRuleRegistry(reEnabled).createRules();

    assertThat(minimalRules).noneMatch(RepeatedSingleUpdateDetector.class::isInstance);
    assertThat(minimalRules).anyMatch(SelectAllDetector.class::isInstance);
    assertThat(reEnabledRules).anyMatch(RepeatedSingleUpdateDetector.class::isInstance);
  }

  @Test
  void passesThresholdsToConfigurableRules() {
    QueryAuditConfig config =
        QueryAuditConfig.builder()
            .nPlusOneThreshold(4)
            .slowQueryWarningMs(100)
            .slowQueryErrorMs(200)
            .build();
    List<DetectionRule> rules = new DetectionRuleRegistry(config).createRules();

    NPlusOneDetector nPlusOne = ruleOfType(rules, NPlusOneDetector.class);
    SlowQueryDetector slowQuery = ruleOfType(rules, SlowQueryDetector.class);

    assertThat(nPlusOne.evaluate(repeatedQueries(3), EMPTY_INDEX)).isEmpty();
    assertThat(nPlusOne.evaluate(repeatedQueries(4), EMPTY_INDEX)).hasSize(1);
    assertThat(slowQuery.evaluate(List.of(query("SELECT id FROM users", 150)), EMPTY_INDEX))
        .singleElement()
        .extracting(Issue::severity)
        .isEqualTo(Severity.WARNING);
    assertThat(slowQuery.evaluate(List.of(query("SELECT id FROM users", 250)), EMPTY_INDEX))
        .singleElement()
        .extracting(Issue::severity)
        .isEqualTo(Severity.ERROR);
  }

  private static List<QueryRecord> repeatedQueries(int count) {
    return Collections.nCopies(count, query("SELECT id FROM users WHERE id = ?", 1));
  }

  private static QueryRecord query(String sql, long executionTimeMs) {
    return new QueryRecord(sql, TimeUnit.MILLISECONDS.toNanos(executionTimeMs), 0L, "");
  }

  private static int indexOf(List<DetectionRule> rules, Class<? extends DetectionRule> type) {
    for (int index = 0; index < rules.size(); index++) {
      if (type.isInstance(rules.get(index))) {
        return index;
      }
    }
    return -1;
  }

  private static <T extends DetectionRule> T ruleOfType(List<DetectionRule> rules, Class<T> type) {
    return rules.stream().filter(type::isInstance).map(type::cast).findFirst().orElseThrow();
  }

  private static final class NoOpRule implements DetectionRule {

    @Override
    public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
      return List.of();
    }
  }

  private static final class DisabledSelectAllRule implements DetectionRule {

    @Override
    public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
      return List.of(
          new Issue(
              IssueType.SELECT_ALL,
              Severity.WARNING,
              queries.get(0).sql(),
              null,
              null,
              "Additional rule finding",
              "Select explicit columns"));
    }

    @Override
    public String getRuleCode() {
      return "select-all";
    }
  }
}
