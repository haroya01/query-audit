package io.queryaudit.core.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.detector.RepeatedSingleUpdateDetector;
import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class QueryAuditConfigExtendedTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());
  private static final IndexMetadata POSTS_PRIMARY_KEY =
      new IndexMetadata(
          Map.of("posts", List.of(new IndexInfo("posts", "PRIMARY", "id", 1, false, 100))));

  private static QueryRecord q(String sql) {
    return new QueryRecord(sql, 1000L, System.currentTimeMillis(), null);
  }

  @Test
  void disabledRulesPreventDetection() {
    QueryAuditConfig config = QueryAuditConfig.builder().addDisabledRule("select-all").build();
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, List.of());

    QueryAuditReport report =
        analyzer.analyze("test", List.of(q("SELECT * FROM users WHERE id = 1")), EMPTY_INDEX);

    // SELECT * should not be detected since the rule is disabled
    assertThat(report.getInfoIssues()).noneMatch(i -> i.type() == IssueType.SELECT_ALL);
    assertThat(report.getConfirmedIssues()).noneMatch(i -> i.type() == IssueType.SELECT_ALL);
  }

  @Test
  void severityOverrideChangesIssueSeverity() {
    QueryAuditConfig config =
        QueryAuditConfig.builder().addSeverityOverride("select-all", Severity.ERROR).build();
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, List.of());

    QueryAuditReport report =
        analyzer.analyze("test", List.of(q("SELECT * FROM users WHERE id = 1")), EMPTY_INDEX);

    // SELECT * should now be ERROR severity (confirmed) instead of INFO
    assertThat(report.getConfirmedIssues())
        .anyMatch(i -> i.type() == IssueType.SELECT_ALL && i.severity() == Severity.ERROR);
  }

  @Test
  void severityOverrideCanDowngradeToInfo() {
    QueryAuditConfig config =
        QueryAuditConfig.builder().addSeverityOverride("where-function", Severity.INFO).build();
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, List.of());

    QueryAuditReport report =
        analyzer.analyze(
            "test", List.of(q("SELECT id FROM users WHERE YEAR(created_at) = 2024")), EMPTY_INDEX);

    // WHERE_FUNCTION should now be INFO instead of ERROR
    assertThat(report.getInfoIssues()).anyMatch(i -> i.type() == IssueType.WHERE_FUNCTION);
    assertThat(report.getConfirmedIssues()).noneMatch(i -> i.type() == IssueType.WHERE_FUNCTION);
  }

  @Test
  void configurableThresholdsWorkForTooManyJoins() {
    // Default threshold is 5, so 4 JOINs should not trigger
    QueryAuditConfig defaultConfig = QueryAuditConfig.defaults();
    QueryAuditAnalyzer defaultAnalyzer = new QueryAuditAnalyzer(defaultConfig, List.of());

    String sql =
        "SELECT u.id FROM users u "
            + "JOIN orders o ON u.id = o.user_id "
            + "JOIN products p ON o.product_id = p.id "
            + "JOIN categories c ON p.category_id = c.id "
            + "WHERE u.id = 1";
    QueryAuditReport report = defaultAnalyzer.analyze("test", List.of(q(sql)), EMPTY_INDEX);
    assertThat(report.getConfirmedIssues()).noneMatch(i -> i.type() == IssueType.TOO_MANY_JOINS);

    // With threshold 2, 3 JOINs should trigger
    QueryAuditConfig strictConfig = QueryAuditConfig.builder().tooManyJoinsThreshold(2).build();
    QueryAuditAnalyzer strictAnalyzer = new QueryAuditAnalyzer(strictConfig, List.of());
    report = strictAnalyzer.analyze("test", List.of(q(sql)), EMPTY_INDEX);
    assertThat(report.getConfirmedIssues()).anyMatch(i -> i.type() == IssueType.TOO_MANY_JOINS);
  }

  @Test
  void isRuleDisabledReturnsTrueForDisabledRule() {
    QueryAuditConfig config =
        QueryAuditConfig.builder().disabledRules(Set.of("n-plus-one", "select-all")).build();
    assertThat(config.isRuleDisabled("n-plus-one")).isTrue();
    assertThat(config.isRuleDisabled("select-all")).isTrue();
    assertThat(config.isRuleDisabled("where-function")).isFalse();
  }

  @Test
  void getEffectiveSeverityReturnsOverrideWhenPresent() {
    QueryAuditConfig config =
        QueryAuditConfig.builder().addSeverityOverride("select-all", Severity.ERROR).build();
    assertThat(config.getEffectiveSeverity("select-all", Severity.INFO)).isEqualTo(Severity.ERROR);
    assertThat(config.getEffectiveSeverity("where-function", Severity.ERROR))
        .isEqualTo(Severity.ERROR); // no override, returns default
  }

  @Test
  void defaultThresholdValues() {
    QueryAuditConfig config = QueryAuditConfig.defaults();
    assertThat(config.getLargeInListThreshold()).isEqualTo(100);
    assertThat(config.getTooManyJoinsThreshold()).isEqualTo(5);
    assertThat(config.getExcessiveColumnThreshold()).isEqualTo(15);
    assertThat(config.getRepeatedInsertThreshold()).isEqualTo(3);
    assertThat(config.getRepeatedUpdateThreshold()).isEqualTo(3);
    assertThat(config.getRepeatedUpdateExcludeTables())
        .containsExactlyInAnyOrderElementsOf(RepeatedSingleUpdateDetector.DEFAULT_EXCLUDE_TABLES);
    assertThat(config.getWriteAmplificationThreshold()).isEqualTo(6);
    assertThat(config.getSlowQueryWarningMs()).isEqualTo(500);
    assertThat(config.getSlowQueryErrorMs()).isEqualTo(3000);
  }

  @Test
  void repeatedUpdateExclusionsAreDefensivelyCopied() {
    Set<String> exclusions = new HashSet<>(Set.of("audit_*"));
    QueryAuditConfig.Builder builder =
        QueryAuditConfig.builder().repeatedUpdateExcludeTables(exclusions);
    exclusions.add("changed_after_assignment");

    QueryAuditConfig config = builder.addRepeatedUpdateExcludeTable("etl_*").build();

    assertThat(config.getRepeatedUpdateExcludeTables())
        .containsExactlyInAnyOrder("audit_*", "etl_*");
  }

  @Test
  void ruleProfilesAndExplicitOverridesApplyToRepeatedUpdates() {
    QueryAuditConfig recommended =
        QueryAuditConfig.builder().ruleProfile(RuleProfile.RECOMMENDED).build();
    QueryAuditConfig minimal = QueryAuditConfig.builder().ruleProfile(RuleProfile.MINIMAL).build();
    QueryAuditConfig enabled =
        QueryAuditConfig.builder()
            .ruleProfile(RuleProfile.MINIMAL)
            .addEnabledRule("repeated-single-update")
            .build();
    QueryAuditConfig disabled =
        QueryAuditConfig.builder()
            .ruleProfile(RuleProfile.MINIMAL)
            .addEnabledRule("repeated-single-update")
            .addDisabledRule("repeated-single-update")
            .build();

    assertThat(repeatedUpdateIssues(recommended)).hasSize(1);
    assertThat(repeatedUpdateIssues(minimal)).isEmpty();
    assertThat(repeatedUpdateIssues(enabled)).hasSize(1);
    assertThat(repeatedUpdateIssues(disabled)).isEmpty();
  }

  @Test
  void severityOverrideAppliesToRepeatedUpdates() {
    QueryAuditConfig config =
        QueryAuditConfig.builder()
            .addSeverityOverride("repeated-single-update", Severity.ERROR)
            .build();

    assertThat(repeatedUpdateIssues(config))
        .singleElement()
        .extracting(Issue::severity)
        .isEqualTo(Severity.ERROR);
  }

  private static List<Issue> repeatedUpdateIssues(QueryAuditConfig config) {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, List.of());
    QueryAuditReport report =
        analyzer.analyze(
            "test",
            List.of(
                q("UPDATE posts SET title = 'one' WHERE id = 1"),
                q("UPDATE posts SET title = 'two' WHERE id = 2"),
                q("UPDATE posts SET title = 'three' WHERE id = 3")),
            POSTS_PRIMARY_KEY);
    return java.util.stream.Stream.concat(
            report.getConfirmedIssues().stream(), report.getInfoIssues().stream())
        .filter(issue -> issue.type() == IssueType.REPEATED_SINGLE_UPDATE)
        .toList();
  }
}
