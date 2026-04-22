package io.queryaudit.core.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class QueryAuditConfigExtendedTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

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
    assertThat(config.getWriteAmplificationThreshold()).isEqualTo(6);
    assertThat(config.getSlowQueryWarningMs()).isEqualTo(500);
    assertThat(config.getSlowQueryErrorMs()).isEqualTo(3000);
  }
}
