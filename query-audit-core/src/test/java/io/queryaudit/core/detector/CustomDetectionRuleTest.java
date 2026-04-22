package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CustomDetectionRuleTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 1000L, System.currentTimeMillis(), "");
  }

  /**
   * A simple custom detection rule that flags any query containing the keyword "FORBIDDEN". Used to
   * verify that custom rules can be passed via the constructor.
   */
  static class ForbiddenKeywordDetector implements DetectionRule {

    @Override
    public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
      List<Issue> issues = new ArrayList<>();
      for (QueryRecord query : queries) {
        if (query.sql() != null && query.sql().toUpperCase().contains("FORBIDDEN")) {
          issues.add(
              new Issue(
                  IssueType.SELECT_ALL, // reuse existing type for test simplicity
                  Severity.WARNING,
                  query.sql(),
                  null,
                  null,
                  "Query contains forbidden keyword",
                  "Remove the forbidden keyword"));
        }
      }
      return issues;
    }
  }

  @Test
  void additionalRulesViaConstructor_areEvaluated() {
    List<DetectionRule> additionalRules = List.of(new ForbiddenKeywordDetector());
    QueryAuditAnalyzer analyzer =
        new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of(), additionalRules);

    List<QueryRecord> queries = List.of(record("SELECT id FROM FORBIDDEN_TABLE WHERE id = 1"));

    QueryAuditReport report = analyzer.analyze("customRuleTest", queries, EMPTY_INDEX);

    // The custom ForbiddenKeywordDetector should fire
    assertThat(report.getConfirmedIssues())
        .anyMatch(i -> "Query contains forbidden keyword".equals(i.detail()));
  }

  @Test
  void additionalRulesRunAlongsideBuiltInRules() {
    List<DetectionRule> additionalRules = List.of(new ForbiddenKeywordDetector());
    QueryAuditAnalyzer analyzer =
        new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of(), additionalRules);

    // This query triggers both SELECT * (built-in) and FORBIDDEN (custom)
    List<QueryRecord> queries = List.of(record("SELECT * FROM FORBIDDEN_TABLE WHERE id = 1"));

    QueryAuditReport report = analyzer.analyze("bothRulesTest", queries, EMPTY_INDEX);

    // Built-in SELECT * detector should fire (INFO severity)
    assertThat(report.getInfoIssues()).anyMatch(i -> i.type() == IssueType.SELECT_ALL);

    // Custom ForbiddenKeywordDetector should also fire (WARNING severity -> confirmed)
    assertThat(report.getConfirmedIssues())
        .anyMatch(i -> "Query contains forbidden keyword".equals(i.detail()));
  }

  @Test
  void nullAdditionalRules_doesNotCauseError() {
    QueryAuditAnalyzer analyzer =
        new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of(), null);

    List<QueryRecord> queries = List.of(record("SELECT id FROM users WHERE id = 1"));

    QueryAuditReport report = analyzer.analyze("nullAdditionalTest", queries, EMPTY_INDEX);

    // Should work normally with only built-in rules
    assertThat(report.getTotalQueryCount()).isEqualTo(1);
  }

  @Test
  void additionalRulesViaPathConstructor_areEvaluated() {
    List<DetectionRule> additionalRules = List.of(new ForbiddenKeywordDetector());
    QueryAuditAnalyzer analyzer =
        new QueryAuditAnalyzer(
            QueryAuditConfig.defaults(), (java.nio.file.Path) null, additionalRules);

    List<QueryRecord> queries = List.of(record("SELECT id FROM FORBIDDEN_TABLE WHERE id = 1"));

    QueryAuditReport report = analyzer.analyze("pathConstructorTest", queries, EMPTY_INDEX);

    assertThat(report.getConfirmedIssues())
        .anyMatch(i -> "Query contains forbidden keyword".equals(i.detail()));
  }

  @Test
  void serviceLoaderDiscovery_loadsExternalRules() {
    // The test META-INF/services/io.queryaudit.core.detector.DetectionRule file
    // registers TestServiceLoaderDetectionRule, which flags queries containing "SERVICELOADER_TEST"
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());

    List<QueryRecord> queries = List.of(record("SELECT id FROM SERVICELOADER_TEST WHERE id = 1"));

    QueryAuditReport report = analyzer.analyze("serviceLoaderTest", queries, EMPTY_INDEX);

    assertThat(report.getConfirmedIssues())
        .anyMatch(i -> "ServiceLoader-discovered rule triggered".equals(i.detail()));
  }

  @Test
  void serviceLoaderAndAdditionalRules_bothWork() {
    List<DetectionRule> additionalRules = List.of(new ForbiddenKeywordDetector());
    QueryAuditAnalyzer analyzer =
        new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of(), additionalRules);

    // This query triggers both the ServiceLoader rule and the programmatic additional rule
    List<QueryRecord> queries =
        List.of(record("SELECT id FROM FORBIDDEN_SERVICELOADER_TEST WHERE id = 1"));

    QueryAuditReport report = analyzer.analyze("bothExtensionTest", queries, EMPTY_INDEX);

    // Programmatic additional rule should fire
    assertThat(report.getConfirmedIssues())
        .anyMatch(i -> "Query contains forbidden keyword".equals(i.detail()));

    // ServiceLoader-discovered rule should also fire
    assertThat(report.getConfirmedIssues())
        .anyMatch(i -> "ServiceLoader-discovered rule triggered".equals(i.detail()));
  }

  @Test
  void getRules_includesServiceLoaderAndAdditionalRules() {
    List<DetectionRule> additionalRules = List.of(new ForbiddenKeywordDetector());
    QueryAuditAnalyzer analyzer =
        new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of(), additionalRules);

    List<DetectionRule> rules = analyzer.getRules();

    // Should include the ForbiddenKeywordDetector
    assertThat(rules).anyMatch(r -> r instanceof ForbiddenKeywordDetector);

    // Should include the ServiceLoader-discovered rule
    assertThat(rules).anyMatch(r -> r instanceof TestServiceLoaderDetectionRule);
  }
}
