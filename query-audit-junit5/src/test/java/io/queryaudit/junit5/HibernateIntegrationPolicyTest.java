package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.queryaudit.core.baseline.BaselineEntry;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.RuleProfile;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.interceptor.LazyLoadTracker;
import io.queryaudit.core.interceptor.LazyLoadTracker.ExplicitLoadRecord;
import io.queryaudit.core.interceptor.LazyLoadTracker.LazyLoadRecord;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class HibernateIntegrationPolicyTest {

  private final HibernateIntegration integration = new HibernateIntegration();

  @Test
  void defaultPolicyKeepsHibernateDetectorSeverities() {
    QueryAuditReport report = mergeAllFindings(analyzer(QueryAuditConfig.defaults()));

    assertThat(report.getConfirmedIssues())
        .singleElement()
        .satisfies(
            issue -> {
              assertThat(issue.type()).isEqualTo(IssueType.N_PLUS_ONE);
              assertThat(issue.severity()).isEqualTo(Severity.ERROR);
            });
    assertThat(report.getInfoIssues())
        .singleElement()
        .satisfies(
            issue -> {
              assertThat(issue.type()).isEqualTo(IssueType.FIND_BY_ID_FOR_ASSOCIATION);
              assertThat(issue.severity()).isEqualTo(Severity.INFO);
            });
    assertThat(report.getAcknowledgedIssues()).isEmpty();
  }

  @Test
  void disabledRulesRemoveAllHibernateFindings() {
    QueryAuditConfig config =
        QueryAuditConfig.builder()
            .addDisabledRule("n-plus-one")
            .addDisabledRule("find-by-id-for-association")
            .build();

    QueryAuditReport report = mergeAllFindings(analyzer(config));

    assertThat(report.getConfirmedIssues()).isEmpty();
    assertThat(report.getInfoIssues()).isEmpty();
    assertThat(report.getAcknowledgedIssues()).isEmpty();
    assertThat(report.hasConfirmedIssues()).isFalse();
  }

  @Test
  void severityOverridesReclassifyBothHibernateFindings() {
    QueryAuditConfig config =
        QueryAuditConfig.builder()
            .addSeverityOverride("n-plus-one", Severity.INFO)
            .addSeverityOverride("find-by-id-for-association", Severity.ERROR)
            .build();

    QueryAuditReport report = mergeAllFindings(analyzer(config));

    assertThat(report.getConfirmedIssues())
        .singleElement()
        .satisfies(
            issue -> {
              assertThat(issue.type()).isEqualTo(IssueType.FIND_BY_ID_FOR_ASSOCIATION);
              assertThat(issue.severity()).isEqualTo(Severity.ERROR);
            });
    assertThat(report.getInfoIssues())
        .singleElement()
        .satisfies(
            issue -> {
              assertThat(issue.type()).isEqualTo(IssueType.N_PLUS_ONE);
              assertThat(issue.severity()).isEqualTo(Severity.INFO);
            });
    assertThat(report.hasConfirmedIssues()).isTrue();
  }

  @Test
  void baselineMovesHibernateFindingsToAcknowledged() {
    QueryAuditAnalyzer analyzer =
        new QueryAuditAnalyzer(
            QueryAuditConfig.defaults(),
            List.of(baseline("n-plus-one"), baseline("find-by-id-for-association")));

    QueryAuditReport report = mergeAllFindings(analyzer);

    assertThat(report.getConfirmedIssues()).isEmpty();
    assertThat(report.getInfoIssues()).isEmpty();
    assertThat(report.getAcknowledgedIssues())
        .extracting(Issue::type)
        .containsExactly(IssueType.N_PLUS_ONE, IssueType.FIND_BY_ID_FOR_ASSOCIATION);
    assertThat(report.hasConfirmedIssues()).isFalse();
  }

  @Test
  void suppressPatternsRemoveAllHibernateFindings() {
    QueryAuditConfig config =
        QueryAuditConfig.builder()
            .suppressPatterns(Set.of("n-plus-one", "find-by-id-for-association"))
            .build();

    QueryAuditReport report = mergeAllFindings(analyzer(config));

    assertThat(report.getConfirmedIssues()).isEmpty();
    assertThat(report.getInfoIssues()).isEmpty();
    assertThat(report.getAcknowledgedIssues()).isEmpty();
  }

  @Test
  void minimalProfileKeepsNPlusOneAndRemovesFindByIdFinding() {
    QueryAuditConfig config = QueryAuditConfig.builder().ruleProfile(RuleProfile.MINIMAL).build();

    QueryAuditReport report = mergeAllFindings(analyzer(config));

    assertThat(report.getConfirmedIssues())
        .extracting(Issue::type)
        .containsExactly(IssueType.N_PLUS_ONE);
    assertThat(report.getInfoIssues()).isEmpty();
    assertThat(report.getAcknowledgedIssues()).isEmpty();
  }

  @Test
  void policyStillAppliesWhenTheReportHasNoQueries() {
    QueryAuditConfig config =
        QueryAuditConfig.builder().addSeverityOverride("n-plus-one", Severity.INFO).build();

    QueryAuditReport report =
        integration.mergeNPlusOneIssues(emptyReport(), trackerWithNPlusOne(), analyzer(config));

    assertThat(report.getConfirmedIssues()).isEmpty();
    assertThat(report.getInfoIssues())
        .extracting(Issue::type)
        .containsExactly(IssueType.N_PLUS_ONE);
  }

  private static LazyLoadTracker trackerWithNPlusOne() {
    LazyLoadTracker tracker = mock(LazyLoadTracker.class);
    when(tracker.getRecords()).thenReturn(List.of(lazyLoad("1"), lazyLoad("2"), lazyLoad("3")));
    return tracker;
  }

  private QueryAuditReport mergeAllFindings(QueryAuditAnalyzer analyzer) {
    LazyLoadTracker tracker = trackerWithAllFindings();
    QueryAuditReport report =
        integration.mergeNPlusOneIssues(reportWithDmlQuery(), tracker, analyzer);
    return integration.mergeFindByIdIssues(report, tracker, analyzer);
  }

  private static LazyLoadTracker trackerWithAllFindings() {
    LazyLoadTracker tracker = trackerWithNPlusOne();
    when(tracker.getExplicitLoads())
        .thenReturn(
            List.of(new ExplicitLoadRecord("example.User", "42", 1_000L, "OrderTest.java:20")));
    return tracker;
  }

  private static QueryAuditAnalyzer analyzer(QueryAuditConfig config) {
    return new QueryAuditAnalyzer(config, List.of());
  }

  private static BaselineEntry baseline(String issueCode) {
    return new BaselineEntry(issueCode, null, null, null, "team", "accepted");
  }

  private static LazyLoadRecord lazyLoad(String ownerId) {
    return new LazyLoadRecord("example.Team.members", "example.Team", ownerId, 1_000L);
  }

  private static QueryAuditReport emptyReport() {
    return new QueryAuditReport(
        "ExampleTest", "loadsMembers", List.of(), List.of(), List.of(), List.of(), 0, 0, 0L);
  }

  private static QueryAuditReport reportWithDmlQuery() {
    List<QueryRecord> queries =
        List.of(
            new QueryRecord(
                "INSERT INTO orders (user_id) VALUES (?)",
                1_000_000L,
                2_000L,
                "OrderTest.java:21"));
    return new QueryAuditReport(
        "ExampleTest", "createsOrder", List.of(), List.of(), List.of(), queries, 1, 1, 1_000_000L);
  }
}
