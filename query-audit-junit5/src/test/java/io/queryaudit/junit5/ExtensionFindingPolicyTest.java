package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.queryaudit.core.baseline.BaselineEntry;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.interceptor.ConnectionUsageTracker;
import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.regression.QueryCountBaseline;
import io.queryaudit.core.regression.QueryCounts;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@DisplayName("QueryAuditExtension external finding policies")
class ExtensionFindingPolicyTest {

  private static final String TEST_CLASS = "PolicyTest";
  private static final String TEST_NAME = "auditedMethod()";
  private static final String TEST_ID =
      "[engine:junit-jupiter]/[class:example.PolicyTest]/[method:auditedMethod()]";
  private static final IndexMetadata INDEX_METADATA = new IndexMetadata(Map.of());

  private final QueryAuditExtension extension = new QueryAuditExtension();

  @ParameterizedTest
  @EnumSource(FindingSource.class)
  void disabledRulesExcludeEveryExternalFinding(FindingSource source) throws Exception {
    QueryAuditConfig config =
        QueryAuditConfig.builder().addDisabledRule(source.issueType.getCode()).build();

    QueryAuditReport report = merge(source, analyzer(config));

    assertThat(allFindings(report)).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(FindingSource.class)
  void suppressPatternsRemoveEveryExternalFinding(FindingSource source) throws Exception {
    QueryAuditConfig config =
        QueryAuditConfig.builder().addSuppressPattern(source.issueType.getCode()).build();

    QueryAuditReport report = merge(source, analyzer(config));

    assertThat(allFindings(report)).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(FindingSource.class)
  void severityOverridesReclassifyEveryExternalFinding(FindingSource source) throws Exception {
    QueryAuditConfig config =
        QueryAuditConfig.builder()
            .addSeverityOverride(source.issueType.getCode(), Severity.ERROR)
            .build();

    QueryAuditReport report = merge(source, analyzer(config));

    assertThat(report.getConfirmedIssues())
        .singleElement()
        .satisfies(
            issue -> {
              assertThat(issue.type()).isEqualTo(source.issueType);
              assertThat(issue.severity()).isEqualTo(Severity.ERROR);
            });
    assertThat(report.getInfoIssues()).isEmpty();
    assertThat(report.getAcknowledgedIssues()).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(FindingSource.class)
  void baselinesAcknowledgeEveryExternalFinding(FindingSource source) throws Exception {
    BaselineEntry entry =
        new BaselineEntry(
            source.issueType.getCode(),
            source.table,
            null,
            source.queryPattern,
            "performance-team",
            "accepted");
    QueryAuditAnalyzer analyzer =
        new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of(entry));

    QueryAuditReport report = merge(source, analyzer);

    assertThat(report.getConfirmedIssues()).isEmpty();
    assertThat(report.getInfoIssues()).isEmpty();
    assertThat(report.getAcknowledgedIssues())
        .singleElement()
        .satisfies(issue -> assertThat(issue.type()).isEqualTo(source.issueType));
  }

  @ParameterizedTest
  @EnumSource(FindingSource.class)
  void mergesPreserveReportMeasurementsAndIndexMetadata(FindingSource source) throws Exception {
    QueryAuditReport report = merge(source, analyzer(QueryAuditConfig.defaults()));

    assertThat(allFindings(report)).extracting(Issue::type).containsExactly(source.issueType);
    assertThat(report.getIndexMetadata()).isSameAs(INDEX_METADATA);
    assertThat(report.getAllQueries()).hasSize(1);
    assertThat(report.getUniquePatternCount()).isEqualTo(7);
    assertThat(report.getTotalQueryCount()).isEqualTo(19);
    assertThat(report.getTotalExecutionTimeNanos()).isEqualTo(1_234L);
  }

  private QueryAuditReport merge(FindingSource source, QueryAuditAnalyzer analyzer)
      throws Exception {
    return switch (source) {
      case QUERY_COUNT_REGRESSION ->
          extension.detectQueryCountRegression(
              regressionContext(),
              baseReport(),
              regressionQueries(),
              TEST_ID,
              TEST_CLASS,
              TEST_NAME,
              analyzer);
      case EXPLAIN ->
          extension.runExplainAnalysis(
              explainContext(), baseReport(), List.of(query("select * from orders")), analyzer);
      case CONNECTION_HELD_IDLE ->
          extension.mergeConnectionHeldIdleIssues(baseReport(), idleInterceptor(), analyzer);
    };
  }

  private static QueryAuditReport baseReport() {
    return new QueryAuditReport(
            TEST_CLASS,
            TEST_NAME,
            List.of(),
            List.of(),
            List.of(),
            List.of(query("select id from orders")),
            7,
            19,
            1_234L)
        .withIndexMetadata(INDEX_METADATA);
  }

  private static List<Issue> allFindings(QueryAuditReport report) {
    List<Issue> findings = new ArrayList<>();
    findings.addAll(report.getConfirmedIssues());
    findings.addAll(report.getInfoIssues());
    findings.addAll(report.getAcknowledgedIssues());
    return findings;
  }

  private static QueryAuditAnalyzer analyzer(QueryAuditConfig config) {
    return new QueryAuditAnalyzer(config, List.of());
  }

  private static ExtensionContext regressionContext() {
    String key = QueryCountBaseline.key(TEST_ID);
    ExtensionContext.Store store = mock(ExtensionContext.Store.class);
    when(store.get("countBaseline")).thenReturn(Map.of(key, new QueryCounts(1, 0, 0, 0, 1)));
    when(store.get("currentCounts")).thenReturn(new HashMap<String, QueryCounts>());
    return contextWithStore(store);
  }

  private static List<QueryRecord> regressionQueries() {
    List<QueryRecord> queries = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      queries.add(query("select id from orders where id = " + i));
    }
    return queries;
  }

  private static ExtensionContext explainContext() throws Exception {
    DataSource dataSource = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(metadata);
    when(metadata.getDatabaseProductName()).thenReturn("Policy Test DB");

    ExtensionContext.Store store = mock(ExtensionContext.Store.class);
    when(store.get("dataSource", DataSource.class)).thenReturn(dataSource);
    return contextWithStore(store);
  }

  private static QueryInterceptor idleInterceptor() {
    QueryInterceptor interceptor = mock(QueryInterceptor.class);
    ConnectionUsageTracker tracker = mock(ConnectionUsageTracker.class);
    ConnectionUsageTracker.ConnectionSession session =
        new ConnectionUsageTracker.ConnectionSession(
            "pool-1", 500L, 10L, true, "OrderService.java:42");
    when(interceptor.getConnectionTracker()).thenReturn(tracker);
    when(tracker.getCompletedSessions()).thenReturn(List.of(session));
    return interceptor;
  }

  private static ExtensionContext contextWithStore(ExtensionContext.Store store) {
    ExtensionContext context = mock(ExtensionContext.class);
    when(context.getStore(any(ExtensionContext.Namespace.class))).thenReturn(store);
    when(context.getParent()).thenReturn(Optional.empty());
    return context;
  }

  private static QueryRecord query(String sql) {
    return new QueryRecord(sql, 100L, 1_000L, "OrderService.java:42");
  }

  private enum FindingSource {
    QUERY_COUNT_REGRESSION(IssueType.QUERY_COUNT_REGRESSION, null, null),
    EXPLAIN(IssueType.FILESORT, "orders", "select * from orders"),
    CONNECTION_HELD_IDLE(IssueType.CONNECTION_HELD_IDLE, null, null);

    private final IssueType issueType;
    private final String table;
    private final String queryPattern;

    FindingSource(IssueType issueType, String table, String queryPattern) {
      this.issueType = issueType;
      this.table = table;
      this.queryPattern = queryPattern;
    }
  }
}
