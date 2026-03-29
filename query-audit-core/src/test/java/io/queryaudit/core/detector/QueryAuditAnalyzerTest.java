package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.baseline.BaselineEntry;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class QueryAuditAnalyzerTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 1000L, System.currentTimeMillis(), "");
  }

  @Test
  void analyzeReturnsReportWithCorrectIssueCounts() {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();

    // Create queries that trigger N+1 (3 identical patterns) and SELECT *
    // With empty stack traces and count == threshold, N+1 is conservative INFO.
    // SELECT * is still a confirmed issue.
    List<QueryRecord> queries =
        List.of(
            record("SELECT * FROM users WHERE id = 1"),
            record("SELECT * FROM users WHERE id = 2"),
            record("SELECT * FROM users WHERE id = 3"));

    QueryAuditReport report = analyzer.analyze("testMethod", queries, EMPTY_INDEX);

    assertThat(report.getTestName()).isEqualTo("testMethod");
    assertThat(report.getTotalQueryCount()).isEqualTo(3);

    // SELECT * is now INFO severity, so it goes to info issues
    assertThat(report.getInfoIssues()).anyMatch(i -> i.type() == IssueType.SELECT_ALL);

    // N+1: SQL-level detection is now INFO (Hibernate-level is authoritative)
    assertThat(report.getInfoIssues()).anyMatch(i -> i.type() == IssueType.N_PLUS_ONE_SUSPECT);
  }

  @Test
  void analyzeReturnsCleanReportForGoodQueries() {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();

    List<QueryRecord> queries =
        List.of(record("SELECT id, name FROM users WHERE id = 1 ORDER BY id LIMIT 10"));

    QueryAuditReport report = analyzer.analyze("goodTest", queries, EMPTY_INDEX);

    assertThat(report.getTestName()).isEqualTo("goodTest");
    assertThat(report.getTotalQueryCount()).isEqualTo(1);
    // Only one query, so N+1 won't trigger; no SELECT *, no functions, etc.
    // Without index metadata, MissingIndex detectors return empty too.
    // LIMIT prevents UnboundedResultSet from firing.
    assertThat(report.getConfirmedIssues()).isEmpty();
  }

  @Test
  void suppressPatternsWork() {
    QueryAuditConfig config = QueryAuditConfig.builder().addSuppressPattern("select-all").build();
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config);

    List<QueryRecord> queries = List.of(record("SELECT * FROM users WHERE id = 1"));

    QueryAuditReport report = analyzer.analyze("suppressTest", queries, EMPTY_INDEX);

    // SELECT * issue should be suppressed
    assertThat(report.getConfirmedIssues().stream().filter(i -> i.type() == IssueType.SELECT_ALL))
        .isEmpty();
  }

  @Test
  void suppressQueriesWork() {
    QueryAuditConfig config = QueryAuditConfig.builder().addSuppressQuery("SELECT 1").build();
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config);

    // Create queries where "SELECT 1" should be filtered out before detection
    List<QueryRecord> queries =
        List.of(
            record("SELECT 1"),
            record("SELECT 1"),
            record("SELECT 1"),
            record("SELECT 1"),
            record("SELECT 1"));

    QueryAuditReport report = analyzer.analyze("suppressQueryTest", queries, EMPTY_INDEX);

    // All queries are suppressed, so no N+1 detection
    assertThat(report.getConfirmedIssues()).isEmpty();
  }

  @Test
  void disabledConfigReturnsEmptyReport() {
    QueryAuditConfig config = QueryAuditConfig.builder().enabled(false).build();
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config);

    List<QueryRecord> queries =
        List.of(
            record("SELECT * FROM users WHERE id = 1"),
            record("SELECT * FROM users WHERE id = 2"),
            record("SELECT * FROM users WHERE id = 3"));

    QueryAuditReport report = analyzer.analyze("disabledTest", queries, EMPTY_INDEX);

    assertThat(report.getConfirmedIssues()).isEmpty();
    assertThat(report.getInfoIssues()).isEmpty();
  }

  @Test
  void emptyQueriesReturnsEmptyReport() {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();

    QueryAuditReport report = analyzer.analyze("emptyTest", List.of(), EMPTY_INDEX);

    assertThat(report.getConfirmedIssues()).isEmpty();
    assertThat(report.getTotalQueryCount()).isEqualTo(0);
  }

  @Test
  void nullQueriesReturnsEmptyReport() {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();

    QueryAuditReport report = analyzer.analyze("nullTest", null, EMPTY_INDEX);

    assertThat(report.getConfirmedIssues()).isEmpty();
    assertThat(report.getAllQueries()).isEmpty();
  }

  @Test
  void reportContainsExecutionTime() {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
    List<QueryRecord> queries = List.of(record("SELECT id FROM users WHERE id = 1"));

    QueryAuditReport report = analyzer.analyze("timeTest", queries, EMPTY_INDEX);

    assertThat(report.getTotalExecutionTimeNanos()).isEqualTo(1000L);
  }

  @Test
  void suppressPatternWithTableAndColumn() {
    QueryAuditConfig config =
        QueryAuditConfig.builder().addSuppressPattern("where-function:orders.created_at").build();
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config);

    List<QueryRecord> queries =
        List.of(record("SELECT * FROM orders WHERE DATE(created_at) = '2024-01-01'"));

    QueryAuditReport report = analyzer.analyze("qualifiedSuppressTest", queries, EMPTY_INDEX);

    // The where-function issue on orders.created_at should be suppressed
    assertThat(
            report.getConfirmedIssues().stream()
                .filter(
                    i -> i.type() == IssueType.WHERE_FUNCTION && "created_at".equals(i.column())))
        .isEmpty();
  }

  // ====================================================================
  //  Mutation: Line 76 -- baselinePath != null negated
  //  Test that providing a non-null baselinePath uses that path (not default)
  // ====================================================================

  @Test
  void constructorWithNullBaselinePath_usesDefaultBaseline() {
    // When baselinePath is null, the analyzer should load from the default file
    // (which doesn't exist, so baseline is empty)
    QueryAuditConfig config = QueryAuditConfig.defaults();
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, (java.nio.file.Path) null);

    // Baseline should be empty since default file doesn't exist
    assertThat(analyzer.getBaseline()).isEmpty();
  }

  @Test
  void constructorWithExplicitBaselinePath_usesProvidedPath() {
    // When baselinePath is non-null but points to a non-existent file, baseline is empty
    QueryAuditConfig config = QueryAuditConfig.defaults();
    QueryAuditAnalyzer analyzer =
        new QueryAuditAnalyzer(
            config, java.nio.file.Paths.get("/tmp/nonexistent-baseline-file-xyz"));

    assertThat(analyzer.getBaseline()).isEmpty();
  }

  // ====================================================================
  //  Mutation: Line 131 -- 4-arg analyze, disabled config
  // ====================================================================

  @Test
  void fourArgAnalyze_disabledConfig_returnsEmptyReport() {
    QueryAuditConfig config = QueryAuditConfig.builder().enabled(false).build();
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, List.of());

    List<QueryRecord> queries =
        List.of(
            record("SELECT * FROM users WHERE id = 1"),
            record("SELECT * FROM users WHERE id = 2"),
            record("SELECT * FROM users WHERE id = 3"));

    QueryAuditReport report = analyzer.analyze("TestClass", "disabledTest", queries, EMPTY_INDEX);

    assertThat(report.getConfirmedIssues()).isEmpty();
    assertThat(report.getInfoIssues()).isEmpty();
    assertThat(report.getTestClass()).isEqualTo("TestClass");
  }

  // ====================================================================
  //  Mutation: Line 131 -- 4-arg analyze, null queries
  // ====================================================================

  @Test
  void fourArgAnalyze_nullQueries_returnsEmptyReport() {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());

    QueryAuditReport report = analyzer.analyze("TestClass", "nullTest", null, EMPTY_INDEX);

    assertThat(report.getConfirmedIssues()).isEmpty();
    assertThat(report.getAllQueries()).isEmpty();
    assertThat(report.getTestClass()).isEqualTo("TestClass");
  }

  // ====================================================================
  //  Mutation: Line 137 -- queries != null ternary in disabled 4-arg path
  // ====================================================================

  @Test
  void fourArgAnalyze_disabledConfig_withNonNullQueries_preservesQueries() {
    QueryAuditConfig config = QueryAuditConfig.builder().enabled(false).build();
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, List.of());

    List<QueryRecord> queries = List.of(record("SELECT 1"));

    QueryAuditReport report = analyzer.analyze("TC", "test", queries, EMPTY_INDEX);

    // Even though disabled, allQueries should be the original list, not empty
    assertThat(report.getAllQueries()).isEqualTo(queries);
  }

  @Test
  void fourArgAnalyze_disabledConfig_withNullQueries_returnsEmptyQueries() {
    QueryAuditConfig config = QueryAuditConfig.builder().enabled(false).build();
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, List.of());

    QueryAuditReport report = analyzer.analyze("TC", "test", null, EMPTY_INDEX);

    // null queries should be replaced with empty list
    assertThat(report.getAllQueries()).isEmpty();
  }

  // ====================================================================
  //  Mutation: Line 189 -- Baseline.isAcknowledged always returns true
  //  Test that acknowledged issues are separated from confirmed issues
  // ====================================================================

  @Test
  void baselineAcknowledgedIssues_separatedFromConfirmed() {
    // Create a baseline entry that acknowledges "select-all" issues
    List<BaselineEntry> baseline =
        List.of(new BaselineEntry("select-all", null, null, null, "dev", "acceptable"));
    QueryAuditConfig config = QueryAuditConfig.defaults();
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, baseline);

    List<QueryRecord> queries = List.of(record("SELECT * FROM users WHERE id = 1"));

    QueryAuditReport report = analyzer.analyze("baselineTest", queries, EMPTY_INDEX);

    // SELECT * should be acknowledged, not in confirmed issues
    assertThat(report.getConfirmedIssues().stream().filter(i -> i.type() == IssueType.SELECT_ALL))
        .isEmpty();
    assertThat(
            report.getAcknowledgedIssues().stream().filter(i -> i.type() == IssueType.SELECT_ALL))
        .isNotEmpty();
  }

  @Test
  void noBaseline_allIssuesAreNonAcknowledged() {
    // Empty baseline means nothing is acknowledged
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());

    List<QueryRecord> queries = List.of(record("SELECT * FROM users WHERE id = 1"));

    QueryAuditReport report = analyzer.analyze("noBaselineTest", queries, EMPTY_INDEX);

    // SELECT * is now INFO severity, so it should be in info issues (not acknowledged)
    assertThat(report.getInfoIssues().stream().filter(i -> i.type() == IssueType.SELECT_ALL))
        .isNotEmpty();
    assertThat(report.getAcknowledgedIssues()).isEmpty();
  }

  // ====================================================================
  //  Mutation: Line 202 -- severity == INFO always returns true
  //  Test that non-INFO issues go to confirmedIssues, not infoIssues
  // ====================================================================

  @Test
  void infoVsConfirmedIssues_correctlySplit() {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());

    // SELECT * triggers WARNING-severity issue (SELECT_ALL)
    // Repeated identical queries trigger INFO-severity N+1
    List<QueryRecord> queries =
        List.of(
            record("SELECT * FROM users WHERE id = 1"),
            record("SELECT * FROM users WHERE id = 2"),
            record("SELECT * FROM users WHERE id = 3"));

    QueryAuditReport report = analyzer.analyze("splitTest", queries, EMPTY_INDEX);

    // Confirmed issues should only contain ERROR or WARNING severity
    for (Issue issue : report.getConfirmedIssues()) {
      assertThat(issue.severity()).isIn(Severity.ERROR, Severity.WARNING);
    }

    // Info issues should only contain INFO severity
    for (Issue issue : report.getInfoIssues()) {
      assertThat(issue.severity()).isEqualTo(Severity.INFO);
    }
  }

  // ====================================================================
  //  Mutation: Line 208 -- sql != null filter always returns true
  //  Test that queries with null normalizedSql are not counted in unique patterns
  // ====================================================================

  @Test
  void uniquePatternCount_excludesNullNormalizedSql() {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());

    // Create a record with null SQL to get null normalizedSql
    QueryRecord nullSqlRecord =
        new QueryRecord(null, null, 1000L, System.currentTimeMillis(), "", 0);
    QueryRecord validRecord = record("SELECT id FROM users WHERE id = 1");

    List<QueryRecord> queries = List.of(nullSqlRecord, validRecord);

    QueryAuditReport report = analyzer.analyze("nullSqlTest", queries, EMPTY_INDEX);

    // uniquePatternCount should be 1, not 2 (null normalizedSql should be excluded)
    assertThat(report.getUniquePatternCount()).isEqualTo(1);
  }

  @Test
  void uniquePatternCount_allNullNormalizedSql_zeroPatterns() {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());

    QueryRecord nullSqlRecord1 =
        new QueryRecord(null, null, 1000L, System.currentTimeMillis(), "", 0);
    QueryRecord nullSqlRecord2 =
        new QueryRecord(null, null, 500L, System.currentTimeMillis(), "", 0);

    List<QueryRecord> queries = List.of(nullSqlRecord1, nullSqlRecord2);

    QueryAuditReport report = analyzer.analyze("allNullSqlTest", queries, EMPTY_INDEX);

    assertThat(report.getUniquePatternCount()).isEqualTo(0);
  }

  // ====================================================================
  //  4-arg analyze with empty queries returns empty report
  // ====================================================================

  @Test
  void fourArgAnalyze_emptyQueries_returnsEmptyReport() {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());

    QueryAuditReport report = analyzer.analyze("TestClass", "emptyTest", List.of(), EMPTY_INDEX);

    assertThat(report.getConfirmedIssues()).isEmpty();
    assertThat(report.getInfoIssues()).isEmpty();
    assertThat(report.getTotalQueryCount()).isEqualTo(0);
    assertThat(report.getTestClass()).isEqualTo("TestClass");
  }

  // ── Additional mutation-killing tests ──────────────────────────────

  /**
   * Kills NegateConditionalsMutator on line 76: baselinePath != null negated. When baselinePath IS
   * non-null, Baseline.load(baselinePath) should be called. When baselinePath IS null,
   * Baseline.load(default) should be called. If negated: non-null path -> uses default, null path
   * -> uses provided (null -> NPE or wrong). We verify that providing a specific path (that exists
   * with content) works differently from the default path.
   */
  @Test
  void constructorWithNonNullBaselinePathUsesProvidedPath_killsLine76() {
    // Create a temporary baseline file
    java.nio.file.Path tempDir;
    try {
      tempDir = java.nio.file.Files.createTempDirectory("queryaudit-test");
      java.nio.file.Path baselineFile = tempDir.resolve(".query-audit-baseline");
      // Write a valid baseline entry
      java.nio.file.Files.writeString(baselineFile, "select-all | | | tester | acceptable\n");

      QueryAuditConfig config = QueryAuditConfig.defaults();
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, baselineFile);

      // Baseline should have the entry we wrote
      assertThat(analyzer.getBaseline()).isNotEmpty();

      // Cleanup
      java.nio.file.Files.deleteIfExists(baselineFile);
      java.nio.file.Files.deleteIfExists(tempDir);
    } catch (java.io.IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void constructorWithNullBaselinePathUsesDefault_killsLine76() {
    // When baselinePath is null, should use default file name (which doesn't exist in test)
    QueryAuditConfig config = QueryAuditConfig.defaults();
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, (java.nio.file.Path) null);

    // Default baseline file doesn't exist -> baseline should be empty
    assertThat(analyzer.getBaseline()).isEmpty();
  }

  /**
   * Kills NegateConditionalsMutator on line 131 (first occurrence): !config.isEnabled() negated. If
   * negated, enabled config would return empty report, disabled would run analysis. We test that
   * enabled config WITH queries produces issues.
   */
  @Test
  void fourArgAnalyze_enabledConfig_producesIssues_killsLine131() {
    QueryAuditConfig config = QueryAuditConfig.defaults(); // enabled by default
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, List.of());

    List<QueryRecord> queries = List.of(record("SELECT * FROM users WHERE id = 1"));

    QueryAuditReport report = analyzer.analyze("TestClass", "enabledTest", queries, EMPTY_INDEX);

    // Enabled config with SELECT * should produce at least one confirmed issue
    assertThat(report.getConfirmedIssues()).isNotEmpty();
    assertThat(report.getTestClass()).isEqualTo("TestClass");
  }

  /**
   * Kills NegateConditionalsMutator on line 131 (second occurrence): queries == null ||
   * queries.isEmpty() negated. If negated, non-null non-empty queries would return empty report.
   * Already partially tested above but be more explicit.
   */
  @Test
  void fourArgAnalyze_nonEmptyQueries_runsAnalysis_killsLine131() {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());

    List<QueryRecord> queries =
        List.of(
            record("SELECT * FROM users WHERE id = 1"),
            record("SELECT * FROM users WHERE id = 2"),
            record("SELECT * FROM users WHERE id = 3"));

    QueryAuditReport report = analyzer.analyze("TC", "test", queries, EMPTY_INDEX);

    // Analysis should run and detect issues (SELECT * at minimum)
    assertThat(report.hasConfirmedIssues()).isTrue();
    assertThat(report.getTotalQueryCount()).isEqualTo(3);
  }

  /**
   * Kills EmptyObjectReturnValsMutator on line 239: getBaseline returns Collections.emptyList. When
   * a non-empty baseline is provided, getBaseline should return the actual entries.
   */
  @Test
  void getBaselineReturnsNonEmptyWhenBaselineProvided_killsLine239() {
    List<BaselineEntry> baseline =
        List.of(new BaselineEntry("select-all", null, null, null, "dev", "test"));
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), baseline);

    List<BaselineEntry> result = analyzer.getBaseline();

    assertThat(result).isNotEmpty();
    assertThat(result).hasSize(1);
    assertThat(result.get(0).issueCode()).isEqualTo("select-all");
  }
}
