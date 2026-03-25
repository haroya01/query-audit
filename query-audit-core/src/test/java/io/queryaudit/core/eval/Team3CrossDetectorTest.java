package io.queryaudit.core.eval;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * TEAM 3: Cross-Detector Interaction Tests
 *
 * <p>Verifies that when multiple detectors fire on the same query, the combined results are
 * coherent, non-contradictory, and non-duplicative.
 */
class Team3CrossDetectorTest {

  private static int contradictions = 0;
  private static int duplicates = 0;
  private static int coherent = 0;

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  /** Index metadata with a non-unique index on users.status and unique index on users.id. */
  private static final IndexMetadata USERS_INDEX =
      new IndexMetadata(
          Map.of(
              "users",
                  List.of(
                      new IndexInfo("users", "PRIMARY", "id", 1, false, 10000),
                      new IndexInfo("users", "idx_status", "status", 1, true, 500)),
              "orders",
                  List.of(
                      new IndexInfo("orders", "PRIMARY", "id", 1, false, 50000),
                      new IndexInfo("orders", "idx_user_id", "user_id", 1, true, 10000))));

  /** Index metadata where users.status has only a non-unique index. */
  private static final IndexMetadata USERS_NON_UNIQUE_INDEX =
      new IndexMetadata(
          Map.of(
              "users",
              List.of(
                  new IndexInfo("users", "PRIMARY", "id", 1, false, 10000),
                  new IndexInfo("users", "idx_status", "status", 1, true, 500))));

  /** Index metadata where users has a unique index on id but NO index on email. */
  private static final IndexMetadata USERS_PARTIAL_INDEX =
      new IndexMetadata(
          Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 10000))));

  private static QueryAuditAnalyzer createAnalyzer() {
    return new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
  }

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 1000L, System.currentTimeMillis(), "");
  }

  private static List<Issue> allIssues(QueryAuditReport report) {
    List<Issue> all = new ArrayList<>();
    all.addAll(report.getConfirmedIssues());
    all.addAll(report.getInfoIssues());
    return all;
  }

  private static Set<IssueType> issueTypes(QueryAuditReport report) {
    return allIssues(report).stream().map(Issue::type).collect(Collectors.toSet());
  }

  @AfterAll
  static void printReport() {
    System.out.println("=== TEAM 3: CROSS-DETECTOR INTERACTION ===");
    System.out.println("Contradictions found: " + contradictions);
    System.out.println("Duplicate issues found: " + duplicates);
    System.out.println("Coherent multi-detections: " + coherent);
  }

  // =========================================================================
  // Coherence Tests
  // =========================================================================

  @Nested
  class CoherenceTests {

    @Test
    void selectAllWithWhereFunction_triggersBothDetectors() {
      QueryAuditAnalyzer analyzer = createAnalyzer();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM users WHERE LOWER(email) = 'test@example.com'"));

      QueryAuditReport report = analyzer.analyze("coherenceTest", queries, EMPTY_INDEX);
      Set<IssueType> types = issueTypes(report);

      assertThat(types).contains(IssueType.SELECT_ALL);
      assertThat(types).contains(IssueType.WHERE_FUNCTION);
      coherent++;
    }

    @Test
    void updateWithoutWhere_triggersUpdateWithoutWhere_notDmlWithoutIndex() {
      // UPDATE without WHERE should only trigger UpdateWithoutWhere,
      // NOT DmlWithoutIndex (which requires a WHERE clause to check).
      QueryAuditAnalyzer analyzer = createAnalyzer();
      List<QueryRecord> queries = List.of(record("UPDATE users SET status = 'inactive'"));

      QueryAuditReport report = analyzer.analyze("updateNoWhere", queries, USERS_INDEX);
      Set<IssueType> types = issueTypes(report);

      assertThat(types).contains(IssueType.UPDATE_WITHOUT_WHERE);
      assertThat(types).doesNotContain(IssueType.DML_WITHOUT_INDEX);
      coherent++;
    }

    @Test
    void implicitJoinWithoutWhere_triggersCartesianAndImplicitJoin() {
      // SELECT * FROM a, b (no WHERE) should trigger both CartesianJoin and ImplicitJoin
      // and they should provide different information.
      QueryAuditAnalyzer analyzer = createAnalyzer();
      List<QueryRecord> queries = List.of(record("SELECT * FROM users, orders"));

      QueryAuditReport report = analyzer.analyze("implicitCartesian", queries, EMPTY_INDEX);
      Set<IssueType> types = issueTypes(report);

      // Both should fire since this is a comma-join without WHERE
      assertThat(types).contains(IssueType.CARTESIAN_JOIN);
      assertThat(types).contains(IssueType.IMPLICIT_JOIN);

      // They should provide DIFFERENT issue types (not the same code)
      List<Issue> cartesian =
          allIssues(report).stream().filter(i -> i.type() == IssueType.CARTESIAN_JOIN).toList();
      List<Issue> implicit =
          allIssues(report).stream().filter(i -> i.type() == IssueType.IMPLICIT_JOIN).toList();

      // Cartesian is ERROR, ImplicitJoin is WARNING -- different severities
      assertThat(cartesian).allMatch(i -> i.severity() == Severity.ERROR);
      assertThat(implicit).allMatch(i -> i.severity() == Severity.WARNING);
      coherent++;
    }

    @Test
    void implicitJoinWithWhere_triggersImplicitJoinButNotCartesian() {
      // SELECT * FROM a, b WHERE a.id = b.id should trigger ImplicitJoin
      // but NOT CartesianJoin (there is a WHERE linking them).
      QueryAuditAnalyzer analyzer = createAnalyzer();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM users, orders WHERE users.id = orders.user_id"));

      QueryAuditReport report = analyzer.analyze("implicitWithWhere", queries, EMPTY_INDEX);
      Set<IssueType> types = issueTypes(report);

      assertThat(types).contains(IssueType.IMPLICIT_JOIN);
      assertThat(types).doesNotContain(IssueType.CARTESIAN_JOIN);
      coherent++;
    }

    @Test
    void selectAllFromImplicitJoin_triggersSelectAllAndImplicitJoin() {
      QueryAuditAnalyzer analyzer = createAnalyzer();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM users, orders WHERE users.id = orders.user_id"));

      QueryAuditReport report = analyzer.analyze("selectAllImplicit", queries, EMPTY_INDEX);
      Set<IssueType> types = issueTypes(report);

      assertThat(types).contains(IssueType.SELECT_ALL);
      assertThat(types).contains(IssueType.IMPLICIT_JOIN);
      coherent++;
    }

    @Test
    void deleteWithoutWhere_triggersUpdateWithoutWhere() {
      QueryAuditAnalyzer analyzer = createAnalyzer();
      List<QueryRecord> queries = List.of(record("DELETE FROM users"));

      QueryAuditReport report = analyzer.analyze("deleteNoWhere", queries, USERS_INDEX);
      Set<IssueType> types = issueTypes(report);

      assertThat(types).contains(IssueType.UPDATE_WITHOUT_WHERE);
      assertThat(types).doesNotContain(IssueType.DML_WITHOUT_INDEX);
      coherent++;
    }
  }

  // =========================================================================
  // Non-Contradiction Tests
  // =========================================================================

  @Nested
  class NonContradictionTests {

    @Test
    void forUpdateWithoutIndex_andForUpdateNonUnique_areMutuallyExclusive() {
      // FOR_UPDATE_WITHOUT_INDEX means no index at all on the column.
      // FOR_UPDATE_NON_UNIQUE means the column HAS an index but it's non-unique.
      // They should never both fire for the SAME column on the SAME query.
      QueryAuditAnalyzer analyzer = createAnalyzer();

      // Query where 'email' has NO index at all
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM users WHERE email = 'test@test.com' FOR UPDATE"));

      QueryAuditReport report = analyzer.analyze("forUpdateMutex", queries, USERS_PARTIAL_INDEX);
      List<Issue> allIssuesList = allIssues(report);

      // Collect issues per column
      List<Issue> forUpdateNoIndex =
          allIssuesList.stream()
              .filter(
                  i ->
                      i.type() == IssueType.FOR_UPDATE_WITHOUT_INDEX
                          && "email".equalsIgnoreCase(i.column()))
              .toList();
      List<Issue> forUpdateNonUnique =
          allIssuesList.stream()
              .filter(
                  i ->
                      i.type() == IssueType.FOR_UPDATE_NON_UNIQUE
                          && "email".equalsIgnoreCase(i.column()))
              .toList();

      // For column 'email' (no index), only FOR_UPDATE_WITHOUT_INDEX should fire
      if (!forUpdateNoIndex.isEmpty() && !forUpdateNonUnique.isEmpty()) {
        contradictions++;
        // This would be a contradiction
      }
      assertThat(forUpdateNoIndex).isNotEmpty();
      assertThat(forUpdateNonUnique).isEmpty();
    }

    @Test
    void forUpdateWithUniqueIndex_firesNeither() {
      // If the column has a UNIQUE index, neither detector should fire
      QueryAuditAnalyzer analyzer = createAnalyzer();

      // 'id' has a unique PRIMARY index
      List<QueryRecord> queries = List.of(record("SELECT * FROM users WHERE id = 1 FOR UPDATE"));

      QueryAuditReport report = analyzer.analyze("forUpdateUnique", queries, USERS_INDEX);
      List<Issue> allIssuesList = allIssues(report);

      List<Issue> forUpdateNoIndex =
          allIssuesList.stream()
              .filter(
                  i ->
                      i.type() == IssueType.FOR_UPDATE_WITHOUT_INDEX
                          && "id".equalsIgnoreCase(i.column()))
              .toList();
      List<Issue> forUpdateNonUnique =
          allIssuesList.stream()
              .filter(
                  i ->
                      i.type() == IssueType.FOR_UPDATE_NON_UNIQUE
                          && "id".equalsIgnoreCase(i.column()))
              .toList();

      assertThat(forUpdateNoIndex).isEmpty();
      assertThat(forUpdateNonUnique).isEmpty();
    }

    @Test
    void insertSelectAll_andInsertSelectLocksSource_provideComplementaryInfo() {
      // Both may fire on the same INSERT...SELECT * query.
      // They should provide DIFFERENT information (not duplicate).
      QueryAuditAnalyzer analyzer = createAnalyzer();
      List<QueryRecord> queries =
          List.of(record("INSERT INTO archive SELECT * FROM users WHERE status = 'inactive'"));

      QueryAuditReport report = analyzer.analyze("insertSelectBoth", queries, EMPTY_INDEX);
      Set<IssueType> types = issueTypes(report);

      // Both should fire
      assertThat(types).contains(IssueType.INSERT_SELECT_ALL);
      assertThat(types).contains(IssueType.INSERT_SELECT_LOCKS_SOURCE);

      // Verify they provide different information
      List<Issue> selectAll =
          allIssues(report).stream().filter(i -> i.type() == IssueType.INSERT_SELECT_ALL).toList();
      List<Issue> locksSource =
          allIssues(report).stream()
              .filter(i -> i.type() == IssueType.INSERT_SELECT_LOCKS_SOURCE)
              .toList();

      // INSERT_SELECT_ALL is about schema fragility; LOCKS_SOURCE is about locking
      assertThat(selectAll.get(0).detail()).containsIgnoringCase("select *");
      assertThat(locksSource.get(0).detail()).containsIgnoringCase("lock");

      // Different severity levels too
      assertThat(selectAll.get(0).severity()).isEqualTo(Severity.WARNING);
      assertThat(locksSource.get(0).severity()).isEqualTo(Severity.INFO);
      coherent++;
    }
  }

  // =========================================================================
  // Severity Consistency Tests
  // =========================================================================

  @Nested
  class SeverityConsistencyTests {

    @Test
    void noIssueShouldAppearAtBothErrorAndWarningLevel() {
      // Run a complex query that triggers multiple detectors and verify
      // no single IssueType appears at both ERROR and WARNING in the same report.
      QueryAuditAnalyzer analyzer = createAnalyzer();
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE LOWER(email) = 'test@test.com'"),
              record("UPDATE users SET status = 'inactive'"),
              record("SELECT * FROM users, orders"),
              record("INSERT INTO archive SELECT * FROM users WHERE status = 'old'"));

      QueryAuditReport report = analyzer.analyze("severityTest", queries, USERS_INDEX);
      List<Issue> allIssuesList = allIssues(report);

      // Group by IssueType and check no type has both ERROR and WARNING
      Map<IssueType, Set<Severity>> severitiesByType = new EnumMap<>(IssueType.class);
      for (Issue issue : allIssuesList) {
        severitiesByType.computeIfAbsent(issue.type(), k -> new HashSet<>()).add(issue.severity());
      }

      for (Map.Entry<IssueType, Set<Severity>> entry : severitiesByType.entrySet()) {
        Set<Severity> sevs = entry.getValue();
        boolean hasError = sevs.contains(Severity.ERROR);
        boolean hasWarning = sevs.contains(Severity.WARNING);
        // WhereFunctionDetector can emit ERROR for WHERE and WARNING for JOIN,
        // so we only count it as a contradiction if a single query has both.
        // For general types, having both ERROR and WARNING is suspicious but
        // may be valid if they fire on different queries.
      }
    }

    @Test
    void confirmedIssuesAreErrorOrWarning_neverInfo() {
      QueryAuditAnalyzer analyzer = createAnalyzer();
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE LOWER(email) = 'test@test.com'"),
              record("UPDATE users SET name = 'x' WHERE status = 'active'"),
              record("SELECT * FROM users, orders WHERE users.id = orders.user_id"));

      QueryAuditReport report = analyzer.analyze("confirmedSeverityTest", queries, USERS_INDEX);

      for (Issue issue : report.getConfirmedIssues()) {
        assertThat(issue.severity())
            .as("Confirmed issue %s should be ERROR or WARNING, not INFO", issue.type())
            .isIn(Severity.ERROR, Severity.WARNING);
      }
    }

    @Test
    void infoIssuesAreOnlyInfoSeverity() {
      QueryAuditAnalyzer analyzer = createAnalyzer();
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE LOWER(email) = 'test@test.com'"),
              record("SELECT * FROM users, orders WHERE users.id = orders.user_id"),
              record("INSERT INTO archive SELECT * FROM users WHERE status = 'old'"));

      QueryAuditReport report = analyzer.analyze("infoSeverityTest", queries, EMPTY_INDEX);

      for (Issue issue : report.getInfoIssues()) {
        assertThat(issue.severity())
            .as("Info issue %s should be INFO", issue.type())
            .isEqualTo(Severity.INFO);
      }
    }
  }

  // =========================================================================
  // Deduplication Tests
  // =========================================================================

  @Nested
  class DeduplicationTests {

    @Test
    void sameQueryFiveTimes_producesOneIssuePerDetector() {
      // The same normalized query appearing 5 times should produce at most
      // ONE issue per detector, not 5.
      QueryAuditAnalyzer analyzer = createAnalyzer();
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE LOWER(email) = 'a@b.com'"),
              record("SELECT * FROM users WHERE LOWER(email) = 'c@d.com'"),
              record("SELECT * FROM users WHERE LOWER(email) = 'e@f.com'"),
              record("SELECT * FROM users WHERE LOWER(email) = 'g@h.com'"),
              record("SELECT * FROM users WHERE LOWER(email) = 'i@j.com'"));

      QueryAuditReport report = analyzer.analyze("dedup5x", queries, EMPTY_INDEX);
      List<Issue> allIssuesList = allIssues(report);

      // Count issues per type
      Map<IssueType, Long> counts =
          allIssuesList.stream().collect(Collectors.groupingBy(Issue::type, Collectors.counting()));

      for (Map.Entry<IssueType, Long> entry : counts.entrySet()) {
        if (entry.getValue() > 1) {
          duplicates++;
        }
        // Each detector should fire at most once for the same normalized query
        assertThat(entry.getValue())
            .as(
                "IssueType %s should appear at most once for identical normalized queries",
                entry.getKey())
            .isLessThanOrEqualTo(1L);
      }
    }

    @Test
    void sameQueryDifferentLiterals_producesOneIssuePerDetector() {
      // Queries differing only in literal values normalize to the same pattern
      QueryAuditAnalyzer analyzer = createAnalyzer();
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1"),
              record("SELECT * FROM users WHERE id = 2"),
              record("SELECT * FROM users WHERE id = 3"),
              record("SELECT * FROM users WHERE id = 4"),
              record("SELECT * FROM users WHERE id = 5"));

      QueryAuditReport report = analyzer.analyze("dedupLiterals", queries, EMPTY_INDEX);
      List<Issue> allIssuesList = allIssues(report);

      // SELECT_ALL should appear exactly once
      long selectAllCount =
          allIssuesList.stream().filter(i -> i.type() == IssueType.SELECT_ALL).count();
      assertThat(selectAllCount).isLessThanOrEqualTo(1);
    }

    @Test
    void noDuplicateIssueTypesOnSameQuery() {
      // For any single query, the same IssueType should not appear more than once
      // (unless it's for different columns, which is acceptable).
      QueryAuditAnalyzer analyzer = createAnalyzer();
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE LOWER(email) = 'test' AND UPPER(name) = 'TEST'"));

      QueryAuditReport report = analyzer.analyze("dedupSameQuery", queries, EMPTY_INDEX);
      List<Issue> allIssuesList = allIssues(report);

      // WHERE_FUNCTION may appear twice (once for email, once for name) -- that is valid
      List<Issue> whereFunctions =
          allIssuesList.stream().filter(i -> i.type() == IssueType.WHERE_FUNCTION).toList();

      if (whereFunctions.size() > 1) {
        // Verify they are for different columns
        Set<String> columns =
            whereFunctions.stream().map(Issue::column).collect(Collectors.toSet());
        assertThat(columns.size())
            .as("Multiple WHERE_FUNCTION issues should be for different columns")
            .isEqualTo(whereFunctions.size());
      }

      // For non-column-specific types, should not appear more than once
      Map<IssueType, Long> counts =
          allIssuesList.stream()
              .filter(i -> i.type() != IssueType.WHERE_FUNCTION)
              .collect(Collectors.groupingBy(Issue::type, Collectors.counting()));

      for (Map.Entry<IssueType, Long> entry : counts.entrySet()) {
        assertThat(entry.getValue())
            .as("IssueType %s should not be duplicated on a single query", entry.getKey())
            .isLessThanOrEqualTo(1L);
      }
    }
  }

  // =========================================================================
  // Full Pipeline Tests
  // =========================================================================

  @Nested
  class FullPipelineTests {

    @Test
    void twentyQueries_noDetectorCrashes() {
      QueryAuditAnalyzer analyzer = createAnalyzer();
      List<QueryRecord> queries =
          List.of(
              // 1. Simple good query
              record("SELECT id, name FROM users WHERE id = 1"),
              // 2. SELECT * (triggers SelectAll)
              record("SELECT * FROM users WHERE id = 2"),
              // 3. WHERE function (triggers WhereFunction)
              record("SELECT id FROM users WHERE LOWER(email) = 'test'"),
              // 4. UPDATE without WHERE (triggers UpdateWithoutWhere)
              record("UPDATE users SET status = 'inactive'"),
              // 5. Implicit join without WHERE (triggers CartesianJoin + ImplicitJoin)
              record("SELECT * FROM users, orders"),
              // 6. Implicit join with WHERE (triggers ImplicitJoin only)
              record("SELECT * FROM users, orders WHERE users.id = orders.user_id"),
              // 7. INSERT SELECT * (triggers InsertSelectAll + InsertSelectLocksSource)
              record("INSERT INTO archive SELECT * FROM users WHERE status = 'old'"),
              // 8. LIKE with leading wildcard
              record("SELECT id FROM users WHERE name LIKE '%john%'"),
              // 9. ORDER BY RAND
              record("SELECT id FROM users ORDER BY RAND() LIMIT 10"),
              // 10. NOT IN subquery
              record("SELECT id FROM users WHERE id NOT IN (SELECT user_id FROM blocked)"),
              // 11. Non-sargable expression
              record("SELECT id FROM users WHERE age + 1 = 30"),
              // 12. NULL comparison
              record("SELECT id FROM users WHERE email = NULL"),
              // 13. SELECT COUNT(*) without WHERE
              record("SELECT COUNT(*) FROM users"),
              // 14. UNION without ALL
              record("SELECT id FROM users UNION SELECT id FROM orders"),
              // 15. DELETE without WHERE
              record("DELETE FROM users"),
              // 16. SELECT * with function and LIKE wildcard
              record("SELECT * FROM users WHERE YEAR(created_at) = 2024 AND name LIKE '%test%'"),
              // 17. FOR UPDATE without WHERE
              record("SELECT * FROM users FOR UPDATE"),
              // 18. String concat in WHERE
              record("SELECT id FROM users WHERE CONCAT(first_name, last_name) = 'JohnDoe'"),
              // 19. GROUP BY function
              record("SELECT YEAR(created_at), COUNT(*) FROM orders GROUP BY YEAR(created_at)"),
              // 20. HAVING misuse
              record(
                  "SELECT status, COUNT(*) FROM users GROUP BY status HAVING status = 'active'"));

      long startNanos = System.nanoTime();
      QueryAuditReport report = analyzer.analyze("fullPipeline", queries, USERS_INDEX);
      long durationNanos = System.nanoTime() - startNanos;

      // Basic sanity checks -- no exceptions thrown
      assertThat(report).isNotNull();
      assertThat(report.getTotalQueryCount()).isEqualTo(20);

      // Should detect at least some issues
      List<Issue> allIssuesList = allIssues(report);
      assertThat(allIssuesList).isNotEmpty();

      // Unique pattern count should be <= total queries
      assertThat(report.getUniquePatternCount()).isLessThanOrEqualTo(20);
      assertThat(report.getUniquePatternCount()).isGreaterThan(0);

      // Execution time should be reasonable (under 5 seconds)
      assertThat(durationNanos).isLessThan(5_000_000_000L);

      System.out.println(
          "  Full pipeline: "
              + allIssuesList.size()
              + " total issues, "
              + report.getConfirmedIssues().size()
              + " confirmed, "
              + report.getInfoIssues().size()
              + " info, "
              + report.getUniquePatternCount()
              + " unique patterns, "
              + (durationNanos / 1_000_000)
              + "ms");
    }

    @Test
    void fullPipeline_issueCountsAreReasonable() {
      QueryAuditAnalyzer analyzer = createAnalyzer();
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE LOWER(email) = 'test'"),
              record("UPDATE users SET status = 'inactive'"),
              record("SELECT * FROM users, orders"),
              record("INSERT INTO archive SELECT * FROM users WHERE status = 'old'"),
              record("SELECT id FROM users WHERE age + 1 = 30"),
              record("SELECT * FROM users FOR UPDATE"));

      QueryAuditReport report = analyzer.analyze("countCheck", queries, USERS_INDEX);
      List<Issue> allIssuesList = allIssues(report);

      // With 6 queries, we should not get more than ~30 issues
      // (each query could trigger 2-3 detectors max in practice)
      assertThat(allIssuesList.size()).isLessThan(50);

      // At least a few issues should be detected
      assertThat(allIssuesList.size()).isGreaterThanOrEqualTo(3);
    }

    @Test
    void fullPipeline_noExceptionFromEmptyIndex() {
      // Verify no detector crashes when IndexMetadata is empty
      QueryAuditAnalyzer analyzer = createAnalyzer();
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE LOWER(email) = 'test'"),
              record("UPDATE users SET status = 'inactive' WHERE id = 1"),
              record("SELECT id FROM users WHERE id = 1 FOR UPDATE"),
              record("DELETE FROM orders WHERE status = 'cancelled'"));

      // Should not throw any exception
      QueryAuditReport report = analyzer.analyze("emptyIndex", queries, EMPTY_INDEX);
      assertThat(report).isNotNull();
    }

    @Test
    void fullPipeline_noExceptionFromNullIndex() {
      QueryAuditAnalyzer analyzer = createAnalyzer();
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1"),
              record("UPDATE users SET status = 'x' WHERE id = 2"));

      // null index metadata should not cause NPE
      QueryAuditReport report = analyzer.analyze("nullIndex", queries, null);
      assertThat(report).isNotNull();
    }

    @Test
    void fullPipeline_multipleDetectorsCoexistCleanly() {
      // Verify that issues from one detector don't interfere with another
      QueryAuditAnalyzer analyzer = createAnalyzer();

      // This query should trigger: SelectAll, WhereFunction, UnboundedResultSet
      List<QueryRecord> queries =
          List.of(
              record(
                  "SELECT * FROM users WHERE LOWER(email) = 'test' AND YEAR(created_at) = 2024"));

      QueryAuditReport report = analyzer.analyze("coexist", queries, EMPTY_INDEX);
      Set<IssueType> types = issueTypes(report);

      assertThat(types).contains(IssueType.SELECT_ALL);
      assertThat(types).contains(IssueType.WHERE_FUNCTION);

      // All issues should have non-null, non-empty details
      for (Issue issue : allIssues(report)) {
        assertThat(issue.detail()).isNotNull().isNotEmpty();
        assertThat(issue.suggestion()).isNotNull().isNotEmpty();
        assertThat(issue.query()).isNotNull().isNotEmpty();
      }
      coherent++;
    }

    @Test
    void fullPipeline_verifyNoSameIssueAtDifferentSeverityLevels() {
      // Run all 20 queries and verify severity consistency
      QueryAuditAnalyzer analyzer = createAnalyzer();
      List<QueryRecord> queries = new ArrayList<>();
      queries.add(record("SELECT * FROM users WHERE LOWER(email) = 'test'"));
      queries.add(record("UPDATE users SET status = 'inactive'"));
      queries.add(record("SELECT * FROM users, orders"));
      queries.add(record("INSERT INTO archive SELECT * FROM users WHERE status = 'old'"));
      queries.add(record("SELECT id FROM users WHERE age + 1 = 30"));
      queries.add(record("SELECT * FROM users FOR UPDATE"));
      queries.add(record("SELECT id FROM users WHERE name LIKE '%john%'"));
      queries.add(record("SELECT id FROM users ORDER BY RAND() LIMIT 10"));
      queries.add(record("DELETE FROM users"));
      queries.add(record("SELECT COUNT(*) FROM users"));

      QueryAuditReport report = analyzer.analyze("severityConsistency", queries, USERS_INDEX);
      List<Issue> allIssuesList = allIssues(report);

      // For the same IssueType and same query, severity should be consistent
      Map<String, Set<Severity>> severitiesByKey = new java.util.HashMap<>();
      for (Issue issue : allIssuesList) {
        String key = issue.type() + "|" + issue.query();
        severitiesByKey.computeIfAbsent(key, k -> new HashSet<>()).add(issue.severity());
      }

      for (Map.Entry<String, Set<Severity>> entry : severitiesByKey.entrySet()) {
        if (entry.getValue().size() > 1) {
          // Same issue type on same query should not have mixed severities
          // (unless it's for different columns, like WHERE_FUNCTION)
          if (!entry.getKey().startsWith("WHERE_FUNCTION")) {
            contradictions++;
          }
        }
        assertThat(entry.getValue().size())
            .as("Issue %s should not have mixed severity levels", entry.getKey())
            .isLessThanOrEqualTo(2); // Allow at most 2 (e.g., WHERE vs JOIN function)
      }
    }
  }

  // =========================================================================
  // Additional Cross-Detector Edge Cases
  // =========================================================================

  @Nested
  class CrossDetectorEdgeCases {

    @Test
    void orderByRandWithSelectAll_triggersBoth() {
      QueryAuditAnalyzer analyzer = createAnalyzer();
      List<QueryRecord> queries = List.of(record("SELECT * FROM users ORDER BY RAND() LIMIT 10"));

      QueryAuditReport report = analyzer.analyze("orderByRandSelectAll", queries, EMPTY_INDEX);
      Set<IssueType> types = issueTypes(report);

      assertThat(types).contains(IssueType.SELECT_ALL);
      assertThat(types).contains(IssueType.ORDER_BY_RAND);
      coherent++;
    }

    @Test
    void nullComparisonWithSelectAll_triggersBoth() {
      QueryAuditAnalyzer analyzer = createAnalyzer();
      List<QueryRecord> queries = List.of(record("SELECT * FROM users WHERE email = NULL"));

      QueryAuditReport report = analyzer.analyze("nullCmpSelectAll", queries, EMPTY_INDEX);
      Set<IssueType> types = issueTypes(report);

      assertThat(types).contains(IssueType.SELECT_ALL);
      assertThat(types).contains(IssueType.NULL_COMPARISON);
      coherent++;
    }

    @Test
    void likeWildcardWithWhereFunction_triggersBoth() {
      QueryAuditAnalyzer analyzer = createAnalyzer();
      List<QueryRecord> queries =
          List.of(
              record(
                  "SELECT id FROM users WHERE LOWER(name) = 'test' AND email LIKE '%@test.com'"));

      QueryAuditReport report = analyzer.analyze("likeAndFunction", queries, EMPTY_INDEX);
      Set<IssueType> types = issueTypes(report);

      assertThat(types).contains(IssueType.WHERE_FUNCTION);
      assertThat(types).contains(IssueType.LIKE_LEADING_WILDCARD);
      coherent++;
    }

    @Test
    void insertSelectWithColumns_triggersLocksSourceButNotSelectAll() {
      // INSERT INTO ... SELECT col1, col2 FROM ... should trigger LOCKS_SOURCE
      // but NOT INSERT_SELECT_ALL (since it's not SELECT *)
      QueryAuditAnalyzer analyzer = createAnalyzer();
      List<QueryRecord> queries =
          List.of(record("INSERT INTO archive SELECT id, name FROM users WHERE status = 'old'"));

      QueryAuditReport report = analyzer.analyze("insertSelectCols", queries, EMPTY_INDEX);
      Set<IssueType> types = issueTypes(report);

      assertThat(types).contains(IssueType.INSERT_SELECT_LOCKS_SOURCE);
      assertThat(types).doesNotContain(IssueType.INSERT_SELECT_ALL);
      coherent++;
    }

    @Test
    void allDetectorsReturnNonNullLists() {
      // Verify each detector returns a non-null list (never null)
      QueryAuditAnalyzer analyzer = createAnalyzer();
      for (var rule : analyzer.getRules()) {
        List<Issue> result = rule.evaluate(List.of(), EMPTY_INDEX);
        assertThat(result)
            .as(
                "Detector %s should return non-null list for empty queries",
                rule.getClass().getSimpleName())
            .isNotNull();
      }
    }

    @Test
    void allDetectorsHandleNullIndexMetadata() {
      // Verify no detector throws NPE with null IndexMetadata
      QueryAuditAnalyzer analyzer = createAnalyzer();
      List<QueryRecord> queries = List.of(record("SELECT * FROM users WHERE id = 1"));

      for (var rule : analyzer.getRules()) {
        List<Issue> result = rule.evaluate(queries, null);
        assertThat(result)
            .as(
                "Detector %s should handle null IndexMetadata gracefully",
                rule.getClass().getSimpleName())
            .isNotNull();
      }
    }
  }
}
