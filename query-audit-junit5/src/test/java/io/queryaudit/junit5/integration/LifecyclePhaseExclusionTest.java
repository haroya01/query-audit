package io.queryaudit.junit5.integration;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.LifecyclePhase;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.junit5.EnableQueryInspector;
import io.queryaudit.junit5.BooleanOverride;
import io.queryaudit.junit5.QueryAudit;
import io.queryaudit.junit5.integration.entity.Member;
import io.queryaudit.junit5.integration.entity.Team;
import io.queryaudit.junit5.integration.repository.MemberRepository;
import io.queryaudit.junit5.integration.repository.TeamRepository;
import jakarta.persistence.EntityManager;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

/**
 * Tests verifying that test lifecycle queries ({@code @BeforeEach} / {@code @AfterEach})
 * should be excluded from query-audit detection to prevent false positives.
 *
 * <p>Issue #35: {@code deleteAll()} in setup triggers {@code update-without-where} (ERROR),
 * and repeated {@code save()} triggers {@code repeated-single-insert} (WARNING).
 * These are test infrastructure, not production code paths.
 *
 * <h3>Test Groups</h3>
 * <ul>
 *   <li><b>FalsePositiveProof</b> — Proves false positives exist with the current approach
 *       (capturing all queries between start/stop). These tests PASS now.</li>
 *   <li><b>DesiredBehavior</b> — Defines the expected behavior after lifecycle phase
 *       awareness is implemented. These tests FAIL until the feature is implemented.</li>
 * </ul>
 */
@SpringBootTest(classes = TestApplication.class)
@EnableQueryInspector
@Transactional
class LifecyclePhaseExclusionTest {

  @Autowired TeamRepository teamRepository;
  @Autowired MemberRepository memberRepository;
  @Autowired QueryInterceptor queryInterceptor;
  @Autowired EntityManager entityManager;

  // ── Group 1: Proving false positives exist (PASS now) ──────────────

  @Nested
  @DisplayName("False Positive Proof — current behavior captures setup queries")
  class FalsePositiveProof {

    @Test
    @DisplayName("deleteAllInBatch() in @BeforeEach triggers update-without-where false positive")
    void deleteAllInSetupTriggersUpdateWithoutWhere() {
      // Simulate what a typical @BeforeEach does: clean up before test
      // deleteAllInBatch() generates bare "DELETE FROM table" without WHERE
      queryInterceptor.start();
      memberRepository.deleteAllInBatch();
      teamRepository.deleteAllInBatch();
      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
      QueryAuditReport report = analyzer.analyze("FalsePositiveProof", "deleteAll", queries, null);

      // PROVES THE PROBLEM: deleteAll() triggers ERROR-level update-without-where
      List<Issue> updateWithoutWhere =
          report.getConfirmedIssues().stream()
              .filter(i -> i.type() == IssueType.UPDATE_WITHOUT_WHERE)
              .toList();

      assertThat(updateWithoutWhere)
          .as("deleteAll() in @BeforeEach should trigger update-without-where (proving false positive exists)")
          .isNotEmpty();
    }

    @Test
    @DisplayName("repeated save() in @BeforeEach triggers repeated-single-insert false positive")
    void repeatedSaveInSetupTriggersRepeatedSingleInsert() {
      // Simulate what a typical @BeforeEach does: create test fixtures
      queryInterceptor.start();
      for (int i = 0; i < 5; i++) {
        Team team = new Team("Team " + i);
        teamRepository.save(team);
      }
      entityManager.flush();
      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
      QueryAuditReport report =
          analyzer.analyze("FalsePositiveProof", "repeatedSave", queries, null);

      // PROVES THE PROBLEM: repeated save() triggers repeated-single-insert
      List<Issue> repeatedInsert =
          report.getConfirmedIssues().stream()
              .filter(i -> i.type() == IssueType.REPEATED_SINGLE_INSERT)
              .toList();

      // Also check INFO-level in case severity differs
      List<Issue> repeatedInsertInfo =
          report.getInfoIssues().stream()
              .filter(i -> i.type() == IssueType.REPEATED_SINGLE_INSERT)
              .toList();

      assertThat(repeatedInsert.size() + repeatedInsertInfo.size())
          .as("Repeated save() in @BeforeEach should trigger repeated-single-insert (proving false positive exists)")
          .isGreaterThan(0);
    }

    @Test
    @DisplayName("Combined setup pattern: deleteAll + repeated save produces multiple false positives")
    void combinedSetupPatternProducesMultipleFalsePositives() {
      // This is the realistic scenario described in the issue
      queryInterceptor.start();

      // Cleanup phase (typical @BeforeEach)
      memberRepository.deleteAllInBatch();
      teamRepository.deleteAllInBatch();

      // Fixture creation phase (typical @BeforeEach)
      for (int i = 0; i < 5; i++) {
        Team team = new Team("Team " + i);
        teamRepository.save(team);
        for (int j = 0; j < 3; j++) {
          Member member =
              new Member("Member " + i + "-" + j, "m" + i + j + "@test.com", "ACTIVE");
          member.setTeam(team);
          memberRepository.save(member);
        }
      }
      entityManager.flush();
      entityManager.clear();

      // Actual test: a simple query
      memberRepository.findByStatus("ACTIVE");

      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
      QueryAuditReport report =
          analyzer.analyze("FalsePositiveProof", "combined", queries, null);

      // Count false positives from setup
      long setupFalsePositives =
          report.getConfirmedIssues().stream()
              .filter(
                  i ->
                      i.type() == IssueType.UPDATE_WITHOUT_WHERE
                          || i.type() == IssueType.REPEATED_SINGLE_INSERT)
              .count();

      assertThat(setupFalsePositives)
          .as("Combined setup pattern should produce multiple false positives from test infrastructure")
          .isGreaterThanOrEqualTo(2);
    }
  }

  // ── Group 2: Desired behavior after implementation (FAIL until implemented) ──

  @Nested
  @DisplayName("Desired Behavior — lifecycle phase awareness")
  class DesiredBehavior {

    @BeforeEach
    void setUpFixtures() {
      for (int i = 0; i < 5; i++) {
        Team team = new Team("Team " + i);
        teamRepository.save(team);
        for (int j = 0; j < 3; j++) {
          Member member =
              new Member("Member " + i + "-" + j, "m" + i + j + "@test.com", "ACTIVE");
          member.setTeam(team);
          memberRepository.save(member);
        }
      }
      entityManager.flush();
      entityManager.clear();
    }

    @Test
    @DisplayName("QueryInterceptor should support lifecycle phase tracking")
    void interceptorSupportsPhaseTracking() {
      queryInterceptor.start();
      queryInterceptor.setPhase(LifecyclePhase.SETUP);

      memberRepository.deleteAllInBatch();

      queryInterceptor.setPhase(LifecyclePhase.TEST);

      memberRepository.findByStatus("ACTIVE");

      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();

      // All queries should have a phase assigned
      assertThat(queries).allSatisfy(q -> assertThat(q.phase()).isNotNull());

      // Setup queries should be tagged as SETUP
      List<QueryRecord> setupQueries =
          queries.stream().filter(q -> q.phase() == LifecyclePhase.SETUP).toList();
      assertThat(setupQueries)
          .as("deleteAllInBatch() queries should be tagged as SETUP")
          .isNotEmpty();

      // Test queries should be tagged as TEST
      List<QueryRecord> testQueries =
          queries.stream().filter(q -> q.phase() == LifecyclePhase.TEST).toList();
      assertThat(testQueries)
          .as("findByStatus() queries should be tagged as TEST")
          .isNotEmpty();
    }

    @Test
    @DisplayName("Analyzer should exclude SETUP-phase queries from detection by default")
    void analyzerExcludesSetupPhaseQueries() {
      queryInterceptor.start();
      queryInterceptor.setPhase(LifecyclePhase.SETUP);

      // Setup: deleteAllInBatch + repeated save → would trigger false positives
      memberRepository.deleteAllInBatch();
      teamRepository.deleteAllInBatch();
      for (int i = 0; i < 5; i++) {
        teamRepository.save(new Team("T" + i));
      }
      entityManager.flush();

      queryInterceptor.setPhase(LifecyclePhase.TEST);

      // Test: a clean query with no issues
      memberRepository.findByStatus("ACTIVE");

      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
      QueryAuditReport report =
          analyzer.analyze("DesiredBehavior", "excludeSetup", queries, null);

      // After the fix: setup-phase false positives should NOT appear
      assertThat(report.getConfirmedIssues())
          .as("SETUP-phase queries should be excluded from confirmed issues")
          .noneMatch(i -> i.type() == IssueType.UPDATE_WITHOUT_WHERE)
          .noneMatch(i -> i.type() == IssueType.REPEATED_SINGLE_INSERT);
    }

    @Test
    @DisplayName("Analyzer should still detect issues in TEST-phase queries")
    void analyzerStillDetectsTestPhaseIssues() {
      queryInterceptor.start();
      queryInterceptor.setPhase(LifecyclePhase.TEST);

      // These happen during the test method — should be detected
      memberRepository.deleteAllInBatch(); // update-without-where: legitimate detection
      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
      QueryAuditReport report =
          analyzer.analyze("DesiredBehavior", "detectTestPhase", queries, null);

      // TEST-phase update-without-where should still be detected
      assertThat(report.getConfirmedIssues())
          .as("TEST-phase queries should still trigger detection rules")
          .anyMatch(i -> i.type() == IssueType.UPDATE_WITHOUT_WHERE);
    }

    @Test
    @DisplayName("TEARDOWN-phase queries should also be excluded by default")
    void teardownPhaseQueriesExcluded() {
      queryInterceptor.start();
      queryInterceptor.setPhase(LifecyclePhase.TEST);

      memberRepository.findByStatus("ACTIVE");

      queryInterceptor.setPhase(LifecyclePhase.TEARDOWN);

      // Cleanup in @AfterEach — should not trigger detection
      memberRepository.deleteAllInBatch();
      teamRepository.deleteAllInBatch();

      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
      QueryAuditReport report =
          analyzer.analyze("DesiredBehavior", "excludeTeardown", queries, null);

      assertThat(report.getConfirmedIssues())
          .as("TEARDOWN-phase queries should be excluded from detection")
          .noneMatch(i -> i.type() == IssueType.UPDATE_WITHOUT_WHERE);
    }

    @Test
    @DisplayName("Default phase should be TEST when not explicitly set")
    void defaultPhaseIsTest() {
      queryInterceptor.start();
      // No explicit setPhase() call — should default to TEST for backward compatibility

      memberRepository.findByStatus("ACTIVE");

      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();

      assertThat(queries)
          .as("When no phase is set, queries should default to TEST phase")
          .allSatisfy(q -> assertThat(q.phase()).isEqualTo(LifecyclePhase.TEST));
    }

    @Test
    @DisplayName("includeSetupQueries config should analyze all phases when enabled")
    void includeSetupQueriesAnalyzesAllPhases() {
      queryInterceptor.start();
      queryInterceptor.setPhase(LifecyclePhase.SETUP);

      memberRepository.deleteAllInBatch();

      queryInterceptor.setPhase(LifecyclePhase.TEST);

      memberRepository.findByStatus("ACTIVE");

      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();

      // With includeSetupQueries = true, setup queries should be analyzed
      QueryAuditConfig config =
          QueryAuditConfig.builder().includeSetupQueries(true).failOnDetection(false).build();
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config);
      QueryAuditReport report =
          analyzer.analyze("DesiredBehavior", "includeSetup", queries, null);

      // When includeSetupQueries is enabled, setup-phase issues should be detected
      assertThat(report.getConfirmedIssues())
          .as("With includeSetupQueries=true, setup-phase issues should be detected")
          .anyMatch(i -> i.type() == IssueType.UPDATE_WITHOUT_WHERE);
    }

    @Test
    @DisplayName("Report should include query count from all phases but only detect issues in TEST phase")
    void reportCountsAllQueriesButDetectsOnlyTestPhase() {
      queryInterceptor.start();
      queryInterceptor.setPhase(LifecyclePhase.SETUP);

      // 2 setup queries (member first due to FK constraint)
      memberRepository.deleteAllInBatch();
      teamRepository.deleteAllInBatch();

      queryInterceptor.setPhase(LifecyclePhase.TEST);

      // 1 test query
      memberRepository.findByStatus("ACTIVE");

      queryInterceptor.stop();

      List<QueryRecord> allQueries = queryInterceptor.getRecordedQueries();
      long setupCount = allQueries.stream().filter(q -> q.phase() == LifecyclePhase.SETUP).count();
      long testCount = allQueries.stream().filter(q -> q.phase() == LifecyclePhase.TEST).count();

      assertThat(setupCount).as("Should have captured setup queries").isGreaterThanOrEqualTo(2);
      assertThat(testCount).as("Should have captured test queries").isGreaterThanOrEqualTo(1);

      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
      QueryAuditReport report =
          analyzer.analyze("DesiredBehavior", "countAll", allQueries, null);

      // Total count includes all phases (for reporting transparency)
      assertThat(report.getTotalQueryCount())
          .as("Total query count should include ALL phases")
          .isEqualTo(allQueries.size());

      // But detection should only run on TEST-phase queries
      assertThat(report.getConfirmedIssues())
          .as("Detection should only run on TEST-phase queries")
          .noneMatch(i -> i.type() == IssueType.UPDATE_WITHOUT_WHERE);
    }
  }

  // ── Group 3: Extension auto-phase — verifies Extension lifecycle callbacks ──

  @Nested
  @DisplayName("Extension Auto-Phase — Extension automatically tags lifecycle phases")
  @QueryAudit(
      failOnDetection = BooleanOverride.TRUE,
      failOn = {IssueType.UPDATE_WITHOUT_WHERE, IssueType.REPEATED_SINGLE_INSERT})
  class ExtensionAutoPhase {

    /**
     * The Extension's beforeEach() starts the interceptor with SETUP phase,
     * then beforeTestExecution() switches to TEST before the @Test method runs.
     * If the Extension does NOT manage phases correctly, deleteAllInBatch() in
     * @BeforeEach would be tagged as TEST and trigger update-without-where,
     * causing failOnDetection=true to throw an AssertionError.
     *
     * <p>This test passing IS the assertion: it proves the Extension auto-excludes
     * @BeforeEach queries from detection.
     */
    @BeforeEach
    void setupWithProblematicQueries() {
      memberRepository.deleteAllInBatch();
      teamRepository.deleteAllInBatch();
      for (int i = 0; i < 5; i++) {
        Team team = new Team("Team " + i);
        teamRepository.save(team);
      }
      entityManager.flush();
      entityManager.clear();
    }

    @Test
    @DisplayName("@BeforeEach queries auto-excluded — failOnDetection=true does not throw")
    void setupQueriesAutoExcludedByExtension() {
      // Clean query in TEST phase — no issues expected
      memberRepository.findByStatus("ACTIVE");
    }

    @Test
    @DisplayName("@BeforeEach with deleteAll + repeated save does not produce false positives")
    void noFalsePositivesFromSetupFixtures() {
      // Another clean query — the @BeforeEach above runs every time,
      // and its deleteAll + repeated save must not trigger detection.
      List<Team> teams = teamRepository.findAll();
      assertThat(teams).hasSize(5);
    }
  }

  // ── Group 4: Extension auto-phase with @AfterEach ──

  @Nested
  @DisplayName("Extension Auto-Phase Teardown — @AfterEach queries also excluded")
  @QueryAudit(
      failOnDetection = BooleanOverride.TRUE,
      failOn = {IssueType.UPDATE_WITHOUT_WHERE, IssueType.REPEATED_SINGLE_INSERT})
  class ExtensionAutoTeardown {

    @BeforeEach
    void createFixtures() {
      Team team = new Team("TeardownTest");
      teamRepository.save(team);
      entityManager.flush();
      entityManager.clear();
    }

    @AfterEach
    void teardownWithProblematicQueries() {
      // deleteAllInBatch() in @AfterEach would trigger update-without-where
      // if captured as TEST phase.
      memberRepository.deleteAllInBatch();
      teamRepository.deleteAllInBatch();
    }

    @Test
    @DisplayName("@AfterEach queries auto-excluded — failOnDetection=true does not throw")
    void teardownQueriesAutoExcludedByExtension() {
      teamRepository.findAll();
    }
  }

  // ── Group 5: includeSetupQueries annotation opt-in ─────────────────────

  @Nested
  @DisplayName("Include Setup Queries — annotation opt-in analyzes all phases")
  class IncludeSetupQueriesAnnotation {

    @Test
    @DisplayName("includeSetupQueries=true via annotation — @BeforeEach issues ARE detected")
    void setupQueriesDetectedWhenOptedIn() {
      // Simulate Extension behavior: tag queries with SETUP phase
      queryInterceptor.start();
      queryInterceptor.setPhase(LifecyclePhase.SETUP);

      memberRepository.deleteAllInBatch();
      teamRepository.deleteAllInBatch();

      queryInterceptor.setPhase(LifecyclePhase.TEST);

      memberRepository.findByStatus("ACTIVE");

      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();

      // With includeSetupQueries=true (as @QueryAudit annotation would set),
      // SETUP-phase queries should be analyzed and issues detected.
      QueryAuditConfig config =
          QueryAuditConfig.builder().includeSetupQueries(true).failOnDetection(false).build();
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config);
      QueryAuditReport report =
          analyzer.analyze("IncludeSetup", "optIn", queries, null);

      assertThat(report.getConfirmedIssues())
          .as("With includeSetupQueries=true, SETUP-phase update-without-where should be detected")
          .anyMatch(i -> i.type() == IssueType.UPDATE_WITHOUT_WHERE);
    }

    @Test
    @DisplayName("includeSetupQueries=false (default) — same queries, @BeforeEach issues excluded")
    void setupQueriesExcludedByDefault() {
      queryInterceptor.start();
      queryInterceptor.setPhase(LifecyclePhase.SETUP);

      memberRepository.deleteAllInBatch();
      teamRepository.deleteAllInBatch();

      queryInterceptor.setPhase(LifecyclePhase.TEST);

      memberRepository.findByStatus("ACTIVE");

      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();

      // Default config: includeSetupQueries=false → SETUP excluded
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
      QueryAuditReport report =
          analyzer.analyze("IncludeSetup", "default", queries, null);

      assertThat(report.getConfirmedIssues())
          .as("With default config, SETUP-phase update-without-where should be excluded")
          .noneMatch(i -> i.type() == IssueType.UPDATE_WITHOUT_WHERE);
    }
  }

  // ── Group 6: Edge cases ───────────────────────────────────────────────

  @Nested
  @DisplayName("Edge Cases — boundary conditions and combined filters")
  class EdgeCases {

    @Test
    @DisplayName("SETUP and TEST both have issues — only TEST-phase issues detected")
    void mixedPhaseIssuesOnlyTestDetected() {
      queryInterceptor.start();
      queryInterceptor.setPhase(LifecyclePhase.SETUP);

      // SETUP: would trigger update-without-where (should be excluded)
      memberRepository.deleteAllInBatch();

      queryInterceptor.setPhase(LifecyclePhase.TEST);

      // TEST: also triggers update-without-where (should be detected)
      teamRepository.deleteAllInBatch();

      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
      QueryAuditReport report = analyzer.analyze("Edge", "mixed", queries, null);

      // TEST-phase update-without-where should still be detected
      assertThat(report.getConfirmedIssues())
          .as("TEST-phase issues should still be detected even when SETUP has same issue type")
          .anyMatch(i -> i.type() == IssueType.UPDATE_WITHOUT_WHERE);
    }

    @Test
    @DisplayName("All queries in SETUP phase — clean report with no confirmed issues")
    void allSetupQueriesProduceCleanReport() {
      queryInterceptor.start();
      queryInterceptor.setPhase(LifecyclePhase.SETUP);

      memberRepository.deleteAllInBatch();
      teamRepository.deleteAllInBatch();

      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();
      assertThat(queries).as("Queries should be captured").isNotEmpty();

      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
      QueryAuditReport report = analyzer.analyze("Edge", "allSetup", queries, null);

      assertThat(report.getConfirmedIssues())
          .as("All-SETUP report should have no confirmed issues")
          .isEmpty();
    }

    @Test
    @DisplayName("start() resets phase back to TEST")
    void startResetsPhase() {
      queryInterceptor.start();
      queryInterceptor.setPhase(LifecyclePhase.SETUP);

      // Calling start() again should reset phase to TEST
      queryInterceptor.start();

      memberRepository.findByStatus("ACTIVE");
      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();
      assertThat(queries)
          .as("After start() reset, queries should default to TEST phase")
          .allSatisfy(q -> assertThat(q.phase()).isEqualTo(LifecyclePhase.TEST));
    }

    @Test
    @DisplayName("Suppress filter and phase filter work together")
    void suppressAndPhaseFilterCombined() {
      queryInterceptor.start();
      queryInterceptor.setPhase(LifecyclePhase.SETUP);

      // SETUP query — excluded by phase filter
      memberRepository.deleteAllInBatch();

      queryInterceptor.setPhase(LifecyclePhase.TEST);

      // TEST query — excluded by suppress filter
      teamRepository.deleteAllInBatch();

      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();

      // Suppress update-without-where — so even TEST-phase won't trigger it
      QueryAuditConfig config =
          QueryAuditConfig.builder().addSuppressPattern("update-without-where").build();
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config);
      QueryAuditReport report = analyzer.analyze("Edge", "combined", queries, null);

      assertThat(report.getConfirmedIssues())
          .as("Both suppress and phase filters should work together")
          .noneMatch(i -> i.type() == IssueType.UPDATE_WITHOUT_WHERE);
    }

    @Test
    @DisplayName("Phase filter does not affect query count in report (all non-suppressed counted)")
    void phaseFilterDoesNotAffectQueryCount() {
      queryInterceptor.start();
      queryInterceptor.setPhase(LifecyclePhase.SETUP);

      memberRepository.deleteAllInBatch();

      queryInterceptor.setPhase(LifecyclePhase.TEST);

      memberRepository.findByStatus("ACTIVE");

      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();
      int totalCaptured = queries.size();

      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
      QueryAuditReport report = analyzer.analyze("Edge", "count", queries, null);

      // totalQueryCount should include ALL phases (not just TEST)
      assertThat(report.getTotalQueryCount())
          .as("Query count should include all phases for transparency")
          .isEqualTo(totalCaptured);
    }
  }
}
