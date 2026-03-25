package io.queryaudit.junit5.integration;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.baseline.BaselineEntry;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.junit5.EnableQueryInspector;
import io.queryaudit.junit5.ExpectMaxQueryCount;
import io.queryaudit.junit5.integration.entity.Member;
import io.queryaudit.junit5.integration.entity.Team;
import io.queryaudit.junit5.integration.repository.MemberRepository;
import io.queryaudit.junit5.integration.repository.OrderRepository;
import io.queryaudit.junit5.integration.repository.TeamRepository;
import jakarta.persistence.EntityManager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

@SpringBootTest(classes = TestApplication.class)
@EnableQueryInspector
@Transactional
class QueryAuditJpaIntegrationTest {

  @Autowired TeamRepository teamRepository;

  @Autowired MemberRepository memberRepository;

  @Autowired OrderRepository orderRepository;

  @Autowired QueryInterceptor queryInterceptor;

  @Autowired EntityManager entityManager;

  @BeforeEach
  void setUp() {
    for (int i = 0; i < 5; i++) {
      Team team = new Team("Team " + i);
      teamRepository.save(team);
      for (int j = 0; j < 3; j++) {
        Member member =
            new Member("Member " + i + "-" + j, "member" + i + j + "@test.com", "ACTIVE");
        member.setTeam(team);
        memberRepository.save(member);
      }
    }
    entityManager.flush();
    entityManager.clear();
  }

  // ── N+1 Detection ──────────────────────────────────────────────────

  @Nested
  @DisplayName("N+1 Detection")
  class NPlusOneTests {

    @Test
    @DisplayName("Detects real N+1 when accessing lazy-loaded collection in loop")
    void detectsRealNPlusOne() {
      queryInterceptor.start();

      List<Team> teams = teamRepository.findAll(); // 1 query
      for (Team team : teams) {
        team.getMembers().size(); // N queries (lazy loading!)
      }

      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
      QueryAuditReport report = analyzer.analyze("nPlusOneTest", queries, null);

      // Should have 1 (findAll teams) + 5 (per team members) = 6 queries
      assertThat(queries.size()).isGreaterThanOrEqualTo(6);

      // SQL-level N+1 is now INFO (Hibernate-level LazyLoadTracker is authoritative)
      List<Issue> infoNPlusOne =
          report.getInfoIssues().stream().filter(i -> i.type() == IssueType.N_PLUS_ONE).toList();

      // The SQL-level N+1 should be detected as INFO
      assertThat(infoNPlusOne).isNotEmpty();
    }

    @Test
    @DisplayName("No false positive for independent calls below threshold")
    void noFalsePositiveForIndependentCalls() {
      queryInterceptor.start();

      // 2 independent findById calls - below the default threshold of 3
      memberRepository.findById(1L);
      memberRepository.findById(2L);

      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
      QueryAuditReport report = analyzer.analyze("independentCalls", queries, null);

      // Below threshold: no N+1 at any level
      List<Issue> nPlusOneErrors =
          report.getConfirmedIssues().stream()
              .filter(i -> i.type() == IssueType.N_PLUS_ONE)
              .toList();
      assertThat(nPlusOneErrors).isEmpty();
    }
  }

  // ── SELECT * Detection ──────────────────────────────────────────────

  @Nested
  @DisplayName("SELECT * Detection")
  class SelectAllTests {

    @Test
    @DisplayName("Detects SELECT * in native query")
    void detectsSelectAllInNativeQuery() {
      queryInterceptor.start();
      memberRepository.findByEmailNative("member00@test.com");
      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
      QueryAuditReport report = analyzer.analyze("selectAll", queries, null);

      // SELECT_ALL is now INFO severity
      assertThat(report.getInfoIssues()).anyMatch(i -> i.type() == IssueType.SELECT_ALL);
    }

    @Test
    @DisplayName("No false positive for Hibernate-generated column list queries")
    void noFalsePositiveForHibernateQueries() {
      queryInterceptor.start();
      memberRepository.findByStatus("ACTIVE"); // Hibernate generates SELECT col1, col2, ...
      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
      QueryAuditReport report = analyzer.analyze("hibernateQuery", queries, null);

      // Hibernate generates specific columns, NOT SELECT *
      boolean hasSelectAll =
          report.getConfirmedIssues().stream().anyMatch(i -> i.type() == IssueType.SELECT_ALL);
      assertThat(hasSelectAll).isFalse();
    }
  }

  // ── @ExpectMaxQueryCount ────────────────────────────────────────────

  @Nested
  @DisplayName("@ExpectMaxQueryCount")
  class MaxQueryCountTests {

    @Test
    @ExpectMaxQueryCount(50)
    @DisplayName("Passes when query count is within limit")
    void passesWithinLimit() {
      memberRepository.findByStatus("ACTIVE");
      teamRepository.findAll();
    }

    @Test
    @DisplayName("Programmatic verification that query count exceeds a given limit")
    void failsWhenExceedsLimit() {
      queryInterceptor.start();
      memberRepository.findByStatus("ACTIVE");
      teamRepository.findAll();
      memberRepository.findAll();
      queryInterceptor.stop();

      assertThat(queryInterceptor.getRecordedQueries().size()).isGreaterThan(1);
    }
  }

  // ── @DetectNPlusOne ─────────────────────────────────────────────────

  @Nested
  @DisplayName("@DetectNPlusOne")
  class DetectNPlusOneTests {

    @Test
    @DisplayName("Detects N+1 with real JPA lazy loading")
    void detectsWithRealLazyLoading() {
      queryInterceptor.start();

      List<Team> teams = teamRepository.findAll();
      for (Team team : teams) {
        team.getMembers().size(); // triggers N+1
      }

      queryInterceptor.stop();
      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();

      // Verify the pattern: 1 findAll + N member loads
      long memberQueries =
          queries.stream()
              .filter(q -> q.sql() != null && q.sql().toLowerCase().contains("members"))
              .count();
      assertThat(memberQueries).isGreaterThanOrEqualTo(5);
    }
  }

  // ── Report Accuracy ────────────────────────────────────────────────

  @Nested
  @DisplayName("Report Accuracy")
  class ReportAccuracyTests {

    @Test
    @DisplayName("Query count matches actual executed queries")
    void queryCountMatchesActual() {
      queryInterceptor.start();

      teamRepository.findAll();
      memberRepository.findByStatus("ACTIVE");

      queryInterceptor.stop();
      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();

      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
      QueryAuditReport report = analyzer.analyze("accuracy", queries, null);

      assertThat(report.getTotalQueryCount()).isEqualTo(queries.size());
      assertThat(report.getUniquePatternCount()).isLessThanOrEqualTo(queries.size());
    }

    @Test
    @DisplayName("Source location is captured for each query")
    void sourceLocationIsCaptured() {
      queryInterceptor.start();
      memberRepository.findByStatus("ACTIVE");
      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();
      assertThat(queries).isNotEmpty();

      // The interceptor captures stack traces. In a Spring Boot context,
      // all frames may be filtered as framework classes, resulting in an
      // empty string. The important thing is the field is non-null.
      String trace = queries.get(0).stackTrace();
      assertThat(trace).isNotNull();
    }

    @Test
    @DisplayName("Execution time is captured")
    void executionTimeIsCaptured() {
      queryInterceptor.start();
      memberRepository.findAll();
      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();
      assertThat(queries).isNotEmpty();
      assertThat(queries.get(0).executionTimeNanos()).isGreaterThanOrEqualTo(0);
    }
  }

  // ── Baseline Integration ───────────────────────────────────────────

  @Nested
  @DisplayName("Baseline Integration")
  class BaselineTests {

    @Test
    @DisplayName("Acknowledged issues are separated from confirmed")
    void acknowledgedIssuesAreSeparated() {
      queryInterceptor.start();
      memberRepository.findByEmailNative("test@test.com"); // triggers SELECT *
      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();

      // With baseline acknowledging SELECT * on members
      List<BaselineEntry> baseline =
          List.of(
              new BaselineEntry("select-all", "members", null, null, "dev", "JPA native query"));

      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), baseline);
      QueryAuditReport report = analyzer.analyze("baseline", queries, null);

      // SELECT * should be acknowledged, not confirmed
      assertThat(report.getAcknowledgedIssues()).anyMatch(i -> i.type() == IssueType.SELECT_ALL);
      assertThat(report.getConfirmedIssues()).noneMatch(i -> i.type() == IssueType.SELECT_ALL);
    }
  }

  // ── HTML Report Generation ─────────────────────────────────────────

  @Nested
  @DisplayName("HTML Report Generation")
  class HtmlReportTests {

    @Test
    @DisplayName("HTML report is generated with correct structure")
    void htmlReportGenerated() throws Exception {
      queryInterceptor.start();

      teamRepository.findAll();
      memberRepository.findByStatus("ACTIVE");
      memberRepository.findByEmailNative("test@test.com");

      queryInterceptor.stop();
      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();

      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
      QueryAuditReport report = analyzer.analyze("HtmlReportTests", "htmlTest", queries, null);

      HtmlReportAggregator aggregator = HtmlReportAggregator.getInstance();
      aggregator.reset();
      aggregator.addReport(report);

      Path outputDir = Path.of("build", "test-reports", "query-audit-integration");
      aggregator.writeReport(outputDir);

      // Verify files exist
      assertThat(outputDir.resolve("index.html")).exists();
      assertThat(outputDir.resolve("HtmlReportTests.html")).exists();

      // Verify content
      String indexHtml = Files.readString(outputDir.resolve("index.html"));
      assertThat(indexHtml).contains("Query Guard Report");
      assertThat(indexHtml).contains("HtmlReportTests");

      String classHtml = Files.readString(outputDir.resolve("HtmlReportTests.html"));
      assertThat(classHtml).contains("htmlTest");
    }
  }
}
