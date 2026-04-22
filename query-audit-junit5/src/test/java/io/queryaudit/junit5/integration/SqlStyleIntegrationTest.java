package io.queryaudit.junit5.integration;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.junit5.EnableQueryInspector;
import io.queryaudit.junit5.integration.entity.Member;
import io.queryaudit.junit5.integration.entity.Team;
import io.queryaudit.junit5.integration.repository.MemberRepository;
import io.queryaudit.junit5.integration.repository.TeamRepository;
import jakarta.persistence.EntityManager;
import java.util.List;
import java.util.stream.Stream;
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
class SqlStyleIntegrationTest {

  @Autowired TeamRepository teamRepository;
  @Autowired MemberRepository memberRepository;
  @Autowired QueryInterceptor queryInterceptor;
  @Autowired EntityManager entityManager;

  @BeforeEach
  void setUp() {
    for (int i = 0; i < 5; i++) {
      Team team = new Team("Team " + i);
      teamRepository.save(team);
      for (int j = 0; j < 3; j++) {
        Member m = new Member("Member " + i + "-" + j, "m" + i + j + "@test.com", "ACTIVE");
        m.setTeam(team);
        memberRepository.save(m);
      }
    }
    entityManager.flush();
    entityManager.clear();
  }

  private QueryAuditReport analyze(String testName, List<QueryRecord> queries) {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
    return analyzer.analyze("SqlStyleIntegrationTest", testName, queries, null);
  }

  private List<Issue> allIssues(QueryAuditReport report) {
    return Stream.concat(report.getConfirmedIssues().stream(), report.getInfoIssues().stream())
        .toList();
  }

  @Nested
  @DisplayName("SelectAll")
  class SelectAllTests {

    @Test
    @DisplayName("SELECT * in native query is detected")
    void detectsSelectAll() {
      queryInterceptor.start();
      entityManager.createNativeQuery("SELECT * FROM members WHERE id = 1").getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("selectAll", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).anyMatch(i -> i.type() == IssueType.SELECT_ALL);
    }
  }

  @Nested
  @DisplayName("DistinctMisuse")
  class DistinctMisuseTests {

    @Test
    @DisplayName("DISTINCT with GROUP BY is redundant")
    void detectsDistinctWithGroupBy() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT DISTINCT status, COUNT(*) FROM members GROUP BY status")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("distinctWithGroupBy", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).anyMatch(i -> i.type() == IssueType.DISTINCT_MISUSE);
    }

    @Test
    @DisplayName("DISTINCT with JOIN may indicate missing condition")
    void detectsDistinctWithJoin() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT DISTINCT m.name FROM members m JOIN teams t ON t.id = m.team_id")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("distinctWithJoin", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).anyMatch(i -> i.type() == IssueType.DISTINCT_MISUSE);
    }
  }

  @Nested
  @DisplayName("UnionWithoutAll")
  class UnionWithoutAllTests {

    @Test
    @DisplayName("UNION without ALL forces deduplication sort")
    void detectsUnionWithoutAll() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT name FROM members WHERE status = 'ACTIVE'"
                  + " UNION"
                  + " SELECT name FROM members WHERE status = 'INACTIVE'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("unionWithoutAll", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).anyMatch(i -> i.type() == IssueType.UNION_WITHOUT_ALL);
    }
  }

  @Nested
  @DisplayName("HavingMisuse")
  class HavingMisuseTests {

    @Test
    @DisplayName("Non-aggregate column in HAVING should be in WHERE")
    void detectsHavingMisuse() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT team_id, COUNT(*) FROM members GROUP BY team_id HAVING team_id > 1")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("havingMisuse", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).anyMatch(i -> i.type() == IssueType.HAVING_MISUSE);
    }
  }

  @Nested
  @DisplayName("GroupByFunction")
  class GroupByFunctionTests {

    @Test
    @DisplayName("Function in GROUP BY prevents index usage")
    void detectsGroupByFunction() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT UPPER(status), COUNT(*) FROM members GROUP BY UPPER(status)")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("groupByFunction", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).anyMatch(i -> i.type() == IssueType.GROUP_BY_FUNCTION);
    }
  }

  @Nested
  @DisplayName("NPlusOne (SQL-level)")
  class NPlusOneTests {

    @Test
    @DisplayName("Repeated SELECT pattern is detected as N+1")
    void detectsNPlusOnePattern() {
      queryInterceptor.start();
      for (int i = 1; i <= 5; i++) {
        entityManager
            .createNativeQuery("SELECT * FROM members WHERE team_id = " + i)
            .getResultList();
      }
      queryInterceptor.stop();

      QueryAuditReport report = analyze("nPlusOne", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(
              i -> i.type() == IssueType.N_PLUS_ONE || i.type() == IssueType.N_PLUS_ONE_SUSPECT);
    }
  }

  @Nested
  @DisplayName("LazyLoadNPlusOne")
  class LazyLoadNPlusOneTests {

    @Test
    @DisplayName("Lazy-loaded collection triggers N+1")
    void detectsLazyLoadNPlusOne() {
      queryInterceptor.start();
      List<Team> teams = teamRepository.findAll();
      for (Team team : teams) {
        team.getMembers().size();
      }
      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();
      // 1 findAll + 5 lazy loads = 6+ queries
      assertThat(queries.size()).isGreaterThanOrEqualTo(6);

      QueryAuditReport report = analyze("lazyLoadNPlusOne", queries);
      // SQL-level N+1 detected as INFO
      assertThat(allIssues(report))
          .anyMatch(
              i -> i.type() == IssueType.N_PLUS_ONE || i.type() == IssueType.N_PLUS_ONE_SUSPECT);
    }
  }
}
