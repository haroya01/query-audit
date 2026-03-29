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
class Team3JoinSubqueryFalsePositiveTest {

  @Autowired TeamRepository teamRepository;
  @Autowired MemberRepository memberRepository;
  @Autowired QueryInterceptor queryInterceptor;
  @Autowired EntityManager entityManager;

  @BeforeEach
  void setUp() {
    Team team = new Team("FP");
    teamRepository.save(team);
    Member m = new Member("M", "fp3@t.com", "ACTIVE");
    m.setTeam(team);
    memberRepository.save(m);
    entityManager.flush();
    entityManager.clear();
  }

  private QueryAuditReport analyze(String testName, List<QueryRecord> queries) {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
    return analyzer.analyze("Team3FP", testName, queries, null);
  }

  private List<Issue> allIssues(QueryAuditReport report) {
    return Stream.concat(
            report.getConfirmedIssues().stream(), report.getInfoIssues().stream())
        .toList();
  }

  @Nested
  @DisplayName("CartesianJoin FP")
  class CartesianJoinFP {

    @Test
    @DisplayName("CROSS JOIN is intentional and should NOT trigger")
    void crossJoinIntentional() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM teams t CROSS JOIN members m")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("crossJoin", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .noneMatch(i -> i.type() == IssueType.CARTESIAN_JOIN);
    }

    @Test
    @DisplayName("Implicit join WITH WHERE should NOT trigger Cartesian")
    void implicitJoinWithWhere() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT * FROM teams t, members m WHERE t.id = m.team_id")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("implicitWithWhere", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .noneMatch(i -> i.type() == IssueType.CARTESIAN_JOIN);
    }
  }

  @Nested
  @DisplayName("UnusedJoin FP")
  class UnusedJoinFP {

    @Test
    @DisplayName("LEFT JOIN referenced in SELECT should NOT trigger")
    void joinUsedInSelect() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT m.name, t.name FROM members m LEFT JOIN teams t ON t.id = m.team_id")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("joinUsed", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .noneMatch(i -> i.type() == IssueType.UNUSED_JOIN);
    }

    @Test
    @DisplayName("LEFT JOIN referenced in WHERE should NOT trigger")
    void joinUsedInWhere() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT m.name FROM members m LEFT JOIN teams t ON t.id = m.team_id WHERE t.name = 'FP'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("joinUsedInWhere", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .noneMatch(i -> i.type() == IssueType.UNUSED_JOIN);
    }
  }

  @Nested
  @DisplayName("TooManyJoins FP")
  class TooManyJoinsFP {

    @Test
    @DisplayName("4 JOINs should NOT trigger (threshold=5)")
    void belowThreshold() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT m.name FROM members m"
                  + " JOIN teams t ON t.id = m.team_id"
                  + " JOIN orders o ON o.member_id = m.id"
                  + " LEFT JOIN teams t2 ON t2.id = m.team_id")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("fewJoins", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .noneMatch(i -> i.type() == IssueType.TOO_MANY_JOINS);
    }
  }

  @Nested
  @DisplayName("LargeInList FP")
  class LargeInListFP {

    @Test
    @DisplayName("Small IN list should NOT trigger")
    void smallInList() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE id IN (1, 2, 3, 4, 5)")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("smallInList", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .noneMatch(i -> i.type() == IssueType.LARGE_IN_LIST);
    }
  }

  @Nested
  @DisplayName("MergeableQueries FP")
  class MergeableQueriesFP {

    @Test
    @DisplayName("Only 2 similar queries should NOT trigger (threshold=3)")
    void belowThreshold() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT name, email FROM members WHERE status = 'ACTIVE'")
          .getResultList();
      entityManager
          .createNativeQuery("SELECT name, email FROM members WHERE name = 'M'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("twoQueries", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .noneMatch(i -> i.type() == IssueType.MERGEABLE_QUERIES);
    }
  }

  @Nested
  @DisplayName("NPlusOne FP")
  class NPlusOneFP {

    @Test
    @DisplayName("2 independent findById calls should NOT trigger (threshold=3)")
    void belowThreshold() {
      queryInterceptor.start();
      memberRepository.findById(1L);
      memberRepository.findById(2L);
      queryInterceptor.stop();

      QueryAuditReport report = analyze("twoFinds", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .noneMatch(i -> i.type() == IssueType.N_PLUS_ONE);
    }
  }
}
