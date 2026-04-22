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
class SqlStyleFalsePositiveTest {

  @Autowired TeamRepository teamRepository;
  @Autowired MemberRepository memberRepository;
  @Autowired QueryInterceptor queryInterceptor;
  @Autowired EntityManager entityManager;

  @BeforeEach
  void setUp() {
    Team team = new Team("FP6");
    teamRepository.save(team);
    Member m = new Member("M6", "fp6@t.com", "ACTIVE");
    m.setTeam(team);
    memberRepository.save(m);
    entityManager.flush();
    entityManager.clear();
  }

  private QueryAuditReport analyze(String testName, List<QueryRecord> queries) {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
    return analyzer.analyze("Team6FP", testName, queries, null);
  }

  private List<Issue> allIssues(QueryAuditReport report) {
    return Stream.concat(report.getConfirmedIssues().stream(), report.getInfoIssues().stream())
        .toList();
  }

  @Nested
  @DisplayName("SelectAll FP")
  class SelectAllFP {

    @Test
    @DisplayName("Hibernate-generated column list should NOT trigger SELECT *")
    void hibernateGeneratedColumns() {
      queryInterceptor.start();
      memberRepository.findByStatus("ACTIVE"); // Hibernate generates explicit column list
      queryInterceptor.stop();

      QueryAuditReport report = analyze("hibernateColumns", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).noneMatch(i -> i.type() == IssueType.SELECT_ALL);
    }
  }

  @Nested
  @DisplayName("DistinctMisuse FP")
  class DistinctMisuseFP {

    @Test
    @DisplayName("DISTINCT without GROUP BY or JOIN should NOT trigger")
    void legitimateDistinct() {
      queryInterceptor.start();
      entityManager.createNativeQuery("SELECT DISTINCT status FROM members").getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("legitimateDistinct", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).noneMatch(i -> i.type() == IssueType.DISTINCT_MISUSE);
    }
  }

  @Nested
  @DisplayName("UnionWithoutAll FP")
  class UnionWithoutAllFP {

    @Test
    @DisplayName("UNION ALL should NOT trigger")
    void unionAll() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT name FROM members WHERE status = 'ACTIVE'"
                  + " UNION ALL"
                  + " SELECT name FROM members WHERE status = 'INACTIVE'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("unionAll", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).noneMatch(i -> i.type() == IssueType.UNION_WITHOUT_ALL);
    }
  }

  @Nested
  @DisplayName("HavingMisuse FP")
  class HavingMisuseFP {

    @Test
    @DisplayName("HAVING with aggregate function should NOT trigger")
    void aggregateInHaving() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT status, COUNT(*) AS cnt FROM members GROUP BY status HAVING COUNT(*) > 1")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("aggregateHaving", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).noneMatch(i -> i.type() == IssueType.HAVING_MISUSE);
    }
  }

  @Nested
  @DisplayName("GroupByFunction FP")
  class GroupByFunctionFP {

    @Test
    @DisplayName("GROUP BY on plain column should NOT trigger")
    void plainColumnGroupBy() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT status, COUNT(*) FROM members GROUP BY status")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("plainGroupBy", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).noneMatch(i -> i.type() == IssueType.GROUP_BY_FUNCTION);
    }
  }

  @Nested
  @DisplayName("NPlusOne FP")
  class NPlusOneFP {

    @Test
    @DisplayName("Single query should NOT trigger N+1")
    void singleQuery() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE status = 'ACTIVE'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("singleQuery", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).noneMatch(i -> i.type() == IssueType.N_PLUS_ONE);
    }
  }
}
