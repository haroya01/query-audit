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
class WhereQualityFalsePositiveTest {

  @Autowired TeamRepository teamRepository;
  @Autowired MemberRepository memberRepository;
  @Autowired QueryInterceptor queryInterceptor;
  @Autowired EntityManager entityManager;

  @BeforeEach
  void setUp() {
    Team team = new Team("FP Team");
    teamRepository.save(team);
    Member m = new Member("User", "fptest@test.com", "ACTIVE");
    m.setTeam(team);
    memberRepository.save(m);
    entityManager.flush();
    entityManager.clear();
  }

  private QueryAuditReport analyze(String testName, List<QueryRecord> queries) {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
    return analyzer.analyze("Team2FP", testName, queries, null);
  }

  private List<Issue> allIssues(QueryAuditReport report) {
    return Stream.concat(report.getConfirmedIssues().stream(), report.getInfoIssues().stream())
        .toList();
  }

  @Nested
  @DisplayName("WhereFunction FP")
  class WhereFunctionFP {

    @Test
    @DisplayName("COALESCE is index-safe and should NOT trigger")
    void coalesceIsSafe() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE COALESCE(status, 'UNKNOWN') = 'ACTIVE'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("coalesce", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).noneMatch(i -> i.type() == IssueType.WHERE_FUNCTION);
    }

    @Test
    @DisplayName("Function on right-hand side should NOT trigger")
    void functionOnRHS() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE name = UPPER('test')")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("rhsFunction", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).noneMatch(i -> i.type() == IssueType.WHERE_FUNCTION);
    }
  }

  @Nested
  @DisplayName("NullComparison FP")
  class NullComparisonFP {

    @Test
    @DisplayName("IS NULL should NOT trigger (correct syntax)")
    void isNullCorrect() {
      queryInterceptor.start();
      entityManager.createNativeQuery("SELECT * FROM members WHERE status IS NULL").getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("isNull", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).noneMatch(i -> i.type() == IssueType.NULL_COMPARISON);
    }

    @Test
    @DisplayName("IS NOT NULL should NOT trigger")
    void isNotNullCorrect() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE status IS NOT NULL")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("isNotNull", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).noneMatch(i -> i.type() == IssueType.NULL_COMPARISON);
    }
  }

  @Nested
  @DisplayName("LikeWildcard FP")
  class LikeWildcardFP {

    @Test
    @DisplayName("LIKE with trailing wildcard should NOT trigger (index usable)")
    void trailingWildcard() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE name LIKE 'User%'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("trailingWildcard", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).noneMatch(i -> i.type() == IssueType.LIKE_LEADING_WILDCARD);
    }
  }

  @Nested
  @DisplayName("CaseInWhere FP")
  class CaseInWhereFP {

    @Test
    @DisplayName("CASE on RHS of comparison should NOT trigger")
    void caseOnRHS() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT * FROM members WHERE status = CASE WHEN 1=1 THEN 'ACTIVE' ELSE 'X' END")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("caseOnRHS", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).noneMatch(i -> i.type() == IssueType.CASE_IN_WHERE);
    }
  }

  @Nested
  @DisplayName("RedundantFilter FP")
  class RedundantFilterFP {

    @Test
    @DisplayName("Different conditions should NOT trigger")
    void differentConditions() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE status = 'ACTIVE' AND name = 'User'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("differentConditions", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).noneMatch(i -> i.type() == IssueType.REDUNDANT_FILTER);
    }
  }
}
