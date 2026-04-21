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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
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
class JoinSubqueryIntegrationTest {

  @Autowired TeamRepository teamRepository;
  @Autowired MemberRepository memberRepository;
  @Autowired QueryInterceptor queryInterceptor;
  @Autowired EntityManager entityManager;

  @BeforeEach
  void setUp() {
    for (int i = 0; i < 3; i++) {
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
    return analyzer.analyze("JoinSubqueryIntegrationTest", testName, queries, null);
  }

  private List<Issue> allIssues(QueryAuditReport report) {
    return Stream.concat(
            report.getConfirmedIssues().stream(), report.getInfoIssues().stream())
        .toList();
  }

  @Nested
  @DisplayName("CartesianJoin")
  class CartesianJoinTests {

    @Test
    @DisplayName("Implicit join without WHERE produces Cartesian product")
    void detectsCartesianJoin() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM teams, members")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("cartesianJoin", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.CARTESIAN_JOIN);
    }
  }

  @Nested
  @DisplayName("ImplicitJoin")
  class ImplicitJoinTests {

    @Test
    @DisplayName("Old-style comma join with WHERE is detected")
    void detectsImplicitJoin() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT * FROM teams t, members m WHERE t.id = m.team_id")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("implicitJoin", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.IMPLICIT_JOIN);
    }
  }

  @Nested
  @DisplayName("UnusedJoin")
  class UnusedJoinTests {

    @Test
    @DisplayName("LEFT JOIN with unreferenced table is detected")
    void detectsUnusedJoin() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT m.name FROM members m LEFT JOIN teams t ON t.id = m.team_id")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("unusedJoin", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.UNUSED_JOIN);
    }
  }

  @Nested
  @DisplayName("TooManyJoins")
  class TooManyJoinsTests {

    @Test
    @DisplayName("Query with 6+ JOINs is detected")
    void detectsTooManyJoins() {
      entityManager
          .createNativeQuery("CREATE TABLE t1 (id BIGINT PRIMARY KEY, val VARCHAR(50))")
          .executeUpdate();
      entityManager
          .createNativeQuery("CREATE TABLE t2 (id BIGINT PRIMARY KEY, val VARCHAR(50))")
          .executeUpdate();
      entityManager
          .createNativeQuery("CREATE TABLE t3 (id BIGINT PRIMARY KEY, val VARCHAR(50))")
          .executeUpdate();
      entityManager
          .createNativeQuery("CREATE TABLE t4 (id BIGINT PRIMARY KEY, val VARCHAR(50))")
          .executeUpdate();
      entityManager.flush();

      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT m.name FROM members m"
                  + " JOIN teams t ON t.id = m.team_id"
                  + " JOIN orders o ON o.member_id = m.id"
                  + " JOIN t1 ON t1.id = m.id"
                  + " JOIN t2 ON t2.id = m.id"
                  + " JOIN t3 ON t3.id = m.id"
                  + " JOIN t4 ON t4.id = m.id")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("tooManyJoins", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.TOO_MANY_JOINS);
    }
  }

  @Nested
  @DisplayName("CorrelatedSubquery")
  class CorrelatedSubqueryTests {

    @Test
    @DisplayName("Correlated subquery in SELECT is detected")
    void detectsCorrelatedSubquery() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT t.name,"
                  + " (SELECT COUNT(*) FROM members m WHERE m.team_id = t.id) AS member_count"
                  + " FROM teams t")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("correlatedSubquery", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.CORRELATED_SUBQUERY);
    }
  }

  @Nested
  @DisplayName("NotInSubquery")
  class NotInSubqueryTests {

    @Test
    @DisplayName("NOT IN with nullable subquery is detected")
    void detectsNotInSubquery() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT * FROM teams WHERE id NOT IN (SELECT team_id FROM members)")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("notInSubquery", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.NOT_IN_SUBQUERY);
    }
  }

  @Nested
  @DisplayName("MergeableQueries")
  class MergeableQueriesTests {

    @Test
    @DisplayName("Similar queries with different WHERE structures are detected as mergeable")
    void detectsMergeableQueries() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT name, email FROM members WHERE status = 'ACTIVE'")
          .getResultList();
      entityManager
          .createNativeQuery("SELECT name, email FROM members WHERE name = 'Member 0-0'")
          .getResultList();
      entityManager
          .createNativeQuery("SELECT name, email FROM members WHERE email = 'm00@test.com'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("mergeableQueries", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.MERGEABLE_QUERIES);
    }
  }

  @Nested
  @DisplayName("LargeInList")
  class LargeInListTests {

    @Test
    @DisplayName("IN clause with 100+ values is detected")
    void detectsLargeInList() {
      String ids =
          IntStream.rangeClosed(1, 150)
              .mapToObj(String::valueOf)
              .collect(Collectors.joining(","));

      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE id IN (" + ids + ")")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("largeInList", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.LARGE_IN_LIST);
    }
  }
}
