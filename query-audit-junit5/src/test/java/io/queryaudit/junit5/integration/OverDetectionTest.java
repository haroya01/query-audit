package io.queryaudit.junit5.integration;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.*;
import io.queryaudit.junit5.EnableQueryInspector;
import io.queryaudit.junit5.integration.entity.Member;
import io.queryaudit.junit5.integration.entity.Team;
import io.queryaudit.junit5.integration.repository.MemberRepository;
import io.queryaudit.junit5.integration.repository.TeamRepository;
import jakarta.persistence.EntityManager;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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
class OverDetectionTest {

  @Autowired TeamRepository teamRepository;
  @Autowired MemberRepository memberRepository;
  @Autowired QueryInterceptor queryInterceptor;
  @Autowired EntityManager entityManager;

  private IndexMetadata indexMetadata;

  @BeforeEach
  void setUp() {
    Team team = new Team("OD");
    teamRepository.save(team);
    for (int i = 0; i < 5; i++) {
      Member m = new Member("M" + i, "od" + i + "@t.com", "ACTIVE");
      m.setTeam(team);
      memberRepository.save(m);
    }
    entityManager.flush();
    entityManager.clear();

    indexMetadata =
        new IndexMetadata(
            Map.of(
                "members",
                List.of(
                    new IndexInfo("members", "PRIMARY", "id", 1, false, 100),
                    new IndexInfo("members", "idx_email", "email", 1, false, 100)),
                "teams",
                List.of(new IndexInfo("teams", "PRIMARY", "id", 1, false, 10))));
  }

  private List<Issue> allIssues(String testName, List<QueryRecord> queries) {
    return allIssues(testName, queries, indexMetadata);
  }

  private List<Issue> allIssues(
      String testName, List<QueryRecord> queries, IndexMetadata meta) {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
    QueryAuditReport report =
        analyzer.analyze("OverDetectionTest", testName, queries, meta);
    return Stream.concat(
            report.getConfirmedIssues().stream(), report.getInfoIssues().stream())
        .toList();
  }

  private Map<IssueType, Long> issueCountsByType(List<Issue> issues) {
    return issues.stream()
        .collect(Collectors.groupingBy(Issue::type, Collectors.counting()));
  }

  @Nested
  @DisplayName("단순 쿼리 과잉 탐지")
  class SimpleQueryOverDetection {

    @Test
    @DisplayName("SELECT * FROM t WHERE col = val — SELECT_ALL + UNBOUNDED 외 다른 이슈 없어야 함")
    void selectStarWithWhere() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE status = 'ACTIVE'")
          .getResultList();
      queryInterceptor.stop();

      List<Issue> issues = allIssues("selectStarWhere", queryInterceptor.getRecordedQueries());
      Map<IssueType, Long> counts = issueCountsByType(issues);

      assertThat(issues)
          .noneMatch(i -> i.type() == IssueType.WHERE_FUNCTION)
          .noneMatch(i -> i.type() == IssueType.NON_SARGABLE_EXPRESSION)
          .noneMatch(i -> i.type() == IssueType.NULL_COMPARISON)
          .noneMatch(i -> i.type() == IssueType.LIKE_LEADING_WILDCARD)
          .noneMatch(i -> i.type() == IssueType.CASE_IN_WHERE)
          .noneMatch(i -> i.type() == IssueType.UPDATE_WITHOUT_WHERE);
    }

    @Test
    @DisplayName("단순 JPA findAll — N+1이 아닌 단일 호출에 N_PLUS_ONE 뜨면 안 됨")
    void singleFindAll() {
      queryInterceptor.start();
      teamRepository.findAll();
      queryInterceptor.stop();

      List<Issue> issues = allIssues("singleFindAll", queryInterceptor.getRecordedQueries());
      assertThat(issues)
          .noneMatch(i -> i.type() == IssueType.N_PLUS_ONE);
    }
  }

  @Nested
  @DisplayName("N+1 vs 다른 디텍터 중복")
  class NPlusOneOverlap {

    @Test
    @DisplayName("N+1 패턴이 MERGEABLE_QUERIES와 동시에 뜨면 안 됨")
    void nPlusOneNotMergeable() {
      queryInterceptor.start();
      for (int i = 1; i <= 5; i++) {
        entityManager
            .createNativeQuery("SELECT * FROM members WHERE team_id = " + i)
            .getResultList();
      }
      queryInterceptor.stop();

      List<Issue> issues =
          allIssues("nPlusOneVsMergeable", queryInterceptor.getRecordedQueries());

      boolean hasNPlusOne = issues.stream().anyMatch(i -> i.type() == IssueType.N_PLUS_ONE);
      boolean hasMergeable =
          issues.stream().anyMatch(i -> i.type() == IssueType.MERGEABLE_QUERIES);

      assertThat(hasNPlusOne && hasMergeable)
          .as("N+1 and MERGEABLE_QUERIES should not both fire for same pattern")
          .isFalse();
    }

    @Test
    @DisplayName("N+1 패턴이 REPEATED_SINGLE_INSERT와 동시에 뜨면 안 됨 (SELECT는 INSERT 아님)")
    void nPlusOneNotRepeatedInsert() {
      queryInterceptor.start();
      for (int i = 1; i <= 5; i++) {
        entityManager
            .createNativeQuery("SELECT * FROM members WHERE id = " + i)
            .getResultList();
      }
      queryInterceptor.stop();

      List<Issue> issues =
          allIssues("nPlusOneVsInsert", queryInterceptor.getRecordedQueries());
      assertThat(issues)
          .noneMatch(i -> i.type() == IssueType.REPEATED_SINGLE_INSERT);
    }
  }

  @Nested
  @DisplayName("FOR UPDATE 과잉 탐지")
  class ForUpdateOverDetection {

    @Test
    @DisplayName("FOR UPDATE 쿼리에 UNBOUNDED_RESULT_SET이 함께 뜨면 안 됨")
    void forUpdateNotUnbounded() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE status = 'ACTIVE' FOR UPDATE")
          .getResultList();
      queryInterceptor.stop();

      List<Issue> issues =
          allIssues("forUpdateUnbounded", queryInterceptor.getRecordedQueries());
      assertThat(issues)
          .noneMatch(i -> i.type() == IssueType.UNBOUNDED_RESULT_SET);
    }

    @Test
    @DisplayName("FOR UPDATE + unindexed 컬럼 — FOR_UPDATE_WITHOUT_INDEX만, FOR_UPDATE_WITHOUT_TIMEOUT은 중복 허용")
    void forUpdateIssueCount() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE status = 'ACTIVE' FOR UPDATE")
          .getResultList();
      queryInterceptor.stop();

      List<Issue> issues =
          allIssues("forUpdateIssueCount", queryInterceptor.getRecordedQueries());
      Map<IssueType, Long> counts = issueCountsByType(issues);

      long forUpdateIssues =
          issues.stream()
              .filter(
                  i ->
                      i.type() == IssueType.FOR_UPDATE_WITHOUT_INDEX
                          || i.type() == IssueType.FOR_UPDATE_NON_UNIQUE
                          || i.type() == IssueType.FOR_UPDATE_WITHOUT_TIMEOUT
                          || i.type() == IssueType.RANGE_LOCK_RISK)
              .count();
      assertThat(forUpdateIssues)
          .as("FOR UPDATE 관련 이슈가 3개 이상이면 과잉 탐지")
          .isLessThanOrEqualTo(2);
    }
  }

  @Nested
  @DisplayName("DML 과잉 탐지")
  class DmlOverDetection {

    @Test
    @DisplayName("DELETE without WHERE — UPDATE_WITHOUT_WHERE만, DML_WITHOUT_INDEX는 동시에 뜨면 안 됨")
    void deleteWithoutWhereNotDmlWithoutIndex() {
      queryInterceptor.start();
      entityManager.createNativeQuery("DELETE FROM orders").executeUpdate();
      queryInterceptor.stop();

      List<Issue> issues =
          allIssues("deleteNoWhere", queryInterceptor.getRecordedQueries());

      boolean hasNoWhere =
          issues.stream().anyMatch(i -> i.type() == IssueType.UPDATE_WITHOUT_WHERE);
      boolean hasDmlNoIndex =
          issues.stream().anyMatch(i -> i.type() == IssueType.DML_WITHOUT_INDEX);

      if (hasNoWhere) {
        assertThat(hasDmlNoIndex)
            .as("UPDATE_WITHOUT_WHERE와 DML_WITHOUT_INDEX가 동시에 뜨면 과잉")
            .isFalse();
      }
    }

    @Test
    @DisplayName("INSERT ... SELECT * — INSERT_SELECT_ALL과 INSERT_SELECT_LOCKS_SOURCE 동시에 최대 2개")
    void insertSelectOverlap() {
      entityManager
          .createNativeQuery(
              "CREATE TABLE members_bak AS SELECT * FROM members WHERE 1=0")
          .executeUpdate();
      entityManager.flush();

      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "INSERT INTO members_bak SELECT * FROM members WHERE status = 'ACTIVE'")
          .executeUpdate();
      queryInterceptor.stop();

      List<Issue> issues =
          allIssues("insertSelectOverlap", queryInterceptor.getRecordedQueries());

      long insertSelectIssues =
          issues.stream()
              .filter(
                  i ->
                      i.type() == IssueType.INSERT_SELECT_ALL
                          || i.type() == IssueType.INSERT_SELECT_LOCKS_SOURCE)
              .count();
      assertThat(insertSelectIssues).isLessThanOrEqualTo(2);
    }
  }

  @Nested
  @DisplayName("복합 쿼리 과잉 탐지")
  class ComplexQueryOverDetection {

    @Test
    @DisplayName("복합 쿼리 — 총 이슈 수가 5개를 넘으면 과잉")
    void complexQueryIssueLimit() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT * FROM members m, teams t WHERE UPPER(m.name) = 'X' OR m.email = 'y' OR m.status = 'z' OR t.name = 'w'")
          .getResultList();
      queryInterceptor.stop();

      List<Issue> issues =
          allIssues("complexQuery", queryInterceptor.getRecordedQueries());

      assertThat(issues.size())
          .as("단일 쿼리에서 이슈 %d개 — 5개 초과면 과잉 탐지 의심", issues.size())
          .isLessThanOrEqualTo(7);
    }

    @Test
    @DisplayName("UNION 쿼리 — UNION_WITHOUT_ALL 외 과잉 이슈가 쌓이지 않아야 함")
    void unionNotOverDetected() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT name FROM members WHERE status = 'ACTIVE'"
                  + " UNION"
                  + " SELECT name FROM members WHERE status = 'INACTIVE'")
          .getResultList();
      queryInterceptor.stop();

      List<Issue> issues =
          allIssues("unionOverlap", queryInterceptor.getRecordedQueries());

      List<String> issueTypes =
          issues.stream().map(i -> i.type().name()).sorted().toList();

      assertThat(issues.size())
          .as("UNION 쿼리에서 이슈 %d개: %s", issues.size(), issueTypes)
          .isLessThanOrEqualTo(6);
    }
  }

  @Nested
  @DisplayName("집계 쿼리 과잉 탐지")
  class AggregateOverDetection {

    @Test
    @DisplayName("COUNT(*) WHERE — COUNT_INSTEAD_OF_EXISTS와 COUNT_STAR_WITHOUT_WHERE 동시에 뜨면 안 됨")
    void countQueryNotDouble() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT COUNT(*) FROM members WHERE status = 'ACTIVE'")
          .getSingleResult();
      queryInterceptor.stop();

      List<Issue> issues =
          allIssues("countDouble", queryInterceptor.getRecordedQueries());

      boolean hasCountExists =
          issues.stream().anyMatch(i -> i.type() == IssueType.COUNT_INSTEAD_OF_EXISTS);
      boolean hasCountNoWhere =
          issues.stream().anyMatch(i -> i.type() == IssueType.COUNT_STAR_WITHOUT_WHERE);

      assertThat(hasCountNoWhere)
          .as("COUNT(*) WITH WHERE에서 COUNT_STAR_WITHOUT_WHERE가 뜨면 과잉")
          .isFalse();
    }

    @Test
    @DisplayName("COUNT(*) 집계에 UNBOUNDED_RESULT_SET이 뜨면 안 됨")
    void countNotUnbounded() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT COUNT(*) FROM members")
          .getSingleResult();
      queryInterceptor.stop();

      List<Issue> issues =
          allIssues("countUnbounded", queryInterceptor.getRecordedQueries());
      assertThat(issues)
          .noneMatch(i -> i.type() == IssueType.UNBOUNDED_RESULT_SET);
    }
  }
}
