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
class DmlSafetyFalsePositiveTest {

  @Autowired TeamRepository teamRepository;
  @Autowired MemberRepository memberRepository;
  @Autowired QueryInterceptor queryInterceptor;
  @Autowired EntityManager entityManager;

  @BeforeEach
  void setUp() {
    Team team = new Team("Team A");
    teamRepository.save(team);
    for (int i = 0; i < 3; i++) {
      Member m = new Member("M" + i, "fp" + i + "@t.com", "ACTIVE");
      m.setTeam(team);
      memberRepository.save(m);
    }
    entityManager.flush();
    entityManager.clear();
  }

  private QueryAuditReport analyze(String testName, List<QueryRecord> queries) {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
    return analyzer.analyze("Team1FP", testName, queries, null);
  }

  private List<Issue> allIssues(QueryAuditReport report) {
    return Stream.concat(
            report.getConfirmedIssues().stream(), report.getInfoIssues().stream())
        .toList();
  }

  @Nested
  @DisplayName("UpdateWithoutWhere FP")
  class UpdateWithoutWhereFP {

    @Test
    @DisplayName("UPDATE with WHERE should NOT trigger")
    void updateWithWhere() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("UPDATE members SET status = 'X' WHERE status = 'ACTIVE'")
          .executeUpdate();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("updateWithWhere", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .noneMatch(i -> i.type() == IssueType.UPDATE_WITHOUT_WHERE);
    }

    @Test
    @DisplayName("DELETE with WHERE should NOT trigger")
    void deleteWithWhere() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("DELETE FROM members WHERE status = 'NONEXISTENT'")
          .executeUpdate();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("deleteWithWhere", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .noneMatch(i -> i.type() == IssueType.UPDATE_WITHOUT_WHERE);
    }
  }

  @Nested
  @DisplayName("RepeatedSingleInsert FP")
  class RepeatedSingleInsertFP {

    @Test
    @DisplayName("Only 2 INSERTs should NOT trigger (threshold=3)")
    void belowThreshold() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("INSERT INTO teams (name) VALUES ('T1')")
          .executeUpdate();
      entityManager
          .createNativeQuery("INSERT INTO teams (name) VALUES ('T2')")
          .executeUpdate();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("belowInsertThreshold", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .noneMatch(i -> i.type() == IssueType.REPEATED_SINGLE_INSERT);
    }

    @Test
    @DisplayName("Multi-row INSERT should NOT trigger")
    void multiRowInsert() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "INSERT INTO teams (name) VALUES ('A'), ('B'), ('C'), ('D')")
          .executeUpdate();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("multiRowInsert", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .noneMatch(i -> i.type() == IssueType.REPEATED_SINGLE_INSERT);
    }
  }

  @Nested
  @DisplayName("SubqueryInDml FP")
  class SubqueryInDmlFP {

    @Test
    @DisplayName("UPDATE without subquery should NOT trigger")
    void updateWithoutSubquery() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("UPDATE members SET status = 'X' WHERE status = 'ACTIVE'")
          .executeUpdate();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("noSubqueryInDml", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .noneMatch(i -> i.type() == IssueType.SUBQUERY_IN_DML);
    }
  }

  @Nested
  @DisplayName("ImplicitColumnsInsert FP")
  class ImplicitColumnsInsertFP {

    @Test
    @DisplayName("INSERT with explicit column list should NOT trigger")
    void explicitColumnsInsert() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("INSERT INTO teams (name) VALUES ('Explicit')")
          .executeUpdate();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("explicitColumns", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .noneMatch(i -> i.type() == IssueType.IMPLICIT_COLUMNS_INSERT);
    }
  }

  @Nested
  @DisplayName("CollectionManagement FP")
  class CollectionManagementFP {

    @Test
    @DisplayName("DELETE + only 1 INSERT should NOT trigger (threshold=2)")
    void belowInsertThreshold() {
      Long teamId =
          (Long)
              entityManager
                  .createNativeQuery("SELECT id FROM teams LIMIT 1")
                  .getSingleResult();

      queryInterceptor.start();
      entityManager
          .createNativeQuery("DELETE FROM members WHERE team_id = " + teamId)
          .executeUpdate();
      entityManager
          .createNativeQuery(
              "INSERT INTO members (name, email, status, team_id) VALUES ('Only', 'only@t.com', 'ACTIVE', " + teamId + ")")
          .executeUpdate();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("singleReinsert", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .noneMatch(i -> i.type() == IssueType.COLLECTION_DELETE_REINSERT);
    }
  }
}
