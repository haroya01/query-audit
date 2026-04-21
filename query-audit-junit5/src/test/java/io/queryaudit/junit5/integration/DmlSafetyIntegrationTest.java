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
class DmlSafetyIntegrationTest {

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
        Member member =
            new Member("Member " + i + "-" + j, "m" + i + j + "@test.com", "ACTIVE");
        member.setTeam(team);
        memberRepository.save(member);
      }
    }
    entityManager.flush();
    entityManager.clear();
  }

  private QueryAuditReport analyze(String testName, List<QueryRecord> queries) {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
    return analyzer.analyze("DmlSafetyIntegrationTest", testName, queries, null);
  }

  private List<Issue> allIssues(QueryAuditReport report) {
    return Stream.concat(
            report.getConfirmedIssues().stream(), report.getInfoIssues().stream())
        .toList();
  }

  @Nested
  @DisplayName("UpdateWithoutWhere")
  class UpdateWithoutWhereTests {

    @Test
    @DisplayName("UPDATE without WHERE is detected")
    void detectsUpdateWithoutWhere() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("UPDATE members SET status = 'INACTIVE'")
          .executeUpdate();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("updateWithoutWhere", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.UPDATE_WITHOUT_WHERE);
    }

    @Test
    @DisplayName("DELETE without WHERE is detected")
    void detectsDeleteWithoutWhere() {
      queryInterceptor.start();
      entityManager.createNativeQuery("DELETE FROM orders").executeUpdate();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("deleteWithoutWhere", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.UPDATE_WITHOUT_WHERE);
    }
  }

  @Nested
  @DisplayName("SubqueryInDml")
  class SubqueryInDmlTests {

    @Test
    @DisplayName("DELETE with subquery in WHERE is detected")
    void detectsDeleteWithSubquery() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "DELETE FROM members WHERE id IN (SELECT m.id FROM members m WHERE m.status = 'X')")
          .executeUpdate();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("deleteWithSubquery", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.SUBQUERY_IN_DML);
    }

    @Test
    @DisplayName("UPDATE with subquery in WHERE is detected")
    void detectsUpdateWithSubquery() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "UPDATE members SET status = 'X' WHERE team_id IN (SELECT id FROM teams WHERE name = 'Team 0')")
          .executeUpdate();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("updateWithSubquery", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.SUBQUERY_IN_DML);
    }
  }

  @Nested
  @DisplayName("InsertSelect")
  class InsertSelectTests {

    @Test
    @DisplayName("INSERT ... SELECT * is detected")
    void detectsInsertSelectAll() {
      entityManager
          .createNativeQuery(
              "CREATE TABLE members_archive AS SELECT * FROM members WHERE 1=0")
          .executeUpdate();
      entityManager.flush();

      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "INSERT INTO members_archive SELECT * FROM members WHERE status = 'ACTIVE'")
          .executeUpdate();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("insertSelectAll", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(
              i ->
                  i.type() == IssueType.INSERT_SELECT_ALL
                      || i.type() == IssueType.INSERT_SELECT_LOCKS_SOURCE);
    }
  }

  @Nested
  @DisplayName("ImplicitColumnsInsert")
  class ImplicitColumnsInsertTests {

    @Test
    @DisplayName("INSERT without column list is detected")
    void detectsImplicitColumnsInsert() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "INSERT INTO teams VALUES (100, 'Implicit Team')")
          .executeUpdate();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("implicitColumnsInsert", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.IMPLICIT_COLUMNS_INSERT);
    }
  }

  @Nested
  @DisplayName("RepeatedSingleInsert")
  class RepeatedSingleInsertTests {

    @Test
    @DisplayName("Repeated single-row INSERTs are detected (threshold >= 3)")
    void detectsRepeatedInserts() {
      queryInterceptor.start();
      for (int i = 0; i < 5; i++) {
        entityManager
            .createNativeQuery(
                "INSERT INTO teams (name) VALUES ('Batch " + i + "')")
            .executeUpdate();
      }
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("repeatedInsert", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.REPEATED_SINGLE_INSERT);
    }
  }

  @Nested
  @DisplayName("DerivedDelete")
  class DerivedDeleteTests {

    @Test
    @DisplayName("Derived delete pattern (SELECT + individual DELETEs) is detected")
    void detectsDerivedDelete() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE status = 'ACTIVE'")
          .getResultList();
      for (int i = 1; i <= 5; i++) {
        entityManager
            .createNativeQuery("DELETE FROM members WHERE id = " + i)
            .executeUpdate();
      }
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("derivedDelete", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.DERIVED_DELETE_LOADS_ENTITIES);
    }
  }

  @Nested
  @DisplayName("CollectionManagement")
  class CollectionManagementTests {

    @Test
    @DisplayName("DELETE-all + re-INSERT pattern is detected via native SQL")
    void detectsCollectionManagementPattern() {
      // Get an actual team ID from setUp data
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
              "INSERT INTO members (name, email, status, team_id) VALUES ('A', 'col_a@t.com', 'ACTIVE', " + teamId + ")")
          .executeUpdate();
      entityManager
          .createNativeQuery(
              "INSERT INTO members (name, email, status, team_id) VALUES ('B', 'col_b@t.com', 'ACTIVE', " + teamId + ")")
          .executeUpdate();
      entityManager
          .createNativeQuery(
              "INSERT INTO members (name, email, status, team_id) VALUES ('C', 'col_c@t.com', 'ACTIVE', " + teamId + ")")
          .executeUpdate();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("collectionManagement", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.COLLECTION_DELETE_REINSERT);
    }
  }
}
