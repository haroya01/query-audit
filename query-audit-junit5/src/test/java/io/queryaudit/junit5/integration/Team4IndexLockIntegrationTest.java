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
class Team4IndexLockIntegrationTest {

  @Autowired TeamRepository teamRepository;
  @Autowired MemberRepository memberRepository;
  @Autowired QueryInterceptor queryInterceptor;
  @Autowired EntityManager entityManager;

  private IndexMetadata indexMetadata;

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

    // Simulate production index metadata:
    // members: PRIMARY(id), UNIQUE(email) — name, status, team_id NOT indexed
    // teams: PRIMARY(id) — name NOT indexed
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

  private QueryAuditReport analyze(String testName, List<QueryRecord> queries) {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
    return analyzer.analyze("Team4IndexLockIntegrationTest", testName, queries, indexMetadata);
  }

  private List<Issue> allIssues(QueryAuditReport report) {
    return Stream.concat(
            report.getConfirmedIssues().stream(), report.getInfoIssues().stream())
        .toList();
  }

  @Nested
  @DisplayName("MissingIndex")
  class MissingIndexTests {

    @Test
    @DisplayName("WHERE on unindexed column is detected")
    void detectsMissingIndex() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE status = 'ACTIVE'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("missingIndex", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.MISSING_WHERE_INDEX);
    }
  }

  @Nested
  @DisplayName("CompositeIndex")
  class CompositeIndexTests {

    @Test
    @DisplayName("Non-leading column of composite index is detected")
    void detectsCompositeIndexViolation() {
      IndexMetadata compositeMetadata =
          new IndexMetadata(
              Map.of(
                  "members",
                  List.of(
                      new IndexInfo("members", "PRIMARY", "id", 1, false, 100),
                      new IndexInfo("members", "idx_status_name", "status", 1, true, 50),
                      new IndexInfo("members", "idx_status_name", "name", 2, true, 100))));

      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE name = 'Member 0-0'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
      QueryAuditReport report =
          analyzer.analyze(
              "Team4IndexLockIntegrationTest",
              "compositeIndex",
              queryInterceptor.getRecordedQueries(),
              compositeMetadata);
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.COMPOSITE_INDEX_LEADING_COLUMN);
    }
  }

  @Nested
  @DisplayName("CoveringIndex")
  class CoveringIndexTests {

    @Test
    @DisplayName("Query that could benefit from covering index is detected")
    void detectsCoveringIndexOpportunity() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT name, email FROM members WHERE email = 'test@test.com'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("coveringIndex", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.COVERING_INDEX_OPPORTUNITY);
    }
  }

  @Nested
  @DisplayName("IndexRedundancy")
  class IndexRedundancyTests {

    @Test
    @DisplayName("Redundant index (prefix of another) is detected")
    void detectsRedundantIndex() {
      // idx_status is prefix of idx_status_name — both non-unique
      IndexMetadata redundantMetadata =
          new IndexMetadata(
              Map.of(
                  "members",
                  List.of(
                      new IndexInfo("members", "PRIMARY", "id", 1, false, 100),
                      new IndexInfo("members", "idx_status", "status", 1, true, 10),
                      new IndexInfo("members", "idx_status_name", "status", 1, true, 50),
                      new IndexInfo("members", "idx_status_name", "name", 2, true, 100))));

      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE email = 'test@test.com'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
      QueryAuditReport report =
          analyzer.analyze(
              "Team4IndexLockIntegrationTest",
              "redundantIndex",
              queryInterceptor.getRecordedQueries(),
              redundantMetadata);
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.REDUNDANT_INDEX);
    }
  }

  @Nested
  @DisplayName("WriteAmplification")
  class WriteAmplificationTests {

    @Test
    @DisplayName("Table with 7+ indexes triggers write amplification warning")
    void detectsWriteAmplification() {
      IndexMetadata heavyIndexMetadata =
          new IndexMetadata(
              Map.of(
                  "members",
                  List.of(
                      new IndexInfo("members", "PRIMARY", "id", 1, false, 100),
                      new IndexInfo("members", "idx_email", "email", 1, true, 100),
                      new IndexInfo("members", "idx_name", "name", 1, true, 100),
                      new IndexInfo("members", "idx_status", "status", 1, true, 10),
                      new IndexInfo("members", "idx_team", "team_id", 1, true, 10),
                      new IndexInfo("members", "idx_name_email", "name", 1, true, 50),
                      new IndexInfo("members", "idx_name_email", "email", 2, true, 50),
                      new IndexInfo("members", "idx_status_team", "status", 1, true, 50),
                      new IndexInfo("members", "idx_status_team", "team_id", 2, true, 50))));

      queryInterceptor.start();
      // Use SELECT to let extractTableNames find 'members' via FROM clause
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE id = 1")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
      QueryAuditReport report =
          analyzer.analyze(
              "Team4IndexLockIntegrationTest",
              "writeAmplification",
              queryInterceptor.getRecordedQueries(),
              heavyIndexMetadata);
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.WRITE_AMPLIFICATION);
    }
  }

  @Nested
  @DisplayName("ForUpdateWithoutIndex")
  class ForUpdateWithoutIndexTests {

    @Test
    @DisplayName("FOR UPDATE on unindexed column causes table-level lock")
    void detectsForUpdateWithoutIndex() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE status = 'ACTIVE' FOR UPDATE")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("forUpdateWithoutIndex", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.FOR_UPDATE_WITHOUT_INDEX);
    }
  }

  @Nested
  @DisplayName("ForUpdateNonUniqueIndex")
  class ForUpdateNonUniqueIndexTests {

    @Test
    @DisplayName("FOR UPDATE on non-unique indexed column causes next-key locks")
    void detectsForUpdateNonUniqueIndex() {
      IndexMetadata nonUniqueMetadata =
          new IndexMetadata(
              Map.of(
                  "members",
                  List.of(
                      new IndexInfo("members", "PRIMARY", "id", 1, false, 100),
                      new IndexInfo("members", "idx_status", "status", 1, true, 10))));

      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE status = 'ACTIVE' FOR UPDATE")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
      QueryAuditReport report =
          analyzer.analyze(
              "Team4IndexLockIntegrationTest",
              "forUpdateNonUnique",
              queryInterceptor.getRecordedQueries(),
              nonUniqueMetadata);
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.FOR_UPDATE_NON_UNIQUE);
    }
  }

  @Nested
  @DisplayName("ForUpdateWithoutTimeout")
  class ForUpdateWithoutTimeoutTests {

    @Test
    @DisplayName("FOR UPDATE without NOWAIT or SKIP LOCKED is detected")
    void detectsForUpdateWithoutTimeout() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE id = 1 FOR UPDATE")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("forUpdateWithoutTimeout", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.FOR_UPDATE_WITHOUT_TIMEOUT);
    }
  }

  @Nested
  @DisplayName("RangeLock")
  class RangeLockTests {

    @Test
    @DisplayName("Range SELECT FOR UPDATE on unindexed column is detected")
    void detectsRangeLock() {
      // Use unindexed column 'name' for range — indexMetadata has no index on 'name'
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT * FROM members WHERE name >= 'A' AND name <= 'Z' FOR UPDATE")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("rangeLock", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.RANGE_LOCK_RISK);
    }
  }

  @Nested
  @DisplayName("OrAbuse")
  class OrAbuseTests {

    @Test
    @DisplayName("3+ OR conditions on different columns is detected")
    void detectsOrAbuse() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT * FROM members WHERE name = 'x' OR email = 'y' OR status = 'z' OR team_id = 0")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("orAbuse", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.OR_ABUSE);
    }
  }
}
