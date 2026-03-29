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
class Team4IndexLockFalsePositiveTest {

  @Autowired TeamRepository teamRepository;
  @Autowired MemberRepository memberRepository;
  @Autowired QueryInterceptor queryInterceptor;
  @Autowired EntityManager entityManager;

  private IndexMetadata indexMetadata;

  @BeforeEach
  void setUp() {
    Team team = new Team("FP");
    teamRepository.save(team);
    Member m = new Member("M", "fp4@t.com", "ACTIVE");
    m.setTeam(team);
    memberRepository.save(m);
    entityManager.flush();
    entityManager.clear();

    indexMetadata =
        new IndexMetadata(
            Map.of(
                "members",
                List.of(
                    new IndexInfo("members", "PRIMARY", "id", 1, false, 100),
                    new IndexInfo("members", "idx_email", "email", 1, false, 100),
                    new IndexInfo("members", "idx_status", "status", 1, true, 10)),
                "teams",
                List.of(new IndexInfo("teams", "PRIMARY", "id", 1, false, 10))));
  }

  private QueryAuditReport analyze(String testName, List<QueryRecord> queries) {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
    return analyzer.analyze("Team4FP", testName, queries, indexMetadata);
  }

  private List<Issue> allIssues(QueryAuditReport report) {
    return Stream.concat(
            report.getConfirmedIssues().stream(), report.getInfoIssues().stream())
        .toList();
  }

  @Nested
  @DisplayName("MissingIndex FP")
  class MissingIndexFP {

    @Test
    @DisplayName("WHERE on indexed column should NOT trigger")
    void indexedColumn() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE email = 'fp4@t.com'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("indexedColumn", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .noneMatch(i -> i.type() == IssueType.MISSING_WHERE_INDEX);
    }

    @Test
    @DisplayName("No IndexMetadata means skip (avoid false positives)")
    void noMetadataSkips() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE status = 'ACTIVE'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
      QueryAuditReport report =
          analyzer.analyze("Team4FP", "noMetadata", queryInterceptor.getRecordedQueries(), null);
      assertThat(allIssues(report))
          .noneMatch(i -> i.type() == IssueType.MISSING_WHERE_INDEX);
    }
  }

  @Nested
  @DisplayName("ForUpdateWithoutIndex FP")
  class ForUpdateWithoutIndexFP {

    @Test
    @DisplayName("FOR UPDATE on indexed column should NOT trigger")
    void forUpdateOnIndexedColumn() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE id = 1 FOR UPDATE")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("forUpdateIndexed", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .noneMatch(i -> i.type() == IssueType.FOR_UPDATE_WITHOUT_INDEX);
    }
  }

  @Nested
  @DisplayName("ForUpdateNonUnique FP")
  class ForUpdateNonUniqueFP {

    @Test
    @DisplayName("FOR UPDATE on unique indexed column should NOT trigger")
    void forUpdateOnUniqueColumn() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE email = 'fp4@t.com' FOR UPDATE")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("forUpdateUnique", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .noneMatch(i -> i.type() == IssueType.FOR_UPDATE_NON_UNIQUE);
    }
  }

  @Nested
  @DisplayName("ForUpdateWithoutTimeout FP")
  class ForUpdateWithoutTimeoutFP {

    @Test
    @DisplayName("Regular SELECT (no FOR UPDATE) should NOT trigger")
    void noForUpdate() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE id = 1")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("noForUpdate", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .noneMatch(i -> i.type() == IssueType.FOR_UPDATE_WITHOUT_TIMEOUT);
    }
  }

  @Nested
  @DisplayName("WriteAmplification FP")
  class WriteAmplificationFP {

    @Test
    @DisplayName("Table with 3 indexes should NOT trigger (threshold=6)")
    void fewIndexes() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE id = 1")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("fewIndexes", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .noneMatch(i -> i.type() == IssueType.WRITE_AMPLIFICATION);
    }
  }

  @Nested
  @DisplayName("OrAbuse FP")
  class OrAbuseFP {

    @Test
    @DisplayName("OR conditions on same column should NOT trigger (use IN instead)")
    void sameColumnOr() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT * FROM members WHERE status = 'ACTIVE' OR status = 'INACTIVE' OR status = 'PENDING' OR status = 'DELETED'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("sameColumnOr", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .noneMatch(i -> i.type() == IssueType.OR_ABUSE);
    }
  }
}
