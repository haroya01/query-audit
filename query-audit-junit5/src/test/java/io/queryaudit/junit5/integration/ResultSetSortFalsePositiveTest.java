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
class ResultSetSortFalsePositiveTest {

  @Autowired TeamRepository teamRepository;
  @Autowired MemberRepository memberRepository;
  @Autowired QueryInterceptor queryInterceptor;
  @Autowired EntityManager entityManager;

  @BeforeEach
  void setUp() {
    Team team = new Team("FP5");
    teamRepository.save(team);
    Member m = new Member("M5", "fp5@t.com", "ACTIVE");
    m.setTeam(team);
    memberRepository.save(m);
    entityManager.flush();
    entityManager.clear();
  }

  private QueryAuditReport analyze(String testName, List<QueryRecord> queries) {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
    return analyzer.analyze("Team5FP", testName, queries, null);
  }

  private List<Issue> allIssues(QueryAuditReport report) {
    return Stream.concat(report.getConfirmedIssues().stream(), report.getInfoIssues().stream())
        .toList();
  }

  @Nested
  @DisplayName("UnboundedResultSet FP")
  class UnboundedResultSetFP {

    @Test
    @DisplayName("Aggregate query (COUNT) should NOT trigger")
    void aggregateQuery() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT COUNT(*) FROM members WHERE status = 'ACTIVE'")
          .getSingleResult();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("aggregate", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).noneMatch(i -> i.type() == IssueType.UNBOUNDED_RESULT_SET);
    }

    @Test
    @DisplayName("PK lookup (JPA findById) should NOT trigger")
    void pkLookup() {
      queryInterceptor.start();
      memberRepository.findById(1L);
      queryInterceptor.stop();

      QueryAuditReport report = analyze("pkLookup", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).noneMatch(i -> i.type() == IssueType.UNBOUNDED_RESULT_SET);
    }

    @Test
    @DisplayName("Query with LIMIT should NOT trigger")
    void withLimit() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE status = 'ACTIVE' LIMIT 10")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("withLimit", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).noneMatch(i -> i.type() == IssueType.UNBOUNDED_RESULT_SET);
    }

    @Test
    @DisplayName("FOR UPDATE query should NOT trigger")
    void forUpdateExempt() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE status = 'ACTIVE' FOR UPDATE")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("forUpdate", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).noneMatch(i -> i.type() == IssueType.UNBOUNDED_RESULT_SET);
    }
  }

  @Nested
  @DisplayName("OffsetPagination FP")
  class OffsetPaginationFP {

    @Test
    @DisplayName("Small OFFSET should NOT trigger (threshold=1000)")
    void smallOffset() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members ORDER BY id LIMIT 10 OFFSET 50")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("smallOffset", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).noneMatch(i -> i.type() == IssueType.OFFSET_PAGINATION);
    }
  }

  @Nested
  @DisplayName("NonDeterministicPagination FP")
  class NonDeterministicPaginationFP {

    @Test
    @DisplayName("ORDER BY unique column should NOT trigger")
    void uniqueColumnOrderBy() {
      IndexMetadata meta =
          new IndexMetadata(
              Map.of("members", List.of(new IndexInfo("members", "PRIMARY", "id", 1, false, 100))));

      queryInterceptor.start();
      entityManager.createNativeQuery("SELECT * FROM members ORDER BY id LIMIT 10").getResultList();
      queryInterceptor.stop();

      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
      QueryAuditReport report =
          analyzer.analyze("Team5FP", "uniqueOrderBy", queryInterceptor.getRecordedQueries(), meta);
      assertThat(
              Stream.concat(report.getConfirmedIssues().stream(), report.getInfoIssues().stream())
                  .toList())
          .noneMatch(i -> i.type() == IssueType.NON_DETERMINISTIC_PAGINATION);
    }
  }

  @Nested
  @DisplayName("CountStarWithoutWhere FP")
  class CountStarFP {

    @Test
    @DisplayName("COUNT(*) WITH WHERE should NOT trigger")
    void countWithWhere() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT COUNT(*) FROM members WHERE status = 'ACTIVE'")
          .getSingleResult();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("countWithWhere", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).noneMatch(i -> i.type() == IssueType.COUNT_STAR_WITHOUT_WHERE);
    }
  }

  @Nested
  @DisplayName("WindowFunction FP")
  class WindowFunctionFP {

    @Test
    @DisplayName("Window function WITH PARTITION BY should NOT trigger")
    void withPartition() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT name, SUM(id) OVER (PARTITION BY status) AS total FROM members")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("withPartition", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .noneMatch(i -> i.type() == IssueType.WINDOW_FUNCTION_WITHOUT_PARTITION);
    }
  }

  @Nested
  @DisplayName("ExcessiveColumnFetch FP")
  class ExcessiveColumnFetchFP {

    @Test
    @DisplayName("SELECT with few columns should NOT trigger")
    void fewColumns() {
      queryInterceptor.start();
      entityManager.createNativeQuery("SELECT id, name, email FROM members").getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("fewColumns", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).noneMatch(i -> i.type() == IssueType.EXCESSIVE_COLUMN_FETCH);
    }
  }
}
