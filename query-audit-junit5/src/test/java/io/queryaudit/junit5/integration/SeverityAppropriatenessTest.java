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
class SeverityAppropriatenessTest {

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
        Member m = new Member("M" + i + j, "sv" + i + j + "@t.com", "ACTIVE");
        m.setTeam(team);
        memberRepository.save(m);
      }
    }
    entityManager.flush();
    entityManager.clear();
  }

  private QueryAuditReport analyze(String testName, List<QueryRecord> queries) {
    return analyze(testName, queries, null);
  }

  private QueryAuditReport analyze(String testName, List<QueryRecord> queries, IndexMetadata meta) {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
    return analyzer.analyze("SeverityTest", testName, queries, meta);
  }

  private List<Issue> warnings(QueryAuditReport report) {
    return report.getConfirmedIssues();
  }

  private List<Issue> infos(QueryAuditReport report) {
    return report.getInfoIssues();
  }

  @Nested
  @DisplayName("SELECT * severity")
  class SelectAllSeverity {

    @Test
    @DisplayName("네이티브 SELECT * — INFO여야 함, WARNING/ERROR이면 과잉")
    void selectAllShouldBeInfo() {
      queryInterceptor.start();
      entityManager.createNativeQuery("SELECT * FROM members WHERE id = 1").getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("selectAllSeverity", queryInterceptor.getRecordedQueries());
      assertThat(warnings(report))
          .as("SELECT * 는 INFO로 충분 — WARNING/ERROR로 올라가면 안 됨")
          .noneMatch(i -> i.type() == IssueType.SELECT_ALL);
      assertThat(infos(report)).anyMatch(i -> i.type() == IssueType.SELECT_ALL);
    }
  }

  @Nested
  @DisplayName("UNION WITHOUT ALL severity")
  class UnionSeverity {

    @Test
    @DisplayName("UNION 없는 ALL — INFO여야 함")
    void unionWithoutAllShouldBeInfo() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT name FROM members WHERE status = 'ACTIVE'"
                  + " UNION"
                  + " SELECT name FROM teams")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("unionSeverity", queryInterceptor.getRecordedQueries());
      assertThat(warnings(report))
          .as("UNION WITHOUT ALL은 INFO — WARNING이면 과잉")
          .noneMatch(i -> i.type() == IssueType.UNION_WITHOUT_ALL);
    }
  }

  @Nested
  @DisplayName("COUNT vs EXISTS severity")
  class CountExistsSeverity {

    @Test
    @DisplayName("COUNT(*) WHERE — 제안 수준(INFO)이어야 함")
    void countInsteadOfExistsShouldBeInfo() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT COUNT(*) FROM members WHERE status = 'ACTIVE'")
          .getSingleResult();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("countExistsSeverity", queryInterceptor.getRecordedQueries());
      assertThat(warnings(report))
          .as("COUNT vs EXISTS는 제안 — WARNING이면 과잉")
          .noneMatch(i -> i.type() == IssueType.COUNT_INSTEAD_OF_EXISTS);
    }
  }

  @Nested
  @DisplayName("Covering index severity")
  class CoveringIndexSeverity {

    @Test
    @DisplayName("커버링 인덱스 기회 — 최적화 제안(INFO)이어야 함")
    void coveringIndexShouldBeInfo() {
      IndexMetadata meta =
          new IndexMetadata(
              Map.of(
                  "members",
                  List.of(
                      new IndexInfo("members", "PRIMARY", "id", 1, false, 100),
                      new IndexInfo("members", "idx_email", "email", 1, false, 100))));

      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT name, email FROM members WHERE email = 'test@t.com'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("coveringSeverity", queryInterceptor.getRecordedQueries(), meta);
      assertThat(warnings(report))
          .as("COVERING_INDEX는 제안 — WARNING이면 과잉")
          .noneMatch(i -> i.type() == IssueType.COVERING_INDEX_OPPORTUNITY);
    }
  }

  @Nested
  @DisplayName("Mergeable queries severity")
  class MergeableSeverity {

    @Test
    @DisplayName("병합 가능 쿼리 — 제안(INFO)이어야 함")
    void mergeableShouldBeInfo() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT name, email FROM members WHERE status = 'ACTIVE'")
          .getResultList();
      entityManager
          .createNativeQuery("SELECT name, email FROM members WHERE name = 'M00'")
          .getResultList();
      entityManager
          .createNativeQuery("SELECT name, email FROM members WHERE email = 'sv00@t.com'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("mergeableSeverity", queryInterceptor.getRecordedQueries());
      assertThat(warnings(report))
          .as("MERGEABLE_QUERIES는 제안 — WARNING이면 과잉")
          .noneMatch(i -> i.type() == IssueType.MERGEABLE_QUERIES);
    }
  }

  @Nested
  @DisplayName("Non-deterministic pagination severity")
  class NonDetPaginationSeverity {

    @Test
    @DisplayName("비결정적 페이지네이션 — 주의(INFO)여야 함")
    void nonDetPaginationShouldBeInfo() {
      IndexMetadata meta =
          new IndexMetadata(
              Map.of(
                  "members",
                  List.of(
                      new IndexInfo("members", "PRIMARY", "id", 1, false, 100),
                      new IndexInfo("members", "idx_status", "status", 1, true, 10))));

      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members ORDER BY status LIMIT 10")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("nonDetSeverity", queryInterceptor.getRecordedQueries(), meta);
      assertThat(warnings(report))
          .as("NON_DETERMINISTIC_PAGINATION은 INFO — WARNING이면 과잉")
          .noneMatch(i -> i.type() == IssueType.NON_DETERMINISTIC_PAGINATION);
    }
  }

  @Nested
  @DisplayName("Excessive column fetch severity")
  class ExcessiveColumnSeverity {

    @Test
    @DisplayName("많은 컬럼 SELECT — 제안(INFO)이어야 함")
    void excessiveColumnShouldBeInfo() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT id, name, description, sku, price, quantity, weight, height,"
                  + " width, depth, color, brand, category, image_url, barcode,"
                  + " notes, created_at, updated_at FROM products")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("excessiveColSeverity", queryInterceptor.getRecordedQueries());
      assertThat(warnings(report))
          .as("EXCESSIVE_COLUMN_FETCH는 제안 — WARNING이면 과잉")
          .noneMatch(i -> i.type() == IssueType.EXCESSIVE_COLUMN_FETCH);
    }
  }

  @Nested
  @DisplayName("SQL-level N+1 severity")
  class SqlNPlusOneSeverity {

    @Test
    @DisplayName("SQL 패턴 N+1 — INFO여야 함 (LazyLoadDetector가 ERROR 권위)")
    void sqlNPlusOneShouldBeInfo() {
      queryInterceptor.start();
      for (int i = 1; i <= 5; i++) {
        entityManager
            .createNativeQuery("SELECT * FROM members WHERE team_id = " + i)
            .getResultList();
      }
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("sqlNPlusOneSeverity", queryInterceptor.getRecordedQueries());
      assertThat(warnings(report))
          .as("SQL-level N+1은 INFO 보조 — WARNING/ERROR이면 과잉")
          .noneMatch(i -> i.type() == IssueType.N_PLUS_ONE);
    }
  }

  @Nested
  @DisplayName("Hibernate 표준 패턴 severity")
  class HibernatePatternSeverity {

    @Test
    @DisplayName("JPA findByStatus — Hibernate 표준 쿼리에 WARNING 없어야 함")
    void jpaFindByStatusNoWarnings() {
      queryInterceptor.start();
      memberRepository.findByStatus("ACTIVE");
      queryInterceptor.stop();

      QueryAuditReport report = analyze("jpaStandard", queryInterceptor.getRecordedQueries());

      List<String> warningTypes =
          warnings(report).stream().map(i -> i.type().name()).collect(Collectors.toList());

      // UNBOUNDED_RESULT_SET can fire without IndexMetadata, so allow it
      List<String> unexpectedWarnings =
          warningTypes.stream()
              .filter(t -> !t.equals("UNBOUNDED_RESULT_SET"))
              .collect(Collectors.toList());

      assertThat(unexpectedWarnings)
          .as("Hibernate 표준 쿼리에 예상 외 WARNING: %s", unexpectedWarnings)
          .isEmpty();
    }

    @Test
    @DisplayName("JPA findById — 단건 조회에 이슈 없어야 함")
    void jpaFindByIdClean() {
      queryInterceptor.start();
      memberRepository.findById(1L);
      queryInterceptor.stop();

      QueryAuditReport report = analyze("jpaFindById", queryInterceptor.getRecordedQueries());
      assertThat(warnings(report)).as("findById에 WARNING이 있으면 과잉").isEmpty();
    }

    @Test
    @DisplayName("JPA findAll — 단순 전체 조회에 ERROR 없어야 함")
    void jpaFindAllNoErrors() {
      queryInterceptor.start();
      teamRepository.findAll();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("jpaFindAll", queryInterceptor.getRecordedQueries());

      List<Issue> errors =
          warnings(report).stream().filter(i -> i.severity() == Severity.ERROR).toList();
      assertThat(errors)
          .as("findAll에 ERROR가 있으면 과잉: %s", errors.stream().map(i -> i.type().name()).toList())
          .isEmpty();
    }
  }

  @Nested
  @DisplayName("ERROR severity 적절성")
  class ErrorSeverityCheck {

    @Test
    @DisplayName("LIKE '%abc' — WARNING이면 적절, ERROR이면 과잉")
    void likeWildcardNotError() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE name LIKE '%abc'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("likeError", queryInterceptor.getRecordedQueries());
      assertThat(
              warnings(report).stream()
                  .filter(i -> i.type() == IssueType.LIKE_LEADING_WILDCARD)
                  .toList())
          .as("LIKE wildcard는 WARNING — ERROR이면 과잉")
          .allMatch(i -> i.severity() == Severity.WARNING);
    }

    @Test
    @DisplayName("IMPLICIT_JOIN — WARNING이면 적절, ERROR이면 과잉")
    void implicitJoinNotError() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM teams t, members m WHERE t.id = m.team_id")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("implicitJoinError", queryInterceptor.getRecordedQueries());
      assertThat(
              warnings(report).stream().filter(i -> i.type() == IssueType.IMPLICIT_JOIN).toList())
          .as("IMPLICIT_JOIN은 WARNING — ERROR이면 과잉")
          .allMatch(i -> i.severity() == Severity.WARNING);
    }

    @Test
    @DisplayName("REPEATED_SINGLE_INSERT — WARNING이면 적절, ERROR이면 과잉")
    void repeatedInsertNotError() {
      queryInterceptor.start();
      for (int i = 0; i < 5; i++) {
        entityManager
            .createNativeQuery("INSERT INTO teams (name) VALUES ('T" + i + "')")
            .executeUpdate();
      }
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("repeatedInsertError", queryInterceptor.getRecordedQueries());
      assertThat(
              warnings(report).stream()
                  .filter(i -> i.type() == IssueType.REPEATED_SINGLE_INSERT)
                  .toList())
          .as("REPEATED_SINGLE_INSERT는 WARNING — ERROR이면 과잉")
          .allMatch(i -> i.severity() == Severity.WARNING);
    }

    @Test
    @DisplayName("DISTINCT_MISUSE — WARNING이면 적절, ERROR이면 과잉")
    void distinctMisuseNotError() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT DISTINCT status, COUNT(*) FROM members GROUP BY status")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("distinctError", queryInterceptor.getRecordedQueries());
      assertThat(
              warnings(report).stream().filter(i -> i.type() == IssueType.DISTINCT_MISUSE).toList())
          .as("DISTINCT_MISUSE는 WARNING — ERROR이면 과잉")
          .allMatch(i -> i.severity() == Severity.WARNING);
    }
  }
}
