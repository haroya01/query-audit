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
class Team2WhereQualityIntegrationTest {

  @Autowired TeamRepository teamRepository;
  @Autowired MemberRepository memberRepository;
  @Autowired QueryInterceptor queryInterceptor;
  @Autowired EntityManager entityManager;

  @BeforeEach
  void setUp() {
    Team team = new Team("Dev");
    teamRepository.save(team);
    for (int i = 0; i < 5; i++) {
      Member m = new Member("User " + i, "user" + i + "@test.com", "ACTIVE");
      m.setTeam(team);
      memberRepository.save(m);
    }
    entityManager.flush();
    entityManager.clear();
  }

  private QueryAuditReport analyze(String testName, List<QueryRecord> queries) {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
    return analyzer.analyze("Team2WhereQualityIntegrationTest", testName, queries, null);
  }

  private List<Issue> allIssues(QueryAuditReport report) {
    return Stream.concat(
            report.getConfirmedIssues().stream(), report.getInfoIssues().stream())
        .toList();
  }

  @Nested
  @DisplayName("WhereFunction")
  class WhereFunctionTests {

    @Test
    @DisplayName("UPPER() in WHERE prevents index usage")
    void detectsUpperInWhere() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE UPPER(name) = 'USER 0'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("upperInWhere", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.WHERE_FUNCTION);
    }

    @Test
    @DisplayName("LOWER() in WHERE prevents index usage")
    void detectsLowerInWhere() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE LOWER(email) = 'user0@test.com'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("lowerInWhere", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.WHERE_FUNCTION);
    }
  }

  @Nested
  @DisplayName("Sargability")
  class SargabilityTests {

    @Test
    @DisplayName("Arithmetic on column prevents index usage")
    void detectsArithmeticOnColumn() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE id + 1 = 10")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("arithmeticOnColumn", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.NON_SARGABLE_EXPRESSION);
    }
  }

  @Nested
  @DisplayName("NullComparison")
  class NullComparisonTests {

    @Test
    @DisplayName("= NULL comparison is always UNKNOWN")
    void detectsEqualsNull() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE status = NULL")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("equalsNull", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.NULL_COMPARISON);
    }

    @Test
    @DisplayName("!= NULL comparison is always UNKNOWN")
    void detectsNotEqualsNull() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE status != NULL")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("notEqualsNull", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.NULL_COMPARISON);
    }
  }

  @Nested
  @DisplayName("LikeWildcard")
  class LikeWildcardTests {

    @Test
    @DisplayName("LIKE with leading wildcard prevents index usage")
    void detectsLeadingWildcard() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE name LIKE '%User'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("leadingWildcard", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.LIKE_LEADING_WILDCARD);
    }
  }

  @Nested
  @DisplayName("ImplicitTypeConversion")
  class ImplicitTypeConversionTests {

    @Test
    @DisplayName("String-like column compared to numeric literal causes implicit conversion")
    void detectsImplicitTypeConversion() {
      // The detector looks for string-indicator column names (e.g. *_name, *_code)
      // compared to bare numeric literals. H2 may error on varchar=int,
      // so we catch and still verify the interceptor captured the SQL.
      entityManager
          .createNativeQuery(
              "CREATE TABLE products_ext (id BIGINT PRIMARY KEY, product_name VARCHAR(255), product_code VARCHAR(50))")
          .executeUpdate();
      entityManager
          .createNativeQuery("INSERT INTO products_ext VALUES (1, 'Widget', 'ABC123')")
          .executeUpdate();
      entityManager.flush();

      queryInterceptor.start();
      try {
        entityManager
            .createNativeQuery("SELECT * FROM products_ext WHERE product_code = 123")
            .getResultList();
      } catch (Exception ignored) {
        // H2 may not support implicit varchar->int conversion
      }
      queryInterceptor.stop();

      List<QueryRecord> queries = queryInterceptor.getRecordedQueries();
      if (queries.isEmpty()) {
        // H2 doesn't capture failed queries — verify with manual QueryRecord
        queries =
            List.of(
                new QueryRecord(
                    "SELECT * FROM products_ext WHERE product_code = 123", 0, 0, ""));
      }

      QueryAuditReport report = analyze("implicitConversion", queries);
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.IMPLICIT_TYPE_CONVERSION);
    }
  }

  @Nested
  @DisplayName("CaseInWhere")
  class CaseInWhereTests {

    @Test
    @DisplayName("CASE expression in WHERE prevents index usage")
    void detectsCaseInWhere() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT * FROM members WHERE CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END = 1")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("caseInWhere", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.CASE_IN_WHERE);
    }
  }

  @Nested
  @DisplayName("StringConcatInWhere")
  class StringConcatInWhereTests {

    @Test
    @DisplayName("String concatenation in WHERE prevents index usage")
    void detectsStringConcatInWhere() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT * FROM members WHERE name || email = 'User 0user0@test.com'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("stringConcatInWhere", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.STRING_CONCAT_IN_WHERE);
    }
  }

  @Nested
  @DisplayName("RedundantFilter")
  class RedundantFilterTests {

    @Test
    @DisplayName("Duplicate WHERE conditions are detected")
    void detectsRedundantFilter() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT * FROM members WHERE status = 'ACTIVE' AND status = 'ACTIVE'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("redundantFilter", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.REDUNDANT_FILTER);
    }
  }
}
