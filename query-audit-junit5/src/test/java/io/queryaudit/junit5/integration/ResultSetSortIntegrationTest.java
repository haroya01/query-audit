package io.queryaudit.junit5.integration;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.*;
import io.queryaudit.junit5.EnableQueryInspector;
import io.queryaudit.junit5.integration.entity.Member;
import io.queryaudit.junit5.integration.entity.Product;
import io.queryaudit.junit5.integration.entity.Team;
import io.queryaudit.junit5.integration.repository.MemberRepository;
import io.queryaudit.junit5.integration.repository.ProductRepository;
import io.queryaudit.junit5.integration.repository.TeamRepository;
import jakarta.persistence.EntityManager;
import java.math.BigDecimal;
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
class ResultSetSortIntegrationTest {

  @Autowired TeamRepository teamRepository;
  @Autowired MemberRepository memberRepository;
  @Autowired ProductRepository productRepository;
  @Autowired QueryInterceptor queryInterceptor;
  @Autowired EntityManager entityManager;

  @BeforeEach
  void setUp() {
    Team team = new Team("Dev");
    teamRepository.save(team);
    for (int i = 0; i < 5; i++) {
      Member m = new Member("User " + i, "u" + i + "@test.com", "ACTIVE");
      m.setTeam(team);
      memberRepository.save(m);
    }
    for (int i = 0; i < 3; i++) {
      Product p = new Product("Product " + i, "SKU" + i, BigDecimal.valueOf(10 + i));
      p.setDescription("Desc " + i);
      p.setQuantity(100);
      p.setWeight(1.5);
      p.setHeight(10.0);
      p.setWidth(5.0);
      p.setDepth(3.0);
      p.setColor("Red");
      p.setBrand("Brand");
      p.setCategory("Cat");
      p.setImageUrl("http://img/" + i);
      p.setBarcode("BAR" + i);
      p.setNotes("Notes " + i);
      productRepository.save(p);
    }
    entityManager.flush();
    entityManager.clear();
  }

  private QueryAuditReport analyze(String testName, List<QueryRecord> queries) {
    return analyze(testName, queries, null);
  }

  private QueryAuditReport analyze(
      String testName, List<QueryRecord> queries, IndexMetadata indexMetadata) {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer();
    return analyzer.analyze("ResultSetSortIntegrationTest", testName, queries, indexMetadata);
  }

  private List<Issue> allIssues(QueryAuditReport report) {
    return Stream.concat(report.getConfirmedIssues().stream(), report.getInfoIssues().stream())
        .toList();
  }

  @Nested
  @DisplayName("UnboundedResultSet")
  class UnboundedResultSetTests {

    @Test
    @DisplayName("SELECT without LIMIT on non-PK column is detected")
    void detectsUnboundedResultSet() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members WHERE status = 'ACTIVE'")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("unboundedResultSet", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).anyMatch(i -> i.type() == IssueType.UNBOUNDED_RESULT_SET);
    }
  }

  @Nested
  @DisplayName("LimitWithoutOrderBy")
  class LimitWithoutOrderByTests {

    @Test
    @DisplayName("LIMIT without ORDER BY returns non-deterministic results")
    void detectsLimitWithoutOrderBy() {
      queryInterceptor.start();
      entityManager.createNativeQuery("SELECT * FROM members LIMIT 10").getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("limitWithoutOrderBy", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).anyMatch(i -> i.type() == IssueType.LIMIT_WITHOUT_ORDER_BY);
    }
  }

  @Nested
  @DisplayName("OffsetPagination")
  class OffsetPaginationTests {

    @Test
    @DisplayName("Large OFFSET value causes full scan")
    void detectsLargeOffset() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members ORDER BY id LIMIT 10 OFFSET 1000")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("largeOffset", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).anyMatch(i -> i.type() == IssueType.OFFSET_PAGINATION);
    }
  }

  @Nested
  @DisplayName("NonDeterministicPagination")
  class NonDeterministicPaginationTests {

    @Test
    @DisplayName("ORDER BY non-unique column with LIMIT is non-deterministic")
    void detectsNonDeterministicPagination() {
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
          analyze("nonDeterministicPagination", queryInterceptor.getRecordedQueries(), meta);
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.NON_DETERMINISTIC_PAGINATION);
    }
  }

  @Nested
  @DisplayName("OrderByRand")
  class OrderByRandTests {

    @Test
    @DisplayName("ORDER BY RAND() forces full sort")
    void detectsOrderByRand() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members ORDER BY RAND() LIMIT 1")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report = analyze("orderByRand", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).anyMatch(i -> i.type() == IssueType.ORDER_BY_RAND);
    }
  }

  @Nested
  @DisplayName("OrderByLimitWithoutIndex")
  class OrderByLimitWithoutIndexTests {

    @Test
    @DisplayName("ORDER BY unindexed column with LIMIT causes filesort")
    void detectsOrderByLimitWithoutIndex() {
      IndexMetadata meta =
          new IndexMetadata(
              Map.of("members", List.of(new IndexInfo("members", "PRIMARY", "id", 1, false, 100))));

      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT * FROM members ORDER BY name LIMIT 10")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("orderByLimitWithoutIndex", queryInterceptor.getRecordedQueries(), meta);
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.ORDER_BY_LIMIT_WITHOUT_INDEX);
    }
  }

  @Nested
  @DisplayName("SelectCountStarWithoutWhere")
  class SelectCountStarTests {

    @Test
    @DisplayName("COUNT(*) without WHERE forces full index scan")
    void detectsCountStarWithoutWhere() {
      queryInterceptor.start();
      entityManager.createNativeQuery("SELECT COUNT(*) FROM members").getSingleResult();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("countStarWithoutWhere", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).anyMatch(i -> i.type() == IssueType.COUNT_STAR_WITHOUT_WHERE);
    }
  }

  @Nested
  @DisplayName("CountInsteadOfExists")
  class CountInsteadOfExistsTests {

    @Test
    @DisplayName("COUNT(*) for existence check should use EXISTS")
    void detectsCountInsteadOfExists() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT COUNT(*) FROM members WHERE status = 'ACTIVE'")
          .getSingleResult();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("countInsteadOfExists", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).anyMatch(i -> i.type() == IssueType.COUNT_INSTEAD_OF_EXISTS);
    }
  }

  @Nested
  @DisplayName("ExcessiveColumnFetch")
  class ExcessiveColumnFetchTests {

    @Test
    @DisplayName("Query fetching 16+ columns is detected")
    void detectsExcessiveColumnFetch() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery(
              "SELECT id, name, description, sku, price, quantity, weight, height,"
                  + " width, depth, color, brand, category, image_url, barcode,"
                  + " notes, created_at, updated_at FROM products")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("excessiveColumnFetch", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report)).anyMatch(i -> i.type() == IssueType.EXCESSIVE_COLUMN_FETCH);
    }
  }

  @Nested
  @DisplayName("WindowFunctionWithoutPartition")
  class WindowFunctionTests {

    @Test
    @DisplayName("Aggregate window function without PARTITION BY processes entire result set")
    void detectsWindowFunctionWithoutPartition() {
      queryInterceptor.start();
      entityManager
          .createNativeQuery("SELECT name, SUM(id) OVER () AS running_total FROM members")
          .getResultList();
      queryInterceptor.stop();

      QueryAuditReport report =
          analyze("windowFunctionNoPartition", queryInterceptor.getRecordedQueries());
      assertThat(allIssues(report))
          .anyMatch(i -> i.type() == IssueType.WINDOW_FUNCTION_WITHOUT_PARTITION);
    }
  }
}
