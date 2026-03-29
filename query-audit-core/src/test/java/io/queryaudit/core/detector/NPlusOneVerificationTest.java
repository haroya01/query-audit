package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Verification tests for NPlusOneDetector (SQL-level, INFO-only).
 *
 * <p>After the migration to Hibernate event-based N+1 detection, the SQL-level NPlusOneDetector now
 * reports ALL repeated query patterns as INFO only. It serves as supplementary information, not a
 * confirmed detection. The authoritative N+1 detection is done by {@link LazyLoadNPlusOneDetector}.
 */
class NPlusOneVerificationTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  // -- helpers --

  private static QueryRecord record(String sql, String stackTrace) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), stackTrace);
  }

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private static QueryRecord filler(int index) {
    return new QueryRecord(
        "SELECT * FROM filler_table_" + index + " WHERE id = 1",
        0L,
        System.currentTimeMillis(),
        "");
  }

  private static List<QueryRecord> buildQueryList(
      String targetSql, List<Integer> positions, int totalSize, String stackTrace) {
    List<QueryRecord> queries = new ArrayList<>();
    for (int i = 0; i < totalSize; i++) {
      if (positions.contains(i)) {
        queries.add(record(targetSql.replace("{idx}", String.valueOf(i)), stackTrace));
      } else {
        queries.add(filler(i));
      }
    }
    return queries;
  }

  private static List<QueryRecord> consecutiveQueries(
      String sql, int start, int count, int totalSize, String stackTrace) {
    List<Integer> positions = new ArrayList<>();
    for (int i = start; i < start + count; i++) {
      positions.add(i);
    }
    return buildQueryList(sql, positions, totalSize, stackTrace);
  }

  private static List<QueryRecord> spreadQueries(
      String sql, int start, int count, int gap, String stackTrace) {
    List<Integer> positions = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      positions.add(start + i * (gap + 1));
    }
    int totalSize = positions.get(positions.size() - 1) + 1;
    return buildQueryList(sql, positions, totalSize, stackTrace);
  }

  // ====================================================================
  //  SQL-level detector now always reports INFO
  // ====================================================================

  @Nested
  class AllQueriesAreInfoOnly {

    /** Consecutive queries (previously ERROR) -> now INFO. */
    @Test
    void consecutiveQueries_shouldBeInfo() {
      String trace =
          "com.example.service.OrderService.listOrders:70\n"
              + "com.example.domain.Order$$HibernateProxy.getMember:45";

      List<QueryRecord> queries =
          consecutiveQueries("SELECT * FROM members WHERE id = {idx}", 10, 5, 20, trace);

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.N_PLUS_ONE_SUSPECT);
      assertThat(issues.get(0).detail()).contains("5 times");
    }

    /** Custom loop pattern (previously ERROR) -> now INFO. */
    @Test
    void customLoopPattern_shouldBeInfo() {
      String trace = "com.example.service.ReportService.generateReport:102";

      List<QueryRecord> queries =
          consecutiveQueries("SELECT * FROM items WHERE id = {idx}", 5, 8, 20, trace);

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    /** Nested patterns: two different query patterns, both detected as INFO. */
    @Test
    void nestedPatterns_bothInfo() {
      String memberTrace = "com.example.service.OrderService.listOrders:70";
      String addressTrace = "com.example.service.OrderService.listOrders:70";

      List<QueryRecord> queries = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        queries.add(record("SELECT * FROM members WHERE id = " + i, memberTrace));
      }
      for (int i = 0; i < 5; i++) {
        queries.add(record("SELECT * FROM addresses WHERE member_id = " + i, addressTrace));
      }

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(2);
      assertThat(issues).allMatch(i -> i.severity() == Severity.INFO);

      List<String> tables = issues.stream().map(Issue::table).toList();
      assertThat(tables).containsExactlyInAnyOrder("members", "addresses");
    }

    /** Interleaved (gap of 1) -> INFO. */
    @Test
    void interleavedGapOf1_shouldBeInfo() {
      String trace = "com.example.service.OrderService.process:50";
      List<QueryRecord> queries =
          spreadQueries("SELECT * FROM items WHERE id = {idx}", 10, 5, 1, trace);

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    /** Spread out queries -> INFO. */
    @Test
    void spreadOut_shouldBeInfo() {
      String trace = "com.example.interceptor.SuspensionCheckInterceptor.preHandle:25";

      List<Integer> positions = List.of(5, 50, 100, 150, 200);
      List<QueryRecord> queries =
          buildQueryList(
              "SELECT * FROM user_suspensions WHERE user_id = {idx}", positions, 210, trace);

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    /** Exactly at threshold -> INFO. */
    @Test
    void exactlyAtThreshold_shouldBeInfo() {
      List<QueryRecord> queries =
          consecutiveQueries("SELECT * FROM users WHERE id = {idx}", 0, 3, 3, "");

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    /** Pagination pattern -> INFO. */
    @Test
    void paginationPattern_shouldBeInfo() {
      String trace = "com.example.controller.ProductController.list:55";

      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM products ORDER BY id LIMIT 20 OFFSET 0", trace),
              record("SELECT * FROM products ORDER BY id LIMIT 20 OFFSET 20", trace),
              record("SELECT * FROM products ORDER BY id LIMIT 20 OFFSET 40", trace),
              record("SELECT * FROM products ORDER BY id LIMIT 20 OFFSET 60", trace),
              record("SELECT * FROM products ORDER BY id LIMIT 20 OFFSET 80", trace));

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    /** Cache pattern in stack trace -> still INFO (no downgrade needed since all are INFO). */
    @Test
    void cachePattern_shouldBeInfo() {
      String trace =
          "com.example.service.ProductService.getProducts:50\n"
              + "com.example.cache.ProductCacheService.loadFromDb:30";

      List<QueryRecord> queries =
          consecutiveQueries("SELECT * FROM products WHERE category_id = {idx}", 0, 10, 10, trace);

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    /** Null stack traces -> INFO. */
    @Test
    void nullStackTraces_shouldBeInfo() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1", (String) null),
              record("SELECT * FROM users WHERE id = 2", (String) null),
              record("SELECT * FROM users WHERE id = 3", (String) null),
              record("SELECT * FROM users WHERE id = 4", (String) null),
              record("SELECT * FROM users WHERE id = 5", (String) null));

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    /** Empty stack traces -> INFO. */
    @Test
    void emptyStackTraces_shouldBeInfo() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1", ""),
              record("SELECT * FROM users WHERE id = 2", ""),
              record("SELECT * FROM users WHERE id = 3", ""),
              record("SELECT * FROM users WHERE id = 4", ""),
              record("SELECT * FROM users WHERE id = 5", ""));

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    /** Detail text references SQL-level detection. */
    @Test
    void detailText_referencesSqlLevel() {
      List<QueryRecord> queries =
          consecutiveQueries("SELECT * FROM data WHERE id = {idx}", 0, 5, 5, "");

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      Issue issue = issues.get(0);
      assertThat(issue.severity()).isEqualTo(Severity.INFO);
      assertThat(issue.detail()).contains("SQL-level detection");
    }

    /** Suggestion references Hibernate-level detection. */
    @Test
    void suggestion_referencesHibernateLevel() {
      List<QueryRecord> queries =
          consecutiveQueries("SELECT * FROM members WHERE id = {idx}", 0, 10, 10, "");

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).suggestion()).contains("Hibernate-level");
    }
  }

  // ====================================================================
  //  BELOW THRESHOLD -- no issues
  // ====================================================================

  @Nested
  class BelowThreshold {

    @Test
    void belowThreshold_noIssue() {
      List<QueryRecord> queries =
          consecutiveQueries("SELECT * FROM users WHERE id = {idx}", 0, 2, 2, "");

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    void differentQueries_notGrouped() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1"),
              record("SELECT * FROM orders WHERE id = 2"),
              record("SELECT * FROM products WHERE id = 3"));

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    void nullSql_safelySkipped() {
      List<QueryRecord> queries =
          List.of(
              new QueryRecord(null, null, 0L, 0L, "", 0),
              record("SELECT * FROM users WHERE id = 1"),
              record("SELECT * FROM users WHERE id = 2"),
              record("SELECT * FROM users WHERE id = 3"));

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      // 3 consecutive user queries -> INFO
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }
  }

  // ====================================================================
  //  extractFirstFrame unit tests (unchanged utility method)
  // ====================================================================

  @Nested
  class ExtractFirstFrameTest {

    @Test
    void nullInput_returnsNull() {
      assertThat(NPlusOneDetector.extractFirstFrame(null)).isNull();
    }

    @Test
    void emptyString_returnsNull() {
      assertThat(NPlusOneDetector.extractFirstFrame("")).isNull();
    }

    @Test
    void singleLine_returnsThatLine() {
      assertThat(NPlusOneDetector.extractFirstFrame("com.example.Foo.bar(Foo.java:10)"))
          .isEqualTo("com.example.Foo.bar(Foo.java:10)");
    }

    @Test
    void multipleLines_returnsFirstNonEmpty() {
      String trace = "at com.example.Foo.bar(Foo.java:10)\nat com.example.Baz.qux(Baz.java:20)";
      assertThat(NPlusOneDetector.extractFirstFrame(trace))
          .isEqualTo("at com.example.Foo.bar(Foo.java:10)");
    }

    @Test
    void linesWithOnlyWhitespace_skipsToFirstContent() {
      String trace =
          "   \n  \n  at com.example.Foo.bar(Foo.java:10)\nat com.example.Baz.qux(Baz.java:20)";
      assertThat(NPlusOneDetector.extractFirstFrame(trace))
          .isEqualTo("at com.example.Foo.bar(Foo.java:10)");
    }

    @Test
    void allWhitespace_returnsNull() {
      assertThat(NPlusOneDetector.extractFirstFrame("   \n  \n  ")).isNull();
    }

    @Test
    void lineWithAtPrefix_preserved() {
      String trace = "at com.example.Service.method(Service.java:42)";
      assertThat(NPlusOneDetector.extractFirstFrame(trace))
          .isEqualTo("at com.example.Service.method(Service.java:42)");
    }

    @Test
    void leadingWhitespaceOnFirstLine_trimmed() {
      String trace = "   com.example.Foo.bar(Foo.java:10)";
      assertThat(NPlusOneDetector.extractFirstFrame(trace))
          .isEqualTo("com.example.Foo.bar(Foo.java:10)");
    }
  }
}
