package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests for the 5 detector improvements based on academic research and real-world false positive
 * analysis.
 */
class DetectorImprovementTest {

  // ═══════════════════════════════════════════════════════════════
  //  Helpers
  // ═══════════════════════════════════════════════════════════════

  private static QueryRecord query(String sql) {
    return new QueryRecord(sql, 1_000_000L, System.currentTimeMillis(), "test.StackTrace");
  }

  private static QueryRecord query(String sql, String stackTrace) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), stackTrace);
  }

  private static IndexInfo idx(String table, String indexName, String column, int seq) {
    return new IndexInfo(table, indexName, column, seq, true, 100);
  }

  private static IndexInfo pk(String table, String column) {
    return new IndexInfo(table, "PRIMARY", column, 1, false, 1000);
  }

  private static IndexInfo uniqueIdx(String table, String indexName, String column, int seq) {
    return new IndexInfo(table, indexName, column, seq, false, 500);
  }

  private static IndexMetadata metadata(IndexInfo... infos) {
    Map<String, List<IndexInfo>> map = new HashMap<>();
    for (IndexInfo info : infos) {
      map.computeIfAbsent(info.tableName(), k -> new ArrayList<>()).add(info);
    }
    return new IndexMetadata(map);
  }

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private List<Issue> runMissing(String sql, IndexMetadata meta) {
    return new MissingIndexDetector().evaluate(List.of(query(sql)), meta);
  }

  private List<Issue> runComposite(String sql, IndexMetadata meta) {
    return new CompositeIndexDetector().evaluate(List.of(query(sql)), meta);
  }

  private static List<QueryRecord> repeat(String sqlTemplate, int count, String stackTrace) {
    List<QueryRecord> records = new ArrayList<>();
    for (int i = 1; i <= count; i++) {
      records.add(query(sqlTemplate.replace("{id}", String.valueOf(i)), stackTrace));
    }
    return records;
  }

  // ═══════════════════════════════════════════════════════════════
  //  Improvement 1: JOIN-through composite index resolution
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Improvement 1: JOIN-through composite index resolution")
  class JoinThroughCompositeIndex {

    @Test
    @DisplayName("JOIN + WHERE on other side suppresses leading column warning")
    void joinTransitiveConstraint_suppressesFalsePositive() {
      // Composite index on message_reactions (message_id, emoji)
      // Query: JOIN messages ON messages.id = message_reactions.message_id WHERE messages.id = ?
      // message_id is effectively constrained via JOIN + WHERE
      IndexMetadata meta =
          metadata(
              pk("messages", "id"),
              pk("message_reactions", "id"),
              idx("message_reactions", "idx_msg_emoji", "message_id", 1),
              idx("message_reactions", "idx_msg_emoji", "emoji", 2));

      String sql =
          "SELECT mr.* FROM messages m "
              + "JOIN message_reactions mr ON m.id = mr.message_id "
              + "WHERE m.id = 1 AND mr.emoji = 'thumbsup'";

      List<Issue> issues = runComposite(sql, meta);

      // message_id is constrained transitively through JOIN (m.id = mr.message_id)
      // and m.id is in WHERE. So leading column IS effectively used.
      assertThat(issues)
          .noneMatch(
              i ->
                  i.type() == IssueType.COMPOSITE_INDEX_LEADING_COLUMN
                      && "emoji".equals(i.column()));
    }

    @Test
    @DisplayName("JOIN without WHERE on either side still flags leading column")
    void joinWithoutWhere_stillFlags() {
      IndexMetadata meta =
          metadata(
              pk("messages", "id"),
              pk("message_reactions", "id"),
              idx("message_reactions", "idx_msg_emoji", "message_id", 1),
              idx("message_reactions", "idx_msg_emoji", "emoji", 2));

      // No WHERE clause constraining messages.id
      String sql =
          "SELECT mr.* FROM messages m "
              + "JOIN message_reactions mr ON m.id = mr.message_id "
              + "WHERE mr.emoji = 'thumbsup'";

      List<Issue> issues = runComposite(sql, meta);

      // message_id is NOT constrained (no WHERE on m.id), so leading column is unused
      assertThat(issues)
          .anyMatch(
              i ->
                  i.type() == IssueType.COMPOSITE_INDEX_LEADING_COLUMN
                      && "emoji".equals(i.column()));
    }

    @Test
    @DisplayName("Transitive JOIN chain: A.x = B.y, B.z = C.w, WHERE A.x = ?")
    void transitiveJoinChain() {
      // Three tables: A -> B -> C
      // Composite on C: (w, extra_col)
      // WHERE A.x = ? constrains B.y via JOIN, which constrains C.w via another JOIN
      IndexMetadata meta =
          metadata(
              pk("table_a", "x"),
              pk("table_b", "y"),
              pk("table_c", "id"),
              idx("table_c", "idx_w_extra", "w", 1),
              idx("table_c", "idx_w_extra", "extra_col", 2));

      String sql =
          "SELECT c.* FROM table_a a "
              + "JOIN table_b b ON a.x = b.y "
              + "JOIN table_c c ON b.y = c.w "
              + "WHERE a.x = 1 AND c.extra_col = 'val'";

      List<Issue> issues = runComposite(sql, meta);

      // w is constrained transitively: WHERE a.x -> JOIN a.x=b.y -> JOIN b.y=c.w
      assertThat(issues)
          .noneMatch(
              i ->
                  i.type() == IssueType.COMPOSITE_INDEX_LEADING_COLUMN
                      && "extra_col".equals(i.column()));
    }

    @Test
    @DisplayName("Non-matching JOIN columns do not create false transitivity")
    void nonMatchingJoinColumns_noFalseTransitivity() {
      IndexMetadata meta =
          metadata(
              pk("orders", "id"),
              pk("items", "id"),
              idx("items", "idx_cat_price", "category", 1),
              idx("items", "idx_cat_price", "price", 2));

      // JOIN is on order_id, not category. WHERE constrains orders.id.
      // This should NOT transitively constrain items.category.
      String sql =
          "SELECT i.* FROM orders o "
              + "JOIN items i ON o.id = i.order_id "
              + "WHERE o.id = 1 AND i.price > 100";

      List<Issue> issues = runComposite(sql, meta);

      // category (leading col) is NOT constrained through the JOIN,
      // so the composite index warning for price should still fire
      assertThat(issues)
          .anyMatch(
              i ->
                  i.type() == IssueType.COMPOSITE_INDEX_LEADING_COLUMN
                      && "price".equals(i.column()));
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  Improvement 2: GROUP BY after narrow WHERE skip
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Improvement 2: GROUP BY after narrow WHERE skip")
  class GroupByAfterNarrowWhere {

    @Test
    @DisplayName("GROUP BY skipped when WHERE has indexed column on same table")
    void groupBySkipped_whenWhereHasIndexedColumn() {
      IndexMetadata meta =
          metadata(
              pk("sticker_poll_votes", "id"),
              idx("sticker_poll_votes", "idx_sticker_id", "sticker_id", 1));

      String sql =
          "SELECT option_index, COUNT(*) FROM sticker_poll_votes "
              + "WHERE sticker_id = 1 GROUP BY option_index";

      List<Issue> issues = runMissing(sql, meta);

      // sticker_id is indexed, so GROUP BY on option_index operates on a small result set
      assertThat(issues)
          .noneMatch(
              i ->
                  i.type() == IssueType.MISSING_GROUP_BY_INDEX
                      && "option_index".equals(i.column()));
    }

    @Test
    @DisplayName("GROUP BY still flagged when no indexed WHERE column")
    void groupByFlagged_whenNoIndexedWhereColumn() {
      IndexMetadata meta = metadata(pk("votes", "id"));

      String sql = "SELECT option_index, COUNT(*) FROM votes GROUP BY option_index";

      List<Issue> issues = runMissing(sql, meta);

      assertThat(issues)
          .anyMatch(
              i ->
                  i.type() == IssueType.MISSING_GROUP_BY_INDEX
                      && "option_index".equals(i.column()));
    }

    @Test
    @DisplayName("GROUP BY still flagged when WHERE column is NOT indexed")
    void groupByFlagged_whenWhereColumnNotIndexed() {
      IndexMetadata meta =
          metadata(
              pk("votes", "id")
              // no index on poll_id
              );

      String sql =
          "SELECT option_index, COUNT(*) FROM votes " + "WHERE poll_id = 1 GROUP BY option_index";

      List<Issue> issues = runMissing(sql, meta);

      assertThat(issues)
          .anyMatch(
              i ->
                  i.type() == IssueType.MISSING_GROUP_BY_INDEX
                      && "option_index".equals(i.column()));
    }

    @Test
    @DisplayName("GROUP BY skipped with aliased table and indexed WHERE")
    void groupBySkipped_withAlias() {
      IndexMetadata meta =
          metadata(
              pk("sticker_poll_votes", "id"),
              idx("sticker_poll_votes", "idx_sticker_id", "sticker_id", 1));

      String sql =
          "SELECT spv.option_index, COUNT(*) FROM sticker_poll_votes spv "
              + "WHERE spv.sticker_id = 1 GROUP BY spv.option_index";

      List<Issue> issues = runMissing(sql, meta);

      assertThat(issues)
          .noneMatch(
              i ->
                  i.type() == IssueType.MISSING_GROUP_BY_INDEX
                      && "option_index".equals(i.column()));
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  Improvement 3: LIKE operator skip
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Improvement 3: LIKE operator skip for missing index")
  class LikeOperatorSkip {

    @Test
    @DisplayName("LIKE column does not produce missing-index warning")
    void likeColumn_noMissingIndexWarning() {
      IndexMetadata meta = metadata(pk("users", "id"));

      String sql = "SELECT * FROM users WHERE name LIKE '%john%'";

      List<Issue> issues = runMissing(sql, meta);

      // LIKE columns should be skipped entirely by MissingIndexDetector
      assertThat(issues)
          .noneMatch(i -> i.type() == IssueType.MISSING_WHERE_INDEX && "name".equals(i.column()));
    }

    @Test
    @DisplayName("Non-LIKE column still produces missing-index warning")
    void nonLikeColumn_stillFlagsMissingIndex() {
      IndexMetadata meta = metadata(pk("users", "id"));

      String sql = "SELECT * FROM users WHERE email = 'test@example.com'";

      List<Issue> issues = runMissing(sql, meta);

      assertThat(issues)
          .anyMatch(i -> i.type() == IssueType.MISSING_WHERE_INDEX && "email".equals(i.column()));
    }

    @Test
    @DisplayName("Mixed LIKE and equality: only equality produces warning")
    void mixedLikeAndEquality_onlyEqualityWarns() {
      IndexMetadata meta = metadata(pk("users", "id"));

      String sql = "SELECT * FROM users WHERE name LIKE '%john%' AND status = 'active'";

      List<Issue> issues = runMissing(sql, meta);

      // LIKE column skipped
      assertThat(issues)
          .noneMatch(i -> i.type() == IssueType.MISSING_WHERE_INDEX && "name".equals(i.column()));
      // Equality column still checked (status is low cardinality name, so it will be INFO)
      // The key point is LIKE is not flagged
    }

    @Test
    @DisplayName("NOT LIKE also skipped")
    void notLike_alsoSkipped() {
      IndexMetadata meta = metadata(pk("products", "id"));

      String sql = "SELECT * FROM products WHERE description NOT LIKE '%discontinued%'";

      List<Issue> issues = runMissing(sql, meta);

      assertThat(issues)
          .noneMatch(
              i -> i.type() == IssueType.MISSING_WHERE_INDEX && "description".equals(i.column()));
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  Improvement 4: Temporal N+1 detection (replaces stack-trace heuristics)
  //
  //  The temporal algorithm classifies based on POSITION, not stack traces.
  //  Interceptor/filter queries that run once per request are naturally
  //  spread out (high medianGap -> INFO), while true N+1 from lazy
  //  loading loops are consecutive (low medianGap -> ERROR).
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Improvement 4: Temporal consecutive detection for interceptor/filter patterns")
  class TemporalInterceptorDetection {

    /** Helper: creates a filler query at the given index (unique SQL). */
    private static QueryRecord filler(int index) {
      return new QueryRecord(
          "SELECT * FROM filler_" + index + " WHERE id = 1", 0L, System.currentTimeMillis(), "");
    }

    /** Helper: builds a query list with target queries at specific positions. */
    private static List<QueryRecord> buildQueryList(
        String targetSql, List<Integer> positions, int totalSize, String stackTrace) {
      List<QueryRecord> queries = new ArrayList<>();
      for (int i = 0; i < totalSize; i++) {
        if (positions.contains(i)) {
          queries.add(query(targetSql.replace("{pos}", String.valueOf(i)), stackTrace));
        } else {
          queries.add(filler(i));
        }
      }
      return queries;
    }

    @Test
    @DisplayName("Interceptor queries spread across requests -> INFO (high medianGap)")
    void interceptorQueriesSpreadOut_shouldBeInfo() {
      String stackTrace = "com.example.interceptor.SuspensionCheckInterceptor.preHandle:30";

      // 10 interceptor queries, each separated by 20+ other queries
      List<Integer> positions = List.of(5, 30, 55, 80, 105, 130, 155, 180, 205, 230);
      List<QueryRecord> queries =
          buildQueryList("SELECT * FROM users WHERE id = {pos}", positions, 240, stackTrace);

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    @Test
    @DisplayName("Filter queries spread across requests -> INFO (high medianGap)")
    void filterQueriesSpreadOut_shouldBeInfo() {
      String stackTrace = "com.example.filter.AuthFilter.doFilterInternal:25";

      List<Integer> positions = List.of(0, 25, 50, 75, 100, 125, 150, 175, 200, 225);
      List<QueryRecord> queries =
          buildQueryList(
              "SELECT * FROM tokens WHERE value = 'tok{pos}'", positions, 230, stackTrace);

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    @Test
    @DisplayName("Normal N+1 (consecutive) reports as INFO with Hibernate-level reference")
    void normalNPlusOne_consecutive_shouldBeInfo() {
      String stackTrace = "com.example.service.OrderService.listOrders:70";

      List<QueryRecord> queries = repeat("SELECT * FROM members WHERE id = {id}", 10, stackTrace);

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
      assertThat(issues.get(0).suggestion()).contains("Hibernate-level");
    }

    @Test
    @DisplayName("Rate limit filter queries spread across requests -> INFO")
    void rateLimitFilter_spreadOut_shouldBeInfo() {
      String stackTrace = "com.example.filter.RateLimitFilter.doFilter:40";

      List<Integer> positions = List.of(0, 40, 80, 120, 160, 200, 240, 280, 320, 360);
      List<QueryRecord> queries =
          buildQueryList(
              "SELECT * FROM rate_limits WHERE ip = '192.168.1.{pos}'", positions, 370, stackTrace);

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  SQL-level detector: all patterns are INFO
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("SQL-level detector: all patterns are INFO")
  class SqlLevelAlwaysInfo {

    private static QueryRecord filler(int index) {
      return new QueryRecord(
          "SELECT * FROM filler_" + index + " WHERE id = 1", 0L, System.currentTimeMillis(), "");
    }

    private static List<QueryRecord> buildQueryList(
        String targetSql, List<Integer> positions, int totalSize, String stackTrace) {
      List<QueryRecord> queries = new ArrayList<>();
      for (int i = 0; i < totalSize; i++) {
        if (positions.contains(i)) {
          queries.add(query(targetSql.replace("{pos}", String.valueOf(i)), stackTrace));
        } else {
          queries.add(filler(i));
        }
      }
      return queries;
    }

    @Test
    @DisplayName("Consecutive -> INFO")
    void consecutive_shouldBeInfo() {
      List<QueryRecord> queries = repeat("SELECT * FROM members WHERE id = {id}", 5, "some.trace");

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    @Test
    @DisplayName("Gap of 2 -> INFO")
    void gapOf2_shouldBeInfo() {
      List<Integer> positions = List.of(0, 3, 6, 9);
      List<QueryRecord> queries =
          buildQueryList("SELECT * FROM members WHERE id = {pos}", positions, 10, "some.trace");

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    @Test
    @DisplayName("Gap of 3 -> INFO")
    void gapOf3_shouldBeInfo() {
      List<Integer> positions = List.of(0, 4, 8, 12);
      List<QueryRecord> queries =
          buildQueryList("SELECT * FROM members WHERE id = {pos}", positions, 13, "some.trace");

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    @Test
    @DisplayName("Gap of 5 -> INFO")
    void gapOf5_shouldBeInfo() {
      List<Integer> positions = List.of(0, 6, 12, 18);
      List<QueryRecord> queries =
          buildQueryList("SELECT * FROM members WHERE id = {pos}", positions, 19, "some.trace");

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    @Test
    @DisplayName("Gap of 6 -> INFO")
    void gapOf6_shouldBeInfo() {
      List<Integer> positions = List.of(0, 7, 14, 21);
      List<QueryRecord> queries =
          buildQueryList("SELECT * FROM members WHERE id = {pos}", positions, 22, "some.trace");

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    @Test
    @DisplayName("Cache pattern -> INFO (no downgrade needed)")
    void cachePattern_shouldBeInfo() {
      String cacheTrace = "com.example.cache.ProductCacheService.loadFromDb:30";

      List<QueryRecord> queries =
          repeat("SELECT * FROM products WHERE category_id = {id}", 10, cacheTrace);

      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }
  }
}
