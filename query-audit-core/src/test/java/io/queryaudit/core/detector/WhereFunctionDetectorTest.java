package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class WhereFunctionDetectorTest {

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());
  private final WhereFunctionDetector detector = new WhereFunctionDetector();

  // ── Improvement 1: Driving table vs lookup table in JOINs ────────

  @Nested
  class JoinDrivingVsLookupTable {

    @Test
    void leftJoin_functionOnDrivingTableColumn_notFlagged() {
      // COALESCE wraps rm.last_read_message_id which is on the driving table (room_members).
      // The lookup is on m.id (messages table), which is NOT wrapped -> no issue.
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM room_members rm "
                          + "LEFT JOIN messages m ON m.id > COALESCE(rm.last_read_message_id, 0)")),
              EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    void leftJoin_functionOnLookupTableColumn_isFlagged() {
      // LOWER wraps m.title which is on the lookup table (messages) -> should flag
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM room_members rm "
                          + "LEFT JOIN messages m ON LOWER(m.title) = rm.search_key")),
              EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).column()).isEqualTo("title");
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    }

    @Test
    void rightJoin_functionOnDrivingTableColumn_notFlagged() {
      // RIGHT JOIN: the FROM table (orders) is the lookup table.
      // Function wraps u.name which is the driving table (users) -> not flagged
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM orders o "
                          + "RIGHT JOIN users u ON o.user_id = LOWER(u.name)")),
              EMPTY_INDEX);

      // u is the driving table in RIGHT JOIN, so LOWER(u.name) is on driving side -> not flagged
      assertThat(issues).isEmpty();
    }

    @Test
    void rightJoin_functionOnLookupTableColumn_isFlagged() {
      // RIGHT JOIN: the FROM table (orders) is the lookup table.
      // YEAR wraps o.created_at which is the lookup table -> flagged
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM orders o "
                          + "RIGHT JOIN users u ON YEAR(o.created_at) = u.join_year")),
              EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).column()).isEqualTo("created_at");
    }

    @Test
    void innerJoin_functionOnEitherSide_isFlagged() {
      // INNER JOIN: both sides need index access -> flag both
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM orders o "
                          + "INNER JOIN users u ON LOWER(o.email) = LOWER(u.email)")),
              EMPTY_INDEX);

      assertThat(issues).hasSize(2);
    }

    @Test
    void joinWithoutExplicitType_functionIsFlagged() {
      // Plain JOIN (implicit INNER) -> flag
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record("SELECT * FROM orders o " + "JOIN users u ON LOWER(u.email) = o.email")),
              EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).column()).isEqualTo("email");
    }

    @Test
    void leftJoin_functionOnDrivingTable_withAlias_notFlagged() {
      // Same as first test but with explicit aliases to verify alias resolution
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM room_members AS rm "
                          + "LEFT JOIN messages AS m ON m.id > COALESCE(rm.last_read_message_id, 0)")),
              EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }

  // ── Improvement 2: Function on comparison value side ─────────────

  @Nested
  class ComparisonValueSide {

    @Test
    void functionOnComparisonValueSide_notFlagged() {
      // WHERE m.id > COALESCE(rm.last_read, 0)
      // Index is on m.id, function wraps rm.last_read (comparison value) -> no issue
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM messages m WHERE m.id > COALESCE(rm.last_read, 0)")),
              EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    void functionOnIndexedColumnSide_isFlagged() {
      // WHERE DATE(created_at) = '2024-01-01'
      // Function wraps the indexed column -> should flag
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM orders WHERE DATE(created_at) = '2024-01-01'")),
              EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).column()).isEqualTo("created_at");
    }

    @Test
    void functionOnBothSides_bothFlagged() {
      // Both sides have functions -> flag both
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM orders WHERE LOWER(email) = LOWER(name)")),
              EMPTY_INDEX);

      // LOWER(email) is on the left (flagged), LOWER(name) is on the right
      // Since left has func and right also has func, both should be flagged
      assertThat(issues).hasSize(2);
    }

    @Test
    void plainColumnComparedToFunctionWrappedValue_notFlagged() {
      // WHERE status = COALESCE(?, 'active')
      // Plain column compared to function-wrapped literal -> not flagged
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE status = COALESCE(?, 'active')")),
              EMPTY_INDEX);

      // The COALESCE wraps a parameter/literal, not an indexed column -> skip
      // Actually, '?' is not a column name, so isKeyword check handles it
      assertThat(issues).isEmpty();
    }

    @Test
    void multipleConditions_onlyFlagsIndexedColumnSide() {
      // WHERE m.id > COALESCE(rm.last_read, 0) AND LOWER(m.title) = 'test'
      // First condition: COALESCE on comparison value -> skip
      // Second condition: LOWER on indexed column -> flag
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM messages m "
                          + "WHERE m.id > COALESCE(rm.last_read, 0) AND LOWER(m.title) = 'test'")),
              EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).column()).isEqualTo("title");
      assertThat(issues.get(0).detail()).contains("LOWER");
    }
  }

  // ── Improvement 3: Function-specific suggestions ─────────────────

  @Nested
  class FunctionSpecificSuggestions {

    @Test
    void dateFunctionSuggestion_usesRangeCondition() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM orders WHERE DATE(created_at) = '2024-01-01'")),
              EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).suggestion()).contains("range condition");
    }

    @Test
    void lowerFunctionSuggestion_suggestsFunctionalIndex() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE LOWER(email) = 'test@example.com'")),
              EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).suggestion()).contains("collation");
      assertThat(issues.get(0).suggestion()).contains("functional index");
    }

    @Test
    void upperFunctionSuggestion_suggestsFunctionalIndex() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE UPPER(name) = 'JOHN'")), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).suggestion()).contains("functional index");
    }

    @Test
    void yearFunctionSuggestion_usesRangeCondition() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM orders WHERE YEAR(created_at) = 2024")), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).suggestion()).contains("range condition");
    }

    @Test
    void coalesceFunctionSuggestion_notFlaggedAsIndexSafe() {
      // COALESCE is index-safe in MySQL 8.0.13+ and should not be flagged
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE COALESCE(nickname, 'unknown') = 'test'")),
              EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    void ifnullFunctionSuggestion_notFlaggedAsIndexSafe() {
      // IFNULL is index-safe in MySQL 8.0.13+ and should not be flagged
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE IFNULL(score, 0) > 100")), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    void md5FunctionSuggestion_suggestsPreComputedHash() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE MD5(email) = 'abc123'")), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).suggestion()).contains("pre-computed hash");
    }

    @Test
    void jsonExtractFunctionSuggestion_suggestsGeneratedColumn() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM events WHERE JSON_EXTRACT(data, '$.type') = 'click'")),
              EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).suggestion()).contains("generated column");
    }
  }

  // ── Regression: existing behavior still works ────────────────────

  @Nested
  class Regression {

    @Test
    void detectsDateFunction() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM orders WHERE DATE(created_at) = '2024-01-01'")),
              EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.WHERE_FUNCTION);
      assertThat(issues.get(0).column()).isEqualTo("created_at");
    }

    @Test
    void doesNotDetectPlainWhere() {
      List<Issue> issues =
          detector.evaluate(List.of(record("SELECT * FROM users WHERE id = 1")), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    void detectsMultipleFunctions() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM users WHERE LOWER(email) = 'test' AND YEAR(created_at) = 2024")),
              EMPTY_INDEX);

      assertThat(issues).hasSize(2);
    }

    @Test
    void deduplicatesSamePattern() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record("SELECT * FROM orders WHERE DATE(created_at) = '2024-01-01'"),
                  record("SELECT * FROM orders WHERE DATE(created_at) = '2024-02-01'")),
              EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }
  }

  // ── Real-world query patterns ────────────────────────────────────

  @Nested
  class RealWorldPatterns {

    @Test
    void unreadCountQuery_coalesceOnDrivingSide_notFlagged() {
      // Real query from Jimotoku: counting unread messages
      String sql =
          "SELECT COUNT(*) FROM room_members rm "
              + "LEFT JOIN messages m ON m.room_id = rm.room_id "
              + "AND m.id > COALESCE(rm.last_read_message_id, 0) "
              + "WHERE rm.user_id = ?";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // COALESCE is on rm.last_read_message_id (driving table in LEFT JOIN)
      // The index lookup is on m.id and m.room_id (lookup table) which are NOT wrapped
      // WHERE condition has no function -> no issues expected
      assertThat(issues.stream().filter(i -> i.detail().contains("COALESCE"))).isEmpty();
    }

    @Test
    void leftJoin_functionOnLookupColumn_isFlagged() {
      String sql =
          "SELECT * FROM users u " + "LEFT JOIN orders o ON DATE(o.created_at) = u.join_date";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).column()).isEqualTo("created_at");
    }
  }
}
