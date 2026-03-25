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

/** Tests for false positive fixes in MissingIndexDetector and RedundantFilterDetector. */
class FalsePositiveFixTest {

  // ═══════════════════════════════════════════════════════════════
  //  Helpers
  // ═══════════════════════════════════════════════════════════════

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private static IndexInfo idx(String table, String indexName, String column, int seq) {
    return new IndexInfo(table, indexName, column, seq, true, 100);
  }

  private static IndexInfo pk(String table, String column) {
    return new IndexInfo(table, "PRIMARY", column, 1, false, 1000);
  }

  private static IndexMetadata metadata(IndexInfo... infos) {
    Map<String, List<IndexInfo>> map = new HashMap<>();
    for (IndexInfo info : infos) {
      map.computeIfAbsent(info.tableName(), k -> new ArrayList<>()).add(info);
    }
    return new IndexMetadata(map);
  }

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  // ═══════════════════════════════════════════════════════════════
  //  Fix 1: Soft-delete deleted_at suppression
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Fix 1: deleted_at suppressed when other WHERE columns exist")
  class SoftDeleteSuppression {

    @Test
    @DisplayName("deleted_at IS NULL suppressed when another unindexed column exists in WHERE")
    void deletedAtSuppressed_withOtherUnindexedColumn() {
      // room_id has no index, deleted_at has no index
      // But deleted_at should still be suppressed because it's not the sole filter
      IndexMetadata meta = metadata(pk("messages", "id"));

      String sql = "SELECT * FROM messages WHERE deleted_at IS NULL AND room_id = ?";

      List<Issue> issues = new MissingIndexDetector().evaluate(List.of(record(sql)), meta);

      // deleted_at should be completely suppressed (not even INFO)
      assertThat(issues)
          .noneMatch(
              i -> i.type() == IssueType.MISSING_WHERE_INDEX && "deleted_at".equals(i.column()));
      // room_id should still be flagged
      assertThat(issues)
          .anyMatch(i -> i.type() == IssueType.MISSING_WHERE_INDEX && "room_id".equals(i.column()));
    }

    @Test
    @DisplayName("deleted_at IS NULL suppressed when another indexed column exists in WHERE")
    void deletedAtSuppressed_withOtherIndexedColumn() {
      IndexMetadata meta =
          metadata(pk("messages", "id"), idx("messages", "idx_room", "room_id", 1));

      String sql = "SELECT * FROM messages WHERE deleted_at IS NULL AND room_id = ?";

      List<Issue> issues = new MissingIndexDetector().evaluate(List.of(record(sql)), meta);

      // deleted_at should be completely suppressed
      assertThat(issues)
          .noneMatch(
              i -> i.type() == IssueType.MISSING_WHERE_INDEX && "deleted_at".equals(i.column()));
    }

    @Test
    @DisplayName("deleted_at IS NULL reported as INFO when it is the ONLY filter column")
    void deletedAtInfo_whenSoleColumn() {
      IndexMetadata meta = metadata(pk("users", "id"));

      String sql = "SELECT count(*) FROM users WHERE deleted_at IS NULL";

      List<Issue> issues = new MissingIndexDetector().evaluate(List.of(record(sql)), meta);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.MISSING_WHERE_INDEX);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
      assertThat(issues.get(0).column()).isEqualTo("deleted_at");
    }

    @Test
    @DisplayName("Real Jimotoku query: deleted_at + room_id + id conditions suppress deleted_at")
    void realQuery_multipleConditions_suppressesDeletedAt() {
      IndexMetadata meta =
          metadata(
              pk("messages", "id"),
              idx("messages", "idx_room_id", "room_id", 1),
              idx("messages", "idx_room_id", "id", 2));

      String sql =
          "SELECT m1_0.id FROM messages m1_0 "
              + "WHERE m1_0.deleted_at IS NULL AND m1_0.room_id = ? "
              + "AND m1_0.id < ? ORDER BY m1_0.id DESC LIMIT ?";

      List<Issue> issues = new MissingIndexDetector().evaluate(List.of(record(sql)), meta);

      assertThat(issues)
          .noneMatch(
              i -> i.type() == IssueType.MISSING_WHERE_INDEX && "deleted_at".equals(i.column()));
    }

    @Test
    @DisplayName("Aliased soft-delete: u1_0.deleted_at IS NULL with other filter columns")
    void aliasedSoftDelete_withOtherColumns() {
      IndexMetadata meta =
          metadata(
              pk("users", "id"),
              idx("users", "uq_nick_disc", "nickname", 1),
              idx("users", "uq_nick_disc", "discriminator", 2));

      String sql =
          "SELECT u1_0.id FROM users u1_0 "
              + "WHERE u1_0.deleted_at IS NULL AND u1_0.nickname = ? "
              + "AND u1_0.discriminator = ? LIMIT ?";

      List<Issue> issues = new MissingIndexDetector().evaluate(List.of(record(sql)), meta);

      assertThat(issues)
          .noneMatch(
              i -> i.type() == IssueType.MISSING_WHERE_INDEX && "deleted_at".equals(i.column()));
    }

    @Test
    @DisplayName("unsuspended_at IS NULL (soft-delete pattern) suppressed with other columns")
    void unsuspendedAtSuppressed_withOtherColumns() {
      IndexMetadata meta =
          metadata(pk("user_suspensions", "id"), idx("user_suspensions", "idx_user", "user_id", 1));

      String sql =
          "SELECT us1_0.id FROM user_suspensions us1_0 "
              + "WHERE us1_0.user_id = ? AND us1_0.unsuspended_at IS NULL";

      List<Issue> issues = new MissingIndexDetector().evaluate(List.of(record(sql)), meta);

      assertThat(issues)
          .noneMatch(
              i ->
                  i.type() == IssueType.MISSING_WHERE_INDEX && "unsuspended_at".equals(i.column()));
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  Fix 1B: Low cardinality suffix patterns (_type, _status)
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Fix 1B: _type and _status suffixes recognized as low cardinality")
  class LowCardinalitySuffixPatterns {

    @Test
    @DisplayName("message_type recognized as low cardinality, downgraded to INFO")
    void messageType_lowCardinality() {
      IndexMetadata meta = metadata(pk("messages", "id"));

      String sql = "SELECT * FROM messages WHERE message_type = 'TEXT'";

      List<Issue> issues = new MissingIndexDetector().evaluate(List.of(record(sql)), meta);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
      assertThat(issues.get(0).column()).isEqualTo("message_type");
    }

    @Test
    @DisplayName("room_type recognized as low cardinality")
    void roomType_lowCardinality() {
      IndexMetadata meta = metadata(pk("rooms", "id"));

      String sql = "SELECT * FROM rooms WHERE room_type = 'LIGHTNING'";

      List<Issue> issues = new MissingIndexDetector().evaluate(List.of(record(sql)), meta);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
      assertThat(issues.get(0).column()).isEqualTo("room_type");
    }

    @Test
    @DisplayName("order_status recognized as low cardinality")
    void orderStatus_lowCardinality() {
      IndexMetadata meta = metadata(pk("orders", "id"));

      String sql = "SELECT * FROM orders WHERE order_status = 'PENDING'";

      List<Issue> issues = new MissingIndexDetector().evaluate(List.of(record(sql)), meta);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
      assertThat(issues.get(0).column()).isEqualTo("order_status");
    }

    @Test
    @DisplayName("_type column suppressed when another indexed column exists")
    void messageType_suppressed_withOtherIndexedColumn() {
      IndexMetadata meta =
          metadata(pk("messages", "id"), idx("messages", "idx_room", "room_id", 1));

      String sql = "SELECT * FROM messages WHERE room_id = ? AND message_type = 'TEXT'";

      List<Issue> issues = new MissingIndexDetector().evaluate(List.of(record(sql)), meta);

      // message_type should be suppressed entirely (other indexed column narrows scan)
      assertThat(issues)
          .noneMatch(
              i -> i.type() == IssueType.MISSING_WHERE_INDEX && "message_type".equals(i.column()));
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  Fix 2A: RedundantFilter table misattribution
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Fix 2A: RedundantFilter resolves correct table via aliases")
  class RedundantFilterTableResolution {

    @Test
    @DisplayName("expires_at duplicate from rooms table not misattributed to room_members")
    void expiresAt_notMisattributedToRoomMembers() {
      // Simulates a JOIN query where expires_at belongs to rooms (alias r1_0),
      // not room_members (the FROM table)
      String sql =
          "SELECT rm1_0.id FROM room_members rm1_0 "
              + "JOIN rooms r1_0 ON rm1_0.room_id = r1_0.id "
              + "WHERE r1_0.expires_at IS NULL AND r1_0.expires_at IS NULL";

      RedundantFilterDetector detector = new RedundantFilterDetector();
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // Should report rooms, not room_members
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).table()).isEqualTo("rooms");
      assertThat(issues.get(0).column()).isEqualTo("expires_at");
    }

    @Test
    @DisplayName("Hibernate alias resolution: m1_0 maps to messages in FROM clause")
    void hibernateAlias_mapsToFromTable() {
      String sql =
          "SELECT m1_0.id FROM messages m1_0 "
              + "WHERE m1_0.deleted_at IS NULL AND m1_0.room_id = ? "
              + "AND m1_0.deleted_at IS NULL";

      RedundantFilterDetector detector = new RedundantFilterDetector();
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).table()).isEqualTo("messages");
      assertThat(issues.get(0).column()).isEqualTo("deleted_at");
    }

    @Test
    @DisplayName("Different tables with same column name: no false redundancy")
    void differentTables_sameColumnName_noFalseRedundancy() {
      // r1_0.id and m1_0.id belong to different tables
      String sql =
          "SELECT m1_0.id FROM messages m1_0 "
              + "JOIN rooms r1_0 ON m1_0.room_id = r1_0.id "
              + "WHERE m1_0.id = ? AND r1_0.id = ?";

      RedundantFilterDetector detector = new RedundantFilterDetector();
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // Should NOT report as redundant: different tables
      assertThat(issues).isEmpty();
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  Fix 2B: RedundantFilter cross-table/subquery false positives
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Fix 2B: RedundantFilter ignores cross-table duplicate column names")
  class RedundantFilterCrossTable {

    @Test
    @DisplayName("story_trading.id not flagged when id appears in different table contexts")
    void storyTradingId_notFalsePositive() {
      // id = ? in WHERE for story_trading, and st.id is also referenced in JOIN
      // but JOIN ON conditions should not contribute to WHERE duplicates
      String sql =
          "SELECT st1_0.id FROM story_trading st1_0 "
              + "JOIN stories s1_0 ON st1_0.story_id = s1_0.id "
              + "WHERE st1_0.id = ? AND s1_0.id = ?";

      RedundantFilterDetector detector = new RedundantFilterDetector();
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // st1_0.id and s1_0.id are different tables -> no redundancy
      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("True redundancy on same table still detected")
    void trueRedundancy_sameTable_stillDetected() {
      String sql =
          "SELECT st1_0.id FROM story_trading st1_0 "
              + "WHERE st1_0.status = 'ACTIVE' AND st1_0.status = 'ACTIVE'";

      RedundantFilterDetector detector = new RedundantFilterDetector();
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).column()).isEqualTo("status");
    }

    @Test
    @DisplayName("Unaliased columns on single-table query still detected")
    void unaliasedSingleTable_stillDetected() {
      String sql =
          "SELECT * FROM users WHERE deleted_at IS NULL AND name = 'test' AND deleted_at IS NULL";

      RedundantFilterDetector detector = new RedundantFilterDetector();
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).column()).isEqualTo("deleted_at");
    }
  }

  // ═══════════════════════════════════════════════════════════════
  //  Fix 3: Bidirectional OR verification
  // ═══════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Fix 3: Bidirectional OR patterns not flagged")
  class BidirectionalOrVerification {

    @Test
    @DisplayName("direct_chat_requests: requester_id and receiver_id not flagged as redundant")
    void directChatRequests_bidirectionalOr_notFlagged() {
      String sql =
          "SELECT dcr1_0.id,dcr1_0.status "
              + "FROM direct_chat_requests dcr1_0 "
              + "WHERE ((dcr1_0.requester_id = ? AND dcr1_0.receiver_id = ?) "
              + "OR (dcr1_0.requester_id = ? AND dcr1_0.receiver_id = ?)) "
              + "AND dcr1_0.status = 'ACCEPTED'";

      RedundantFilterDetector detector = new RedundantFilterDetector();
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // requester_id and receiver_id appear in different OR branches -> not redundant
      assertThat(issues).noneMatch(i -> "requester_id".equals(i.column()));
      assertThat(issues).noneMatch(i -> "receiver_id".equals(i.column()));
    }

    @Test
    @DisplayName("connections: user_id_small and user_id_large in OR branches not flagged")
    void connections_bidirectionalOr_notFlagged() {
      String sql =
          "SELECT * FROM connections "
              + "WHERE (user_id_small = ? AND user_id_large = ?) "
              + "OR (user_id_small = ? AND user_id_large = ?)";

      RedundantFilterDetector detector = new RedundantFilterDetector();
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("sender_id/receiver_id bidirectional pattern not flagged")
    void senderReceiver_bidirectional_notFlagged() {
      String sql =
          "SELECT * FROM direct_chat "
              + "WHERE (sender_id = ? AND receiver_id = ?) "
              + "OR (sender_id = ? AND receiver_id = ?)";

      RedundantFilterDetector detector = new RedundantFilterDetector();
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }
}
