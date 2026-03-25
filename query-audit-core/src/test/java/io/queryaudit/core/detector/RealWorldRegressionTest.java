package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.ColumnReference;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Regression tests using REAL SQL queries captured from the Jimotoku project's Hibernate-generated
 * output. These tests verify that query-audit detectors produce accurate results on
 * production-quality SQL patterns.
 */
class RealWorldRegressionTest {

  // ── Helper methods ──────────────────────────────────────────────────

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private static QueryRecord recordWithStack(String sql, String stackTrace) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), stackTrace);
  }

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryAuditAnalyzer defaultAnalyzer() {
    return new QueryAuditAnalyzer(QueryAuditConfig.defaults(), Collections.emptyList());
  }

  // ═══════════════════════════════════════════════════════════════════
  // A. Hibernate-generated queries from Jimotoku test output
  //    Verify normalize(), extractTableNames(), extractWhereColumns(),
  //    and no NPE/crash on any query.
  // ═══════════════════════════════════════════════════════════════════

  @Nested
  class HibernateGeneratedQueries {

    // --- User-related queries ---

    @Test
    void userLookupByNicknameAndDiscriminator() {
      String sql =
          "select u1_0.id from users u1_0 "
              + "where u1_0.deleted_at IS NULL and u1_0.nickname=? and u1_0.discriminator=? limit ?";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("users");
      List<ColumnReference> cols = SqlParser.extractWhereColumns(sql);
      assertThat(cols)
          .extracting(ColumnReference::columnName)
          .contains("deleted_at", "nickname", "discriminator");
    }

    @Test
    void selectAllUsersNative() {
      String sql = "SELECT * FROM users WHERE oauth_provider = ? AND oauth_sub = ?";
      assertNoCrash(sql);
      assertThat(SqlParser.hasSelectAll(sql)).isTrue();
      assertThat(SqlParser.extractTableNames(sql)).contains("users");
    }

    @Test
    void selectAllUsersByIdNative() {
      String sql = "SELECT * FROM users WHERE id = ?";
      assertNoCrash(sql);
      assertThat(SqlParser.hasSelectAll(sql)).isTrue();
    }

    @Test
    void userCountWhereDeletedAtIsNull() {
      String sql = "select count(*) from users u1_0 where u1_0.deleted_at IS NULL";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("users");
    }

    @Test
    void softDeletedUsersSelect() {
      String sql = "SELECT * FROM users WHERE deleted_at IS NOT NULL AND deleted_at < ?";
      assertNoCrash(sql);
      assertThat(SqlParser.hasSelectAll(sql)).isTrue();
      List<ColumnReference> cols = SqlParser.extractWhereColumns(sql);
      assertThat(cols).extracting(ColumnReference::columnName).contains("deleted_at");
    }

    // --- User suspension queries ---

    @Test
    void userSuspensionLookup() {
      String sql =
          "select us1_0.id,us1_0.is_permanent,us1_0.reason,us1_0.report_id,"
              + "us1_0.suspended_at,us1_0.suspended_by,us1_0.suspended_until,"
              + "us1_0.unsuspended_at,us1_0.unsuspended_by,us1_0.user_id "
              + "from user_suspensions us1_0 "
              + "where us1_0.user_id=? and us1_0.unsuspended_at is null "
              + "and (us1_0.is_permanent=1 or us1_0.suspended_until>?) "
              + "order by us1_0.suspended_at desc";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("user_suspensions");
      List<ColumnReference> cols = SqlParser.extractWhereColumns(sql);
      assertThat(cols)
          .extracting(ColumnReference::columnName)
          .contains("user_id", "unsuspended_at", "is_permanent", "suspended_until");
      // Contains OR
      assertThat(SqlParser.countOrConditions(sql)).isEqualTo(1);
    }

    // --- User location queries ---

    @Test
    void userLocationCount() {
      String sql = "select count(ul1_0.user_id)>0 from user_location ul1_0 where ul1_0.user_id=?";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("user_location");
    }

    // --- User blocks queries ---

    @Test
    void userBlocksCount() {
      String sql =
          "select count(*) from user_blocks ub1_0 where (ub1_0.blocked_id,ub1_0.blocker_id)=(?,?)";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("user_blocks");
    }

    // --- Room member queries ---

    @Test
    void roomMemberCount() {
      String sql =
          "select count(*) from room_members rm1_0 where rm1_0.room_id=? "
              + "and rm1_0.role in ('OWNER','ADMIN','MEMBER')";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("room_members");
    }

    @Test
    void roomMemberMaxPinOrder() {
      String sql =
          "select max(rm1_0.pin_order) from room_members rm1_0 "
              + "where rm1_0.user_id=? and rm1_0.is_pinned=1";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("room_members");
    }

    @Test
    void roomMemberSelectByUserAndRoomIds() {
      String sql =
          "select m1_0.room_id,m1_0.user_id,m1_0.banned_at,m1_0.banned_by,"
              + "m1_0.display_profile_image_url,m1_0.is_pinned,m1_0.joined_at,"
              + "m1_0.last_accessible_message_id,m1_0.last_read_message_id,"
              + "m1_0.left_at,m1_0.nickname_adjective_key,m1_0.nickname_noun_key,"
              + "m1_0.pin_order,m1_0.role,m1_0.sort_order "
              + "from room_members m1_0 where m1_0.user_id=? and m1_0.room_id in (?,?)";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("room_members");
    }

    // --- Message queries (complex JOINs) ---

    @Test
    void messageWithLeftJoinsAndSubquery() {
      String sql =
          "select count(m1_0.id) from messages m1_0 "
              + "left join user_blocks ub1_0 on m1_0.sender_id=ub1_0.blocked_id and ub1_0.blocker_id=? "
              + "where m1_0.deleted_at IS NULL and m1_0.room_id=? and m1_0.deleted_at is null "
              + "and m1_0.message_type<>'SYSTEM' and m1_0.sender_id<>? "
              + "and m1_0.id>(select coalesce(rm1_0.last_read_message_id,0) from room_members rm1_0 "
              + "where rm1_0.room_id=? and rm1_0.user_id=?) "
              + "and m1_0.created_at>=(select rm2_0.joined_at from room_members rm2_0 "
              + "where rm2_0.room_id=? and rm2_0.user_id=?) and ub1_0.blocked_id is null";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("messages", "user_blocks");
    }

    @Test
    void messagePaginatedByRoom() {
      String sql =
          "select m1_0.id from messages m1_0 "
              + "left join user_blocks ub1_0 on m1_0.sender_id=ub1_0.blocked_id and ub1_0.blocker_id=? "
              + "where m1_0.deleted_at IS NULL and m1_0.room_id=? and m1_0.id<? "
              + "and (? is null or m1_0.sender_id is null or ub1_0.blocked_id is null) "
              + "order by m1_0.id desc limit ?";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("messages", "user_blocks");
    }

    @Test
    void messageFullDetailWithManyLeftJoins() {
      String sql =
          "select m1_0.id,am1_0.id,am1_0.album_name,am1_0.artist_name,"
              + "am1_0.artwork_url,am1_0.created_at,am1_0.message_id,am1_0.preview_url "
              + "from messages m1_0 "
              + "left join message_apple_music am1_0 on m1_0.id=am1_0.message_id "
              + "left join message_events e1_0 on m1_0.id=e1_0.message_id "
              + "left join message_locations l1_0 on m1_0.id=l1_0.message_id "
              + "left join message_polls p1_0 on m1_0.id=p1_0.message_id "
              + "where m1_0.id=?";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql))
          .contains(
              "messages",
              "message_apple_music",
              "message_events",
              "message_locations",
              "message_polls");
    }

    @Test
    void messageWithSubqueryMaxGroupBy() {
      String sql =
          "select m1_0.id,m1_0.content,m1_0.created_at,m1_0.deleted_at "
              + "from messages m1_0 where m1_0.deleted_at IS NULL "
              + "and m1_0.id in (select max(m2_0.id) from messages m2_0 "
              + "where m2_0.deleted_at IS NULL and m2_0.room_id in (?,?,?) group by m2_0.room_id)";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("messages");
    }

    @Test
    void messageIdWithHashtagJoin() {
      String sql =
          "select m1_0.id from message_hashtags mh1_0 "
              + "join hashtags h1_0 on h1_0.id=mh1_0.hashtag_id "
              + "join messages m1_0 on m1_0.id=mh1_0.message_id "
              + "left join user_blocks ub1_0 on m1_0.sender_id=ub1_0.blocked_id and ub1_0.blocker_id=? "
              + "where h1_0.tag=? and m1_0.room_id=? and m1_0.parent_message_id is null "
              + "and (? is null or m1_0.id<?) "
              + "and (? is null or m1_0.sender_id is null or ub1_0.blocked_id is null) "
              + "order by m1_0.id desc limit ?";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql))
          .contains("message_hashtags", "hashtags", "messages", "user_blocks");
    }

    @Test
    void messageReactionGroupByPopular() {
      String sql =
          "select m1_0.id from messages m1_0 "
              + "left join message_reactions mr1_0 on mr1_0.message_id=m1_0.id "
              + "left join user_blocks ub1_0 on m1_0.sender_id=ub1_0.blocked_id and ub1_0.blocker_id=? "
              + "where m1_0.deleted_at IS NULL and m1_0.room_id=? "
              + "and m1_0.parent_message_id is null and m1_0.created_at>=? "
              + "and (? is null or m1_0.sender_id is null or ub1_0.blocked_id is null) "
              + "group by m1_0.id order by count(mr1_0.id) desc,m1_0.id desc limit ?";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql))
          .contains("messages", "message_reactions", "user_blocks");
    }

    // --- Connection queries ---

    @Test
    void connectionsByUserPair() {
      String sql =
          "select c1_0.id,c1_0.chat_interaction_count,c1_0.chat_score,"
              + "c1_0.created_at,c1_0.last_interaction_at "
              + "from connections c1_0 "
              + "where c1_0.user_id_small=? and c1_0.user_id_large=?";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("connections");
    }

    @Test
    void connectionWithOrCondition() {
      String sql =
          "select c1_0.id,c1_0.level,c1_0.score "
              + "from connections c1_0 "
              + "where (c1_0.user_id_small=? or c1_0.user_id_large=?) "
              + "and c1_0.level>=? order by c1_0.level desc,c1_0.score desc limit ?";
      assertNoCrash(sql);
      assertThat(SqlParser.countOrConditions(sql)).isEqualTo(1);
    }

    @Test
    void connectionSharedLocationGroupBy() {
      String sql =
          "select csl1_0.location_id,count(distinct c1_0.id) "
              + "from connections c1_0 "
              + "join connection_shared_locations csl1_0 on c1_0.id=csl1_0.connection_id "
              + "where (c1_0.user_id_small=? or c1_0.user_id_large=?) "
              + "and csl1_0.location_id in (?,?) and c1_0.level>=1 "
              + "group by csl1_0.location_id";
      assertNoCrash(sql);
      assertThat(SqlParser.extractGroupByColumns(sql))
          .extracting(ColumnReference::columnName)
          .contains("location_id");
    }

    // --- Direct chat request queries ---

    @Test
    void directChatRequestWithJoins() {
      String sql =
          "select dcr1_0.id,dcr1_0.created_at,dcr1_0.expires_at,dcr1_0.message,"
              + "dcr1_0.receiver_id,r2_0.id,r2_0.bio "
              + "from direct_chat_requests dcr1_0 "
              + "join users r1_0 on r1_0.id=dcr1_0.requester_id "
              + "join users r2_0 on r2_0.id=dcr1_0.receiver_id "
              + "where dcr1_0.id=?";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("direct_chat_requests", "users");
    }

    @Test
    void directChatRequestPendingWithComplexOr() {
      String sql =
          "select dcr1_0.id,dcr1_0.status "
              + "from direct_chat_requests dcr1_0 "
              + "where ((dcr1_0.requester_id=? and dcr1_0.receiver_id=?) "
              + "or (dcr1_0.requester_id=? and dcr1_0.receiver_id=?)) "
              + "and dcr1_0.status='ACCEPTED'";
      assertNoCrash(sql);
      assertThat(SqlParser.countOrConditions(sql)).isEqualTo(1);
    }

    // --- Notification queries ---

    @Test
    void notificationUnreadCount() {
      String sql =
          "select count(n1_0.id) from notifications n1_0 "
              + "where n1_0.recipient_id=? and n1_0.is_read=0 and n1_0.deleted_at is null";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("notifications");
      List<ColumnReference> cols = SqlParser.extractWhereColumns(sql);
      assertThat(cols)
          .extracting(ColumnReference::columnName)
          .contains("recipient_id", "is_read", "deleted_at");
    }

    @Test
    void roomNotificationMuteCheck() {
      String sql =
          "select case when count(rnm1_0.id)>0 then 1 else 0 end "
              + "from room_notification_mutes rnm1_0 "
              + "where rnm1_0.user_id=? and rnm1_0.room_id=?";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("room_notification_mutes");
    }

    // --- Story queries ---

    @Test
    void storyLikeCheck() {
      String sql =
          "select case when count(sl1_0.id)>0 then 1 else 0 end "
              + "from story_likes sl1_0 where sl1_0.story_id=? and sl1_0.user_id=?";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("story_likes");
    }

    @Test
    void storyBookmarkCheck() {
      String sql =
          "select case when count(sb1_0.id)>0 then 1 else 0 end "
              + "from story_bookmarks sb1_0 where sb1_0.story_id=? and sb1_0.user_id=?";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("story_bookmarks");
    }

    // --- Hashtag queries ---

    @Test
    void hashtagLikePrefix() {
      String sql =
          "select h1_0.id,h1_0.created_at,h1_0.tag,h1_0.updated_at,h1_0.usage_count "
              + "from hashtags h1_0 where h1_0.tag like concat(?,'%') escape '\\\\' "
              + "order by h1_0.usage_count desc limit ?";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("hashtags");
    }

    @Test
    void hashtagPopular() {
      String sql =
          "select h1_0.id,h1_0.tag,h1_0.usage_count "
              + "from hashtags h1_0 where h1_0.usage_count>0 "
              + "order by h1_0.usage_count desc limit ?";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("hashtags");
    }

    // --- Location queries ---

    @Test
    void locationBySlug() {
      String sql =
          "select l1_0.id,l1_0.created_at,l1_0.image_url,l1_0.name_ja,"
              + "l1_0.name_ko,l1_0.slug_en,l1_0.updated_at "
              + "from locations l1_0 where l1_0.slug_en=?";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("locations");
    }

    @Test
    void userLocationHistoryDistinct() {
      String sql =
          "select distinct ulh1_0.user_id from user_location_history ulh1_0 "
              + "where ulh1_0.location_id=? and ulh1_0.user_id<>? "
              + "and exists(select 1 from user_location_history ulh2_0 "
              + "where ulh2_0.user_id=ulh1_0.user_id and ulh2_0.location_id in (?,?,?))";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("user_location_history");
    }

    // --- Delete/Update queries ---

    @Test
    void deleteWithAlias() {
      String sql =
          "delete mpv1_0 from message_poll_votes mpv1_0 "
              + "where mpv1_0.poll_option_id=? and mpv1_0.user_id=?";
      assertNoCrash(sql);
      // DELETE is not a SELECT query
      assertThat(SqlParser.isSelectQuery(sql)).isFalse();
    }

    @Test
    void deleteReactionWithAlias() {
      String sql =
          "delete mr1_0 from message_reactions mr1_0 "
              + "where mr1_0.message_id=? and mr1_0.user_id=? and mr1_0.emoji=?";
      assertNoCrash(sql);
      assertThat(SqlParser.isSelectQuery(sql)).isFalse();
    }

    @Test
    void insertReadReceipts() {
      String sql =
          "INSERT INTO message_read_receipts (message_id, user_id, read_at) "
              + "SELECT m.id, ?, NOW(6) FROM messages m "
              + "WHERE m.room_id = ? AND m.id <= ? AND m.deleted_at IS NULL "
              + "AND NOT EXISTS (SELECT 1 FROM message_read_receipts mrr "
              + "WHERE mrr.message_id = m.id AND mrr.user_id = ?)";
      assertNoCrash(sql);
    }

    // --- Poll queries ---

    @Test
    void pollVoteCount() {
      String sql =
          "select count(mpv1_0.id) from message_poll_votes mpv1_0 "
              + "join message_poll_options po1_0 on po1_0.id=mpv1_0.poll_option_id "
              + "where po1_0.poll_id=? and mpv1_0.user_id=?";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql))
          .contains("message_poll_votes", "message_poll_options");
    }

    // --- Lightning review queries ---

    @Test
    void lightningReviewLeftJoin() {
      String sql =
          "select lr1_0.id from lightning_review lr1_0 "
              + "left join users r1_0 on r1_0.id=lr1_0.reviewer_id "
              + "where lr1_0.room_id=? and r1_0.id=? limit ?";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("lightning_review", "users");
    }

    // --- Global notification settings ---

    @Test
    void globalNotificationSettingsById() {
      String sql =
          "select gns1_0.user_id,gns1_0.announcement_enabled,gns1_0.created_at "
              + "from global_notification_settings gns1_0 where gns1_0.user_id=?";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("global_notification_settings");
    }

    // --- Message event queries ---

    @Test
    void messageEventBetweenDates() {
      String sql =
          "select m1_0.id,m1_0.content from messages m1_0 "
              + "left join message_events e1_0 on m1_0.id=e1_0.message_id "
              + "where m1_0.deleted_at IS NULL and m1_0.message_type='EVENT' "
              + "and m1_0.deleted_at is null and e1_0.start_time between ? and ?";
      assertNoCrash(sql);
      List<ColumnReference> cols = SqlParser.extractWhereColumns(sql);
      assertThat(cols)
          .extracting(ColumnReference::columnName)
          .contains("deleted_at", "message_type", "start_time");
    }

    // --- Sticker queries ---

    @Test
    void stickerSliderAverage() {
      String sql =
          "select avg(ssr1_0.slider_value) from sticker_slider_responses ssr1_0 "
              + "where ssr1_0.sticker_id=?";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("sticker_slider_responses");
    }

    @Test
    void messageBookmarkCount() {
      String sql = "select count(mb1_0.id) from message_bookmarks mb1_0 where mb1_0.message_id=?";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("message_bookmarks");
    }

    // --- Warning count ---

    @Test
    void userWarningCount() {
      String sql = "select count(uw1_0.id) from user_warnings uw1_0 where uw1_0.user_id=?";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("user_warnings");
    }

    // --- Report count ---

    @Test
    void resolvedReportCount() {
      String sql =
          "select count(mr1_0.id) from message_reports mr1_0 "
              + "where mr1_0.deleted_at IS NULL and mr1_0.reported_user_id=? "
              + "and mr1_0.status='RESOLVED'";
      assertNoCrash(sql);
      assertThat(SqlParser.extractTableNames(sql)).contains("message_reports");
    }

    /**
     * Parameterized test: normalize() and extractTableNames() must never crash on any of these real
     * Hibernate queries.
     */
    @ParameterizedTest
    @ValueSource(
        strings = {
          "delete from global_notification_settings where user_id=?",
          "delete from locations where id=?",
          "DELETE FROM messages WHERE room_id IN (?,?)",
          "delete from notifications where id=?",
          "delete from room_members where room_id=? and user_id=?",
          "delete from room_notification_mutes where id=?",
          "delete from story_media where id=?",
          "delete from user_blocks where blocked_id=? and blocker_id=?",
          "delete from user_devices where id=?",
          "delete from user_location where user_id=?",
          "delete from user_location_history where id=?",
          "insert into rooms (title,type,location_id,owner_id,created_at,updated_at) values (?,?,?,?,?,?)",
          "insert into messages (content,created_at,room_id,sender_id,message_type) values (?,?,?,?,?)",
          "insert into message_reactions (created_at,emoji,message_id,user_id) values (?,?,?,?)",
          "insert into notifications (body,created_at,recipient_id,type,title) values (?,?,?,?,?)",
          "update rooms set last_message_at=?,updated_at=? where id=?",
          "select l1_0.id,l1_0.slug_en from locations l1_0",
          "select gns1_0.user_id from global_notification_settings gns1_0 where gns1_0.user_id in (?)",
          "select c1_0.id from connections c1_0",
          "select csl1_0.id from connection_shared_locations csl1_0",
          "select dcr1_0.id from direct_chat_requests dcr1_0"
        })
    void noExceptionOnAnyRealQuery(String sql) {
      assertThatCode(
              () -> {
                SqlParser.normalize(sql);
                SqlParser.extractTableNames(sql);
                SqlParser.extractWhereColumns(sql);
                SqlParser.extractJoinColumns(sql);
                SqlParser.extractOrderByColumns(sql);
                SqlParser.extractGroupByColumns(sql);
                SqlParser.detectWhereFunctions(sql);
                SqlParser.countOrConditions(sql);
                SqlParser.extractOffsetValue(sql);
                SqlParser.hasSelectAll(sql);
                SqlParser.isSelectQuery(sql);
              })
          .doesNotThrowAnyException();
    }

    private void assertNoCrash(String sql) {
      assertThatCode(
              () -> {
                SqlParser.normalize(sql);
                SqlParser.extractTableNames(sql);
                SqlParser.extractWhereColumns(sql);
                SqlParser.extractJoinColumns(sql);
                SqlParser.extractOrderByColumns(sql);
                SqlParser.extractGroupByColumns(sql);
                SqlParser.detectWhereFunctions(sql);
                SqlParser.countOrConditions(sql);
                SqlParser.extractOffsetValue(sql);
                SqlParser.hasSelectAll(sql);
                SqlParser.isSelectQuery(sql);
              })
          .doesNotThrowAnyException();
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // B. Known false positive scenarios (regression prevention)
  //    Each test replicates a bug that was previously fixed.
  // ═══════════════════════════════════════════════════════════════════

  @Nested
  class KnownFalsePositiveRegression {

    /**
     * Bug #1: Hibernate alias like m1_0 was previously reported as a table name by
     * MissingIndexDetector. After fix, the alias should resolve to the actual table name or be
     * skipped.
     */
    @Test
    void hibernateAlias_shouldNotBeReportedAsTable() {
      String sql = "select m1_0.id from messages m1_0 where m1_0.room_id=? and m1_0.id<?";
      IndexMetadata metadata = createJimotokuIndexMetadata();

      MissingIndexDetector detector = new MissingIndexDetector();
      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      // Should NOT report issues with table "m1_0"
      for (Issue issue : issues) {
        assertThat(issue.table()).isNotEqualTo("m1_0");
      }
      // Should resolve alias to actual table "messages"
      issues.stream()
          .filter(i -> i.table() != null)
          .forEach(i -> assertThat(i.table()).doesNotMatch("[a-z]{1,3}\\d+_\\d+"));
    }

    /**
     * Bug #2: Independent N+1 - per-request interceptor queries are spread across multiple HTTP
     * requests with many other queries between them. Temporal algorithm: high medianGap -> INFO.
     */
    @Test
    void independentNPlusOne_spreadAcrossRequests_shouldBeInfo() {
      NPlusOneDetector detector = new NPlusOneDetector(3);
      String sql = "select us1_0.id from user_suspensions us1_0 where us1_0.user_id=?";

      // Simulate 3 HTTP requests, each with a suspension check + 30 other queries
      List<QueryRecord> queries = new ArrayList<>();
      for (int req = 0; req < 3; req++) {
        // Suspension check at start of each request
        queries.add(recordWithStack(sql, "com.example.UserService.checkSuspension:42"));
        // 30 other queries (the actual request processing)
        for (int j = 0; j < 30; j++) {
          queries.add(record("SELECT * FROM filler_" + req + "_" + j + " WHERE id = 1"));
        }
      }

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    /**
     * Bug #3: Test code N+1 - repeated queries from different test setup calls. In real execution,
     * each test's setup is separated by many test queries. Temporal algorithm: high medianGap ->
     * INFO.
     */
    @Test
    void testCodeNPlusOne_spreadAcrossTests_shouldBeInfo() {
      NPlusOneDetector detector = new NPlusOneDetector(3);
      String sql = "SELECT * FROM users WHERE oauth_provider = ? AND oauth_sub = ?";

      // Simulate 5 test methods, each with a setup query + 20 test queries
      List<QueryRecord> queries = new ArrayList<>();
      for (int test = 0; test < 5; test++) {
        queries.add(recordWithStack(sql, "com.example.TestHelper.cleanupTestData:100"));
        for (int j = 0; j < 20; j++) {
          queries.add(record("SELECT * FROM test_filler_" + test + "_" + j + " WHERE id = 1"));
        }
      }

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    /**
     * Bug #4: Pagination N+1 - consecutive pagination queries (same normalized SQL). Temporal
     * algorithm: consecutive -> medianGap=0 -> ERROR. Pagination is a known edge case handled at
     * the caller level.
     */
    @Test
    void paginationNPlusOne_consecutive_shouldBeInfo() {
      NPlusOneDetector detector = new NPlusOneDetector(3);
      String sql =
          "select m1_0.id from messages m1_0 where m1_0.room_id=? "
              + "order by m1_0.id desc limit ? offset ?";

      List<QueryRecord> queries =
          IntStream.range(0, 5)
              .mapToObj(i -> recordWithStack(sql, "com.example.MessageService.getPage:100"))
              .toList();

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    /**
     * Bug #5: Event handler N+1 - events dispatched at different times. In real execution, each
     * event handling is separated by many queries. Temporal algorithm: high medianGap -> INFO.
     */
    @Test
    void eventHandlerNPlusOne_spreadAcrossEvents_shouldBeInfo() {
      NPlusOneDetector detector = new NPlusOneDetector(3);
      String sql = "select n1_0.id from notifications n1_0 where n1_0.recipient_id=?";

      // Simulate 4 events, each with a notification query + 25 other queries
      List<QueryRecord> queries = new ArrayList<>();
      for (int evt = 0; evt < 4; evt++) {
        queries.add(recordWithStack(sql, "com.example.NotificationEventHandler.onMessageSent:45"));
        for (int j = 0; j < 25; j++) {
          queries.add(record("SELECT * FROM event_filler_" + evt + "_" + j + " WHERE id = 1"));
        }
      }

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    /**
     * Bug #6: Composite index with standalone - user_blocks.blocked_id has its own standalone index
     * idx_blocked, so CompositeIndexDetector should NOT flag it even though it is also part of the
     * PK composite (blocker_id, blocked_id).
     */
    @Test
    void compositeIndex_withStandaloneIndex_shouldNotFlag() {
      String sql = "select count(*) from user_blocks ub1_0 where ub1_0.blocked_id=?";
      IndexMetadata metadata = createJimotokuIndexMetadata();

      CompositeIndexDetector detector = new CompositeIndexDetector();
      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      // blocked_id has standalone idx_blocked, should NOT flag composite leading column issue
      assertThat(issues).isEmpty();
    }

    /**
     * Bug #7: GROUP BY with function expression (COALESCE, COUNT, etc.) should NOT extract the
     * inner column as a GROUP BY column.
     */
    @Test
    void groupByFunction_shouldNotExtractInnerColumn() {
      String sql =
          "select count(distinct c1_0.id) from connections c1_0 "
              + "group by COALESCE(c1_0.level, 0)";
      List<ColumnReference> cols = SqlParser.extractGroupByColumns(sql);
      // Should skip function expressions entirely
      assertThat(cols).isEmpty();
    }

    /** Bug #8: SELECT DISTINCT * should still be detected as SELECT * */
    @Test
    void selectDistinctStar_shouldDetectAsSelectAll() {
      String sql = "SELECT DISTINCT * FROM users WHERE deleted_at IS NULL";
      assertThat(SqlParser.hasSelectAll(sql)).isTrue();
    }

    /** Bug #9: NULL IndexInfo columnName should not cause NPE in CompositeIndexDetector. */
    @Test
    void nullIndexColumnName_shouldNotCauseNPE() {
      String sql = "select u1_0.id from users u1_0 where u1_0.nickname=?";

      // Create metadata with a null column name (functional index scenario)
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "users",
                  List.of(
                      new IndexInfo("users", "PRIMARY", "id", 1, false, 1000),
                      new IndexInfo("users", "idx_functional", null, 1, true, 500))));

      CompositeIndexDetector detector = new CompositeIndexDetector();
      assertThatCode(() -> detector.evaluate(List.of(record(sql)), metadata))
          .doesNotThrowAnyException();

      MissingIndexDetector missingDetector = new MissingIndexDetector();
      assertThatCode(() -> missingDetector.evaluate(List.of(record(sql)), metadata))
          .doesNotThrowAnyException();
    }

    /**
     * Bug #10: Unknown table in metadata - should NOT report missing index for tables not in
     * metadata (we don't know their indexes).
     */
    @Test
    void unknownTable_shouldNotReportMissingIndex() {
      String sql = "select x1_0.id from unknown_table x1_0 where x1_0.some_column=?";

      // Metadata only knows about "users" table
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 1000))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      // Should NOT report issues for unknown_table since it's not in metadata
      assertThat(issues)
          .allSatisfy(issue -> assertThat(issue.table()).isNotEqualTo("unknown_table"));
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // C. Detection accuracy on real Jimotoku patterns
  //    For each real query, verify the CORRECT detection behavior.
  // ═══════════════════════════════════════════════════════════════════

  @Nested
  class DetectionAccuracy {

    /**
     * user_suspensions: user_id has idx_user, unsuspended_at does NOT have a standalone index.
     * unsuspended_at IS NULL is a soft-delete-like pattern — when user_id is already indexed, the
     * soft-delete column should be SKIPPED (not flagged) since user_id narrows the scan.
     * is_permanent is a low-cardinality column (boolean prefix pattern) — also skipped when another
     * column is indexed.
     */
    @Test
    void userSuspensionQuery_missingIndexAccuracy() {
      String sql =
          "select us1_0.id,us1_0.is_permanent,us1_0.reason,us1_0.report_id,"
              + "us1_0.suspended_at,us1_0.suspended_by,us1_0.suspended_until,"
              + "us1_0.unsuspended_at,us1_0.unsuspended_by,us1_0.user_id "
              + "from user_suspensions us1_0 "
              + "where us1_0.user_id=? and us1_0.unsuspended_at is null "
              + "and (us1_0.is_permanent=1 or us1_0.suspended_until>?) "
              + "order by us1_0.suspended_at desc";

      IndexMetadata metadata = createJimotokuIndexMetadata();

      MissingIndexDetector detector = new MissingIndexDetector();
      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      // user_id is covered by idx_user (user_id, suspended_at) composite
      // unsuspended_at IS NULL is a soft-delete pattern — SKIPPED because user_id is indexed
      // is_permanent is a low-cardinality column (is_* prefix) — SKIPPED because user_id is indexed
      // suspended_until is part of idx_active composite — SKIPPED by composite check

      List<String> flaggedColumns = issues.stream().map(Issue::column).toList();

      // user_id should NOT be flagged (has idx_user composite)
      assertThat(flaggedColumns).doesNotContain("user_id");
      // unsuspended_at should NOT be flagged (soft-delete pattern with indexed user_id)
      assertThat(flaggedColumns).doesNotContain("unsuspended_at");
      // is_permanent should NOT be flagged (low-cardinality with indexed user_id)
      assertThat(flaggedColumns).doesNotContain("is_permanent");
    }

    /**
     * messages table: room_id has idx_room_id composite index (room_id, id). Should NOT flag
     * room_id as missing index.
     */
    @Test
    void messagesQuery_roomIdHasIndex() {
      String sql =
          "select m1_0.id from messages m1_0 "
              + "where m1_0.deleted_at IS NULL and m1_0.room_id=? "
              + "order by m1_0.id desc limit ?";

      IndexMetadata metadata = createJimotokuIndexMetadata();

      MissingIndexDetector detector = new MissingIndexDetector();
      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      // room_id is covered by idx_room_id (room_id, id) composite
      List<String> flaggedColumns = issues.stream().map(Issue::column).toList();
      assertThat(flaggedColumns).doesNotContain("room_id");
    }

    /**
     * user_blocks table has PK (blocker_id, blocked_id) and standalone indexes. Querying by
     * (blocked_id, blocker_id) tuple - blocked_id has idx_blocked standalone.
     */
    @Test
    void userBlocksQuery_blockedIdHasStandaloneIndex() {
      String sql =
          "select count(*) from user_blocks ub1_0 "
              + "where ub1_0.blocked_id=? and ub1_0.blocker_id=?";

      IndexMetadata metadata = createJimotokuIndexMetadata();

      MissingIndexDetector detector = new MissingIndexDetector();
      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      // Both blocked_id (has idx_blocked) and blocker_id (PK leading column) have indexes
      List<String> flaggedColumns = issues.stream().map(Issue::column).toList();
      assertThat(flaggedColumns).doesNotContain("blocked_id");
      assertThat(flaggedColumns).doesNotContain("blocker_id");
    }

    /**
     * connections table: querying by (user_id_small, user_id_large). Has uq_connection_users unique
     * index on (user_id_small, user_id_large).
     */
    @Test
    void connectionsQuery_uniqueIndexOnUserPair() {
      String sql =
          "select c1_0.id from connections c1_0 "
              + "where c1_0.user_id_small=? and c1_0.user_id_large=?";

      IndexMetadata metadata = createJimotokuIndexMetadata();

      MissingIndexDetector detector = new MissingIndexDetector();
      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      // user_id_small is leading column of uq_connection_users
      List<String> flaggedColumns = issues.stream().map(Issue::column).toList();
      assertThat(flaggedColumns).doesNotContain("user_id_small");
    }

    /** SELECT * from native queries should be flagged. */
    @Test
    void nativeSelectAll_shouldBeDetected() {
      String sql1 = "SELECT * FROM users WHERE oauth_provider = ? AND oauth_sub = ?";
      String sql2 = "SELECT * FROM users WHERE id = ?";

      SelectAllDetector detector = new SelectAllDetector();
      List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), EMPTY_INDEX);

      assertThat(issues).hasSize(2);
      assertThat(issues)
          .allSatisfy(
              issue -> {
                assertThat(issue.type()).isEqualTo(IssueType.SELECT_ALL);
                assertThat(issue.table()).isEqualTo("users");
              });
    }

    /**
     * Room notification mutes: uk_user_room unique index on (user_id, room_id). Querying by both
     * should not flag missing index.
     */
    @Test
    void roomNotificationMutes_uniqueIndexCovers() {
      String sql =
          "select case when count(rnm1_0.id)>0 then 1 else 0 end "
              + "from room_notification_mutes rnm1_0 "
              + "where rnm1_0.user_id=? and rnm1_0.room_id=?";

      IndexMetadata metadata = createJimotokuIndexMetadata();

      MissingIndexDetector detector = new MissingIndexDetector();
      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      // user_id is leading column of uk_user_room and also has idx_user_id standalone
      List<String> flaggedColumns = issues.stream().map(Issue::column).toList();
      assertThat(flaggedColumns).doesNotContain("user_id");
    }

    /**
     * notifications table: idx_notification_recipient_read on (recipient_id, is_read, created_at).
     * Querying by recipient_id + is_read + deleted_at. deleted_at has NO index.
     */
    @Test
    void notificationQuery_compositeIndexPartialCoverage() {
      String sql =
          "select count(n1_0.id) from notifications n1_0 "
              + "where n1_0.recipient_id=? and n1_0.is_read=0 and n1_0.deleted_at is null";

      IndexMetadata metadata = createJimotokuIndexMetadata();

      MissingIndexDetector detector = new MissingIndexDetector();
      List<Issue> issues = detector.evaluate(List.of(record(sql)), metadata);

      // recipient_id is covered (leading column of composite)
      // is_read is covered (2nd column of composite)
      // deleted_at has NO index - but it might be in a composite index
      List<String> flaggedColumns = issues.stream().map(Issue::column).toList();
      assertThat(flaggedColumns).doesNotContain("recipient_id");
    }

    /**
     * Full analyzer test with real Jimotoku queries. Verify the analyzer produces a valid report
     * without crashing.
     */
    @Test
    void fullAnalyzer_realJimotokuQueries_producesValidReport() {
      IndexMetadata metadata = createJimotokuIndexMetadata();

      List<QueryRecord> queries =
          List.of(
              record(
                  "select u1_0.id from users u1_0 where u1_0.deleted_at IS NULL and u1_0.nickname=? and u1_0.discriminator=? limit ?"),
              record(
                  "select us1_0.id from user_suspensions us1_0 where us1_0.user_id=? and us1_0.unsuspended_at is null"),
              record(
                  "select count(ul1_0.user_id)>0 from user_location ul1_0 where ul1_0.user_id=?"),
              record("SELECT * FROM users WHERE oauth_provider = ? AND oauth_sub = ?"),
              record(
                  "select m1_0.id from messages m1_0 where m1_0.room_id=? order by m1_0.id desc limit ?"),
              record(
                  "select count(*) from user_blocks ub1_0 where (ub1_0.blocked_id,ub1_0.blocker_id)=(?,?)"),
              record(
                  "select count(*) from room_members rm1_0 where rm1_0.room_id=? and rm1_0.role in ('OWNER','ADMIN','MEMBER')"),
              record(
                  "select case when count(rnm1_0.id)>0 then 1 else 0 end from room_notification_mutes rnm1_0 where rnm1_0.user_id=? and rnm1_0.room_id=?"),
              record(
                  "select count(n1_0.id) from notifications n1_0 where n1_0.recipient_id=? and n1_0.is_read=0 and n1_0.deleted_at is null"),
              record(
                  "select h1_0.id,h1_0.tag from hashtags h1_0 where h1_0.usage_count>0 order by h1_0.usage_count desc limit ?"));

      QueryAuditAnalyzer analyzer = defaultAnalyzer();
      QueryAuditReport report = analyzer.analyze("JimotokuRegression", queries, metadata);

      assertThat(report).isNotNull();
      assertThat(report.getTestName()).isEqualTo("JimotokuRegression");
      assertThat(report.getTotalQueryCount()).isEqualTo(10);
      assertThat(report.getUniquePatternCount()).isEqualTo(10);

      // SELECT * should be detected (now INFO severity, so check info issues)
      boolean hasSelectAllIssue =
          report.getInfoIssues().stream().anyMatch(i -> i.type() == IssueType.SELECT_ALL);
      assertThat(hasSelectAllIssue).isTrue();
    }

    /**
     * OR abuse threshold: connections query with multiple OR should be detected when threshold is
     * met.
     */
    @Test
    void connectionQueryWithMultipleOr_aboveThreshold() {
      String sql =
          "select c1_0.id from connections c1_0 "
              + "where (c1_0.user_id_small=? and c1_0.user_id_large in (?)) "
              + "or (c1_0.user_id_large=? and c1_0.user_id_small in (?)) "
              + "or (c1_0.user_id_small=? and c1_0.user_id_large=?) "
              + "or c1_0.level>=?";

      OrAbuseDetector detector = new OrAbuseDetector(3);
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // 3+ OR conditions should trigger
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.OR_ABUSE);
    }

    @Test
    void connectionQueryWithOneOr_belowThreshold() {
      String sql =
          "select c1_0.id from connections c1_0 "
              + "where (c1_0.user_id_small=? or c1_0.user_id_large=?) and c1_0.level>=?";

      OrAbuseDetector detector = new OrAbuseDetector(3);
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // Only 1 OR, below threshold of 3
      assertThat(issues).isEmpty();
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // D. Full Jimotoku schema index metadata builder
  //    Based on actual migration files V1-V35.
  // ═══════════════════════════════════════════════════════════════════

  /**
   * Builds IndexMetadata matching Jimotoku's actual database schema, based on migration files V1
   * through V35.
   */
  static IndexMetadata createJimotokuIndexMetadata() {
    Map<String, List<IndexInfo>> indexesByTable = new HashMap<>();

    // ── users (V1) ──
    indexesByTable.put(
        "users",
        List.of(
            new IndexInfo("users", "PRIMARY", "id", 1, false, 10000),
            new IndexInfo("users", "uq_oauth", "oauth_provider", 1, false, 10000),
            new IndexInfo("users", "uq_oauth", "oauth_sub", 2, false, 10000),
            new IndexInfo("users", "uq_nick_disc", "nickname", 1, false, 8000),
            new IndexInfo("users", "uq_nick_disc", "discriminator", 2, false, 10000),
            new IndexInfo("users", "uq_handle", "handle", 1, false, 10000)));

    // ── locations (V2) ──
    indexesByTable.put(
        "locations",
        List.of(
            new IndexInfo("locations", "PRIMARY", "id", 1, false, 100),
            new IndexInfo("locations", "uk_slug", "slug_en", 1, false, 100)));

    // ── user_location (V2) ──
    indexesByTable.put(
        "user_location",
        List.of(
            new IndexInfo("user_location", "PRIMARY", "user_id", 1, false, 5000),
            new IndexInfo("user_location", "idx_location_id", "location_id", 1, true, 100)));

    // ── rooms (V3) ──
    indexesByTable.put(
        "rooms",
        List.of(
            new IndexInfo("rooms", "PRIMARY", "id", 1, false, 1000),
            new IndexInfo("rooms", "fk_rooms_location", "location_id", 1, true, 100)));

    // ── room_members (V3) - PK is composite (room_id, user_id) ──
    indexesByTable.put(
        "room_members",
        List.of(
            new IndexInfo("room_members", "PRIMARY", "room_id", 1, false, 5000),
            new IndexInfo("room_members", "PRIMARY", "user_id", 2, false, 5000),
            new IndexInfo("room_members", "fk_room_members_user", "user_id", 1, true, 5000)));

    // ── messages (V4) ──
    indexesByTable.put(
        "messages",
        List.of(
            new IndexInfo("messages", "PRIMARY", "id", 1, false, 100000),
            new IndexInfo("messages", "idx_room_id", "room_id", 1, true, 1000),
            new IndexInfo("messages", "idx_room_id", "id", 2, true, 100000),
            new IndexInfo("messages", "idx_parent", "parent_message_id", 1, true, 50000),
            new IndexInfo("messages", "idx_sender", "sender_id", 1, true, 10000)));

    // ── message_media (V4) ──
    indexesByTable.put(
        "message_media",
        List.of(
            new IndexInfo("message_media", "PRIMARY", "id", 1, false, 50000),
            new IndexInfo("message_media", "idx_message", "message_id", 1, true, 50000),
            new IndexInfo("message_media", "idx_message", "display_order", 2, true, 50000)));

    // ── message_read_receipts (V4) ──
    indexesByTable.put(
        "message_read_receipts",
        List.of(
            new IndexInfo("message_read_receipts", "PRIMARY", "id", 1, false, 500000),
            new IndexInfo(
                "message_read_receipts", "idx_message_read_at", "message_id", 1, true, 100000),
            new IndexInfo(
                "message_read_receipts", "idx_message_read_at", "read_at", 2, true, 500000),
            new IndexInfo("message_read_receipts", "idx_user_read_at", "user_id", 1, true, 10000),
            new IndexInfo(
                "message_read_receipts", "idx_user_read_at", "read_at", 2, true, 500000)));

    // ── message_reactions (V4) ──
    indexesByTable.put(
        "message_reactions",
        List.of(
            new IndexInfo("message_reactions", "PRIMARY", "id", 1, false, 50000),
            new IndexInfo("message_reactions", "idx_emoji", "message_id", 1, true, 30000),
            new IndexInfo("message_reactions", "idx_emoji", "emoji", 2, true, 50000),
            new IndexInfo("message_reactions", "idx_user", "user_id", 1, true, 10000)));

    // ── message_mentions (V4) ──
    indexesByTable.put(
        "message_mentions",
        List.of(
            new IndexInfo("message_mentions", "PRIMARY", "id", 1, false, 10000),
            new IndexInfo("message_mentions", "idx_message", "message_id", 1, true, 10000),
            new IndexInfo(
                "message_mentions", "idx_mentioned_user", "mentioned_user_id", 1, true, 5000)));

    // ── message_polls (V4) ──
    indexesByTable.put(
        "message_polls",
        List.of(
            new IndexInfo("message_polls", "PRIMARY", "id", 1, false, 500),
            new IndexInfo("message_polls", "message_id", "message_id", 1, false, 500)));

    // ── message_poll_options (V4) ──
    indexesByTable.put(
        "message_poll_options",
        List.of(
            new IndexInfo("message_poll_options", "PRIMARY", "id", 1, false, 2000),
            new IndexInfo("message_poll_options", "idx_poll_order", "poll_id", 1, true, 500),
            new IndexInfo(
                "message_poll_options", "idx_poll_order", "display_order", 2, true, 2000)));

    // ── message_poll_votes (V4) ──
    indexesByTable.put(
        "message_poll_votes",
        List.of(
            new IndexInfo("message_poll_votes", "PRIMARY", "id", 1, false, 5000),
            new IndexInfo("message_poll_votes", "idx_user_votes", "user_id", 1, true, 3000),
            new IndexInfo("message_poll_votes", "idx_user_votes", "voted_at", 2, true, 5000),
            new IndexInfo(
                "message_poll_votes", "idx_poll_option", "poll_option_id", 1, true, 2000)));

    // ── message_locations (V4) ──
    indexesByTable.put(
        "message_locations",
        List.of(
            new IndexInfo("message_locations", "PRIMARY", "id", 1, false, 1000),
            new IndexInfo("message_locations", "message_id", "message_id", 1, false, 1000),
            new IndexInfo("message_locations", "idx_coordinates", "latitude", 1, true, 1000),
            new IndexInfo("message_locations", "idx_coordinates", "longitude", 2, true, 1000)));

    // ── message_apple_music (V4) ──
    indexesByTable.put(
        "message_apple_music",
        List.of(
            new IndexInfo("message_apple_music", "PRIMARY", "id", 1, false, 500),
            new IndexInfo("message_apple_music", "message_id", "message_id", 1, false, 500),
            new IndexInfo("message_apple_music", "idx_track_id", "track_id", 1, true, 500)));

    // ── message_reports (V13) ──
    indexesByTable.put(
        "message_reports",
        List.of(
            new IndexInfo("message_reports", "PRIMARY", "id", 1, false, 100),
            new IndexInfo("message_reports", "idx_reporter", "reporter_id", 1, true, 50),
            new IndexInfo("message_reports", "idx_reporter", "created_at", 2, true, 100),
            new IndexInfo("message_reports", "idx_reported_user", "reported_user_id", 1, true, 50),
            new IndexInfo("message_reports", "idx_reported_user", "created_at", 2, true, 100),
            new IndexInfo("message_reports", "idx_status", "status", 1, true, 5),
            new IndexInfo("message_reports", "idx_status", "created_at", 2, true, 100),
            new IndexInfo("message_reports", "idx_room", "room_id", 1, true, 50),
            new IndexInfo("message_reports", "idx_room", "created_at", 2, true, 100),
            new IndexInfo("message_reports", "idx_reporter_message", "reporter_id", 1, true, 50),
            new IndexInfo(
                "message_reports", "idx_reporter_message", "reported_message_id", 2, true, 100)));

    // ── user_suspensions (V13) ──
    indexesByTable.put(
        "user_suspensions",
        List.of(
            new IndexInfo("user_suspensions", "PRIMARY", "id", 1, false, 50),
            new IndexInfo("user_suspensions", "idx_user", "user_id", 1, true, 50),
            new IndexInfo("user_suspensions", "idx_user", "suspended_at", 2, true, 50),
            new IndexInfo("user_suspensions", "idx_active", "user_id", 1, true, 50),
            new IndexInfo("user_suspensions", "idx_active", "suspended_until", 2, true, 50)));

    // ── admin_action_logs (V13) ──
    indexesByTable.put(
        "admin_action_logs",
        List.of(
            new IndexInfo("admin_action_logs", "PRIMARY", "id", 1, false, 100),
            new IndexInfo("admin_action_logs", "idx_admin", "admin_id", 1, true, 10),
            new IndexInfo("admin_action_logs", "idx_admin", "created_at", 2, true, 100),
            new IndexInfo("admin_action_logs", "idx_user", "user_id", 1, true, 50),
            new IndexInfo("admin_action_logs", "idx_user", "created_at", 2, true, 100),
            new IndexInfo("admin_action_logs", "idx_report", "report_id", 1, true, 50)));

    // ── user_blocks (V15) - PK is composite (blocker_id, blocked_id) ──
    indexesByTable.put(
        "user_blocks",
        List.of(
            new IndexInfo("user_blocks", "PRIMARY", "blocker_id", 1, false, 1000),
            new IndexInfo("user_blocks", "PRIMARY", "blocked_id", 2, false, 1000),
            new IndexInfo("user_blocks", "idx_blocker", "blocker_id", 1, true, 1000),
            new IndexInfo("user_blocks", "idx_blocked", "blocked_id", 1, true, 1000)));

    // ── user_devices (V16) ──
    indexesByTable.put(
        "user_devices",
        List.of(
            new IndexInfo("user_devices", "PRIMARY", "id", 1, false, 5000),
            new IndexInfo("user_devices", "uq_device_token", "device_token", 1, false, 5000),
            new IndexInfo("user_devices", "idx_user_devices_user_id", "user_id", 1, true, 3000)));

    // ── global_notification_settings (V17) - PK is user_id ──
    indexesByTable.put(
        "global_notification_settings",
        List.of(
            new IndexInfo("global_notification_settings", "PRIMARY", "user_id", 1, false, 5000)));

    // ── room_notification_mutes (V18) ──
    indexesByTable.put(
        "room_notification_mutes",
        List.of(
            new IndexInfo("room_notification_mutes", "PRIMARY", "id", 1, false, 2000),
            new IndexInfo("room_notification_mutes", "uk_user_room", "user_id", 1, false, 2000),
            new IndexInfo("room_notification_mutes", "uk_user_room", "room_id", 2, false, 2000),
            new IndexInfo("room_notification_mutes", "idx_user_id", "user_id", 1, true, 2000),
            new IndexInfo("room_notification_mutes", "idx_room_id", "room_id", 1, true, 1000)));

    // ── notifications (V23) ──
    indexesByTable.put(
        "notifications",
        List.of(
            new IndexInfo("notifications", "PRIMARY", "id", 1, false, 100000),
            new IndexInfo(
                "notifications", "idx_notification_recipient_read", "recipient_id", 1, true, 5000),
            new IndexInfo(
                "notifications", "idx_notification_recipient_read", "is_read", 2, true, 2),
            new IndexInfo(
                "notifications", "idx_notification_recipient_read", "created_at", 3, true, 100000),
            new IndexInfo(
                "notifications",
                "idx_notification_recipient_created",
                "recipient_id",
                1,
                true,
                5000),
            new IndexInfo(
                "notifications",
                "idx_notification_recipient_created",
                "created_at",
                2,
                true,
                100000)));

    // ── user_location_history (V22) ──
    indexesByTable.put(
        "user_location_history",
        List.of(
            new IndexInfo("user_location_history", "PRIMARY", "id", 1, false, 10000),
            new IndexInfo(
                "user_location_history", "uq_user_location_history", "user_id", 1, false, 5000),
            new IndexInfo(
                "user_location_history", "uq_user_location_history", "location_id", 2, false, 100),
            new IndexInfo(
                "user_location_history", "idx_ulh_location", "location_id", 1, true, 100)));

    // ── connections (V22) ──
    indexesByTable.put(
        "connections",
        List.of(
            new IndexInfo("connections", "PRIMARY", "id", 1, false, 10000),
            new IndexInfo("connections", "uq_connection_users", "user_id_small", 1, false, 5000),
            new IndexInfo("connections", "uq_connection_users", "user_id_large", 2, false, 10000),
            new IndexInfo("connections", "idx_conn_user_large", "user_id_large", 1, true, 5000)));

    // ── connection_shared_locations (V22) ──
    indexesByTable.put(
        "connection_shared_locations",
        List.of(
            new IndexInfo("connection_shared_locations", "PRIMARY", "id", 1, false, 5000),
            new IndexInfo(
                "connection_shared_locations", "uq_conn_location", "connection_id", 1, false, 5000),
            new IndexInfo(
                "connection_shared_locations", "uq_conn_location", "location_id", 2, false, 100),
            new IndexInfo(
                "connection_shared_locations", "idx_csl_location", "location_id", 1, true, 100)));

    // ── direct_chat_requests (V27) ──
    indexesByTable.put(
        "direct_chat_requests",
        List.of(
            new IndexInfo("direct_chat_requests", "PRIMARY", "id", 1, false, 500),
            new IndexInfo("direct_chat_requests", "idx_dcr_expires", "expires_at", 1, true, 500),
            new IndexInfo(
                "direct_chat_requests", "uq_pending_request", "requester_id", 1, false, 500),
            new IndexInfo(
                "direct_chat_requests", "uq_pending_request", "receiver_id", 2, false, 500),
            new IndexInfo("direct_chat_requests", "uq_pending_request", "status", 3, false, 5)));

    // ── message_bookmarks (V29) ──
    indexesByTable.put(
        "message_bookmarks",
        List.of(
            new IndexInfo("message_bookmarks", "PRIMARY", "id", 1, false, 5000),
            new IndexInfo("message_bookmarks", "uk_user_message", "user_id", 1, false, 3000),
            new IndexInfo("message_bookmarks", "uk_user_message", "message_id", 2, false, 5000),
            new IndexInfo("message_bookmarks", "idx_user_bookmarks", "user_id", 1, true, 3000),
            new IndexInfo("message_bookmarks", "idx_user_bookmarks", "created_at", 2, true, 5000),
            new IndexInfo(
                "message_bookmarks", "idx_message_bookmarks", "message_id", 1, true, 5000)));

    // ── stories (V34) ──
    indexesByTable.put(
        "stories",
        List.of(
            new IndexInfo("stories", "PRIMARY", "id", 1, false, 10000),
            new IndexInfo("stories", "idx_story_location_created", "location_id", 1, true, 100),
            new IndexInfo("stories", "idx_story_location_created", "created_at", 2, true, 10000),
            new IndexInfo("stories", "idx_story_location_linked_room", "location_id", 1, true, 100),
            new IndexInfo(
                "stories", "idx_story_location_linked_room", "linked_room_id", 2, true, 1000),
            new IndexInfo(
                "stories", "idx_story_location_linked_room", "created_at", 3, true, 10000),
            new IndexInfo("stories", "idx_story_author", "author_id", 1, true, 5000),
            new IndexInfo("stories", "idx_story_author", "created_at", 2, true, 10000)));

    // ── story_media (V34) ──
    indexesByTable.put(
        "story_media",
        List.of(
            new IndexInfo("story_media", "PRIMARY", "id", 1, false, 20000),
            new IndexInfo("story_media", "idx_story_media_story", "story_id", 1, true, 10000),
            new IndexInfo(
                "story_media", "idx_story_media_story", "display_order", 2, true, 20000)));

    // ── story_likes (V35) ──
    indexesByTable.put(
        "story_likes",
        List.of(
            new IndexInfo("story_likes", "PRIMARY", "id", 1, false, 20000),
            new IndexInfo("story_likes", "uk_story_likes_story_user", "story_id", 1, false, 10000),
            new IndexInfo("story_likes", "uk_story_likes_story_user", "user_id", 2, false, 5000),
            new IndexInfo("story_likes", "idx_story_likes_story", "story_id", 1, true, 10000),
            new IndexInfo("story_likes", "idx_story_likes_user", "user_id", 1, true, 5000),
            new IndexInfo("story_likes", "idx_story_likes_user", "created_at", 2, true, 20000)));

    // ── story_comments (V35) ──
    indexesByTable.put(
        "story_comments",
        List.of(
            new IndexInfo("story_comments", "PRIMARY", "id", 1, false, 5000),
            new IndexInfo("story_comments", "idx_story_comments_story", "story_id", 1, true, 1000),
            new IndexInfo(
                "story_comments", "idx_story_comments_story", "created_at", 2, true, 5000),
            new IndexInfo(
                "story_comments", "idx_story_comments_parent", "parent_id", 1, true, 2000),
            new IndexInfo(
                "story_comments", "idx_story_comments_author", "author_id", 1, true, 3000)));

    // ── story_bookmarks (V35) ──
    indexesByTable.put(
        "story_bookmarks",
        List.of(
            new IndexInfo("story_bookmarks", "PRIMARY", "id", 1, false, 3000),
            new IndexInfo(
                "story_bookmarks", "uk_story_bookmarks_story_user", "story_id", 1, false, 2000),
            new IndexInfo(
                "story_bookmarks", "uk_story_bookmarks_story_user", "user_id", 2, false, 3000),
            new IndexInfo("story_bookmarks", "idx_story_bookmarks_user", "user_id", 1, true, 2000),
            new IndexInfo(
                "story_bookmarks", "idx_story_bookmarks_user", "created_at", 2, true, 3000),
            new IndexInfo(
                "story_bookmarks", "idx_story_bookmarks_story", "story_id", 1, true, 2000)));

    // ── hashtags (V31 - inferred from queries) ──
    indexesByTable.put(
        "hashtags",
        List.of(
            new IndexInfo("hashtags", "PRIMARY", "id", 1, false, 1000),
            new IndexInfo("hashtags", "uk_tag", "tag", 1, false, 1000)));

    // ── message_hashtags (V31 - inferred) ──
    indexesByTable.put(
        "message_hashtags",
        List.of(
            new IndexInfo("message_hashtags", "PRIMARY", "id", 1, false, 5000),
            new IndexInfo("message_hashtags", "idx_message", "message_id", 1, true, 5000),
            new IndexInfo("message_hashtags", "idx_hashtag", "hashtag_id", 1, true, 1000)));

    // ── lightning_review (V39 - inferred) ──
    indexesByTable.put(
        "lightning_review",
        List.of(
            new IndexInfo("lightning_review", "PRIMARY", "id", 1, false, 500),
            new IndexInfo("lightning_review", "idx_room", "room_id", 1, true, 200),
            new IndexInfo("lightning_review", "idx_reviewer", "reviewer_id", 1, true, 300)));

    // ── lightning_like (V39 - inferred) ──
    indexesByTable.put(
        "lightning_like",
        List.of(
            new IndexInfo("lightning_like", "PRIMARY", "id", 1, false, 500),
            new IndexInfo("lightning_like", "idx_room_from", "room_id", 1, true, 200),
            new IndexInfo("lightning_like", "idx_room_from", "from_user_id", 2, true, 500)));

    return new IndexMetadata(indexesByTable);
  }
}
