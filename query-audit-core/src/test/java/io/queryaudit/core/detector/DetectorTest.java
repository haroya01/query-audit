package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class DetectorTest {

  // Helper to create a QueryRecord with minimal args
  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  // ── NPlusOneDetector ────────────────────────────────────────────────

  @Nested
  class NPlusOneDetectorTest {

    @Test
    void detectsWhenSameQueryRepeated5Times() {
      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1"),
              record("SELECT * FROM users WHERE id = 2"),
              record("SELECT * FROM users WHERE id = 3"),
              record("SELECT * FROM users WHERE id = 4"),
              record("SELECT * FROM users WHERE id = 5"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.N_PLUS_ONE);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.INFO);
      assertThat(issues.get(0).detail()).contains("5 times");
    }

    @Test
    void doesNotDetectWhenSameQueryRepeated2Times() {
      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1"),
              record("SELECT * FROM users WHERE id = 2"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void exactlyAtThresholdIsDetected() {
      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1"),
              record("SELECT * FROM users WHERE id = 2"),
              record("SELECT * FROM users WHERE id = 3"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
    }

    @Test
    void differentQueriesNotGrouped() {
      NPlusOneDetector detector = new NPlusOneDetector(3);
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1"),
              record("SELECT * FROM orders WHERE id = 2"),
              record("SELECT * FROM products WHERE id = 3"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // ── SelectAllDetector ───────────────────────────────────────────────

  @Nested
  class SelectAllDetectorTest {

    @Test
    void detectsSelectStar() {
      SelectAllDetector detector = new SelectAllDetector();
      List<QueryRecord> queries = List.of(record("SELECT * FROM users"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.SELECT_ALL);
    }

    @Test
    void doesNotDetectSpecificColumns() {
      SelectAllDetector detector = new SelectAllDetector();
      List<QueryRecord> queries = List.of(record("SELECT id, name FROM users"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void deduplicatesSamePattern() {
      SelectAllDetector detector = new SelectAllDetector();
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE id = 1"),
              record("SELECT * FROM users WHERE id = 2"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
    }
  }

  // ── WhereFunctionDetector ───────────────────────────────────────────

  @Nested
  class WhereFunctionDetectorTest {

    @Test
    void detectsDateFunction() {
      WhereFunctionDetector detector = new WhereFunctionDetector();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM orders WHERE DATE(created_at) = '2024-01-01'"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.WHERE_FUNCTION);
      assertThat(issues.get(0).column()).isEqualTo("created_at");
    }

    @Test
    void doesNotDetectPlainWhere() {
      WhereFunctionDetector detector = new WhereFunctionDetector();
      List<QueryRecord> queries = List.of(record("SELECT * FROM users WHERE id = 1"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void detectsMultipleFunctions() {
      WhereFunctionDetector detector = new WhereFunctionDetector();
      List<QueryRecord> queries =
          List.of(
              record(
                  "SELECT * FROM users WHERE LOWER(email) = 'test' AND YEAR(created_at) = 2024"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(2);
    }
  }

  // ── OrAbuseDetector ─────────────────────────────────────────────────

  @Nested
  class OrAbuseDetectorTest {

    @Test
    void detectsFourOrs() {
      OrAbuseDetector detector = new OrAbuseDetector(3);
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM users WHERE a = 1 OR b = 2 OR c = 3 OR d = 4 OR e = 5"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.OR_ABUSE);
    }

    @Test
    void doesNotDetectOneOr() {
      OrAbuseDetector detector = new OrAbuseDetector(3);
      List<QueryRecord> queries = List.of(record("SELECT * FROM users WHERE a = 1 OR b = 2"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void exactlyAtThresholdIsDetected() {
      OrAbuseDetector detector = new OrAbuseDetector(3);
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM users WHERE a = 1 OR b = 2 OR c = 3 OR d = 4"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
    }

    @Test
    void doesNotFlagSameColumnOrConditions() {
      // Bug 3: JPA generates "type = 'A' OR type = 'B' OR type = 'C'" which is
      // equivalent to "type IN ('A', 'B', 'C')" - should not be flagged as OR abuse
      OrAbuseDetector detector = new OrAbuseDetector(3);
      List<QueryRecord> queries =
          List.of(
              record(
                  "SELECT * FROM users WHERE type = 'A' OR type = 'B' OR type = 'C' OR type = 'D'"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void flagsMixedColumnOrConditions() {
      // Different columns in OR should still be flagged
      OrAbuseDetector detector = new OrAbuseDetector(3);
      List<QueryRecord> queries =
          List.of(
              record(
                  "SELECT * FROM users WHERE type = 'A' OR status = 'B' OR type = 'C' OR role = 'D'"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
    }

    @Test
    void doesNotFlagOptionalParameterPattern() {
      // JPA dynamic query: (? IS NULL OR column = ?) is NOT OR abuse
      OrAbuseDetector detector = new OrAbuseDetector(3);
      List<QueryRecord> queries =
          List.of(
              record(
                  "SELECT * FROM users WHERE (? IS NULL OR name = ?) AND (? IS NULL OR status = ?) AND (? IS NULL OR role = ?) AND (? IS NULL OR age = ?)"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void flagsRealOrsEvenWithSomeOptionalParameters() {
      // Mix of optional parameter patterns and real ORs
      OrAbuseDetector detector = new OrAbuseDetector(3);
      List<QueryRecord> queries =
          List.of(
              record(
                  "SELECT * FROM users WHERE (? IS NULL OR name = ?) AND (a = 1 OR b = 2 OR c = 3 OR d = 4)"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
    }
  }

  // ── OffsetPaginationDetector ────────────────────────────────────────

  @Nested
  class OffsetPaginationDetectorTest {

    @Test
    void detectsLargeOffset() {
      OffsetPaginationDetector detector = new OffsetPaginationDetector(1000);
      List<QueryRecord> queries = List.of(record("SELECT * FROM users LIMIT 10 OFFSET 5000"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.OFFSET_PAGINATION);
      assertThat(issues.get(0).detail()).contains("5000");
    }

    @Test
    void doesNotDetectSmallOffset() {
      OffsetPaginationDetector detector = new OffsetPaginationDetector(1000);
      List<QueryRecord> queries = List.of(record("SELECT * FROM users LIMIT 10 OFFSET 10"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void detectsMySqlCommaFormat() {
      OffsetPaginationDetector detector = new OffsetPaginationDetector(1000);
      List<QueryRecord> queries = List.of(record("SELECT * FROM users LIMIT 5000, 10"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
    }

    @Test
    void noOffsetIsNotDetected() {
      OffsetPaginationDetector detector = new OffsetPaginationDetector(1000);
      List<QueryRecord> queries = List.of(record("SELECT * FROM users LIMIT 10"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // ── MissingIndexDetector ────────────────────────────────────────────

  @Nested
  class MissingIndexDetectorTest {

    @Test
    void detectsMissingWhereIndex() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 100))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM users WHERE email = 'test@example.com'"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      assertThat(issues).isNotEmpty();
      assertThat(issues)
          .anyMatch(i -> i.type() == IssueType.MISSING_WHERE_INDEX && "email".equals(i.column()));
    }

    @Test
    void doesNotDetectWhenIndexExists() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "users",
                  List.of(
                      new IndexInfo("users", "PRIMARY", "id", 1, false, 100),
                      new IndexInfo("users", "idx_email", "email", 1, true, 100))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM users WHERE email = 'test@example.com'"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      assertThat(issues.stream().filter(i -> i.type() == IssueType.MISSING_WHERE_INDEX)).isEmpty();
    }

    @Test
    void skipsWhenNoIndexMetadata() {
      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM users WHERE email = 'test@example.com'"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void skipsNonSelectQueries() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 100))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries = List.of(record("INSERT INTO users (name) VALUES ('John')"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      assertThat(issues).isEmpty();
    }

    @Test
    void skipsUnindexedColumnWhenAnotherWhereColumnHasUniqueIndex() {
      // Bug 1: WHERE user_id = ? AND deleted_at IS NULL
      // user_id has a unique index, so deleted_at should be skipped entirely
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "messages",
                  List.of(
                      new IndexInfo("messages", "PRIMARY", "id", 1, false, 1000),
                      new IndexInfo("messages", "idx_user_id", "user_id", 1, false, 500))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM messages WHERE user_id = 42 AND deleted_at IS NULL"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      // deleted_at should NOT be flagged since user_id has a unique index
      assertThat(
              issues.stream()
                  .filter(
                      i ->
                          i.type() == IssueType.MISSING_WHERE_INDEX
                              && "deleted_at".equals(i.column())))
          .isEmpty();
    }

    @Test
    void downgradesUnindexedColumnWhenAnotherWhereColumnHasRegularIndex() {
      // When another WHERE column has a regular (non-unique) index,
      // severity should be downgraded to WARNING instead of ERROR.
      // Use a non-soft-delete, non-low-cardinality column name to test pure downgrade logic.
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "messages",
                  List.of(
                      new IndexInfo("messages", "PRIMARY", "id", 1, false, 1000),
                      new IndexInfo("messages", "idx_status", "status", 1, true, 10))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM messages WHERE status = 'active' AND sender_name = 'John'"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      List<Issue> senderNameIssues =
          issues.stream()
              .filter(
                  i ->
                      i.type() == IssueType.MISSING_WHERE_INDEX && "sender_name".equals(i.column()))
              .toList();
      assertThat(senderNameIssues).hasSize(1);
      assertThat(senderNameIssues.get(0).severity()).isEqualTo(Severity.WARNING);
    }

    @Test
    void groupBySkipsColumnsWhenAllPkColumnsPresent() {
      // Bug 2: GROUP BY room_id, user_id, last_read_message_id
      // (room_id, user_id) is the PK, so last_read_message_id should not be flagged
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "read_receipts",
                  List.of(
                      new IndexInfo("read_receipts", "PRIMARY", "room_id", 1, false, 100),
                      new IndexInfo("read_receipts", "PRIMARY", "user_id", 2, false, 100))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries =
          List.of(
              record(
                  "SELECT room_id, user_id, last_read_message_id FROM read_receipts GROUP BY room_id, user_id, last_read_message_id"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      assertThat(
              issues.stream()
                  .filter(
                      i ->
                          i.type() == IssueType.MISSING_GROUP_BY_INDEX
                              && "last_read_message_id".equals(i.column())))
          .isEmpty();
    }

    @Test
    void groupByStillFlagsWhenNotAllPkColumnsPresent() {
      // If only some PK columns are in GROUP BY, non-indexed columns should still be flagged
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "read_receipts",
                  List.of(
                      new IndexInfo("read_receipts", "PRIMARY", "room_id", 1, false, 100),
                      new IndexInfo("read_receipts", "PRIMARY", "user_id", 2, false, 100))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries =
          List.of(
              record(
                  "SELECT room_id, last_read_message_id FROM read_receipts GROUP BY room_id, last_read_message_id"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      assertThat(
              issues.stream()
                  .filter(
                      i ->
                          i.type() == IssueType.MISSING_GROUP_BY_INDEX
                              && "last_read_message_id".equals(i.column())))
          .isNotEmpty();
    }

    // ── Improvement 1: Low cardinality column detection ─────────

    @Test
    void lowCardinalityColumnSkippedWhenOtherIndexedColumnExists() {
      // 'status' is a low-cardinality column name; when another indexed column
      // (email) exists in WHERE, the low-cardinality column should be skipped entirely.
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "users",
                  List.of(
                      new IndexInfo("users", "PRIMARY", "id", 1, false, 1000),
                      new IndexInfo("users", "idx_email", "email", 1, true, 1000))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE email = 'test@example.com' AND status = 'active'"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      assertThat(
              issues.stream()
                  .filter(
                      i ->
                          i.type() == IssueType.MISSING_WHERE_INDEX && "status".equals(i.column())))
          .isEmpty();
    }

    @Test
    void lowCardinalityColumnAloneDowngradesToInfo() {
      // When a low-cardinality column is the ONLY column in WHERE (no other indexed column),
      // it should be flagged as INFO with an appropriate message.
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 1000))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries = List.of(record("SELECT * FROM users WHERE is_active = true"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      List<Issue> activeIssues =
          issues.stream()
              .filter(
                  i -> i.type() == IssueType.MISSING_WHERE_INDEX && "is_active".equals(i.column()))
              .toList();
      assertThat(activeIssues).hasSize(1);
      assertThat(activeIssues.get(0).severity()).isEqualTo(Severity.INFO);
      assertThat(activeIssues.get(0).detail()).contains("Low cardinality");
    }

    @Test
    void lowCardinalityDetectedByPrefixPatterns() {
      // Columns starting with is_, has_, or flag should be recognized as low cardinality
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of("posts", List.of(new IndexInfo("posts", "PRIMARY", "id", 1, false, 1000))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries = List.of(record("SELECT * FROM posts WHERE has_comments = true"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      List<Issue> hasCommentsIssues =
          issues.stream()
              .filter(
                  i ->
                      i.type() == IssueType.MISSING_WHERE_INDEX
                          && "has_comments".equals(i.column()))
              .toList();
      assertThat(hasCommentsIssues).hasSize(1);
      assertThat(hasCommentsIssues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    @Test
    void lowCardinalityDetectedByExactNameMatch() {
      // Exact name match: "category" is in the low-cardinality name set
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "tickets", List.of(new IndexInfo("tickets", "PRIMARY", "id", 1, false, 1000))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries = List.of(record("SELECT * FROM tickets WHERE category = 'bug'"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      List<Issue> categoryIssues =
          issues.stream()
              .filter(
                  i -> i.type() == IssueType.MISSING_WHERE_INDEX && "category".equals(i.column()))
              .toList();
      assertThat(categoryIssues).hasSize(1);
      assertThat(categoryIssues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    // ── Improvement 2: Soft-delete pattern recognition ──────────

    @Test
    void softDeleteColumnSkippedWhenOtherIndexedColumnExists() {
      // deleted_at IS NULL should be skipped entirely when another indexed column
      // already narrows the result set.
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "messages",
                  List.of(
                      new IndexInfo("messages", "PRIMARY", "id", 1, false, 1000),
                      new IndexInfo("messages", "idx_status", "status", 1, true, 10))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM messages WHERE status = 'active' AND deleted_at IS NULL"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      assertThat(
              issues.stream()
                  .filter(
                      i ->
                          i.type() == IssueType.MISSING_WHERE_INDEX
                              && "deleted_at".equals(i.column())))
          .isEmpty();
    }

    @Test
    void softDeleteColumnAloneDowngradesToInfo() {
      // When deleted_at IS NULL is the only condition, downgrade to INFO
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "messages", List.of(new IndexInfo("messages", "PRIMARY", "id", 1, false, 1000))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM messages WHERE deleted_at IS NULL"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      List<Issue> deletedAtIssues =
          issues.stream()
              .filter(
                  i -> i.type() == IssueType.MISSING_WHERE_INDEX && "deleted_at".equals(i.column()))
              .toList();
      assertThat(deletedAtIssues).hasSize(1);
      assertThat(deletedAtIssues.get(0).severity()).isEqualTo(Severity.INFO);
      assertThat(deletedAtIssues.get(0).detail()).contains("Soft-delete");
    }

    @Test
    void softDeleteRecognizesIsDeletedColumn() {
      // is_deleted = false should also be recognized as a soft-delete pattern
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 1000))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries = List.of(record("SELECT * FROM users WHERE is_deleted = false"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      List<Issue> isDeletedIssues =
          issues.stream()
              .filter(
                  i -> i.type() == IssueType.MISSING_WHERE_INDEX && "is_deleted".equals(i.column()))
              .toList();
      assertThat(isDeletedIssues).hasSize(1);
      assertThat(isDeletedIssues.get(0).severity()).isEqualTo(Severity.INFO);
      assertThat(isDeletedIssues.get(0).detail()).contains("Soft-delete");
    }

    @Test
    void softDeleteWithUniqueIndexSkipsEntirely() {
      // deleted_at IS NULL with a unique-indexed WHERE column should be skipped entirely
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "users",
                  List.of(
                      new IndexInfo("users", "PRIMARY", "id", 1, false, 1000),
                      new IndexInfo("users", "idx_email", "email", 1, false, 1000))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries =
          List.of(
              record(
                  "SELECT * FROM users WHERE email = 'test@example.com' AND deleted_at IS NULL"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      assertThat(
              issues.stream()
                  .filter(
                      i ->
                          i.type() == IssueType.MISSING_WHERE_INDEX
                              && "deleted_at".equals(i.column())))
          .isEmpty();
    }

    // ── Improvement 3: Post-filter estimation for ORDER BY ──────

    @Test
    void orderBySkippedWhenWhereHasUniqueEqualityIndex() {
      // If WHERE has a unique/PK index with equality, the result set is at most 1 row.
      // Flagging ORDER BY filesort on 1 row is noise.
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "orders",
                  List.of(
                      new IndexInfo("orders", "PRIMARY", "id", 1, false, 10000),
                      new IndexInfo("orders", "idx_user_unique", "user_id", 1, false, 5000))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM orders WHERE user_id = 42 ORDER BY created_at DESC"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      assertThat(
              issues.stream()
                  .filter(
                      i ->
                          i.type() == IssueType.MISSING_ORDER_BY_INDEX
                              && "created_at".equals(i.column())))
          .isEmpty();
    }

    @Test
    void orderByDowngradedToInfoWhenWhereHasRegularIndex() {
      // If WHERE has a regular (non-unique) index, ORDER BY severity should be INFO
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "orders",
                  List.of(
                      new IndexInfo("orders", "PRIMARY", "id", 1, false, 10000),
                      new IndexInfo("orders", "idx_status", "status", 1, true, 10))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM orders WHERE status = 'pending' ORDER BY created_at DESC"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      List<Issue> orderByIssues =
          issues.stream()
              .filter(
                  i ->
                      i.type() == IssueType.MISSING_ORDER_BY_INDEX
                          && "created_at".equals(i.column()))
              .toList();
      assertThat(orderByIssues).hasSize(1);
      assertThat(orderByIssues.get(0).severity()).isEqualTo(Severity.INFO);
    }

    @Test
    void orderByStillWarningWithoutWhereIndex() {
      // When there's no WHERE index at all, ORDER BY should remain WARNING
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of("orders", List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 10000))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries = List.of(record("SELECT * FROM orders ORDER BY created_at DESC"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      List<Issue> orderByIssues =
          issues.stream()
              .filter(
                  i ->
                      i.type() == IssueType.MISSING_ORDER_BY_INDEX
                          && "created_at".equals(i.column()))
              .toList();
      assertThat(orderByIssues).hasSize(1);
      assertThat(orderByIssues.get(0).severity()).isEqualTo(Severity.WARNING);
    }

    // ── Improvement 4: Improved suggestion text ─────────────────

    @Test
    void suggestsCompositeIndexWhenIndexedWhereColumnExists() {
      // When WHERE has an indexed column and an unindexed column,
      // suggestion should propose a composite index extending the existing one.
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "orders",
                  List.of(
                      new IndexInfo("orders", "PRIMARY", "id", 1, false, 10000),
                      new IndexInfo("orders", "idx_user_id", "user_id", 1, true, 5000))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM orders WHERE user_id = 42 AND total_amount > 100"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      List<Issue> amountIssues =
          issues.stream()
              .filter(
                  i ->
                      i.type() == IssueType.MISSING_WHERE_INDEX
                          && "total_amount".equals(i.column()))
              .toList();
      assertThat(amountIssues).hasSize(1);
      assertThat(amountIssues.get(0).suggestion()).contains("user_id", "total_amount");
      assertThat(amountIssues.get(0).suggestion()).contains("composite");
    }

    @Test
    void suggestsCompositeIndexForWhereAndOrderBy() {
      // When WHERE has an indexed column and ORDER BY has an unindexed column,
      // the ORDER BY suggestion should propose composite (where_col, order_col).
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "orders",
                  List.of(
                      new IndexInfo("orders", "PRIMARY", "id", 1, false, 10000),
                      new IndexInfo("orders", "idx_status", "status", 1, true, 10))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM orders WHERE status = 'pending' ORDER BY created_at DESC"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      List<Issue> orderByIssues =
          issues.stream()
              .filter(
                  i ->
                      i.type() == IssueType.MISSING_ORDER_BY_INDEX
                          && "created_at".equals(i.column()))
              .toList();
      assertThat(orderByIssues).hasSize(1);
      assertThat(orderByIssues.get(0).suggestion()).contains("status", "created_at");
    }

    @Test
    void suggestsStandaloneIndexWhenNoOtherIndexedWhereColumn() {
      // When there is no indexed WHERE column, suggest a standalone index.
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 1000))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM users WHERE email = 'test@example.com'"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      List<Issue> emailIssues =
          issues.stream()
              .filter(i -> i.type() == IssueType.MISSING_WHERE_INDEX && "email".equals(i.column()))
              .toList();
      assertThat(emailIssues).hasSize(1);
      assertThat(emailIssues.get(0).suggestion()).contains("idx_email");
      assertThat(emailIssues.get(0).suggestion()).contains("ADD INDEX");
    }

    // ── Hibernate 6 alias resolution ──────────────────────────────────

    @Test
    void detectsMissingWhereIndexWithHibernate6Alias() {
      // Hibernate 6 generates aliases like u1_0, o1_0, etc.
      // The detector should resolve u1_0 -> users and detect missing index on 'email'.
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 1000))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries =
          List.of(
              record(
                  "SELECT u1_0.id, u1_0.name FROM users u1_0 WHERE u1_0.email = 'test@example.com'"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      assertThat(issues)
          .anyMatch(
              i ->
                  i.type() == IssueType.MISSING_WHERE_INDEX
                      && "email".equals(i.column())
                      && "users".equals(i.table()));
    }

    @Test
    void detectsMissingJoinIndexWithMultipleHibernate6Aliases() {
      // Multiple Hibernate 6 aliases: u1_0 -> users, o1_0 -> orders
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "users",
                  List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 1000)),
                  "orders",
                  List.of(new IndexInfo("orders", "PRIMARY", "id", 1, false, 5000))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries =
          List.of(
              record(
                  "SELECT u1_0.id, o1_0.total FROM users u1_0 JOIN orders o1_0 ON u1_0.id = o1_0.user_id"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      // o1_0.user_id should be flagged as missing join index on 'orders' table
      assertThat(issues)
          .anyMatch(
              i ->
                  i.type() == IssueType.MISSING_JOIN_INDEX
                      && "user_id".equals(i.column())
                      && "orders".equals(i.table()));
    }

    @Test
    void hibernate6AliasResolverMapsSubqueryAliases() {
      // Verify that resolveAliases correctly maps subquery aliases too.
      // Note: extractWhereColumnsWithOperators removes subqueries before parsing,
      // so subquery WHERE columns are not checked by MissingIndexDetector.
      // This test verifies the alias map itself is correct.
      Map<String, String> aliases =
          MissingIndexDetector.resolveAliases(
              "SELECT u1_0.id FROM users u1_0 WHERE u1_0.id IN (SELECT s1_0.user_id FROM subscriptions s1_0 WHERE s1_0.plan = 'premium')");

      assertThat(aliases).containsEntry("u1_0", "users");
      assertThat(aliases).containsEntry("users", "users");
      assertThat(aliases).containsEntry("s1_0", "subscriptions");
      assertThat(aliases).containsEntry("subscriptions", "subscriptions");
    }

    @Test
    void detectsMissingIndexInSelfJoinWithHibernate6Aliases() {
      // Self-join: u1_0 -> users, u2_0 -> users
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "users",
                  List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 1000))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<QueryRecord> queries =
          List.of(
              record(
                  "SELECT u1_0.id, u2_0.name FROM users u1_0 JOIN users u2_0 ON u1_0.manager_id = u2_0.id"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      // u1_0.manager_id should be flagged as missing join index on 'users' table
      assertThat(issues)
          .anyMatch(
              i ->
                  i.type() == IssueType.MISSING_JOIN_INDEX
                      && "manager_id".equals(i.column())
                      && "users".equals(i.table()));
    }
  }

  // ── CompositeIndexDetector ──────────────────────────────────────────

  @Nested
  class CompositeIndexDetectorTest {

    @Test
    void detectsNonLeadingColumnUsage() {
      // Composite index on (status, created_at), but query uses only created_at
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "orders",
                  List.of(
                      new IndexInfo("orders", "idx_status_created", "status", 1, true, 100),
                      new IndexInfo("orders", "idx_status_created", "created_at", 2, true, 100))));

      CompositeIndexDetector detector = new CompositeIndexDetector();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM orders WHERE created_at > '2024-01-01'"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.COMPOSITE_INDEX_LEADING_COLUMN);
    }

    @Test
    void doesNotDetectWhenLeadingColumnUsed() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "orders",
                  List.of(
                      new IndexInfo("orders", "idx_status_created", "status", 1, true, 100),
                      new IndexInfo("orders", "idx_status_created", "created_at", 2, true, 100))));

      CompositeIndexDetector detector = new CompositeIndexDetector();
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM orders WHERE status = 'active' AND created_at > '2024-01-01'"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      assertThat(issues).isEmpty();
    }

    @Test
    void skipsWhenNoIndexMetadata() {
      CompositeIndexDetector detector = new CompositeIndexDetector();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM orders WHERE created_at > '2024-01-01'"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    void skipsNonSelectQueries() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "orders",
                  List.of(
                      new IndexInfo("orders", "idx_status_created", "status", 1, true, 100),
                      new IndexInfo("orders", "idx_status_created", "created_at", 2, true, 100))));

      CompositeIndexDetector detector = new CompositeIndexDetector();
      List<QueryRecord> queries =
          List.of(record("UPDATE orders SET status = 'shipped' WHERE created_at > '2024-01-01'"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      assertThat(issues).isEmpty();
    }

    @Test
    void skipsWhenStandaloneIndexExistsForNonLeadingColumn() {
      // Bug 4: story_bookmarks has composite (story_id, user_id) but also
      // standalone idx_story_bookmarks_user(user_id). Should not flag user_id.
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "story_bookmarks",
                  List.of(
                      new IndexInfo("story_bookmarks", "idx_story_user", "story_id", 1, true, 100),
                      new IndexInfo("story_bookmarks", "idx_story_user", "user_id", 2, true, 100),
                      new IndexInfo(
                          "story_bookmarks", "idx_story_bookmarks_user", "user_id", 1, true, 50))));

      CompositeIndexDetector detector = new CompositeIndexDetector();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM story_bookmarks WHERE user_id = 42"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      assertThat(issues).isEmpty();
    }

    @Test
    void skipsWhenColumnIsLeadingInAnotherCompositeIndex() {
      // Bug 4: Column is non-leading in one composite but leading in another
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "story_bookmarks",
                  List.of(
                      new IndexInfo("story_bookmarks", "idx_story_user", "story_id", 1, true, 100),
                      new IndexInfo("story_bookmarks", "idx_story_user", "user_id", 2, true, 100),
                      new IndexInfo("story_bookmarks", "idx_user_created", "user_id", 1, true, 50),
                      new IndexInfo(
                          "story_bookmarks", "idx_user_created", "created_at", 2, true, 50))));

      CompositeIndexDetector detector = new CompositeIndexDetector();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM story_bookmarks WHERE user_id = 42"));

      List<Issue> issues = detector.evaluate(queries, metadata);
      assertThat(issues).isEmpty();
    }
  }
}
