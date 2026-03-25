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
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class CollectionManagementDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private final CollectionManagementDetector detector = new CollectionManagementDetector();

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  // ── Positive: Issues detected ───────────────────────────────────────

  @Nested
  @DisplayName("Detects DELETE-all + re-INSERT pattern")
  class PositiveCases {

    @Test
    @DisplayName("Detects classic unidirectional @OneToMany pattern")
    void detectsClassicPattern() {
      List<QueryRecord> queries =
          List.of(
              record("DELETE FROM team_members WHERE team_id = 1"),
              record("INSERT INTO team_members (team_id, member_id) VALUES (1, 10)"),
              record("INSERT INTO team_members (team_id, member_id) VALUES (1, 20)"),
              record("INSERT INTO team_members (team_id, member_id) VALUES (1, 30)"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.COLLECTION_DELETE_REINSERT);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
      assertThat(issues.get(0).table()).isEqualTo("team_members");
      assertThat(issues.get(0).column()).isEqualTo("team_id");
    }

    @Test
    @DisplayName("Detects @ManyToMany join table pattern")
    void detectsManyToManyPattern() {
      List<QueryRecord> queries =
          List.of(
              record("DELETE FROM user_roles WHERE user_id = ?"),
              record("INSERT INTO user_roles (user_id, role_id) VALUES (?, ?)"),
              record("INSERT INTO user_roles (user_id, role_id) VALUES (?, ?)"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).table()).isEqualTo("user_roles");
    }

    @Test
    @DisplayName("Detail includes insert count")
    void detailIncludesInsertCount() {
      List<QueryRecord> queries =
          List.of(
              record("DELETE FROM tags WHERE post_id = 5"),
              record("INSERT INTO tags (post_id, tag_name) VALUES (5, 'java')"),
              record("INSERT INTO tags (post_id, tag_name) VALUES (5, 'spring')"),
              record("INSERT INTO tags (post_id, tag_name) VALUES (5, 'jpa')"),
              record("INSERT INTO tags (post_id, tag_name) VALUES (5, 'hibernate')"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).detail()).contains("4 re-INSERTs");
    }

    @Test
    @DisplayName("Suggestion mentions Set<> and bidirectional mapping")
    void suggestionMentionsFix() {
      List<QueryRecord> queries =
          List.of(
              record("DELETE FROM order_items WHERE order_id = 1"),
              record("INSERT INTO order_items (order_id, product_id) VALUES (1, 100)"),
              record("INSERT INTO order_items (order_id, product_id) VALUES (1, 200)"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues.get(0).suggestion()).contains("Set<>");
      assertThat(issues.get(0).suggestion()).contains("bidirectional");
    }

    @Test
    @DisplayName("Handles backtick-quoted table names")
    void handlesBacktickQuotedTables() {
      List<QueryRecord> queries =
          List.of(
              record("DELETE FROM `team_members` WHERE team_id = 1"),
              record("INSERT INTO `team_members` (team_id, member_id) VALUES (1, 10)"),
              record("INSERT INTO `team_members` (team_id, member_id) VALUES (1, 20)"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }
  }

  // ── Negative: No issues ─────────────────────────────────────────────

  @Nested
  @DisplayName("No issue cases")
  class NegativeCases {

    @Test
    @DisplayName("No issue when DELETE has multiple WHERE columns (composite key)")
    void noIssueWithMultipleWhereColumns() {
      List<QueryRecord> queries =
          List.of(
              record("DELETE FROM team_members WHERE team_id = 1 AND member_id = 10"),
              record("INSERT INTO team_members (team_id, member_id) VALUES (1, 20)"),
              record("INSERT INTO team_members (team_id, member_id) VALUES (1, 30)"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue when only one INSERT follows DELETE")
    void noIssueWithSingleInsert() {
      List<QueryRecord> queries =
          List.of(
              record("DELETE FROM team_members WHERE team_id = 1"),
              record("INSERT INTO team_members (team_id, member_id) VALUES (1, 10)"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue when INSERTs are to a different table")
    void noIssueWithDifferentTable() {
      List<QueryRecord> queries =
          List.of(
              record("DELETE FROM team_members WHERE team_id = 1"),
              record("INSERT INTO audit_log (action, entity_id) VALUES ('delete', 1)"),
              record("INSERT INTO audit_log (action, entity_id) VALUES ('insert', 2)"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue when DELETE has no WHERE clause")
    void noIssueWithoutWhere() {
      List<QueryRecord> queries =
          List.of(
              record("DELETE FROM team_members"),
              record("INSERT INTO team_members (team_id, member_id) VALUES (1, 10)"),
              record("INSERT INTO team_members (team_id, member_id) VALUES (1, 20)"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Empty query list")
    void emptyQueryList() {
      List<Issue> issues = detector.evaluate(List.of(), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Only SELECT queries")
    void onlySelectQueries() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM team_members WHERE team_id = 1"),
              record("SELECT * FROM users WHERE id = 10"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }

  // ── Edge Cases ──────────────────────────────────────────────────────

  @Nested
  @DisplayName("Edge Cases")
  class EdgeCases {

    @Test
    @DisplayName("Non-consecutive INSERTs interrupted by other query type stops counting")
    void interruptedByOtherQuery() {
      List<QueryRecord> queries =
          List.of(
              record("DELETE FROM team_members WHERE team_id = 1"),
              record("INSERT INTO team_members (team_id, member_id) VALUES (1, 10)"),
              record("UPDATE teams SET member_count = 3 WHERE id = 1"),
              record("INSERT INTO team_members (team_id, member_id) VALUES (1, 20)"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      // Only 1 INSERT before the interruption
      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Multiple DELETE+re-INSERT patterns for different tables")
    void multiplePatternsForDifferentTables() {
      List<QueryRecord> queries = new ArrayList<>();
      queries.add(record("DELETE FROM team_members WHERE team_id = 1"));
      queries.add(record("INSERT INTO team_members (team_id, member_id) VALUES (1, 10)"));
      queries.add(record("INSERT INTO team_members (team_id, member_id) VALUES (1, 20)"));
      queries.add(record("DELETE FROM user_roles WHERE user_id = 5"));
      queries.add(record("INSERT INTO user_roles (user_id, role_id) VALUES (5, 1)"));
      queries.add(record("INSERT INTO user_roles (user_id, role_id) VALUES (5, 2)"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(2);
    }

    @Test
    @DisplayName("Same table flagged only once")
    void sameTableFlaggedOnce() {
      List<QueryRecord> queries = new ArrayList<>();
      // First batch
      queries.add(record("DELETE FROM team_members WHERE team_id = 1"));
      queries.add(record("INSERT INTO team_members (team_id, member_id) VALUES (1, 10)"));
      queries.add(record("INSERT INTO team_members (team_id, member_id) VALUES (1, 20)"));
      // Second batch for same table
      queries.add(record("DELETE FROM team_members WHERE team_id = 2"));
      queries.add(record("INSERT INTO team_members (team_id, member_id) VALUES (2, 30)"));
      queries.add(record("INSERT INTO team_members (team_id, member_id) VALUES (2, 40)"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Custom minimum inserts threshold")
    void customThreshold() {
      CollectionManagementDetector strict = new CollectionManagementDetector(5);

      List<QueryRecord> queries =
          List.of(
              record("DELETE FROM team_members WHERE team_id = 1"),
              record("INSERT INTO team_members (team_id, member_id) VALUES (1, 10)"),
              record("INSERT INTO team_members (team_id, member_id) VALUES (1, 20)"),
              record("INSERT INTO team_members (team_id, member_id) VALUES (1, 30)"));

      List<Issue> issues = strict.evaluate(queries, EMPTY_INDEX);

      // Only 3 INSERTs, threshold is 5
      assertThat(issues).isEmpty();
    }
  }
}
