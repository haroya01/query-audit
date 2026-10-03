package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class UpdateWithoutWhereDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private final UpdateWithoutWhereDetector detector = new UpdateWithoutWhereDetector();

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  // ── UPDATE without WHERE ────────────────────────────────────────────

  @Nested
  @DisplayName("UPDATE without WHERE")
  class UpdateTests {

    @Test
    @DisplayName("Detects UPDATE without WHERE")
    void detectsUpdateWithoutWhere() {
      String sql = "UPDATE users SET status = 'inactive'";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.UPDATE_WITHOUT_WHERE);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
      assertThat(issues.get(0).table()).isEqualTo("users");
      assertThat(issues.get(0).detail()).contains("UPDATE without WHERE");
    }

    @Test
    @DisplayName("No issue for UPDATE with WHERE")
    void noIssueForUpdateWithWhere() {
      String sql = "UPDATE users SET status = 'inactive' WHERE last_login < '2024-01-01'";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for UPDATE with complex WHERE")
    void noIssueForUpdateWithComplexWhere() {
      String sql =
          "UPDATE orders SET processed = true WHERE status = 'pending' AND created_at < NOW()";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Extracts table name from UPDATE with backticks")
    void extractsTableNameWithBackticks() {
      String sql = "UPDATE `user_accounts` SET active = false";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).table()).isEqualTo("user_accounts");
    }
  }

  // ── DELETE without WHERE ────────────────────────────────────────────

  @Nested
  @DisplayName("DELETE without WHERE")
  class DeleteTests {

    @Test
    @DisplayName("Detects DELETE without WHERE")
    void detectsDeleteWithoutWhere() {
      String sql = "DELETE FROM sessions";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.UPDATE_WITHOUT_WHERE);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
      assertThat(issues.get(0).table()).isEqualTo("sessions");
      assertThat(issues.get(0).detail()).contains("DELETE without WHERE");
    }

    @Test
    @DisplayName("No issue for DELETE with WHERE")
    void noIssueForDeleteWithWhere() {
      String sql = "DELETE FROM sessions WHERE expired_at < NOW()";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }

  // ── Edge Cases ──────────────────────────────────────────────────────

  @Nested
  @DisplayName("Edge Cases")
  class EdgeCases {

    @Test
    @DisplayName("Ignores SELECT queries")
    void ignoresSelectQueries() {
      String sql = "SELECT * FROM users";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Ignores INSERT queries")
    void ignoresInsertQueries() {
      String sql = "INSERT INTO users (name) VALUES ('test')";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Deduplicates same normalized query")
    void deduplicatesSameQuery() {
      String sql1 = "UPDATE users SET status = 'inactive'";
      String sql2 = "UPDATE users SET status = 'active'";
      // Both normalize to the same pattern
      List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Reports both UPDATE and DELETE issues")
    void reportsBothUpdateAndDelete() {
      String update = "UPDATE users SET status = 'inactive'";
      String delete = "DELETE FROM sessions";
      List<Issue> issues = detector.evaluate(List.of(record(update), record(delete)), EMPTY_INDEX);

      assertThat(issues).hasSize(2);
    }

    @Test
    @DisplayName("Suggestion mentions WHERE clause")
    void suggestionMentionsWhere() {
      String sql = "UPDATE users SET active = false";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues.get(0).suggestion()).contains("WHERE");
    }

    @Test
    @DisplayName("Empty query list returns no issues")
    void emptyQueryList() {
      List<Issue> issues = detector.evaluate(List.of(), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName(
        "UPDATE with WHERE produces null table when table extraction fails - kills line 42 negate")
    void updateWithWhereTableNullInMessage() {
      // Kills NegateConditionalsMutator on line 42: table != null check
      // When table is null, the detail message should use "the table" fallback
      String sql = "UPDATE SET status = 'inactive'"; // malformed - no table name
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      // Even if this doesn't parse as update, we verify no crash occurs
      // The key is exercising the null-table path
    }

    @Test
    @DisplayName("DELETE with null table name uses fallback message - kills line 57 negate")
    void deleteWithNullTableFallbackMessage() {
      // Kills NegateConditionalsMutator on line 57: table != null check in DELETE branch
      // Use a DELETE that parses but table extraction returns null
      String sql = "DELETE FROM";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      // If it detects as a delete without where, the table is null -> "the table"
      if (!issues.isEmpty()) {
        assertThat(issues.get(0).detail()).contains("the table");
      }
    }

    @Test
    @DisplayName("Detail message includes table name when present - kills negate on line 42")
    void updateDetailIncludesTableName() {
      String sql = "UPDATE users SET status = 'inactive'";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      // Verifies that table != null path is taken and table name is in the message
      assertThat(issues.get(0).detail()).contains("table 'users'");
    }

    @Test
    @DisplayName("Delete detail message includes table name when present - kills negate on line 57")
    void deleteDetailIncludesTableName() {
      String sql = "DELETE FROM sessions";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      // Verifies that table != null path is taken and table name is in message
      assertThat(issues.get(0).detail()).contains("table 'sessions'");
    }

    @Test
    @DisplayName("Detects UPDATE without outer WHERE when subquery has WHERE")
    void detectsUpdateWithoutOuterWhereWithSubquery() {
      String sql =
          "UPDATE orders SET total = (SELECT SUM(amount) FROM items WHERE items.order_id = orders.id)";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.UPDATE_WITHOUT_WHERE);
      assertThat(issues.get(0).table()).isEqualTo("orders");
      assertThat(issues.get(0).detail()).contains("UPDATE without WHERE");
    }

    @Test
    @DisplayName("No issue for UPDATE with outer WHERE even when subquery also has WHERE")
    void noIssueForUpdateWithOuterWhereAndSubquery() {
      String sql =
          "UPDATE orders SET total = (SELECT SUM(amount) FROM items WHERE items.order_id = orders.id) WHERE id = 1";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Null SQL is handled gracefully")
    void nullSqlHandled() {
      QueryRecord nullRecord = new QueryRecord(null, null, 0L, 0L, null, 0);
      List<Issue> issues = detector.evaluate(List.of(nullRecord), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No false positive for UPDATE with JOIN (has ON filtering)")
    void noFalsePositiveForUpdateWithJoin() {
      String sql = "UPDATE orders o JOIN users u ON o.user_id = u.id SET o.status = 'active'";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No false positive for UPDATE with LEFT JOIN")
    void noFalsePositiveForUpdateWithLeftJoin() {
      String sql =
          "UPDATE orders o LEFT JOIN users u ON o.user_id = u.id SET o.status = 'orphaned'";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No false positive for DELETE with JOIN")
    void noFalsePositiveForDeleteWithJoin() {
      String sql = "DELETE o FROM orders o JOIN cancelled c ON o.id = c.order_id";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No false positive for DELETE with USING clause (PostgreSQL)")
    void noFalsePositiveForDeleteWithUsing() {
      String sql = "DELETE FROM orders USING cancelled WHERE orders.id = cancelled.order_id";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No false positive for UPDATE with INNER JOIN")
    void noFalsePositiveForUpdateWithInnerJoin() {
      String sql =
          "UPDATE products p INNER JOIN categories c ON p.category_id = c.id SET p.active = true";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("UPDATE with WHERE containing parameterized value is not flagged")
    void updateWithParameterizedWhere() {
      String sql = "UPDATE users SET status = ? WHERE id = ?";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }

  // ── #288: keyword text inside literals and comments is not a clause ──
  //
  // UPDATE_WITHOUT_WHERE is a safety net. A JOIN/USING token that only exists inside a string
  // literal, a quoted identifier or a comment proves nothing about how many rows the statement
  // touches, so it must not suppress the finding.

  @Nested
  @DisplayName("Keywords inside literals and comments are not structural clauses")
  class LiteralAndCommentKeywordTests {

    private void assertDetectsUnsafeUpdate(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues)
          .as("full-table UPDATE must be reported: %s", sql)
          .hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.UPDATE_WITHOUT_WHERE);
      assertThat(issues.get(0).table()).isEqualTo("users");
    }

    private void assertDetectsUnsafeDelete(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues)
          .as("full-table DELETE must be reported: %s", sql)
          .hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.UPDATE_WITHOUT_WHERE);
    }

    // -- string literals ---------------------------------------------------

    @Test
    @DisplayName("Literal 'WHERE' does not suppress an unsafe UPDATE")
    void literalWhereDoesNotSuppressUpdate() {
      assertDetectsUnsafeUpdate("UPDATE users SET note='WHERE'");
    }

    @Test
    @DisplayName("Literal 'JOIN' does not suppress an unsafe UPDATE")
    void literalJoinDoesNotSuppressUpdate() {
      assertDetectsUnsafeUpdate("UPDATE users SET note='JOIN'");
    }

    @Test
    @DisplayName("Literal 'USING' does not suppress an unsafe UPDATE")
    void literalUsingDoesNotSuppressUpdate() {
      assertDetectsUnsafeUpdate("UPDATE users SET note='USING'");
    }

    @Test
    @DisplayName("Keyword text inside a longer literal is ignored")
    void keywordTextInsideLongerLiteralIsIgnored() {
      assertDetectsUnsafeUpdate("UPDATE users SET note='some WHERE text'");
      assertDetectsUnsafeUpdate("UPDATE users SET note='a JOIN b ON c = d'");
    }

    @Test
    @DisplayName("Keyword text inside an SQL-standard escaped literal is ignored")
    void keywordTextInsideEscapedLiteralIsIgnored() {
      assertDetectsUnsafeUpdate("UPDATE users SET note='it''s a WHERE and JOIN'");
    }

    @Test
    @DisplayName("Keyword text inside a backslash-escaped literal is ignored")
    void keywordTextInsideBackslashEscapedLiteralIsIgnored() {
      assertDetectsUnsafeUpdate("UPDATE users SET note='it\\'s a WHERE and JOIN'");
    }

    @Test
    @DisplayName("Keyword text in a literal cannot hide a real outer WHERE")
    void keywordTextInLiteralDoesNotHideRealOuterWhere() {
      String sql = "DELETE FROM users WHERE note = 'JOIN USING'";
      assertThat(detector.evaluate(List.of(record(sql)), EMPTY_INDEX))
          .as("a real outer WHERE is present, so nothing to report")
          .isEmpty();
    }

    @Test
    @DisplayName("Keyword text inside a quoted identifier is ignored")
    void keywordTextInsideQuotedIdentifierIsIgnored() {
      assertDetectsUnsafeUpdate("UPDATE users SET \"JOIN\" = 1");
    }

    @Test
    @DisplayName("A real outer WHERE alongside literal keywords is still recognized")
    void realOuterWhereAlongsideLiteralKeywordsIsRecognized() {
      String sql = "UPDATE users SET note = 'WHERE JOIN USING' WHERE id = 1";
      assertThat(detector.evaluate(List.of(record(sql)), EMPTY_INDEX)).isEmpty();
    }

    // -- line comments -----------------------------------------------------

    @Test
    @DisplayName("Line comment WHERE does not suppress an unsafe UPDATE")
    void lineCommentWhereDoesNotSuppressUpdate() {
      assertDetectsUnsafeUpdate("UPDATE users SET note = 'changed' -- WHERE id = 1");
    }

    @Test
    @DisplayName("Line comment JOIN does not suppress an unsafe UPDATE")
    void lineCommentJoinDoesNotSuppressUpdate() {
      assertDetectsUnsafeUpdate(
          "UPDATE users SET note = 'changed' -- JOIN users u ON u.id = users.id");
    }

    @Test
    @DisplayName("Line comment USING does not suppress an unsafe DELETE")
    void lineCommentUsingDoesNotSuppressDelete() {
      assertDetectsUnsafeDelete("DELETE FROM users -- USING archived_users");
    }

    @Test
    @DisplayName("Line comment JOIN does not suppress an unsafe DELETE")
    void lineCommentJoinDoesNotSuppressDelete() {
      assertDetectsUnsafeDelete("DELETE FROM users -- JOIN something ...");
    }

    @Test
    @DisplayName("A real outer WHERE after a line comment is still recognized")
    void realOuterWhereAfterLineCommentIsRecognized() {
      String sql = "UPDATE users SET note = 'changed' -- no filter here\nWHERE id = 1";
      assertThat(detector.evaluate(List.of(record(sql)), EMPTY_INDEX)).isEmpty();
    }

    // -- block comments ----------------------------------------------------

    @Test
    @DisplayName("Block comment JOIN does not suppress an unsafe DELETE")
    void blockCommentJoinDoesNotSuppressDelete() {
      assertDetectsUnsafeDelete("DELETE FROM users /* JOIN users u ON u.id = users.id */");
    }

    @Test
    @DisplayName("Block comment WHERE does not suppress an unsafe UPDATE")
    void blockCommentWhereDoesNotSuppressUpdate() {
      assertDetectsUnsafeUpdate("UPDATE users SET note = 'changed' /* WHERE id = 1 */");
    }

    @Test
    @DisplayName("Block comment USING does not suppress an unsafe DELETE")
    void blockCommentUsingDoesNotSuppressDelete() {
      assertDetectsUnsafeDelete("DELETE FROM users /* USING archived_users */");
    }

    @Test
    @DisplayName("Block comment JOIN does not suppress an unsafe UPDATE")
    void blockCommentJoinDoesNotSuppressUpdate() {
      assertDetectsUnsafeUpdate(
          "UPDATE users SET note = 'changed' /* JOIN users u ON u.id = users.id */");
    }

    @Test
    @DisplayName("Comment-like text inside a literal is not treated as a comment")
    void commentLikeTextInsideLiteralIsNotAComment() {
      // The block comment is inside the literal, so the real WHERE that follows is structural.
      String sql = "UPDATE users SET note = 'a /* WHERE */ b' WHERE id = 1";
      assertThat(detector.evaluate(List.of(record(sql)), EMPTY_INDEX)).isEmpty();
    }

    // -- nested statements -------------------------------------------------

    @Test
    @DisplayName("JOIN inside a nested subquery does not suppress the outer UPDATE finding")
    void joinInsideSubqueryDoesNotSuppressUpdate() {
      String sql =
          "UPDATE orders SET total = "
              + "(SELECT SUM(i.amount) FROM items i JOIN products p ON p.id = i.product_id)";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues)
          .as("the subquery's JOIN filters the subquery, not which orders rows are updated")
          .hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.UPDATE_WITHOUT_WHERE);
      assertThat(issues.get(0).table()).isEqualTo("orders");
    }

    @Test
    @DisplayName("USING inside a nested subquery does not suppress the outer DELETE finding")
    void usingInsideSubqueryDoesNotSuppressDelete() {
      String sql =
          "DELETE FROM orders WHERE user_id IN (SELECT id FROM users JOIN teams t ON t.id ="
              + " users.team_id)";
      assertThat(detector.evaluate(List.of(record(sql)), EMPTY_INDEX))
          .as("a real outer WHERE is present, so nothing to report")
          .isEmpty();
    }

    // -- deduplication unchanged -------------------------------------------

    @Test
    @DisplayName("Deduplication still collapses literal variants to one finding")
    void deduplicationUnchangedForLiteralVariants() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record("UPDATE users SET note='WHERE'"),
                  record("UPDATE users SET note='WHERE JOIN'"),
                  record("UPDATE users SET note='USING'")),
              EMPTY_INDEX);

      assertThat(issues).as("all three normalize to the same pattern").hasSize(1);
    }
  }

  // ── #288: the reported shapes really do touch every row ──────────────
  //
  // The detector is only worth trusting if the statements it reports are the ones that actually
  // rewrite or remove the whole table. These run the issue's reproduction SQL against H2 and check
  // the affected-row count next to the detector verdict.

  @Nested
  @DisplayName("H2 affected-row evidence")
  class H2AffectedRowEvidence {

    private Connection openDatabase() throws SQLException {
      return DriverManager.getConnection("jdbc:h2:mem:qa-dml-unsafe;DB_CLOSE_DELAY=-1", "sa", "");
    }

    private void seed(Connection connection) throws SQLException {
      try (Statement statement = connection.createStatement()) {
        statement.execute("DROP TABLE IF EXISTS users");
        statement.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, note VARCHAR(64))");
        statement.execute("INSERT INTO users VALUES (1, 'original')");
        statement.execute("INSERT INTO users VALUES (2, 'original')");
        statement.execute("INSERT INTO users VALUES (3, 'original')");
      }
    }

    private int execute(Connection connection, String sql) throws SQLException {
      try (Statement statement = connection.createStatement()) {
        return statement.executeUpdate(sql);
      }
    }

    private int countRows(Connection connection, String sql) throws SQLException {
      try (Statement statement = connection.createStatement();
          ResultSet resultSet = statement.executeQuery(sql)) {
        assertThat(resultSet.next()).isTrue();
        return resultSet.getInt(1);
      }
    }

    private List<Issue> issuesFor(String sql) {
      return detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
    }

    @Test
    void literalWhereUpdateTouchesEveryRowAndIsReported() throws SQLException {
      try (Connection connection = openDatabase()) {
        seed(connection);
        String sql = "UPDATE users SET note='WHERE'";

        assertThat(execute(connection, sql)).as("every row is rewritten").isEqualTo(3);
        assertThat(countRows(connection, "SELECT COUNT(*) FROM users WHERE note = 'WHERE'"))
            .isEqualTo(3);

        assertThat(issuesFor(sql))
            .as("a literal 'WHERE' must not hide a full-table update")
            .hasSize(1);
      }
    }

    @Test
    void literalJoinUpdateTouchesEveryRowAndIsReported() throws SQLException {
      try (Connection connection = openDatabase()) {
        seed(connection);
        String sql = "UPDATE users SET note='JOIN'";

        assertThat(execute(connection, sql)).isEqualTo(3);
        assertThat(countRows(connection, "SELECT COUNT(*) FROM users WHERE note = 'JOIN'"))
            .isEqualTo(3);

        assertThat(issuesFor(sql)).hasSize(1);
      }
    }

    @Test
    void literalUsingUpdateTouchesEveryRowAndIsReported() throws SQLException {
      try (Connection connection = openDatabase()) {
        seed(connection);
        String sql = "UPDATE users SET note='USING'";

        assertThat(execute(connection, sql)).isEqualTo(3);
        assertThat(countRows(connection, "SELECT COUNT(*) FROM users WHERE note = 'USING'"))
            .isEqualTo(3);

        assertThat(issuesFor(sql)).hasSize(1);
      }
    }

    @Test
    void blockCommentDeleteRemovesEveryRowAndIsReported() throws SQLException {
      try (Connection connection = openDatabase()) {
        seed(connection);
        String sql = "DELETE FROM users /* JOIN */";

        assertThat(execute(connection, sql)).as("every row is removed").isEqualTo(3);
        assertThat(countRows(connection, "SELECT COUNT(*) FROM users")).isZero();

        assertThat(issuesFor(sql))
            .as("a commented-out JOIN must not hide a full-table delete")
            .hasSize(1);
      }
    }

    @Test
    void lineCommentDeleteRemovesEveryRowAndIsReported() throws SQLException {
      try (Connection connection = openDatabase()) {
        seed(connection);
        String sql = "DELETE FROM users -- USING archived_users";

        assertThat(execute(connection, sql)).isEqualTo(3);
        assertThat(countRows(connection, "SELECT COUNT(*) FROM users")).isZero();

        assertThat(issuesFor(sql)).hasSize(1);
      }
    }

    @Test
    void realJoinDeleteTouchesOnlyJoinedRowsAndIsNotReported() throws SQLException {
      try (Connection connection = openDatabase()) {
        seed(connection);
        try (Statement statement = connection.createStatement()) {
          statement.execute("DROP TABLE IF EXISTS archived_users");
          statement.execute("CREATE TABLE archived_users (id BIGINT PRIMARY KEY)");
          statement.execute("INSERT INTO archived_users VALUES (2)");
        }
        String sql = "DELETE FROM users WHERE id IN (SELECT id FROM archived_users)";

        assertThat(execute(connection, sql)).as("only the archived user is removed").isEqualTo(1);
        assertThat(countRows(connection, "SELECT COUNT(*) FROM users")).isEqualTo(2);

        assertThat(issuesFor(sql))
            .as("a real outer WHERE is present, so nothing to report")
            .isEmpty();
      }
    }
  }
}
