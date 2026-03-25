package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ImplicitColumnsInsertDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private final ImplicitColumnsInsertDetector detector = new ImplicitColumnsInsertDetector();

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  // ── Positive: Issues detected ───────────────────────────────────────

  @Nested
  @DisplayName("Detects INSERT without explicit column list")
  class PositiveCases {

    @Test
    @DisplayName("Detects basic INSERT INTO table VALUES")
    void detectsBasicPattern() {
      String sql = "INSERT INTO users VALUES (1, 'Alice', 'alice@test.com')";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.IMPLICIT_COLUMNS_INSERT);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
      assertThat(issues.get(0).table()).isEqualTo("users");
    }

    @Test
    @DisplayName("Detects INSERT with placeholder values")
    void detectsPlaceholders() {
      String sql = "INSERT INTO orders VALUES (?, ?, ?, ?)";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Case insensitive detection")
    void caseInsensitive() {
      String sql = "insert into users values (1, 'test')";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Detail mentions fragility")
    void detailMentionsFragility() {
      String sql = "INSERT INTO users VALUES (1, 'test')";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues.get(0).detail()).contains("silently break");
    }

    @Test
    @DisplayName("Suggestion mentions column names")
    void suggestionMentionsColumnNames() {
      String sql = "INSERT INTO users VALUES (1, 'test')";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues.get(0).suggestion()).contains("column names");
    }
  }

  // ── Negative: No issues ─────────────────────────────────────────────

  @Nested
  @DisplayName("No issue cases")
  class NegativeCases {

    @Test
    @DisplayName("No issue for INSERT with explicit columns")
    void noIssueForExplicitColumns() {
      String sql = "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@test.com')";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for INSERT ... SELECT")
    void noIssueForInsertSelect() {
      String sql = "INSERT INTO archive SELECT * FROM orders";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for SELECT query")
    void noIssueForSelect() {
      String sql = "SELECT * FROM users";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for UPDATE query")
    void noIssueForUpdate() {
      String sql = "UPDATE users SET name = 'test' WHERE id = 1";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Empty query list")
    void emptyQueryList() {
      List<Issue> issues = detector.evaluate(List.of(), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  // ── Edge Cases ──────────────────────────────────────────────────────

  @Nested
  @DisplayName("Edge Cases")
  class EdgeCases {

    @Test
    @DisplayName("Deduplicates same normalized query")
    void deduplicates() {
      String sql1 = "INSERT INTO users VALUES (1, 'Alice')";
      String sql2 = "INSERT INTO users VALUES (2, 'Bob')";
      List<Issue> issues = detector.evaluate(List.of(record(sql1), record(sql2)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Multiple whitespace between keywords")
    void multipleWhitespace() {
      String sql = "INSERT   INTO   users   VALUES (1, 'test')";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Detects INSERT with backtick-quoted table name without column list")
    void detectsBacktickQuotedTable() {
      String sql = "INSERT INTO `users` VALUES (1, 'Alice', 'alice@test.com')";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.IMPLICIT_COLUMNS_INSERT);
    }

    @Test
    @DisplayName("No false positive for INSERT with backtick-quoted table and explicit columns")
    void noIssueForBacktickQuotedTableWithColumns() {
      String sql = "INSERT INTO `users` (id, name) VALUES (1, 'Alice')";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }
  }
}
