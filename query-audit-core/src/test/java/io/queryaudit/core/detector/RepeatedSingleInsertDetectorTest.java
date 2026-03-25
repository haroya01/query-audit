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

class RepeatedSingleInsertDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private final RepeatedSingleInsertDetector detector = new RepeatedSingleInsertDetector();

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private static List<QueryRecord> repeat(String sql, int times) {
    List<QueryRecord> records = new ArrayList<>();
    for (int i = 0; i < times; i++) {
      records.add(record(sql));
    }
    return records;
  }

  // ── Positive: Issues detected ───────────────────────────────────────

  @Nested
  @DisplayName("Detects repeated single INSERT")
  class PositiveCases {

    @Test
    @DisplayName("Detects INSERT repeated 3 times (at threshold)")
    void detectsAtThreshold() {
      List<QueryRecord> queries =
          repeat("INSERT INTO users (name, email) VALUES ('Alice', 'alice@test.com')", 3);

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.REPEATED_SINGLE_INSERT);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
      assertThat(issues.get(0).table()).isEqualTo("users");
    }

    @Test
    @DisplayName("Detects INSERT repeated 10 times")
    void detectsAboveThreshold() {
      List<QueryRecord> queries =
          repeat("INSERT INTO orders (product_id, quantity) VALUES (1, 5)", 10);

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).detail()).contains("10 times");
    }

    @Test
    @DisplayName("Detail includes execution count")
    void detailIncludesCount() {
      List<QueryRecord> queries = repeat("INSERT INTO logs (message) VALUES ('test')", 7);

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues.get(0).detail()).contains("7 times");
      assertThat(issues.get(0).detail()).contains("logs");
    }

    @Test
    @DisplayName("Suggestion mentions batch insert")
    void suggestionMentionsBatch() {
      List<QueryRecord> queries = repeat("INSERT INTO users (name) VALUES ('test')", 5);

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues.get(0).suggestion()).contains("batch");
    }

    @Test
    @DisplayName("Different tables produce separate issues")
    void differentTables() {
      List<QueryRecord> queries = new ArrayList<>();
      queries.addAll(repeat("INSERT INTO users (name) VALUES ('Alice')", 3));
      queries.addAll(repeat("INSERT INTO orders (total) VALUES (100)", 3));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(2);
    }
  }

  // ── Negative: No issues ─────────────────────────────────────────────

  @Nested
  @DisplayName("No issue cases")
  class NegativeCases {

    @Test
    @DisplayName("No issue when below threshold (2 inserts)")
    void belowThreshold() {
      List<QueryRecord> queries = repeat("INSERT INTO users (name) VALUES ('Alice')", 2);

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("No issue for single INSERT")
    void singleInsert() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("INSERT INTO users (name) VALUES ('Alice')")), EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Ignores multi-row INSERT (already batched)")
    void ignoresMultiRowInsert() {
      String sql = "INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Charlie')";
      List<QueryRecord> queries = repeat(sql, 5);

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Ignores SELECT queries")
    void ignoresSelectQueries() {
      List<QueryRecord> queries = repeat("SELECT * FROM users", 10);

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Ignores UPDATE queries")
    void ignoresUpdateQueries() {
      List<QueryRecord> queries = repeat("UPDATE users SET name = 'test' WHERE id = 1", 10);

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

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
    @DisplayName("Custom threshold works")
    void customThreshold() {
      RepeatedSingleInsertDetector strict = new RepeatedSingleInsertDetector(2);

      List<QueryRecord> queries = repeat("INSERT INTO users (name) VALUES ('test')", 2);

      List<Issue> issues = strict.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Different column values normalize to same pattern")
    void normalizesToSamePattern() {
      List<QueryRecord> queries =
          List.of(
              record("INSERT INTO users (name) VALUES ('Alice')"),
              record("INSERT INTO users (name) VALUES ('Bob')"),
              record("INSERT INTO users (name) VALUES ('Charlie')"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      // All three normalize to: insert into users (name) values (?)
      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("INSERT...SELECT is treated separately from INSERT...VALUES")
    void insertSelectNotConfusedWithInsertValues() {
      List<QueryRecord> queries = new ArrayList<>();
      queries.addAll(repeat("INSERT INTO archive SELECT * FROM orders WHERE id = 1", 5));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      // INSERT...SELECT queries should still be counted but as their own group
      // (they don't have VALUES so they pass the multi-row check)
      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("No false positive for INSERT below threshold")
    void noFalsePositiveBelowDefaultThreshold() {
      List<QueryRecord> queries = repeat("INSERT INTO users (name) VALUES (?)", 2);

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Different column sets are separate patterns")
    void differentColumnSets() {
      List<QueryRecord> queries = new ArrayList<>();
      // Pattern 1: name only
      queries.addAll(repeat("INSERT INTO users (name) VALUES ('test')", 3));
      // Pattern 2: name + email
      queries.addAll(repeat("INSERT INTO users (name, email) VALUES ('test', 'test@test.com')", 3));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

      // Two different normalized patterns → two issues
      assertThat(issues).hasSize(2);
    }
  }
}
