package io.queryaudit.core.regression;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Comprehensive tests for the query count regression detection system, covering all query types
 * (SELECT, INSERT, UPDATE, DELETE) now that DML queries are captured.
 */
class RegressionDetectionTest {

  // ── Helper ───────────────────────────────────────────────────────────

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 1_000_000L, System.currentTimeMillis(), "");
  }

  // =====================================================================
  // QueryCounts.from()
  // =====================================================================

  @Nested
  class QueryCountsFromTest {

    @Test
    void correctlyCountsSelectQueries() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users"),
              record("SELECT id FROM rooms"),
              record("SELECT name FROM messages"));

      QueryCounts counts = QueryCounts.from(queries);

      assertThat(counts.selectCount()).isEqualTo(3);
      assertThat(counts.insertCount()).isEqualTo(0);
      assertThat(counts.updateCount()).isEqualTo(0);
      assertThat(counts.deleteCount()).isEqualTo(0);
    }

    @Test
    void correctlyCountsInsertQueries() {
      List<QueryRecord> queries =
          List.of(
              record("INSERT INTO users (name) VALUES ('alice')"),
              record("INSERT INTO rooms (title) VALUES ('general')"),
              record("INSERT INTO messages (body) VALUES ('hello')"));

      QueryCounts counts = QueryCounts.from(queries);

      assertThat(counts.insertCount()).isEqualTo(3);
      assertThat(counts.selectCount()).isEqualTo(0);
      assertThat(counts.updateCount()).isEqualTo(0);
      assertThat(counts.deleteCount()).isEqualTo(0);
    }

    @Test
    void correctlyCountsUpdateQueries() {
      List<QueryRecord> queries =
          List.of(
              record("UPDATE users SET name = 'bob' WHERE id = 1"),
              record("UPDATE rooms SET title = 'random' WHERE id = 2"));

      QueryCounts counts = QueryCounts.from(queries);

      assertThat(counts.updateCount()).isEqualTo(2);
      assertThat(counts.selectCount()).isEqualTo(0);
      assertThat(counts.insertCount()).isEqualTo(0);
      assertThat(counts.deleteCount()).isEqualTo(0);
    }

    @Test
    void correctlyCountsDeleteQueries() {
      List<QueryRecord> queries =
          List.of(
              record("DELETE FROM users WHERE id = 1"),
              record("DELETE FROM messages WHERE room_id = 5"));

      QueryCounts counts = QueryCounts.from(queries);

      assertThat(counts.deleteCount()).isEqualTo(2);
      assertThat(counts.selectCount()).isEqualTo(0);
      assertThat(counts.insertCount()).isEqualTo(0);
      assertThat(counts.updateCount()).isEqualTo(0);
    }

    @Test
    void mixedQueryTypesCountedCorrectly() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users"),
              record("INSERT INTO users (name) VALUES ('alice')"),
              record("UPDATE users SET name = 'bob'"),
              record("DELETE FROM users WHERE id = 1"),
              record("SELECT id FROM rooms"),
              record("INSERT INTO rooms (title) VALUES ('general')"));

      QueryCounts counts = QueryCounts.from(queries);

      assertThat(counts.selectCount()).isEqualTo(2);
      assertThat(counts.insertCount()).isEqualTo(2);
      assertThat(counts.updateCount()).isEqualTo(1);
      assertThat(counts.deleteCount()).isEqualTo(1);
      assertThat(counts.totalCount()).isEqualTo(6);
    }

    @Test
    void emptyListReturnsAllZeros() {
      QueryCounts counts = QueryCounts.from(List.of());

      assertThat(counts.selectCount()).isEqualTo(0);
      assertThat(counts.insertCount()).isEqualTo(0);
      assertThat(counts.updateCount()).isEqualTo(0);
      assertThat(counts.deleteCount()).isEqualTo(0);
      assertThat(counts.totalCount()).isEqualTo(0);
    }

    @Test
    void nullAndBlankSqlEntriesAreSkipped() {
      List<QueryRecord> queries =
          List.of(
              new QueryRecord(null, null, 0, 0, null, 0),
              new QueryRecord("", null, 0, 0, null, 0),
              new QueryRecord("   ", null, 0, 0, null, 0),
              record("SELECT * FROM users"));

      QueryCounts counts = QueryCounts.from(queries);

      assertThat(counts.selectCount()).isEqualTo(1);
      assertThat(counts.insertCount()).isEqualTo(0);
      assertThat(counts.totalCount()).isEqualTo(1);
    }

    @Test
    void caseInsensitiveCounting() {
      List<QueryRecord> queries =
          List.of(
              record("select * from users"),
              record("SELECT * FROM rooms"),
              record("insert into users values (1)"),
              record("INSERT INTO rooms VALUES (2)"),
              record("update users set name = 'x'"),
              record("UPDATE rooms SET title = 'y'"),
              record("delete from users where id = 1"),
              record("DELETE FROM rooms WHERE id = 2"));

      QueryCounts counts = QueryCounts.from(queries);

      assertThat(counts.selectCount()).isEqualTo(2);
      assertThat(counts.insertCount()).isEqualTo(2);
      assertThat(counts.updateCount()).isEqualTo(2);
      assertThat(counts.deleteCount()).isEqualTo(2);
    }

    @Test
    void totalIsSumOfAllTypes() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT 1"),
              record("SELECT 2"),
              record("SELECT 3"),
              record("INSERT INTO t VALUES (1)"),
              record("INSERT INTO t VALUES (2)"),
              record("UPDATE t SET x = 1"),
              record("DELETE FROM t WHERE id = 1"));

      QueryCounts counts = QueryCounts.from(queries);

      assertThat(counts.totalCount())
          .isEqualTo(
              counts.selectCount()
                  + counts.insertCount()
                  + counts.updateCount()
                  + counts.deleteCount());
      assertThat(counts.totalCount()).isEqualTo(7);
    }

    @Test
    void leadingWhitespaceIsStripped() {
      List<QueryRecord> queries =
          List.of(
              record("  SELECT * FROM users"),
              record("\tINSERT INTO users VALUES (1)"),
              record("\n  UPDATE users SET name = 'x'"),
              record("  \t DELETE FROM users WHERE id = 1"));

      QueryCounts counts = QueryCounts.from(queries);

      assertThat(counts.selectCount()).isEqualTo(1);
      assertThat(counts.insertCount()).isEqualTo(1);
      assertThat(counts.updateCount()).isEqualTo(1);
      assertThat(counts.deleteCount()).isEqualTo(1);
    }
  }

  // =====================================================================
  // QueryCountRegressionDetector
  // =====================================================================

  @Nested
  class DetectorTest {

    private final QueryCountRegressionDetector detector = new QueryCountRegressionDetector();

    @Test
    void noRegressionWhenCountsAreEqual() {
      QueryCounts baseline = new QueryCounts(10, 2, 1, 1, 14);
      QueryCounts current = new QueryCounts(10, 2, 1, 1, 14);

      List<Issue> issues = detector.detect("TestClass", "testMethod", current, baseline);

      assertThat(issues).isEmpty();
    }

    @Test
    void noRegressionWhenCountsDecrease() {
      QueryCounts baseline = new QueryCounts(20, 5, 3, 2, 30);
      QueryCounts current = new QueryCounts(10, 2, 1, 1, 14);

      List<Issue> issues = detector.detect("TestClass", "testMethod", current, baseline);

      assertThat(issues).isEmpty();
    }

    @Test
    void noRegressionWhenIncreaseBelowRatioThreshold() {
      // 100 -> 110: ratio 1.1 (below 1.5), absolute +10 (above 5)
      QueryCounts baseline = new QueryCounts(100, 0, 0, 0, 100);
      QueryCounts current = new QueryCounts(110, 0, 0, 0, 110);

      List<Issue> issues = detector.detect("TestClass", "testMethod", current, baseline);

      assertThat(issues).isEmpty();
    }

    @Test
    void noRegressionWhenAbsoluteIncreaseBelowThreshold() {
      // 2 -> 6: ratio 3.0 (above 1.5), absolute +4 (below 5)
      QueryCounts baseline = new QueryCounts(2, 0, 0, 0, 2);
      QueryCounts current = new QueryCounts(6, 0, 0, 0, 6);

      List<Issue> issues = detector.detect("TestClass", "testMethod", current, baseline);

      assertThat(issues).isEmpty();
    }

    @Test
    void warningWhen1point5xIncreaseWithSufficientAbsoluteIncrease() {
      // 10 -> 16: ratio 1.6 (>= 1.5), absolute +6 (>= 5)
      QueryCounts baseline = new QueryCounts(10, 0, 0, 0, 10);
      QueryCounts current = new QueryCounts(16, 0, 0, 0, 16);

      List<Issue> issues = detector.detect("TestClass", "testMethod", current, baseline);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.QUERY_COUNT_REGRESSION);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    }

    @Test
    void warningWhen2xIncrease() {
      // 10 -> 20: ratio 2.0, absolute +10
      QueryCounts baseline = new QueryCounts(10, 0, 0, 0, 10);
      QueryCounts current = new QueryCounts(20, 0, 0, 0, 20);

      List<Issue> issues = detector.detect("TestClass", "testMethod", current, baseline);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    }

    @Test
    void errorWhen3xOrMoreIncrease() {
      // 10 -> 30: ratio 3.0, absolute +20
      QueryCounts baseline = new QueryCounts(10, 0, 0, 0, 10);
      QueryCounts current = new QueryCounts(30, 0, 0, 0, 30);

      List<Issue> issues = detector.detect("TestClass", "testMethod", current, baseline);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
    }

    @Test
    void noRegressionWhenBaselineIsNull() {
      QueryCounts current = new QueryCounts(50, 10, 5, 3, 68);

      List<Issue> issues = detector.detect("TestClass", "testMethod", current, null);

      assertThat(issues).isEmpty();
    }

    @Test
    void selectSpecificRegressionDetectedSeparately() {
      // Total: 20 -> 22 (ratio 1.1, +2 -- below total thresholds)
      // SELECT: 5 -> 15 (ratio 3.0, +10 -- above SELECT thresholds)
      QueryCounts baseline = new QueryCounts(5, 10, 3, 2, 20);
      QueryCounts current = new QueryCounts(15, 5, 1, 1, 22);

      List<Issue> issues = detector.detect("TestClass", "testMethod", current, baseline);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).detail()).contains("SELECT count regression");
      assertThat(issues.get(0).detail()).contains("5 -> 15");
      assertThat(issues.get(0).severity()).isEqualTo(Severity.WARNING);
    }

    @Test
    void dmlRegressionAlsoDetected() {
      // INSERT count goes from 2 -> 10, but we check total-level detection
      // baseline total: 5+2+1+0=8, current total: 5+10+1+0=16
      // ratio: 2.0, absolute: +8 -- triggers total regression
      QueryCounts baseline = new QueryCounts(5, 2, 1, 0, 8);
      QueryCounts current = new QueryCounts(5, 10, 1, 0, 16);

      List<Issue> issues = detector.detect("TestClass", "testMethod", current, baseline);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.QUERY_COUNT_REGRESSION);
      // Detail message includes INSERT counts
      assertThat(issues.get(0).detail()).contains("8 -> 16");
      assertThat(issues.get(0).detail()).contains("10 INSERT");
    }

    @Test
    void detailMessageIncludesAllDmlCounts() {
      QueryCounts baseline = new QueryCounts(5, 1, 1, 1, 8);
      QueryCounts current = new QueryCounts(15, 3, 2, 2, 22);

      List<Issue> issues = detector.detect("TestClass", "testMethod", current, baseline);

      assertThat(issues).hasSize(1);
      String detail = issues.get(0).detail();
      // Baseline counts
      assertThat(detail).contains("5 SELECT");
      assertThat(detail).contains("1 INSERT");
      assertThat(detail).contains("1 UPDATE");
      assertThat(detail).contains("1 DELETE");
      // Current counts
      assertThat(detail).contains("15 SELECT");
      assertThat(detail).contains("3 INSERT");
      assertThat(detail).contains("2 UPDATE");
      assertThat(detail).contains("2 DELETE");
    }

    @Test
    void selectSpecificRegressionNotDuplicatedWhenTotalAlreadyFlagged() {
      // Both total and SELECT thresholds exceeded -- should only get 1 issue
      // Total: 10 -> 30 (3x, +20)
      // SELECT: 10 -> 30 (3x, +20)
      QueryCounts baseline = new QueryCounts(10, 0, 0, 0, 10);
      QueryCounts current = new QueryCounts(30, 0, 0, 0, 30);

      List<Issue> issues = detector.detect("TestClass", "testMethod", current, baseline);

      assertThat(issues).hasSize(1);
      // Should be the total regression, not SELECT-specific
      assertThat(issues.get(0).detail()).contains("Query count regression");
    }

    @Test
    void suggestionIncludesBaselineUpdateCommand() {
      QueryCounts baseline = new QueryCounts(10, 0, 0, 0, 10);
      QueryCounts current = new QueryCounts(20, 0, 0, 0, 20);

      List<Issue> issues = detector.detect("TestClass", "testMethod", current, baseline);

      assertThat(issues.get(0).suggestion()).contains("queryGuard.updateBaseline=true");
    }
  }

  // =====================================================================
  // QueryCountBaseline (file I/O)
  // =====================================================================

  @Nested
  class BaselineFileTest {

    @TempDir Path tempDir;

    @Test
    void saveAndLoadRoundTrip() throws IOException {
      Path file = tempDir.resolve(".query-audit-counts");

      Map<String, QueryCounts> counts = new LinkedHashMap<>();
      counts.put(
          QueryCountBaseline.key("RoomApiTest", "testCreateRoom"),
          new QueryCounts(12, 3, 1, 0, 16));
      counts.put(
          QueryCountBaseline.key("MessageApiTest", "testSendMessage"),
          new QueryCounts(8, 0, 2, 1, 11));

      QueryCountBaseline.save(file, counts);
      Map<String, QueryCounts> loaded = QueryCountBaseline.load(file);

      assertThat(loaded).hasSize(2);

      QueryCounts room = loaded.get(QueryCountBaseline.key("RoomApiTest", "testCreateRoom"));
      assertThat(room).isNotNull();
      assertThat(room.selectCount()).isEqualTo(12);
      assertThat(room.insertCount()).isEqualTo(3);
      assertThat(room.updateCount()).isEqualTo(1);
      assertThat(room.deleteCount()).isEqualTo(0);
      assertThat(room.totalCount()).isEqualTo(16);

      QueryCounts message = loaded.get(QueryCountBaseline.key("MessageApiTest", "testSendMessage"));
      assertThat(message).isNotNull();
      assertThat(message.selectCount()).isEqualTo(8);
      assertThat(message.insertCount()).isEqualTo(0);
      assertThat(message.updateCount()).isEqualTo(2);
      assertThat(message.deleteCount()).isEqualTo(1);
      assertThat(message.totalCount()).isEqualTo(11);
    }

    @Test
    void loadFromNonExistentFileReturnsEmptyMap() {
      Map<String, QueryCounts> loaded = QueryCountBaseline.load(tempDir.resolve("does-not-exist"));
      assertThat(loaded).isEmpty();
    }

    @Test
    void keyFormatIsCorrect() {
      String key = QueryCountBaseline.key("com.example.MyTest", "testSomething");
      assertThat(key).isEqualTo("com.example.MyTest|testSomething");
      // Key uses pipe as separator, no spaces
      assertThat(key).doesNotContain(" ");
    }

    @Test
    void mergePreservesExistingEntries() throws IOException {
      Path file = tempDir.resolve(".query-audit-counts");

      // Save initial entries
      Map<String, QueryCounts> initial = new LinkedHashMap<>();
      initial.put(QueryCountBaseline.key("TestA", "method1"), new QueryCounts(5, 1, 0, 0, 6));
      initial.put(QueryCountBaseline.key("TestB", "method2"), new QueryCounts(3, 0, 1, 0, 4));
      QueryCountBaseline.save(file, initial);

      // Load, add new entries, save again
      Map<String, QueryCounts> loaded = new LinkedHashMap<>(QueryCountBaseline.load(file));
      loaded.put(QueryCountBaseline.key("TestC", "method3"), new QueryCounts(7, 2, 0, 1, 10));
      QueryCountBaseline.save(file, loaded);

      // Reload and verify all entries present
      Map<String, QueryCounts> reloaded = QueryCountBaseline.load(file);
      assertThat(reloaded).hasSize(3);
      assertThat(reloaded).containsKey(QueryCountBaseline.key("TestA", "method1"));
      assertThat(reloaded).containsKey(QueryCountBaseline.key("TestB", "method2"));
      assertThat(reloaded).containsKey(QueryCountBaseline.key("TestC", "method3"));

      // Verify original entries are unchanged
      QueryCounts testA = reloaded.get(QueryCountBaseline.key("TestA", "method1"));
      assertThat(testA.selectCount()).isEqualTo(5);
      assertThat(testA.insertCount()).isEqualTo(1);
      assertThat(testA.totalCount()).isEqualTo(6);
    }

    @Test
    void savedFileIncludesAllDmlCountsInOutput() throws IOException {
      Path file = tempDir.resolve(".query-audit-counts");

      Map<String, QueryCounts> counts = new LinkedHashMap<>();
      counts.put(QueryCountBaseline.key("DmlTest", "testAll"), new QueryCounts(10, 5, 3, 2, 20));
      QueryCountBaseline.save(file, counts);

      String content = Files.readString(file);
      assertThat(content).contains("DmlTest | testAll | 10 | 5 | 3 | 2 | 20");
    }

    @Test
    void loadHandlesNullPath() {
      Map<String, QueryCounts> loaded = QueryCountBaseline.load(null);
      assertThat(loaded).isEmpty();
    }
  }
}
