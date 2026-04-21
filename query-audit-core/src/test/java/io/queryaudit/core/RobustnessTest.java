package io.queryaudit.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.parser.SqlParser;
import io.queryaudit.core.reporter.ConsoleReporter;
import io.queryaudit.core.reporter.HtmlReporter;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Spacecraft-grade robustness tests for query-audit.
 *
 * <p>Goal: ZERO crashes, ZERO NPEs, ZERO infinite loops regardless of input. Uses fuzz testing,
 * property-based testing, boundary analysis, and pathological input generation to verify defensive
 * robustness.
 */
class RobustnessTest {

  // =========================================================================
  // 1. FUZZ SqlParser — every method must survive ANY string input
  // =========================================================================

  @Nested
  class SqlParserFuzzTests {

    private final Random random = new Random(42); // deterministic seed for reproducibility

    @RepeatedTest(100)
    void sqlParserNeverCrashesOnRandomInput() {
      String randomSql = generateRandomSql();
      assertDoesNotThrow(() -> SqlParser.normalize(randomSql));
      assertDoesNotThrow(() -> SqlParser.isSelectQuery(randomSql));
      assertDoesNotThrow(() -> SqlParser.hasSelectAll(randomSql));
      assertDoesNotThrow(() -> SqlParser.extractWhereColumns(randomSql));
      assertDoesNotThrow(() -> SqlParser.extractJoinColumns(randomSql));
      assertDoesNotThrow(() -> SqlParser.extractOrderByColumns(randomSql));
      assertDoesNotThrow(() -> SqlParser.extractGroupByColumns(randomSql));
      assertDoesNotThrow(() -> SqlParser.detectWhereFunctions(randomSql));
      assertDoesNotThrow(() -> SqlParser.countOrConditions(randomSql));
      assertDoesNotThrow(() -> SqlParser.extractOffsetValue(randomSql));
      assertDoesNotThrow(() -> SqlParser.extractTableNames(randomSql));
    }

    @Test
    void sqlParserHandlesNullInput() {
      assertDoesNotThrow(() -> SqlParser.normalize(null));
      assertDoesNotThrow(() -> SqlParser.isSelectQuery(null));
      assertDoesNotThrow(() -> SqlParser.hasSelectAll(null));
      assertDoesNotThrow(() -> SqlParser.extractWhereColumns(null));
      assertDoesNotThrow(() -> SqlParser.extractJoinColumns(null));
      assertDoesNotThrow(() -> SqlParser.extractOrderByColumns(null));
      assertDoesNotThrow(() -> SqlParser.extractGroupByColumns(null));
      assertDoesNotThrow(() -> SqlParser.detectWhereFunctions(null));
      assertDoesNotThrow(() -> SqlParser.countOrConditions(null));
      assertDoesNotThrow(() -> SqlParser.extractOffsetValue(null));
      assertDoesNotThrow(() -> SqlParser.extractTableNames(null));
    }

    @Test
    void sqlParserHandlesEmptyString() {
      allParserMethods("");
    }

    @Test
    void sqlParserHandlesWhitespaceOnly() {
      allParserMethods("   ");
      allParserMethods("\t\n\r");
      allParserMethods("   \t   \n   ");
    }

    @Test
    void sqlParserHandlesOnlyNumbers() {
      allParserMethods("12345");
      allParserMethods("0");
      allParserMethods("999999999999999999999999999999");
    }

    @Test
    void sqlParserHandlesUnicodeInput() {
      allParserMethods("\u0000\u0001\u0002");
      allParserMethods("\uD83D\uDE00\uD83D\uDE01\uD83D\uDE02"); // emoji
      allParserMethods("\u200B\u200C\u200D\uFEFF"); // zero-width characters
      allParserMethods("\u0410\u0411\u0412"); // Cyrillic
      allParserMethods("\u4E00\u4E01\u4E02"); // CJK
      allParserMethods("SELECT * FROM \u30C6\u30FC\u30D6\u30EB WHERE \u540D\u524D = '\u5024'");
    }

    @Test
    void sqlParserHandlesControlCharacters() {
      allParserMethods("\0");
      allParserMethods("\0\0\0");
      allParserMethods("SELECT\0*\0FROM\0table");
      allParserMethods("\u0007\u0008\u001B"); // bell, backspace, escape
    }

    @Test
    void sqlParserHandlesSqlInjectionPatterns() {
      allParserMethods("'; DROP TABLE users; --");
      allParserMethods("1' OR '1'='1");
      allParserMethods("UNION SELECT username, password FROM users --");
      allParserMethods("'; EXEC xp_cmdshell('dir'); --");
      allParserMethods("1; UPDATE users SET admin=1 WHERE id=1; --");
      allParserMethods("' UNION ALL SELECT NULL,NULL,NULL--");
      allParserMethods("admin'--");
      allParserMethods("' OR 1=1 /*");
      allParserMethods("*/; DROP TABLE users; /*");
    }

    @Test
    void sqlParserHandlesUnbalancedQuotes() {
      allParserMethods("WHERE name = 'unclosed");
      allParserMethods("WHERE name = \"unclosed");
      allParserMethods("WHERE name = '''");
      allParserMethods("WHERE name = '''' AND x = '");
      allParserMethods("'''''''''''''''''''''");
      allParserMethods("\"\"\"\"\"\"\"\"\"\"\"\"");
    }

    @Test
    void sqlParserHandlesUnbalancedParentheses() {
      allParserMethods("WHERE (a = 1 AND (b = 2)");
      allParserMethods("WHERE a = 1)))))");
      allParserMethods("(((((((((((");
      allParserMethods(")))))))))))");
      allParserMethods("SELECT (((((((((())))))))))");
      allParserMethods("IN (((((((1,2,3");
    }

    @Test
    void sqlParserHandlesDeeplyNestedParentheses() {
      StringBuilder sb = new StringBuilder("SELECT * FROM t WHERE ");
      for (int i = 0; i < 500; i++) sb.append("(");
      sb.append("a = 1");
      for (int i = 0; i < 500; i++) sb.append(")");
      allParserMethods(sb.toString());
    }

    @Test
    void sqlParserHandlesVeryLongStrings() {
      // 50,000+ character string
      allParserMethods("SELECT ".repeat(7200));
      allParserMethods("a".repeat(50001));
      allParserMethods("WHERE a = " + "1 AND a = ".repeat(5000) + "1");
    }

    @Test
    void sqlParserHandlesRepeatedKeywords() {
      allParserMethods("SELECT SELECT SELECT FROM FROM FROM WHERE WHERE WHERE");
      allParserMethods("JOIN JOIN JOIN ON ON ON");
      allParserMethods("ORDER BY ORDER BY ORDER BY");
      allParserMethods("GROUP BY GROUP BY GROUP BY");
      allParserMethods("LIMIT LIMIT LIMIT OFFSET OFFSET OFFSET");
    }

    @Test
    void sqlParserHandlesMixedCaseAndGarbage() {
      allParserMethods("sElEcT * fRoM tAbLe WhErE iD = 1");
      allParserMethods("SELECT @#$%^& FROM !@#$ WHERE &*() = ~~~");
      allParserMethods("SELECT\n\t\r*\n\t\rFROM\n\t\rtable\n\t\rWHERE\n\t\rid = 1");
    }

    private String generateRandomSql() {
      int choice = random.nextInt(10);
      return switch (choice) {
        case 0 -> generateRandomChars(random.nextInt(200));
        case 1 -> generateRandomSqlWithKeywords();
        case 2 -> generateDeepNesting(random.nextInt(100) + 1);
        case 3 -> generateLongString(random.nextInt(5000) + 50000);
        case 4 -> generateUnicodeGarbage(random.nextInt(200));
        case 5 -> generateControlChars(random.nextInt(100));
        case 6 -> generateInjectionAttempt();
        case 7 -> generateUnbalancedQuotes();
        case 8 -> generateUnbalancedParens();
        case 9 -> generateValidishSql();
        default -> "";
      };
    }

    private String generateRandomChars(int length) {
      StringBuilder sb = new StringBuilder(length);
      for (int i = 0; i < length; i++) {
        sb.append((char) random.nextInt(0x10000));
      }
      return sb.toString();
    }

    private String generateRandomSqlWithKeywords() {
      String[] keywords = {
        "SELECT",
        "FROM",
        "WHERE",
        "AND",
        "OR",
        "JOIN",
        "ON",
        "ORDER BY",
        "GROUP BY",
        "HAVING",
        "LIMIT",
        "OFFSET",
        "IN",
        "LIKE",
        "BETWEEN",
        "IS NULL",
        "IS NOT NULL",
        "EXISTS",
        "UNION",
        "INSERT",
        "UPDATE",
        "DELETE",
        "CREATE",
        "DROP",
        "ALTER",
        "*",
        "?",
        "=",
        "<>",
        "(",
        ")",
        ",",
        ";",
        "--",
        "/*",
        "*/",
        "'",
        "\""
      };
      StringBuilder sb = new StringBuilder();
      int count = random.nextInt(50) + 1;
      for (int i = 0; i < count; i++) {
        sb.append(keywords[random.nextInt(keywords.length)]);
        sb.append(random.nextBoolean() ? " " : "");
      }
      return sb.toString();
    }

    private String generateDeepNesting(int depth) {
      StringBuilder sb = new StringBuilder("SELECT * FROM t WHERE ");
      sb.append("(".repeat(depth));
      sb.append("a = 1");
      sb.append(")".repeat(depth));
      return sb.toString();
    }

    private String generateLongString(int length) {
      char[] chars = new char[length];
      for (int i = 0; i < length; i++) {
        chars[i] = (char) ('a' + random.nextInt(26));
      }
      return new String(chars);
    }

    private String generateUnicodeGarbage(int length) {
      StringBuilder sb = new StringBuilder(length);
      for (int i = 0; i < length; i++) {
        int codePoint = random.nextInt(0x10FFFF + 1);
        if (Character.isValidCodePoint(codePoint) && !Character.isSurrogate((char) codePoint)) {
          sb.appendCodePoint(codePoint);
        }
      }
      return sb.toString();
    }

    private String generateControlChars(int length) {
      StringBuilder sb = new StringBuilder(length);
      for (int i = 0; i < length; i++) {
        sb.append((char) random.nextInt(32));
      }
      return sb.toString();
    }

    private String generateInjectionAttempt() {
      String[] injections = {
        "'; DROP TABLE users; --",
        "1' OR '1'='1",
        "UNION SELECT * FROM information_schema.tables --",
        "'; EXEC xp_cmdshell('dir'); --",
        "1; WAITFOR DELAY '0:0:10'; --",
        "' AND 1=CONVERT(int,(SELECT TOP 1 table_name FROM information_schema.tables))--",
        "';SHUTDOWN--",
        "' UNION SELECT NULL,NULL,NULL--",
        "1' AND (SELECT COUNT(*) FROM users) > 0 --"
      };
      return injections[random.nextInt(injections.length)];
    }

    private String generateUnbalancedQuotes() {
      StringBuilder sb = new StringBuilder("WHERE name = ");
      int quoteCount = random.nextInt(10) + 1;
      for (int i = 0; i < quoteCount; i++) {
        sb.append(random.nextBoolean() ? "'" : "\"");
      }
      return sb.toString();
    }

    private String generateUnbalancedParens() {
      StringBuilder sb = new StringBuilder("SELECT * FROM t WHERE ");
      int openCount = random.nextInt(20);
      int closeCount = random.nextInt(20);
      sb.append("(".repeat(openCount));
      sb.append("a = 1");
      sb.append(")".repeat(closeCount));
      return sb.toString();
    }

    private String generateValidishSql() {
      String[] templates = {
        "SELECT id, name FROM users WHERE id = ?",
        "SELECT * FROM orders JOIN users ON orders.user_id = users.id WHERE orders.status = ?",
        "SELECT COUNT(*) FROM messages WHERE room_id = ? AND deleted_at IS NULL",
        "SELECT a FROM t GROUP BY a HAVING COUNT(*) > ? ORDER BY a LIMIT ? OFFSET ?",
        "SELECT * FROM t WHERE a IN (?, ?, ?) AND b LIKE '%test%' OR c BETWEEN ? AND ?",
        "SELECT LOWER(name) FROM t WHERE YEAR(created_at) = ?",
        "UPDATE users SET name = ? WHERE id = ?",
        "DELETE FROM sessions WHERE expired_at < ?",
        "INSERT INTO logs (level, message) VALUES (?, ?)"
      };
      return templates[random.nextInt(templates.length)];
    }

    private void allParserMethods(String sql) {
      assertDoesNotThrow(() -> SqlParser.normalize(sql), "normalize failed for: " + truncate(sql));
      assertDoesNotThrow(
          () -> SqlParser.isSelectQuery(sql), "isSelectQuery failed for: " + truncate(sql));
      assertDoesNotThrow(
          () -> SqlParser.hasSelectAll(sql), "hasSelectAll failed for: " + truncate(sql));
      assertDoesNotThrow(
          () -> SqlParser.extractWhereColumns(sql),
          "extractWhereColumns failed for: " + truncate(sql));
      assertDoesNotThrow(
          () -> SqlParser.extractJoinColumns(sql),
          "extractJoinColumns failed for: " + truncate(sql));
      assertDoesNotThrow(
          () -> SqlParser.extractOrderByColumns(sql),
          "extractOrderByColumns failed for: " + truncate(sql));
      assertDoesNotThrow(
          () -> SqlParser.extractGroupByColumns(sql),
          "extractGroupByColumns failed for: " + truncate(sql));
      assertDoesNotThrow(
          () -> SqlParser.detectWhereFunctions(sql),
          "detectWhereFunctions failed for: " + truncate(sql));
      assertDoesNotThrow(
          () -> SqlParser.countOrConditions(sql), "countOrConditions failed for: " + truncate(sql));
      assertDoesNotThrow(
          () -> SqlParser.extractOffsetValue(sql),
          "extractOffsetValue failed for: " + truncate(sql));
      assertDoesNotThrow(
          () -> SqlParser.extractTableNames(sql), "extractTableNames failed for: " + truncate(sql));
    }

    private String truncate(String s) {
      if (s == null) return "null";
      if (s.length() <= 80) return s;
      return s.substring(0, 77) + "...";
    }
  }

  // =========================================================================
  // 2. FUZZ QueryAuditAnalyzer — must survive any QueryRecord combination
  // =========================================================================

  @Nested
  class AnalyzerFuzzTests {

    private final Random random = new Random(42);

    @RepeatedTest(50)
    void analyzerNeverCrashesOnRandomInput() {
      List<QueryRecord> queries = generateRandomQueryRecords();
      IndexMetadata metadata = generateRandomIndexMetadata();
      QueryAuditConfig config = QueryAuditConfig.defaults();

      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, List.of());
      assertDoesNotThrow(() -> analyzer.analyze("test", queries, metadata));
    }

    @Test
    void analyzerHandlesZeroQueries() {
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
      assertDoesNotThrow(() -> analyzer.analyze("test", List.of(), null));
    }

    @Test
    void analyzerHandlesNullQueries() {
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
      assertDoesNotThrow(() -> analyzer.analyze("test", null, null));
    }

    @Test
    void analyzerHandlesSingleQuery() {
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
      List<QueryRecord> queries =
          List.of(
              new QueryRecord(
                  "SELECT * FROM users WHERE id = 1", 1000, System.currentTimeMillis(), "stack"));
      assertDoesNotThrow(() -> analyzer.analyze("test", queries, null));
    }

    @Test
    void analyzerHandlesQueriesWithNullSql() {
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
      List<QueryRecord> queries = new ArrayList<>();
      queries.add(new QueryRecord(null, 0, 0, null));
      queries.add(new QueryRecord("SELECT 1", 100, System.currentTimeMillis(), null));
      queries.add(new QueryRecord(null, 0, 0, ""));
      assertDoesNotThrow(() -> analyzer.analyze("test", queries, null));
    }

    @Test
    void analyzerHandlesQueriesWithEmptyStrings() {
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
      List<QueryRecord> queries = new ArrayList<>();
      queries.add(new QueryRecord("", 0, 0, ""));
      queries.add(new QueryRecord("", 0, 0, ""));
      assertDoesNotThrow(() -> analyzer.analyze("test", queries, null));
    }

    @Test
    void analyzerHandlesVeryLongSql() {
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
      String longSql = "SELECT * FROM users WHERE id IN (" + "?,".repeat(10000) + "?)";
      List<QueryRecord> queries =
          List.of(new QueryRecord(longSql, 1000, System.currentTimeMillis(), "stack"));
      assertDoesNotThrow(() -> analyzer.analyze("test", queries, null));
    }

    @Test
    void analyzerHandlesNullIndexMetadata() {
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
      List<QueryRecord> queries =
          List.of(
              new QueryRecord(
                  "SELECT * FROM users WHERE id = 1", 1000, System.currentTimeMillis(), "stack"));
      assertDoesNotThrow(() -> analyzer.analyze("test", queries, null));
    }

    @Test
    void analyzerHandlesEmptyIndexMetadata() {
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
      IndexMetadata metadata = new IndexMetadata(Map.of());
      List<QueryRecord> queries =
          List.of(
              new QueryRecord(
                  "SELECT * FROM users WHERE id = 1", 1000, System.currentTimeMillis(), "stack"));
      assertDoesNotThrow(() -> analyzer.analyze("test", queries, metadata));
    }

    @Test
    void analyzerHandlesIndexMetadataWithNullColumnNames() {
      Map<String, List<IndexInfo>> indexes = new HashMap<>();
      List<IndexInfo> infos = new ArrayList<>();
      infos.add(new IndexInfo("users", "idx_users_id", null, 1, false, 100));
      infos.add(new IndexInfo("users", "idx_users_email", "email", 1, false, 100));
      infos.add(new IndexInfo(null, null, null, 0, false, 0));
      indexes.put("users", infos);

      IndexMetadata metadata = new IndexMetadata(indexes);
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
      List<QueryRecord> queries =
          List.of(
              new QueryRecord(
                  "SELECT * FROM users WHERE id = 1 AND email = 'test@test.com'",
                  1000,
                  System.currentTimeMillis(),
                  "stack"));
      assertDoesNotThrow(() -> analyzer.analyze("test", queries, metadata));
    }

    @Test
    void analyzerHandlesDisabledConfig() {
      QueryAuditConfig config = QueryAuditConfig.builder().enabled(false).build();
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, List.of());
      List<QueryRecord> queries =
          List.of(
              new QueryRecord("SELECT * FROM users", 1000, System.currentTimeMillis(), "stack"));
      assertDoesNotThrow(() -> analyzer.analyze("test", queries, null));
    }

    private List<QueryRecord> generateRandomQueryRecords() {
      int count = random.nextInt(100); // 0 to 99
      List<QueryRecord> records = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        records.add(generateRandomQueryRecord());
      }
      return records;
    }

    private QueryRecord generateRandomQueryRecord() {
      int choice = random.nextInt(6);
      return switch (choice) {
        case 0 -> new QueryRecord(null, 0, 0, null);
        case 1 -> new QueryRecord("", 0, 0, "");
        case 2 ->
            new QueryRecord(
                "SELECT * FROM t" + random.nextInt(10) + " WHERE id = " + random.nextInt(),
                1000,
                System.currentTimeMillis(),
                "stack" + random.nextInt());
        case 3 -> {
          String trace3 = generateRandomChars(random.nextInt(200));
          yield new QueryRecord(
              generateRandomChars(random.nextInt(500)),
              generateRandomChars(random.nextInt(500)),
              random.nextLong(),
              random.nextLong(),
              trace3,
              trace3.hashCode());
        }
        case 4 -> {
          String trace4 = Thread.currentThread().getStackTrace().toString();
          yield new QueryRecord(
              "SELECT * FROM users WHERE id = " + random.nextInt(),
              "select * from users where id = ?",
              random.nextLong(0, Long.MAX_VALUE),
              System.currentTimeMillis(),
              trace4,
              trace4.hashCode());
        }
        default ->
            new QueryRecord(
                "INSERT INTO logs VALUES (" + random.nextInt() + ")",
                100,
                System.currentTimeMillis(),
                null);
      };
    }

    private IndexMetadata generateRandomIndexMetadata() {
      if (random.nextInt(4) == 0) return null;

      Map<String, List<IndexInfo>> indexes = new HashMap<>();
      int tableCount = random.nextInt(5);
      for (int t = 0; t < tableCount; t++) {
        String tableName = "table_" + t;
        List<IndexInfo> infos = new ArrayList<>();
        int indexCount = random.nextInt(5);
        for (int i = 0; i < indexCount; i++) {
          String colName = random.nextBoolean() ? "col_" + i : null;
          infos.add(
              new IndexInfo(
                  tableName,
                  "idx_" + t + "_" + i,
                  colName,
                  i + 1,
                  random.nextBoolean(),
                  random.nextLong(0, 10000)));
        }
        indexes.put(tableName, infos);
      }
      return new IndexMetadata(indexes);
    }

    private String generateRandomChars(int length) {
      StringBuilder sb = new StringBuilder(length);
      for (int i = 0; i < length; i++) {
        sb.append((char) random.nextInt(0x10000));
      }
      return sb.toString();
    }
  }

  // =========================================================================
  // 3. BOUNDARY VALUE ANALYSIS for all thresholds
  // =========================================================================

  @Nested
  class BoundaryTests {

    @Test
    void nPlusOneThresholdBoundary() {
      // Default threshold is 3.
      // Prosopite algorithm: queries with null stack traces all have stackHash=0,
      // so they group together. count >= threshold = N+1 detected.
      String sql = "SELECT * FROM users WHERE id = ?";
      QueryAuditConfig config = QueryAuditConfig.defaults();
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, List.of());

      // At threshold-1 (2 queries with null stacks): no N+1
      List<QueryRecord> twoQueries = createRepeatedQueriesNoStack(sql, 2);
      QueryAuditReport r2 = analyzer.analyze("test", twoQueries, null);
      assertThat(
              r2.getConfirmedIssues().stream()
                  .filter(i -> i.type().getCode().equals("n-plus-one"))
                  .count())
          .isEqualTo(0);

      // At threshold (3 consecutive queries with null stacks): SQL-level N+1 suspect is INFO
      List<QueryRecord> threeQueries = createRepeatedQueriesNoStack(sql, 3);
      QueryAuditReport r3 = analyzer.analyze("test", threeQueries, null);
      assertThat(
              r3.getInfoIssues().stream()
                  .filter(i -> i.type().getCode().equals("n-plus-one-suspect"))
                  .count())
          .isGreaterThan(0);

      // At threshold+2 (5 queries with null stacks): SQL-level N+1 suspect is INFO
      List<QueryRecord> fiveQueries = createRepeatedQueriesNoStack(sql, 5);
      QueryAuditReport r5 = analyzer.analyze("test", fiveQueries, null);
      assertThat(
              r5.getInfoIssues().stream()
                  .filter(i -> i.type().getCode().equals("n-plus-one-suspect"))
                  .count())
          .isGreaterThan(0);
    }

    @Test
    void offsetThresholdBoundary() {
      QueryAuditConfig config = QueryAuditConfig.builder().offsetPaginationThreshold(1000).build();
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, List.of());

      // Offset 999: below threshold
      QueryAuditReport r999 =
          analyzer.analyze(
              "test",
              List.of(
                  new QueryRecord(
                      "SELECT * FROM t LIMIT 10 OFFSET 999", 100, System.currentTimeMillis(), "s")),
              null);

      // Offset 1000: at threshold
      QueryAuditReport r1000 =
          analyzer.analyze(
              "test",
              List.of(
                  new QueryRecord(
                      "SELECT * FROM t LIMIT 10 OFFSET 1000",
                      100,
                      System.currentTimeMillis(),
                      "s")),
              null);

      // Offset 1001: above threshold
      QueryAuditReport r1001 =
          analyzer.analyze(
              "test",
              List.of(
                  new QueryRecord(
                      "SELECT * FROM t LIMIT 10 OFFSET 1001",
                      100,
                      System.currentTimeMillis(),
                      "s")),
              null);

      // No crash for any
      assertThat(r999).isNotNull();
      assertThat(r1000).isNotNull();
      assertThat(r1001).isNotNull();
    }

    @Test
    void orClauseThresholdBoundary() {
      QueryAuditConfig config = QueryAuditConfig.builder().orClauseThreshold(3).build();
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config, List.of());

      // 2 ORs: below threshold
      String sql2 = "SELECT * FROM t WHERE a = 1 OR b = 2 OR c = 3";
      QueryAuditReport r2 =
          analyzer.analyze(
              "test", List.of(new QueryRecord(sql2, 100, System.currentTimeMillis(), "s")), null);

      // 3 ORs: at threshold
      String sql3 = "SELECT * FROM t WHERE a = 1 OR b = 2 OR c = 3 OR d = 4";
      QueryAuditReport r3 =
          analyzer.analyze(
              "test", List.of(new QueryRecord(sql3, 100, System.currentTimeMillis(), "s")), null);

      // 4 ORs: above threshold
      String sql4 = "SELECT * FROM t WHERE a = 1 OR b = 2 OR c = 3 OR d = 4 OR e = 5";
      QueryAuditReport r4 =
          analyzer.analyze(
              "test", List.of(new QueryRecord(sql4, 100, System.currentTimeMillis(), "s")), null);

      assertThat(r2).isNotNull();
      assertThat(r3).isNotNull();
      assertThat(r4).isNotNull();
    }

    @Test
    void customThresholdsViaConfigBuilder() {
      // Extreme low thresholds
      QueryAuditConfig lowConfig =
          QueryAuditConfig.builder()
              .nPlusOneThreshold(1)
              .offsetPaginationThreshold(0)
              .orClauseThreshold(0)
              .build();
      QueryAuditAnalyzer lowAnalyzer = new QueryAuditAnalyzer(lowConfig, List.of());
      assertDoesNotThrow(
          () ->
              lowAnalyzer.analyze(
                  "test",
                  List.of(
                      new QueryRecord(
                          "SELECT * FROM t WHERE a = 1 OR b = 2",
                          100,
                          System.currentTimeMillis(),
                          "s")),
                  null));

      // Extreme high thresholds
      QueryAuditConfig highConfig =
          QueryAuditConfig.builder()
              .nPlusOneThreshold(Integer.MAX_VALUE)
              .offsetPaginationThreshold(Integer.MAX_VALUE)
              .orClauseThreshold(Integer.MAX_VALUE)
              .build();
      QueryAuditAnalyzer highAnalyzer = new QueryAuditAnalyzer(highConfig, List.of());
      assertDoesNotThrow(
          () ->
              highAnalyzer.analyze(
                  "test",
                  List.of(
                      new QueryRecord(
                          "SELECT * FROM t WHERE a = 1 OR b = 2 LIMIT 10 OFFSET 999999",
                          100,
                          System.currentTimeMillis(),
                          "s")),
                  null));
    }

    private List<QueryRecord> createRepeatedQueries(String sql, int count) {
      List<QueryRecord> queries = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        queries.add(new QueryRecord(sql, 1000, System.currentTimeMillis(), "stack"));
      }
      return queries;
    }

    private List<QueryRecord> createRepeatedQueriesNoStack(String sql, int count) {
      List<QueryRecord> queries = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        queries.add(new QueryRecord(sql, 1000, System.currentTimeMillis(), null));
      }
      return queries;
    }
  }

  // =========================================================================
  // 4. NULL SAFETY — every public method with null inputs
  // =========================================================================

  @Nested
  class NullSafetyTests {

    @Test
    void sqlParserAllMethodsWithNull() {
      assertThat(SqlParser.normalize(null)).isNull();
      assertThat(SqlParser.isSelectQuery(null)).isFalse();
      assertThat(SqlParser.hasSelectAll(null)).isFalse();
      assertThat(SqlParser.extractWhereColumns(null)).isEmpty();
      assertThat(SqlParser.extractJoinColumns(null)).isEmpty();
      assertThat(SqlParser.extractOrderByColumns(null)).isEmpty();
      assertThat(SqlParser.extractGroupByColumns(null)).isEmpty();
      assertThat(SqlParser.detectWhereFunctions(null)).isEmpty();
      assertThat(SqlParser.countOrConditions(null)).isEqualTo(0);
      assertThat(SqlParser.extractOffsetValue(null)).isEmpty();
      assertThat(SqlParser.extractTableNames(null)).isEmpty();
    }

    @Test
    void indexMetadataHasIndexOnWithNulls() {
      IndexMetadata metadata = new IndexMetadata(Map.of());
      assertDoesNotThrow(() -> metadata.hasIndexOn(null, "col"));
      assertDoesNotThrow(() -> metadata.hasIndexOn("table", null));
      assertDoesNotThrow(() -> metadata.hasIndexOn(null, null));
    }

    @Test
    void indexMetadataGetIndexesForTableWithNull() {
      IndexMetadata metadata = new IndexMetadata(Map.of());
      assertDoesNotThrow(() -> metadata.getIndexesForTable(null));
      assertDoesNotThrow(() -> metadata.getCompositeIndexes(null));
      assertDoesNotThrow(() -> metadata.hasTable(null));
    }

    @Test
    void queryRecordWithNulls() {
      assertDoesNotThrow(() -> new QueryRecord(null, null, 0, 0, null, 0));
      assertDoesNotThrow(() -> new QueryRecord(null, 0, 0, null));
    }

    @Test
    void queryGuardConfigBuilderWithNulls() {
      assertDoesNotThrow(() -> QueryAuditConfig.builder().baselinePath(null).build());
    }

    @Test
    void analyzerAnalyzeWithNullTestName() {
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
      assertDoesNotThrow(() -> analyzer.analyze(null, List.of(), null));
      assertDoesNotThrow(() -> analyzer.analyze(null, null, null));
    }

    @Test
    void analyzerAnalyzeWithNullMetadata() {
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
      List<QueryRecord> queries =
          List.of(
              new QueryRecord(
                  "SELECT * FROM t WHERE id = 1", 100, System.currentTimeMillis(), "s"));
      assertDoesNotThrow(() -> analyzer.analyze("test", queries, null));
    }

    @Test
    void analyzerFourArgAnalyzeWithNulls() {
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
      assertDoesNotThrow(() -> analyzer.analyze(null, null, null, null));
      assertDoesNotThrow(() -> analyzer.analyze(null, null, List.of(), null));
      assertDoesNotThrow(() -> analyzer.analyze("cls", "test", null, null));
    }

    @Test
    void consoleReporterWithNullReport() {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(baos);
      ConsoleReporter reporter = new ConsoleReporter(ps, false);

      // Create a minimal non-null report (ConsoleReporter.report expects non-null)
      QueryAuditReport emptyReport =
          new QueryAuditReport(null, null, List.of(), List.of(), List.of(), List.of(), 0, 0, 0L);
      assertDoesNotThrow(() -> reporter.report(emptyReport));
    }

    @Test
    void consoleReporterWithReportContainingNulls() {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(baos);
      ConsoleReporter reporter = new ConsoleReporter(ps, false);

      // Report where queries have null sql
      List<QueryRecord> queries = new ArrayList<>();
      queries.add(new QueryRecord(null, 0, 0, null));
      QueryAuditReport report =
          new QueryAuditReport(null, null, List.of(), List.of(), List.of(), queries, 0, 1, 0L);
      assertDoesNotThrow(() -> reporter.report(report));
    }

    @Test
    void htmlReporterWriteToFileWithEmptyReports(@TempDir Path tempDir) {
      HtmlReporter reporter = new HtmlReporter();
      assertDoesNotThrow(() -> reporter.writeToFile(tempDir, List.of()));
    }

    @Test
    void htmlReporterWriteToFileWithReports(@TempDir Path tempDir) {
      HtmlReporter reporter = new HtmlReporter();
      QueryAuditReport report =
          new QueryAuditReport(
              "TestClass", "testMethod", List.of(), List.of(), List.of(), List.of(), 0, 0, 0L);
      assertDoesNotThrow(() -> reporter.writeToFile(tempDir, List.of(report)));
    }

    @Test
    void queryGuardConfigIsSuppressedWithNulls() {
      QueryAuditConfig config = QueryAuditConfig.defaults();
      assertDoesNotThrow(() -> config.isSuppressed(null, null, null));
      assertDoesNotThrow(() -> config.isSuppressed("n-plus-one", null, null));
      assertDoesNotThrow(() -> config.isSuppressed(null, "table", null));
      assertDoesNotThrow(() -> config.isSuppressed(null, null, "column"));
    }

    @Test
    void queryGuardConfigIsQuerySuppressedWithNull() {
      QueryAuditConfig config = QueryAuditConfig.defaults();
      assertDoesNotThrow(() -> config.isQuerySuppressed(null));
      assertDoesNotThrow(() -> config.isQuerySuppressed(""));
    }
  }

  // =========================================================================
  // 5. REGEX TIMEOUT PROTECTION — pathological inputs
  // =========================================================================

  @Nested
  class RegexTimeoutTests {

    @Test
    @Timeout(5)
    void regexDoesNotHangOnRepeatedOrConditions() {
      String evil = "SELECT * FROM t WHERE " + "a OR ".repeat(10000) + "b = 1";
      assertDoesNotThrow(() -> SqlParser.normalize(evil));
      assertDoesNotThrow(() -> SqlParser.countOrConditions(evil));
      assertDoesNotThrow(() -> SqlParser.extractWhereColumns(evil));
    }

    @Test
    @Timeout(5)
    void regexDoesNotHangOnLargeInList() {
      String evil = "SELECT * FROM t WHERE id IN (" + "?,".repeat(100000) + "?)";
      assertDoesNotThrow(() -> SqlParser.normalize(evil));
      assertDoesNotThrow(() -> SqlParser.extractWhereColumns(evil));
    }

    @Test
    @Timeout(5)
    void regexDoesNotHangOnRepeatedLikeWildcards() {
      String evil = "SELECT * FROM t WHERE name LIKE '" + "%".repeat(10000) + "'";
      assertDoesNotThrow(() -> SqlParser.normalize(evil));
      assertDoesNotThrow(() -> SqlParser.extractOffsetValue(evil));
    }

    @Test
    @Timeout(5)
    void regexDoesNotHangOnDeeplyNestedParentheses() {
      String evil = "SELECT * FROM t WHERE " + "(".repeat(5000) + "a" + ")".repeat(5000);
      assertDoesNotThrow(() -> SqlParser.normalize(evil));
      assertDoesNotThrow(() -> SqlParser.extractWhereColumns(evil));
      assertDoesNotThrow(() -> SqlParser.countOrConditions(evil));
    }

    @Test
    @Timeout(5)
    void regexDoesNotHangOnRepeatedJoins() {
      StringBuilder sb = new StringBuilder("SELECT * FROM t0 ");
      for (int i = 1; i < 1000; i++) {
        sb.append("JOIN t")
            .append(i)
            .append(" ON t")
            .append(i)
            .append(".id = t")
            .append(i - 1)
            .append(".id ");
      }
      sb.append("WHERE t0.x = 1");
      String evil = sb.toString();
      assertDoesNotThrow(() -> SqlParser.extractJoinColumns(evil));
      assertDoesNotThrow(() -> SqlParser.extractTableNames(evil));
    }

    @Test
    @Timeout(5)
    void regexDoesNotHangOnRepeatedWhitespace() {
      String evil =
          "SELECT"
              + " ".repeat(100000)
              + "*"
              + " ".repeat(100000)
              + "FROM"
              + " ".repeat(100000)
              + "t";
      assertDoesNotThrow(() -> SqlParser.normalize(evil));
      assertDoesNotThrow(() -> SqlParser.isSelectQuery(evil));
    }

    @Test
    @Timeout(5)
    void regexDoesNotHangOnAlternatingQuotes() {
      String evil = "SELECT * FROM t WHERE x = " + "'a'".repeat(10000);
      assertDoesNotThrow(() -> SqlParser.normalize(evil));
    }

    @Test
    @Timeout(5)
    void analyzerDoesNotHangOnPathologicalInput() {
      String evil = "SELECT * FROM t WHERE " + "a OR ".repeat(5000) + "b = 1";
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
      List<QueryRecord> queries =
          List.of(new QueryRecord(evil, 100, System.currentTimeMillis(), "s"));
      assertDoesNotThrow(() -> analyzer.analyze("test", queries, null));
    }
  }

  // =========================================================================
  // 6. IDEMPOTENCY — analyzing same queries twice gives same result
  // =========================================================================

  @Nested
  class IdempotencyTests {

    @Test
    void analysisIsIdempotent() {
      List<QueryRecord> queries = createSampleQueries();
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());

      QueryAuditReport r1 = analyzer.analyze("test", queries, null);
      QueryAuditReport r2 = analyzer.analyze("test", queries, null);

      assertThat(r1.getConfirmedIssues()).hasSameSizeAs(r2.getConfirmedIssues());
      assertThat(r1.getInfoIssues()).hasSameSizeAs(r2.getInfoIssues());
      assertThat(r1.getTotalQueryCount()).isEqualTo(r2.getTotalQueryCount());
      assertThat(r1.getUniquePatternCount()).isEqualTo(r2.getUniquePatternCount());
      assertThat(r1.getTotalExecutionTimeNanos()).isEqualTo(r2.getTotalExecutionTimeNanos());
    }

    @Test
    void analysisIsIdempotentWithIndexMetadata() {
      List<QueryRecord> queries = createSampleQueries();
      Map<String, List<IndexInfo>> indexes = new HashMap<>();
      indexes.put("users", List.of(new IndexInfo("users", "idx_users_id", "id", 1, false, 1000)));
      IndexMetadata metadata = new IndexMetadata(indexes);

      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());

      QueryAuditReport r1 = analyzer.analyze("test", queries, metadata);
      QueryAuditReport r2 = analyzer.analyze("test", queries, metadata);

      assertThat(r1.getConfirmedIssues()).hasSameSizeAs(r2.getConfirmedIssues());
      assertThat(r1.getTotalQueryCount()).isEqualTo(r2.getTotalQueryCount());
    }

    @Test
    void normalizeIsIdempotent() {
      String sql = "SELECT * FROM users WHERE id = 42 AND name = 'John'";
      String normalized1 = SqlParser.normalize(sql);
      String normalized2 = SqlParser.normalize(sql);
      assertThat(normalized1).isEqualTo(normalized2);

      // Also: normalizing a normalized result should be stable
      String normalized3 = SqlParser.normalize(normalized1);
      assertThat(normalized3).isNotNull();
    }

    private List<QueryRecord> createSampleQueries() {
      List<QueryRecord> queries = new ArrayList<>();
      // N+1 pattern
      for (int i = 0; i < 5; i++) {
        queries.add(
            new QueryRecord(
                "SELECT * FROM users WHERE id = " + i, 1000, System.currentTimeMillis(), "stack"));
      }
      // SELECT * query
      queries.add(
          new QueryRecord(
              "SELECT * FROM orders WHERE user_id = 1", 2000, System.currentTimeMillis(), "stack"));
      // OR abuse
      queries.add(
          new QueryRecord(
              "SELECT * FROM items WHERE a = 1 OR b = 2 OR c = 3 OR d = 4",
              3000,
              System.currentTimeMillis(),
              "stack"));
      // Large offset
      queries.add(
          new QueryRecord(
              "SELECT * FROM logs LIMIT 10 OFFSET 50000",
              4000,
              System.currentTimeMillis(),
              "stack"));
      return queries;
    }
  }

  // =========================================================================
  // 7. MEMORY PRESSURE — large inputs don't OOM
  // =========================================================================

  @Nested
  class MemoryPressureTests {

    @Test
    @Timeout(120)
    void handlesLargeNumberOfQueriesWithoutOOM() {
      List<QueryRecord> queries = new ArrayList<>();
      for (int i = 0; i < 50000; i++) {
        queries.add(
            new QueryRecord(
                "SELECT * FROM table_" + (i % 100) + " WHERE id = " + i,
                1000,
                System.currentTimeMillis(),
                "stack" + i));
      }

      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
      assertDoesNotThrow(() -> analyzer.analyze("large", queries, null));
    }

    @Test
    // 30s budget: 10k distinct queries each pay a JSqlParser parse cost (~0.5-1ms); the cache
    // helps only when SQL strings repeat, which is not the case here.
    @Timeout(30)
    void handlesManyDistinctQueryPatterns() {
      List<QueryRecord> queries = new ArrayList<>();
      for (int i = 0; i < 10000; i++) {
        queries.add(
            new QueryRecord(
                "SELECT col_" + i + " FROM table_" + i + " WHERE id_" + i + " = " + i,
                100,
                System.currentTimeMillis(),
                "s"));
      }

      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
      assertDoesNotThrow(() -> analyzer.analyze("distinct", queries, null));
    }

    @Test
    @Timeout(10)
    void handlesLargeIndexMetadata() {
      Map<String, List<IndexInfo>> indexes = new HashMap<>();
      for (int t = 0; t < 500; t++) {
        String tableName = "table_" + t;
        List<IndexInfo> infos = new ArrayList<>();
        for (int c = 0; c < 20; c++) {
          infos.add(
              new IndexInfo(tableName, "idx_" + t + "_" + c, "col_" + c, c + 1, false, 10000));
        }
        indexes.put(tableName, infos);
      }

      IndexMetadata metadata = new IndexMetadata(indexes);
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());

      List<QueryRecord> queries = new ArrayList<>();
      for (int i = 0; i < 100; i++) {
        queries.add(
            new QueryRecord(
                "SELECT * FROM table_" + (i % 500) + " WHERE col_0 = " + i,
                100,
                System.currentTimeMillis(),
                "s"));
      }

      assertDoesNotThrow(() -> analyzer.analyze("large-metadata", queries, metadata));
    }

    @Test
    @Timeout(10)
    void consoleReporterHandlesLargeReport() {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(baos);
      ConsoleReporter reporter = new ConsoleReporter(ps, false);

      List<QueryRecord> queries = new ArrayList<>();
      for (int i = 0; i < 5000; i++) {
        queries.add(
            new QueryRecord(
                "SELECT * FROM t" + (i % 50) + " WHERE id = " + i,
                100,
                System.currentTimeMillis(),
                "s"));
      }

      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
      QueryAuditReport report = analyzer.analyze("large", queries, null);
      assertDoesNotThrow(() -> reporter.report(report));
    }
  }

  // =========================================================================
  // 8. REAL-WORLD HIBERNATE SQL PATTERNS
  // =========================================================================

  @Nested
  class HibernateSqlPatternTests {

    private static final String[] HIBERNATE_SQLS = {
      // Basic entity queries
      "select u1_0.id,u1_0.created_at,u1_0.deleted_at,u1_0.email from users u1_0 where u1_0.deleted_at is null and u1_0.id=?",
      "select r1_0.id,r1_0.title from rooms r1_0 left join room_members rm1_0 on r1_0.id=rm1_0.room_id where rm1_0.user_id=? and rm1_0.role in (?,?,?) order by rm1_0.role",
      "select count(*) from messages m1_0 where m1_0.room_id=? and m1_0.id>? and m1_0.deleted_at is null and m1_0.sender_id<>?",
      "select us1_0.id,us1_0.is_permanent,us1_0.reason from user_suspensions us1_0 where us1_0.user_id=? and us1_0.unsuspended_at is null and (us1_0.is_permanent=? or us1_0.suspended_until>?) order by us1_0.suspended_at desc",

      // Complex joins
      "select o1_0.id,o1_0.amount,o1_0.created_at,o1_0.status,u1_0.id,u1_0.name from orders o1_0 join users u1_0 on o1_0.user_id=u1_0.id left join order_items oi1_0 on o1_0.id=oi1_0.order_id where o1_0.status=? and u1_0.deleted_at is null order by o1_0.created_at desc",
      "select t1_0.id,t1_0.name,count(m1_0.id) from teams t1_0 left join members m1_0 on t1_0.id=m1_0.team_id group by t1_0.id,t1_0.name having count(m1_0.id)>?",

      // Subqueries
      "select u1_0.id,u1_0.name from users u1_0 where u1_0.id in (select rm1_0.user_id from room_members rm1_0 where rm1_0.room_id=?)",
      "select * from products p1_0 where p1_0.price>(select avg(p2_0.price) from products p2_0 where p2_0.category_id=p1_0.category_id)",

      // EXISTS
      "select u1_0.id from users u1_0 where exists (select 1 from orders o1_0 where o1_0.user_id=u1_0.id and o1_0.status=?)",

      // CASE WHEN
      "select u1_0.id,case when u1_0.role='ADMIN' then 'Administrator' when u1_0.role='USER' then 'Regular' else 'Unknown' end from users u1_0 where u1_0.active=?",

      // UNION
      "select u1_0.id,u1_0.name,'user' as type from users u1_0 where u1_0.active=? union all select a1_0.id,a1_0.name,'admin' as type from admins a1_0 where a1_0.active=?",

      // Pagination
      "select m1_0.id,m1_0.content,m1_0.created_at from messages m1_0 where m1_0.room_id=? order by m1_0.created_at desc limit ? offset ?",

      // LIKE patterns
      "select u1_0.id,u1_0.name from users u1_0 where u1_0.name like ? and u1_0.email like ?",
      "select p1_0.id,p1_0.title from posts p1_0 where p1_0.title like '%search%' or p1_0.content like '%search%'",

      // Function usage in WHERE
      "select u1_0.id from users u1_0 where lower(u1_0.email)=? and year(u1_0.created_at)=?",
      "select * from events e1_0 where date(e1_0.start_time)=? and trim(e1_0.title)<>?",

      // Complex WHERE conditions
      "select * from items i1_0 where (i1_0.status=? or i1_0.status=?) and i1_0.category_id=? and (i1_0.price between ? and ?) and i1_0.deleted_at is null",

      // Multiple JOINs
      "select o1_0.id,oi1_0.product_name,p1_0.price from orders o1_0 inner join order_items oi1_0 on o1_0.id=oi1_0.order_id inner join products p1_0 on oi1_0.product_id=p1_0.id inner join users u1_0 on o1_0.user_id=u1_0.id where o1_0.created_at>? and u1_0.country=?",

      // INSERT/UPDATE/DELETE
      "insert into audit_logs (action,entity_type,entity_id,user_id,created_at) values (?,?,?,?,?)",
      "update users set last_login_at=?,login_count=login_count+1 where id=?",
      "delete from sessions where expired_at<? and user_id=?",

      // Hibernate batch insert
      "insert into order_items (order_id,product_id,quantity,price) values (?,?,?,?),(?,?,?,?),(?,?,?,?)",

      // Sequence queries (Hibernate)
      "select nextval('users_id_seq')",
      "select currval('orders_id_seq')",

      // Schema queries
      "select table_name from information_schema.tables where table_schema='public'",

      // Very long IN clause
      "select * from users where id in (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    };

    @Test
    void allHibernateSqlParsedWithoutCrash() {
      for (String sql : HIBERNATE_SQLS) {
        assertDoesNotThrow(() -> SqlParser.normalize(sql), "normalize failed for: " + sql);
        assertDoesNotThrow(() -> SqlParser.isSelectQuery(sql), "isSelectQuery failed for: " + sql);
        assertDoesNotThrow(() -> SqlParser.hasSelectAll(sql), "hasSelectAll failed for: " + sql);
        assertDoesNotThrow(
            () -> SqlParser.extractWhereColumns(sql), "extractWhereColumns failed for: " + sql);
        assertDoesNotThrow(
            () -> SqlParser.extractJoinColumns(sql), "extractJoinColumns failed for: " + sql);
        assertDoesNotThrow(
            () -> SqlParser.extractOrderByColumns(sql), "extractOrderByColumns failed for: " + sql);
        assertDoesNotThrow(
            () -> SqlParser.extractGroupByColumns(sql), "extractGroupByColumns failed for: " + sql);
        assertDoesNotThrow(
            () -> SqlParser.detectWhereFunctions(sql), "detectWhereFunctions failed for: " + sql);
        assertDoesNotThrow(
            () -> SqlParser.countOrConditions(sql), "countOrConditions failed for: " + sql);
        assertDoesNotThrow(
            () -> SqlParser.extractOffsetValue(sql), "extractOffsetValue failed for: " + sql);
        assertDoesNotThrow(
            () -> SqlParser.extractTableNames(sql), "extractTableNames failed for: " + sql);
      }
    }

    @Test
    void allHibernateSqlAnalyzedWithoutCrash() {
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
      for (String sql : HIBERNATE_SQLS) {
        List<QueryRecord> queries =
            List.of(new QueryRecord(sql, 1000, System.currentTimeMillis(), "HibernateQuery"));
        assertDoesNotThrow(
            () -> analyzer.analyze("hibernate-test", queries, null), "Analyzer crashed on: " + sql);
      }
    }

    @Test
    void hibernateSqlProducesReasonableResults() {
      // Basic SELECT should be detected as a select query
      assertThat(SqlParser.isSelectQuery(HIBERNATE_SQLS[0])).isTrue();

      // Query with tables should extract at least one table
      assertThat(SqlParser.extractTableNames(HIBERNATE_SQLS[0])).isNotEmpty();

      // Query with WHERE should extract at least one column
      assertThat(SqlParser.extractWhereColumns(HIBERNATE_SQLS[0])).isNotEmpty();

      // Query with JOIN should extract join columns
      assertThat(SqlParser.extractJoinColumns(HIBERNATE_SQLS[4])).isNotEmpty();

      // Query with ORDER BY should extract order by columns
      assertThat(SqlParser.extractOrderByColumns(HIBERNATE_SQLS[3])).isNotEmpty();

      // Query with GROUP BY should extract group by columns
      assertThat(SqlParser.extractGroupByColumns(HIBERNATE_SQLS[5])).isNotEmpty();

      // Query with LOWER/YEAR should detect functions in WHERE
      assertThat(SqlParser.detectWhereFunctions(HIBERNATE_SQLS[14])).isNotEmpty();

      // Query with OFFSET should extract offset value
      assertThat(SqlParser.extractOffsetValue(HIBERNATE_SQLS[11])).isEmpty(); // ? is not a number

      // INSERT should not be a select query
      assertThat(SqlParser.isSelectQuery(HIBERNATE_SQLS[18])).isFalse();
    }

    @Test
    void nPlusOneDetectedOnRepeatedHibernateQueries() {
      QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
      List<QueryRecord> queries = new ArrayList<>();
      // Simulate N+1: same query pattern repeated 5 times
      for (int i = 0; i < 5; i++) {
        queries.add(
            new QueryRecord(
                "select u1_0.id,u1_0.name from users u1_0 where u1_0.id=" + i,
                1000,
                System.currentTimeMillis(),
                "stack"));
      }
      QueryAuditReport report = analyzer.analyze("n+1-test", queries, null);
      // SQL-level N+1 suspect is INFO (Hibernate-level is authoritative)
      assertThat(report.getInfoIssues()).isNotEmpty();
      assertThat(
              report.getInfoIssues().stream()
                  .anyMatch(i -> i.type().getCode().equals("n-plus-one-suspect")))
          .isTrue();
    }
  }
}
