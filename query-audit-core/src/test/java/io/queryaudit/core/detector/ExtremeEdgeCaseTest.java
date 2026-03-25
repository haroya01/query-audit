package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Extreme edge-case stress test that runs every conceivable dangerous SQL input through ALL
 * detectors via {@link QueryAuditAnalyzer}.
 *
 * <p>For a mission-critical system: no crashes, no exceptions, no infinite loops, no null reports.
 * Every query must produce a valid {@link QueryAuditReport} in under 5 seconds.
 */
@Timeout(60)
class ExtremeEdgeCaseTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private QueryAuditAnalyzer analyzer;

  @BeforeEach
  void setUp() {
    analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
  }

  // ── helpers ─────────────────────────────────────────────────────────

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  /**
   * Feeds every SQL in the list through the analyzer and asserts: 1. No exception of any kind
   * (catches Throwable) 2. Report is not null 3. Completes in under 5 seconds per query
   */
  private void assertAllSafe(List<String> sqls, String category) {
    for (String sql : sqls) {
      long start = System.nanoTime();
      try {
        QueryAuditReport report =
            analyzer.analyze("ExtremeEdgeCaseTest", List.of(record(sql)), EMPTY_INDEX);
        assertThat(report).as("Null report for [%s]: %s", category, truncate(sql)).isNotNull();
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        assertThat(elapsedMs)
            .as("Timeout (>5s) for [%s]: %s", category, truncate(sql))
            .isLessThan(5_000);
      } catch (Throwable t) {
        fail(
            "Crashed on [%s] input: %s\nException: %s: %s",
            category, truncate(sql), t.getClass().getName(), t.getMessage());
      }
    }
  }

  /** Like {@link #assertAllSafe} but accepts multiple records (for batch/N+1 tests). */
  private void assertAllSafeMultiRecord(List<QueryRecord> records, String category) {
    long start = System.nanoTime();
    try {
      QueryAuditReport report = analyzer.analyze("ExtremeEdgeCaseTest", records, EMPTY_INDEX);
      assertThat(report).as("Null report for multi-record [%s]", category).isNotNull();
      long elapsedMs = (System.nanoTime() - start) / 1_000_000;
      assertThat(elapsedMs).as("Timeout (>5s) for multi-record [%s]", category).isLessThan(5_000);
    } catch (Throwable t) {
      fail(
          "Crashed on multi-record [%s]\nException: %s: %s",
          category, t.getClass().getName(), t.getMessage());
    }
  }

  private static String truncate(String s) {
    if (s == null) return "<null>";
    return s.length() > 120 ? s.substring(0, 120) + "..." : s;
  }

  // ═══════════════════════════════════════════════════════════════════
  // 1. MALFORMED SQL
  // ═══════════════════════════════════════════════════════════════════

  @Nested
  class MalformedSql {

    @Test
    void emptyAndWhitespace() {
      assertAllSafe(
          List.of("", " ", "   ", "\t", "\n", "\r\n", "\t\n\r  \t", "  \n  \n  "),
          "empty/whitespace");
    }

    @Test
    void nullSql() {
      // null SQL inside QueryRecord
      try {
        QueryRecord rec = new QueryRecord(null, 0L, System.currentTimeMillis(), "");
        QueryAuditReport report =
            analyzer.analyze("ExtremeEdgeCaseTest", List.of(rec), EMPTY_INDEX);
        assertThat(report).isNotNull();
      } catch (Throwable t) {
        fail("Crashed on null SQL: %s: %s", t.getClass().getName(), t.getMessage());
      }
    }

    @Test
    void sqlFragments() {
      assertAllSafe(
          List.of(
              "SELECT",
              "WHERE",
              "FROM users",
              "INSERT INTO",
              "UPDATE",
              "DELETE",
              "DROP TABLE",
              "ALTER TABLE",
              "CREATE INDEX",
              "GRANT ALL",
              "REVOKE",
              "TRUNCATE",
              "MERGE",
              "REPLACE",
              "UPSERT",
              "ON DUPLICATE KEY UPDATE",
              "GROUP BY",
              "ORDER BY",
              "HAVING",
              "LIMIT",
              "OFFSET",
              "FETCH NEXT",
              "FOR UPDATE"),
          "sql-fragments");
    }

    @Test
    void unclosedParentheses() {
      assertAllSafe(
          List.of(
              "SELECT * FROM (SELECT * FROM users",
              "SELECT * FROM users WHERE id IN (1, 2, 3",
              "SELECT (((1 + 2) * 3)",
              "INSERT INTO t VALUES (1, 2",
              "SELECT * FROM (((((users)))))",
              "SELECT * FROM t WHERE (a = 1 AND (b = 2 OR (c = 3))"),
          "unclosed-parens");
    }

    @Test
    void unclosedQuotes() {
      assertAllSafe(
          List.of(
              "SELECT * FROM users WHERE name = 'test",
              "SELECT * FROM users WHERE name = \"test",
              "SELECT * FROM users WHERE name = 'it''s",
              "SELECT * FROM users WHERE name = 'test\\",
              "SELECT 'unclosed",
              "SELECT \"double unclosed"),
          "unclosed-quotes");
    }

    @Test
    void doubleSemicolons() {
      assertAllSafe(
          List.of(
              "SELECT 1;; SELECT 2",
              ";;;",
              "SELECT 1;",
              ";SELECT 1",
              "SELECT 1;;;;;;",
              ";;SELECT * FROM users;;"),
          "double-semicolons");
    }

    @Test
    void sqlComments() {
      assertAllSafe(
          List.of(
              "SELECT /* comment */ * FROM users",
              "SELECT -- comment\n* FROM users",
              "/* entire query is comment */",
              "-- entire line comment",
              "SELECT * FROM users /* unclosed comment",
              "SELECT * FROM /* nested /* comment */ */ users",
              "SELECT * -- trailing\nFROM users -- trailing\nWHERE id = 1",
              "#mysql style comment\nSELECT 1",
              "SELECT 1 -- ",
              "SELECT /* */ * /* */ FROM /* */ users"),
          "sql-comments");
    }

    @Test
    void extremelyNested() {
      assertAllSafe(
          List.of(
              "SELECT * FROM ((((((((users))))))))",
              "SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT 1)))",
              "SELECT ((((((((((1))))))))))",
              "SELECT * FROM t WHERE ((((((((id = 1))))))))"),
          "extremely-nested");
    }

    @Test
    void mixedCaseKeywords() {
      assertAllSafe(
          List.of(
              "sElEcT * fRoM users wHeRe id = 1",
              "SeLeCt DiStInCt NaMe FrOm UsErS",
              "INSERT into USERS (name) values ('test')",
              "update USERS set NAME = 'x' where ID = 1",
              "delete FROM users WHERE id = 1"),
          "mixed-case");
    }

    @Test
    void garbledInput() {
      assertAllSafe(
          List.of(
              "asdfghjkl",
              "12345",
              "!@#$%^&*()",
              "<html>SELECT * FROM users</html>",
              "{\"json\": true}",
              "<?xml version=\"1.0\"?>",
              "function() { return 1; }",
              "0x0000DEADBEEF",
              "NaN",
              "Infinity",
              "true",
              "false",
              "undefined",
              "null"),
          "garbled");
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // 2. EXTREMELY LONG INPUTS
  // ═══════════════════════════════════════════════════════════════════

  @Nested
  class ExtremelyLongInputs {

    @Test
    void queryWith1000Columns() {
      String cols =
          IntStream.rangeClosed(1, 1000).mapToObj(i -> "col" + i).collect(Collectors.joining(", "));
      String sql = "SELECT " + cols + " FROM t";
      assertAllSafe(List.of(sql), "1000-columns");
    }

    @Test
    void queryWith100Joins() {
      StringBuilder sb = new StringBuilder("SELECT * FROM t0");
      for (int i = 1; i <= 100; i++) {
        sb.append(" JOIN t")
            .append(i)
            .append(" ON t")
            .append(i - 1)
            .append(".id = t")
            .append(i)
            .append(".fk");
      }
      assertAllSafe(List.of(sb.toString()), "100-joins");
    }

    @Test
    void whereClauseWith100AndConditions() {
      String conds =
          IntStream.rangeClosed(1, 100)
              .mapToObj(i -> "col" + i + " = ?")
              .collect(Collectors.joining(" AND "));
      String sql = "SELECT * FROM users WHERE " + conds;
      assertAllSafe(List.of(sql), "100-and-conditions");
    }

    @Test
    void inClauseWith500Values() {
      String vals =
          IntStream.rangeClosed(1, 500).mapToObj(String::valueOf).collect(Collectors.joining(", "));
      String sql = "SELECT * FROM users WHERE id IN (" + vals + ")";
      assertAllSafe(List.of(sql), "500-in-values");
    }

    @Test
    void queryThatIs100KBLong() {
      // Repeat a safe pattern until we hit ~100KB
      String pattern = "col_abcdefghij = ? AND ";
      int repeats = 100_000 / pattern.length();
      String conds = pattern.repeat(repeats);
      // trim trailing AND
      conds = conds.substring(0, conds.length() - 5);
      String sql = "SELECT * FROM t WHERE " + conds;
      assertThat(sql.length()).isGreaterThan(90_000);
      assertAllSafe(List.of(sql), "100KB-query");
    }

    @Test
    void deeplyNestedSubqueries20Levels() {
      StringBuilder sql = new StringBuilder("SELECT * FROM ");
      for (int i = 0; i < 20; i++) {
        sql.append("(SELECT * FROM ");
      }
      sql.append("users");
      for (int i = 0; i < 20; i++) {
        sql.append(") sub").append(i);
      }
      assertAllSafe(List.of(sql.toString()), "20-nested-subqueries");
    }

    @Test
    void queryWith100OrConditions() {
      String conds =
          IntStream.rangeClosed(1, 100)
              .mapToObj(i -> "id = " + i)
              .collect(Collectors.joining(" OR "));
      String sql = "SELECT * FROM users WHERE " + conds;
      assertAllSafe(List.of(sql), "100-or-conditions");
    }

    @Test
    void queryWith50UnionAlls() {
      String sql =
          IntStream.rangeClosed(1, 50)
              .mapToObj(i -> "SELECT " + i + " AS id FROM dual")
              .collect(Collectors.joining(" UNION ALL "));
      assertAllSafe(List.of(sql), "50-union-alls");
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // 3. UNICODE AND SPECIAL CHARACTERS
  // ═══════════════════════════════════════════════════════════════════

  @Nested
  class UnicodeAndSpecialCharacters {

    @Test
    void unicodeTableAndColumnNames() {
      assertAllSafe(
          List.of(
              "SELECT * FROM \u7528\u6237 WHERE \u540D\u524D = ?",
              "SELECT * FROM \u00FCser WHERE stra\u00DFe = ?",
              "SELECT * FROM \u0442\u0430\u0431\u043B\u0438\u0446\u0430 WHERE \u043A\u043E\u043B\u043E\u043D\u043A\u0430 = ?",
              "SELECT * FROM \uD14C\uC774\uBE14 WHERE \uCEEC\uB7FC = ?",
              "SELECT \u0639\u0645\u0648\u062F FROM \u062C\u062F\u0648\u0644",
              "SELECT * FROM caf\u00E9 WHERE r\u00E9sum\u00E9 = ?"),
          "unicode-names");
    }

    @Test
    void emojiInLiterals() {
      assertAllSafe(
          List.of(
              "INSERT INTO logs (msg) VALUES ('\uD83D\uDE80')",
              "SELECT * FROM users WHERE bio LIKE '%\uD83D\uDE00%'",
              "UPDATE posts SET title = '\uD83C\uDF1F Star Post \uD83C\uDF1F' WHERE id = 1",
              "INSERT INTO t (c) VALUES ('\uD83D\uDC68\u200D\uD83D\uDC69\u200D\uD83D\uDC67\u200D\uD83D\uDC66')"),
          "emoji");
    }

    @Test
    void nullBytesInQuery() {
      assertAllSafe(
          List.of(
              "SELECT * FROM users\0WHERE id = 1",
              "SELECT\0*\0FROM\0users",
              "\0SELECT * FROM users",
              "SELECT * FROM users\0",
              "SELECT * FROM users WHERE name = '\0'"),
          "null-bytes");
    }

    @Test
    void tabAndNewlineCharacters() {
      assertAllSafe(
          List.of(
              "SELECT\t*\tFROM\tusers",
              "SELECT\n*\nFROM\nusers",
              "SELECT\r\n*\r\nFROM\r\nusers",
              "SELECT\t\n\t*\t\n\tFROM\t\n\tusers\t\n\tWHERE\t\n\tid = 1",
              "\n\n\nSELECT * FROM users\n\n\n"),
          "tabs-newlines");
    }

    @Test
    void backtickHeavyQueries() {
      assertAllSafe(
          List.of(
              "SELECT `id`, `name` FROM `users` WHERE `status` = ?",
              "SELECT `t`.`id`, `t`.`name` FROM `schema`.`table` `t`",
              "SELECT `` FROM ``",
              "SELECT `col with spaces` FROM `table with spaces`",
              "INSERT INTO `order` (`select`, `from`) VALUES (?, ?)"),
          "backticks");
    }

    @Test
    void specialStringEscapes() {
      assertAllSafe(
          List.of(
              "SELECT * FROM users WHERE name = 'O''Brien'",
              "SELECT * FROM users WHERE path = 'C:\\\\temp\\\\file.txt'",
              "SELECT * FROM users WHERE data = '\\n\\t\\r'",
              "SELECT * FROM users WHERE name = ''",
              "SELECT * FROM users WHERE name = ''''"),
          "string-escapes");
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // 4. DATABASE-SPECIFIC EDGE CASES
  // ═══════════════════════════════════════════════════════════════════

  @Nested
  class DatabaseSpecificEdgeCases {

    @Test
    void mySqlSpecificSyntax() {
      assertAllSafe(
          List.of(
              "SELECT /*!50000 SQL_NO_CACHE */ * FROM users",
              "SELECT /*+ INDEX(users idx_name) */ * FROM users",
              "SELECT SQL_CALC_FOUND_ROWS * FROM users LIMIT 10",
              "SELECT FOUND_ROWS()",
              "INSERT INTO t VALUES (1) ON DUPLICATE KEY UPDATE c = VALUES(c)",
              "REPLACE INTO t (id, name) VALUES (1, 'test')",
              "SELECT * FROM users FORCE INDEX (idx_name) WHERE name = ?",
              "SELECT * FROM users USE INDEX (idx_name) WHERE name = ?",
              "SELECT * FROM users IGNORE INDEX (idx_name) WHERE name = ?"),
          "mysql-syntax");
    }

    @Test
    void variablesAndSession() {
      assertAllSafe(
          List.of(
              "SET @var = 1; SELECT @var",
              "SET SESSION wait_timeout = 28800",
              "SET NAMES utf8mb4",
              "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
              "SELECT @@version",
              "SELECT @@global.max_connections",
              "SET @rownum := 0; SELECT @rownum := @rownum + 1 AS rank FROM users"),
          "variables");
    }

    @Test
    void prepareAndExecute() {
      assertAllSafe(
          List.of(
              "PREPARE stmt FROM 'SELECT * FROM users WHERE id = ?'",
              "EXECUTE stmt USING @id",
              "DEALLOCATE PREPARE stmt",
              "PREPARE stmt FROM 'INSERT INTO t VALUES (?)'",
              "EXECUTE IMMEDIATE 'SELECT 1'"),
          "prepare-execute");
    }

    @Test
    void callProcedure() {
      assertAllSafe(
          List.of(
              "CALL procedure(?)",
              "CALL schema.my_procedure(1, 'test', NULL)",
              "CALL sp_no_args()",
              "{call myproc(?, ?)}"),
          "call-procedure");
    }

    @Test
    void adminStatements() {
      assertAllSafe(
          List.of(
              "SHOW TABLES",
              "SHOW DATABASES",
              "SHOW CREATE TABLE users",
              "SHOW INDEX FROM users",
              "SHOW VARIABLES LIKE 'max%'",
              "SHOW PROCESSLIST",
              "DESCRIBE users",
              "DESC users",
              "EXPLAIN SELECT * FROM users WHERE id = 1",
              "EXPLAIN ANALYZE SELECT * FROM users",
              "ANALYZE TABLE users",
              "OPTIMIZE TABLE users",
              "CHECK TABLE users"),
          "admin-statements");
    }

    @Test
    void lockStatements() {
      assertAllSafe(
          List.of(
              "LOCK TABLES users WRITE",
              "LOCK TABLES users READ, orders WRITE",
              "UNLOCK TABLES",
              "SELECT * FROM users WHERE id = 1 FOR SHARE",
              "SELECT * FROM users WHERE id = 1 FOR UPDATE SKIP LOCKED",
              "SELECT * FROM users WHERE id = 1 FOR UPDATE NOWAIT",
              "SELECT * FROM users WHERE id = 1 LOCK IN SHARE MODE"),
          "lock-statements");
    }

    @Test
    void transactionStatements() {
      assertAllSafe(
          List.of(
              "BEGIN",
              "BEGIN WORK",
              "START TRANSACTION",
              "COMMIT",
              "COMMIT WORK",
              "ROLLBACK",
              "ROLLBACK WORK",
              "SAVEPOINT sp1",
              "ROLLBACK TO SAVEPOINT sp1",
              "RELEASE SAVEPOINT sp1",
              "SET autocommit = 0",
              "SET autocommit = 1",
              "XA START 'xid1'",
              "XA END 'xid1'",
              "XA PREPARE 'xid1'",
              "XA COMMIT 'xid1'"),
          "transaction-statements");
    }

    @Test
    void cteQueries() {
      assertAllSafe(
          List.of(
              "WITH cte AS (SELECT * FROM users) SELECT * FROM cte",
              "WITH RECURSIVE cte AS (SELECT 1 UNION ALL SELECT n+1 FROM cte WHERE n < 10) SELECT * FROM cte",
              "WITH a AS (SELECT 1), b AS (SELECT 2) SELECT * FROM a, b",
              "WITH cte AS (SELECT * FROM users WHERE id = ?) SELECT cte.* FROM cte JOIN orders ON cte.id = orders.user_id"),
          "cte-queries");
    }

    @Test
    void windowFunctions() {
      assertAllSafe(
          List.of(
              "SELECT *, ROW_NUMBER() OVER (ORDER BY id) FROM users",
              "SELECT *, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) FROM employees",
              "SELECT *, LAG(salary) OVER (ORDER BY hire_date) FROM employees",
              "SELECT *, SUM(amount) OVER (PARTITION BY user_id ORDER BY created_at ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM orders"),
          "window-functions");
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // 5. SQL INJECTION PATTERNS
  // ═══════════════════════════════════════════════════════════════════

  @Nested
  class SqlInjectionPatterns {

    @Test
    void classicInjections() {
      assertAllSafe(
          List.of(
              "SELECT * FROM users WHERE id = 1 OR 1=1",
              "SELECT * FROM users; DROP TABLE users; --",
              "SELECT * FROM users WHERE name = '' OR '1'='1'",
              "SELECT * FROM users WHERE id = 1 UNION SELECT * FROM passwords",
              "SELECT * FROM users WHERE id = 1; DELETE FROM users; --",
              "SELECT * FROM users WHERE name = 'admin'--",
              "SELECT * FROM users WHERE name = 'admin'/*",
              "SELECT * FROM users WHERE id = 1 AND 1=CONVERT(int,(SELECT TOP 1 name FROM sysobjects))",
              "SELECT * FROM users WHERE name = '' UNION SELECT NULL, username, password FROM users--",
              "SELECT * FROM users WHERE id = sleep(5)",
              "SELECT * FROM users WHERE id = BENCHMARK(10000000, SHA1('test'))"),
          "classic-injections");
    }

    @Test
    void blindInjectionPatterns() {
      assertAllSafe(
          List.of(
              "SELECT * FROM users WHERE id = 1 AND SUBSTRING(username,1,1) = 'a'",
              "SELECT * FROM users WHERE id = 1 AND (SELECT COUNT(*) FROM information_schema.tables) > 0",
              "SELECT * FROM users WHERE id = 1 AND IF(1=1, 1, 0)",
              "SELECT * FROM users WHERE id = 1 WAITFOR DELAY '00:00:05'",
              "SELECT * FROM users WHERE id = 1 AND ORD(MID((SELECT IFNULL(CAST(username AS CHAR),0x20) FROM users LIMIT 0,1),1,1))>64"),
          "blind-injections");
    }

    @Test
    void stackedQueries() {
      assertAllSafe(
          List.of(
              "SELECT 1; SELECT 2; SELECT 3",
              "SELECT * FROM users; INSERT INTO audit(action) VALUES('hack'); SELECT 1",
              "UPDATE users SET admin=1 WHERE id=1; COMMIT; SELECT * FROM users"),
          "stacked-queries");
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // 6. REAL HIBERNATE EDGE CASES
  // ═══════════════════════════════════════════════════════════════════

  @Nested
  class HibernateEdgeCases {

    @Test
    void hibernateBatchInserts() {
      // Simulate Hibernate batch: many identical inserts
      List<QueryRecord> records =
          IntStream.rangeClosed(1, 50)
              .mapToObj(i -> record("INSERT INTO users (name, email) VALUES (?, ?)"))
              .toList();
      assertAllSafeMultiRecord(records, "hibernate-batch-insert");
    }

    @Test
    void hibernateSequenceQueries() {
      assertAllSafe(
          List.of(
              "select next_val as id_val from hibernate_sequence for update",
              "select nextval('hibernate_sequence')",
              "select hibernate_sequence.nextval from dual",
              "call next value for hibernate_sequence",
              "select next value for hibernate_sequence"),
          "hibernate-sequence");
    }

    @Test
    void hibernateSchemaValidation() {
      assertAllSafe(
          List.of(
              "select * from information_schema.tables",
              "select * from information_schema.columns where table_name = ?",
              "select * from information_schema.table_constraints",
              "select table_name from information_schema.tables where table_schema = ?",
              "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS"),
          "schema-validation");
    }

    @Test
    void springSessionQueries() {
      assertAllSafe(
          List.of(
              "SELECT * FROM SPRING_SESSION WHERE SESSION_ID = ?",
              "DELETE FROM SPRING_SESSION WHERE SESSION_ID = ?",
              "DELETE FROM SPRING_SESSION_ATTRIBUTES WHERE SESSION_PRIMARY_ID = ?",
              "INSERT INTO SPRING_SESSION (PRIMARY_ID, SESSION_ID, CREATION_TIME, LAST_ACCESS_TIME, MAX_INACTIVE_INTERVAL, EXPIRY_TIME, PRINCIPAL_NAME) VALUES (?, ?, ?, ?, ?, ?, ?)",
              "UPDATE SPRING_SESSION SET LAST_ACCESS_TIME = ?, MAX_INACTIVE_INTERVAL = ?, EXPIRY_TIME = ? WHERE PRIMARY_ID = ?",
              "DELETE FROM SPRING_SESSION WHERE EXPIRY_TIME < ?"),
          "spring-session");
    }

    @Test
    void flywayMigration() {
      assertAllSafe(
          List.of(
              "INSERT INTO flyway_schema_history (installed_rank, version, description, type, script, checksum, installed_by, execution_time, success) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
              "SELECT * FROM flyway_schema_history ORDER BY installed_rank",
              "CREATE TABLE IF NOT EXISTS flyway_schema_history (installed_rank INT NOT NULL)",
              "DELETE FROM flyway_schema_history WHERE success = 0"),
          "flyway");
    }

    @Test
    void hibernateNPlusOnePattern() {
      // Simulate N+1: one parent query + many child queries from different "call sites"
      List<QueryRecord> records = new ArrayList<>();
      records.add(
          new QueryRecord(
              "SELECT * FROM orders WHERE user_id = ?",
              0L,
              System.currentTimeMillis(),
              "com.example.OrderRepository.findByUserId(OrderRepository.java:25)",
              1001));
      for (int i = 0; i < 20; i++) {
        records.add(
            new QueryRecord(
                "SELECT * FROM order_items WHERE order_id = ?",
                0L,
                System.currentTimeMillis(),
                "com.example.OrderItemRepository.findByOrderId(OrderItemRepository.java:30)",
                2002));
      }
      assertAllSafeMultiRecord(records, "hibernate-n-plus-one");
    }

    @Test
    void jpaGeneratedQueries() {
      assertAllSafe(
          List.of(
              "select user0_.id as id1_0_, user0_.name as name2_0_ from users user0_ where user0_.id=?",
              "select user0_.id as id1_0_0_, user0_.name as name2_0_0_ from users user0_ where user0_.id in (?, ?, ?, ?, ?)",
              "select count(user0_.id) as col_0_0_ from users user0_",
              "select user0_.id as id1_0_, user0_.name as name2_0_ from users user0_ left outer join orders order1_ on user0_.id=order1_.user_id where order1_.status=?",
              "insert into users (name, email) values (?, ?)",
              "update users set name=?, email=? where id=?",
              "delete from users where id=?"),
          "jpa-generated");
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // 7. BOUNDARY CONDITIONS & DEGENERATE INPUTS
  // ═══════════════════════════════════════════════════════════════════

  @Nested
  class BoundaryConditions {

    @Test
    void singleCharacterInputs() {
      assertAllSafe(
          List.of(
              "S", "W", "F", "I", "U", "D", "*", ";", "(", ")", "'", "\"", "`", ".", ",", "=", "<",
              ">", "?", "+", "-", "/", "%", "@", "#"),
          "single-chars");
    }

    @Test
    void repeatedKeywords() {
      assertAllSafe(
          List.of(
              "SELECT SELECT SELECT",
              "FROM FROM FROM",
              "WHERE WHERE WHERE",
              "AND AND AND AND",
              "OR OR OR OR OR",
              "JOIN JOIN JOIN JOIN",
              "NULL NULL NULL",
              "NOT NOT NOT NOT NOT"),
          "repeated-keywords");
    }

    @Test
    void emptyListAndCollections() {
      // Empty query list
      try {
        QueryAuditReport report = analyzer.analyze("ExtremeEdgeCaseTest", List.of(), EMPTY_INDEX);
        assertThat(report).isNotNull();
      } catch (Throwable t) {
        fail("Crashed on empty query list: %s", t.getMessage());
      }

      // Null query list
      try {
        QueryAuditReport report = analyzer.analyze("ExtremeEdgeCaseTest", null, EMPTY_INDEX);
        assertThat(report).isNotNull();
      } catch (Throwable t) {
        fail("Crashed on null query list: %s", t.getMessage());
      }
    }

    @Test
    void extremeNumericValues() {
      assertAllSafe(
          List.of(
              "SELECT * FROM users WHERE id = 0",
              "SELECT * FROM users WHERE id = -1",
              "SELECT * FROM users WHERE id = 2147483647",
              "SELECT * FROM users WHERE id = -2147483648",
              "SELECT * FROM users WHERE id = 9999999999999999999",
              "SELECT * FROM users WHERE amount = 0.0",
              "SELECT * FROM users WHERE amount = -0.0",
              "SELECT * FROM users WHERE amount = 1.7976931348623157E308",
              "SELECT * FROM users WHERE amount = NaN",
              "SELECT * FROM users WHERE amount = Infinity"),
          "extreme-numerics");
    }

    @Test
    void queryWithOnlyComments() {
      assertAllSafe(
          List.of(
              "/* just a comment */",
              "-- just a comment",
              "/* comment1 */ /* comment2 */ /* comment3 */",
              "-- line1\n-- line2\n-- line3",
              "/* multi\nline\ncomment */"),
          "only-comments");
    }

    @Test
    void duplicateColumnsAndTables() {
      assertAllSafe(
          List.of(
              "SELECT id, id, id FROM users",
              "SELECT * FROM users, users, users",
              "SELECT * FROM users u1, users u2, users u3",
              "SELECT * FROM users JOIN users ON users.id = users.id"),
          "duplicates");
    }

    @Test
    void likeWithSpecialPatterns() {
      assertAllSafe(
          List.of(
              "SELECT * FROM users WHERE name LIKE '%'",
              "SELECT * FROM users WHERE name LIKE '_'",
              "SELECT * FROM users WHERE name LIKE '%%%'",
              "SELECT * FROM users WHERE name LIKE '\\%'",
              "SELECT * FROM users WHERE name LIKE ''",
              "SELECT * FROM users WHERE name LIKE '%_%_%_%'",
              "SELECT * FROM users WHERE name NOT LIKE '%test%'"),
          "like-special");
    }

    @Test
    void complexCasting() {
      assertAllSafe(
          List.of(
              "SELECT CAST(id AS VARCHAR) FROM users",
              "SELECT CONVERT(id, CHAR) FROM users",
              "SELECT CAST(CAST(CAST(id AS VARCHAR) AS INT) AS VARCHAR) FROM users",
              "SELECT * FROM users WHERE CAST(id AS VARCHAR) = ?",
              "SELECT * FROM users WHERE id = CAST(? AS INT)"),
          "complex-casting");
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // 8. MULTI-STATEMENT AND COMPLEX PATTERNS
  // ═══════════════════════════════════════════════════════════════════

  @Nested
  class ComplexPatterns {

    @Test
    void complexJoinPatterns() {
      assertAllSafe(
          List.of(
              "SELECT * FROM a NATURAL JOIN b",
              "SELECT * FROM a CROSS JOIN b",
              "SELECT * FROM a LEFT OUTER JOIN b ON a.id = b.id",
              "SELECT * FROM a RIGHT OUTER JOIN b ON a.id = b.id",
              "SELECT * FROM a FULL OUTER JOIN b ON a.id = b.id",
              "SELECT * FROM a STRAIGHT_JOIN b",
              "SELECT * FROM a JOIN b USING (id)"),
          "join-patterns");
    }

    @Test
    void subqueriesInVariousPositions() {
      assertAllSafe(
          List.of(
              "SELECT (SELECT MAX(id) FROM users) AS max_id",
              "SELECT * FROM (SELECT 1) AS t",
              "SELECT * FROM users WHERE id = (SELECT MAX(id) FROM users)",
              "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)",
              "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
              "SELECT * FROM users WHERE id > ALL (SELECT user_id FROM orders)",
              "SELECT * FROM users WHERE id > ANY (SELECT user_id FROM orders)",
              "INSERT INTO archive SELECT * FROM users WHERE created_at < '2020-01-01'",
              "UPDATE users SET status = 'active' WHERE id IN (SELECT user_id FROM recent_logins)"),
          "subquery-positions");
    }

    @Test
    void aggregateFunctionEdgeCases() {
      assertAllSafe(
          List.of(
              "SELECT COUNT(*) FROM users",
              "SELECT COUNT(DISTINCT name) FROM users",
              "SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM orders",
              "SELECT GROUP_CONCAT(name SEPARATOR ', ') FROM users",
              "SELECT COUNT(*) FROM users GROUP BY name HAVING COUNT(*) > 1",
              "SELECT COUNT(1) FROM users",
              "SELECT COUNT(NULL) FROM users"),
          "aggregate-edge-cases");
    }

    @Test
    void insertVariations() {
      assertAllSafe(
          List.of(
              "INSERT INTO t DEFAULT VALUES",
              "INSERT INTO t (a) SELECT a FROM t2",
              "INSERT INTO t VALUES (1), (2), (3), (4), (5)",
              "INSERT IGNORE INTO t (id) VALUES (1)",
              "INSERT INTO t (id) VALUES (1) ON CONFLICT (id) DO NOTHING",
              "INSERT INTO t (id, name) VALUES (1, 'test') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name"),
          "insert-variations");
    }

    @Test
    void deleteVariations() {
      assertAllSafe(
          List.of(
              "DELETE FROM users",
              "DELETE FROM users WHERE 1=1",
              "DELETE FROM users LIMIT 100",
              "DELETE u FROM users u JOIN inactive i ON u.id = i.user_id",
              "DELETE FROM users USING users JOIN orders ON users.id = orders.user_id WHERE orders.total < 0",
              "TRUNCATE TABLE users"),
          "delete-variations");
    }

    @Test
    void updateVariations() {
      assertAllSafe(
          List.of(
              "UPDATE users SET name = 'test'",
              "UPDATE users SET name = 'test' WHERE 1=0",
              "UPDATE users SET name = (SELECT name FROM defaults LIMIT 1)",
              "UPDATE users u JOIN orders o ON u.id = o.user_id SET u.total = o.amount",
              "UPDATE users SET a=1, b=2, c=3, d=4, e=5 WHERE id = ?"),
          "update-variations");
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  // 9. REGRESSION: large batch of identical queries (RepeatedSingleInsert)
  // ═══════════════════════════════════════════════════════════════════

  @Nested
  class BatchAndRepetition {

    @Test
    void manyIdenticalSelects() {
      List<QueryRecord> records =
          IntStream.rangeClosed(1, 100)
              .mapToObj(i -> record("SELECT * FROM users WHERE id = ?"))
              .toList();
      assertAllSafeMultiRecord(records, "100-identical-selects");
    }

    @Test
    void manyDistinctQueries() {
      List<QueryRecord> records =
          IntStream.rangeClosed(1, 100)
              .mapToObj(i -> record("SELECT * FROM table" + i + " WHERE id = ?"))
              .toList();
      assertAllSafeMultiRecord(records, "100-distinct-queries");
    }

    @Test
    void alternatingQueryTypes() {
      List<QueryRecord> records = new ArrayList<>();
      for (int i = 0; i < 50; i++) {
        records.add(record("SELECT * FROM users WHERE id = ?"));
        records.add(record("INSERT INTO audit (user_id, action) VALUES (?, ?)"));
        records.add(record("UPDATE users SET last_login = NOW() WHERE id = ?"));
      }
      assertAllSafeMultiRecord(records, "alternating-150-queries");
    }
  }
}
