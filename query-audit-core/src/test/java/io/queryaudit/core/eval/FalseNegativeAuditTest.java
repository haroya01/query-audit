package io.queryaudit.core.eval;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.detector.*;
import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * FALSE NEGATIVE AUDIT
 *
 * <p>Tests subtle variants of known SQL anti-patterns to measure the detection rate. Each test
 * generates anti-pattern SQL that SHOULD be detected but might slip through due to regex
 * limitations, whitespace handling, or edge cases in detection logic.
 */
class FalseNegativeAuditTest {

  static int totalAntiPatterns = 0;
  static int detected = 0;
  static int missed = 0;
  static final List<String> missedQueries = Collections.synchronizedList(new ArrayList<>());

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static IndexMetadata indexWithTable(String table, String... columns) {
    List<IndexInfo> indexes = new ArrayList<>();
    for (String col : columns) {
      indexes.add(
          new IndexInfo(
              table,
              col.equals("id") ? "PRIMARY" : "idx_" + col,
              col,
              1,
              !col.equals("id"),
              1000L));
    }
    return new IndexMetadata(Map.of(table, indexes));
  }

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private static QueryRecord record(String sql, String stackTrace) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), stackTrace);
  }

  private static void check(List<Issue> issues, IssueType expectedType, String sql) {
    totalAntiPatterns++;
    boolean found = issues.stream().anyMatch(i -> i.type() == expectedType);
    if (found) {
      detected++;
    } else {
      missed++;
      missedQueries.add("[" + expectedType.getCode() + "] " + sql);
    }
  }

  private static void checkAny(List<Issue> issues, String sql, IssueType... expectedTypes) {
    totalAntiPatterns++;
    boolean found = false;
    for (IssueType type : expectedTypes) {
      if (issues.stream().anyMatch(i -> i.type() == type)) {
        found = true;
        break;
      }
    }
    if (found) {
      detected++;
    } else {
      missed++;
      String types = "";
      for (IssueType t : expectedTypes) types += t.getCode() + ",";
      missedQueries.add("[" + types + "] " + sql);
    }
  }

  @AfterAll
  static void printReport() {
    System.out.println();
    System.out.println("=== FALSE NEGATIVE AUDIT ===");
    System.out.println("Total anti-patterns tested: " + totalAntiPatterns);
    System.out.println("Correctly detected: " + detected);
    System.out.println("Missed (false negatives): " + missed);
    if (totalAntiPatterns > 0) {
      System.out.printf("Detection rate: %.1f%%\n", (detected * 100.0 / totalAntiPatterns));
    }
    if (!missedQueries.isEmpty()) {
      System.out.println();
      System.out.println("--- MISSED QUERIES (false negatives) ---");
      for (String q : missedQueries) {
        System.out.println("  MISS: " + q);
      }
    }
    System.out.println("=== END ===");
    System.out.println();
  }

  // -----------------------------------------------------------------------
  // 1. SELECT * subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class SelectAllSubtleVariants {
    private final SelectAllDetector detector = new SelectAllDetector();

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.SELECT_ALL, sql);
    }

    @Test
    void commentBeforeSelectStar() {
      test("/* hint */ SELECT * FROM users");
    }

    @Test
    void leadingSpacesSelectStar() {
      test("   SELECT * FROM users");
    }

    @Test
    void tabBeforeSelectStar() {
      test("\tSELECT * FROM users");
    }

    @Test
    void newlineBeforeSelectStar() {
      test("\nSELECT * FROM users");
    }

    @Test
    void selectStarWithTableAlias() {
      test("SELECT t.* FROM users t");
    }

    @Test
    void selectStarWithSchemaPrefix() {
      test("SELECT mydb.users.* FROM mydb.users");
    }

    @Test
    void selectStarInSubquery() {
      test("SELECT id FROM (SELECT * FROM users) sub");
    }

    @Test
    void selectStarWithDistinct() {
      test("SELECT DISTINCT * FROM users");
    }

    @Test
    void selectStarWithMultilineFormatting() {
      test("SELECT\n  *\nFROM\n  users");
    }

    @Test
    void selectStarCaseVariation() {
      test("select * from users");
    }

    @Test
    void selectAllKeywordStar() {
      test("SELECT ALL * FROM users");
    }

    @Test
    void selectStarWithMultipleTables() {
      test("SELECT * FROM users, orders WHERE users.id = orders.user_id");
    }

    @Test
    void selectStarWithJoin() {
      test("SELECT * FROM users JOIN orders ON users.id = orders.user_id");
    }

    @Test
    void selectStarWithWhereClause() {
      test("SELECT * FROM users WHERE active = 1");
    }

    @Test
    void selectStarMixedCaseKeywords() {
      test("SeLeCt * FrOm users");
    }
  }

  // -----------------------------------------------------------------------
  // 2. N+1 subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class NPlusOneSubtleVariants {
    private final NPlusOneDetector detector = new NPlusOneDetector(3);

    @Test
    void nPlusOneWithVaryingWhitespace() {
      // Same normalized pattern, different whitespace
      List<QueryRecord> queries =
          List.of(
              record("SELECT name FROM users WHERE id = 1"),
              record("SELECT name FROM users WHERE id = 2"),
              record("SELECT name FROM users WHERE id = 3"),
              record("SELECT name FROM users WHERE id = 4"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      check(issues, IssueType.N_PLUS_ONE, "N+1 with varying params");
    }

    @Test
    void nPlusOneWithStringParams() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM users WHERE email = 'alice@test.com'"),
              record("SELECT * FROM users WHERE email = 'bob@test.com'"),
              record("SELECT * FROM users WHERE email = 'carol@test.com'"),
              record("SELECT * FROM users WHERE email = 'dave@test.com'"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      check(issues, IssueType.N_PLUS_ONE, "N+1 with string params");
    }

    @Test
    void nPlusOneWithPreparedStatementStyle() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT name FROM users WHERE id = ?"),
              record("SELECT name FROM users WHERE id = ?"),
              record("SELECT name FROM users WHERE id = ?"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      check(issues, IssueType.N_PLUS_ONE, "N+1 with ? placeholders (identical)");
    }

    @Test
    void nPlusOneWithMixedCase() {
      List<QueryRecord> queries =
          List.of(
              record("select name from users where id = 10"),
              record("SELECT name FROM users WHERE id = 20"),
              record("Select Name From Users Where Id = 30"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      check(issues, IssueType.N_PLUS_ONE, "N+1 with mixed case");
    }

    @Test
    void nPlusOneWithJoinQuery() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE o.id = 1"),
              record("SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE o.id = 2"),
              record("SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE o.id = 3"),
              record(
                  "SELECT u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE o.id = 4"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      check(issues, IssueType.N_PLUS_ONE, "N+1 with JOIN query");
    }

    @Test
    void nPlusOneExactlyAtThreshold() {
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM products WHERE category_id = 100"),
              record("SELECT * FROM products WHERE category_id = 200"),
              record("SELECT * FROM products WHERE category_id = 300"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      check(issues, IssueType.N_PLUS_ONE, "N+1 exactly at threshold=3");
    }

    @Test
    void nPlusOneWithLeadingWhitespace() {
      List<QueryRecord> queries =
          List.of(
              record("  SELECT id FROM items WHERE parent_id = 1"),
              record("  SELECT id FROM items WHERE parent_id = 2"),
              record("  SELECT id FROM items WHERE parent_id = 3"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      check(issues, IssueType.N_PLUS_ONE, "N+1 with leading whitespace");
    }
  }

  // -----------------------------------------------------------------------
  // 3. WHERE function subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class WhereFunctionSubtleVariants {
    private final WhereFunctionDetector detector = new WhereFunctionDetector();

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.WHERE_FUNCTION, sql);
    }

    @Test
    void nestedFunctions() {
      test("SELECT * FROM events WHERE YEAR(DATE(created_at)) = 2024");
    }

    @Test
    void functionWithTableAlias() {
      test("SELECT * FROM events e WHERE YEAR(e.created_at) = 2024");
    }

    @Test
    void lowerInWhere() {
      test("SELECT * FROM users WHERE LOWER(email) = 'test@example.com'");
    }

    @Test
    void upperInWhere() {
      test("SELECT * FROM users WHERE UPPER(name) = 'JOHN'");
    }

    @Test
    void trimInWhere() {
      test("SELECT * FROM users WHERE TRIM(name) = 'John'");
    }

    @Test
    void substringInWhere() {
      test("SELECT * FROM users WHERE SUBSTRING(phone, 1, 3) = '010'");
    }

    @Test
    void castInWhere() {
      test("SELECT * FROM orders WHERE CAST(amount AS INT) > 100");
    }

    @Test
    void jsonExtractInWhere() {
      test("SELECT * FROM configs WHERE JSON_EXTRACT(data, '$.key') = 'value'");
    }

    @Test
    void coalesceInWhere() {
      test("SELECT * FROM users WHERE COALESCE(nickname, name) = 'John'");
    }

    @Test
    void dateInWhere() {
      test("SELECT * FROM logs WHERE DATE(created_at) = '2024-01-01'");
    }

    @Test
    void monthInWhere() {
      test("SELECT * FROM events WHERE MONTH(event_date) = 12");
    }

    @Test
    void concatInWhere() {
      test("SELECT * FROM users WHERE CONCAT(first_name, last_name) = 'JohnDoe'");
    }

    @Test
    void md5InWhere() {
      test("SELECT * FROM tokens WHERE MD5(token) = 'abc123'");
    }

    @Test
    void unixTimestampInWhere() {
      test("SELECT * FROM logs WHERE UNIX_TIMESTAMP(created_at) > 1700000000");
    }

    @Test
    void functionInWhereWithAnd() {
      test("SELECT * FROM users WHERE LOWER(email) = 'test@test.com' AND active = 1");
    }

    @Test
    void functionInJoinOn() {
      test("SELECT * FROM users u JOIN profiles p ON LOWER(u.email) = LOWER(p.email)");
    }

    @Test
    void extractInWhere() {
      test("SELECT * FROM events WHERE EXTRACT(YEAR FROM event_date) = 2024");
    }

    @Test
    void absInWhere() {
      test("SELECT * FROM accounts WHERE ABS(balance) > 1000");
    }

    @Test
    void roundInWhere() {
      test("SELECT * FROM products WHERE ROUND(price) = 10");
    }

    @Test
    void lengthInWhere() {
      test("SELECT * FROM users WHERE LENGTH(password) < 8");
    }
  }

  // -----------------------------------------------------------------------
  // 4. UPDATE without WHERE subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class UpdateWithoutWhereSubtleVariants {
    private final UpdateWithoutWhereDetector detector = new UpdateWithoutWhereDetector();

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.UPDATE_WITHOUT_WHERE, sql);
    }

    @Test
    void updateWithSubqueryInSet() {
      test("UPDATE users SET score = (SELECT AVG(score) FROM scores)");
    }

    @Test
    void updateWithLeadingSpaces() {
      test("   UPDATE users SET active = 0");
    }

    @Test
    void updateWithMixedCaseTable() {
      test("UPDATE UserAccounts SET status = 'inactive'");
    }

    @Test
    void updateWithBacktickTable() {
      test("UPDATE `users` SET deleted = 1");
    }

    @Test
    void deleteWithoutWhere() {
      test("DELETE FROM sessions");
    }

    @Test
    void deleteWithLeadingWhitespace() {
      test("  DELETE FROM temp_data");
    }

    @Test
    void updateMultipleColumns() {
      test("UPDATE products SET price = 0, stock = 0");
    }

    @Test
    void updateWithNewlines() {
      test("UPDATE\n  orders\nSET\n  status = 'cancelled'");
    }

    @Test
    void updateLowercaseKeywords() {
      test("update users set active = false");
    }

    @Test
    void deleteFromBacktickTable() {
      test("DELETE FROM `audit_log`");
    }
  }

  // -----------------------------------------------------------------------
  // 5. NOT IN subquery subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class NotInSubquerySubtleVariants {
    private final NotInSubqueryDetector detector = new NotInSubqueryDetector();

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.NOT_IN_SUBQUERY, sql);
    }

    @Test
    void notInWithDeeplyNestedSubquery() {
      test(
          "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM (SELECT user_id FROM banned_users) sub)");
    }

    @Test
    void notInWithWhitespace() {
      test("SELECT * FROM orders WHERE customer_id NOT  IN (SELECT id FROM blacklisted)");
    }

    @Test
    void notInWithNewlines() {
      test(
          "SELECT * FROM products WHERE category_id NOT\nIN\n(\nSELECT id FROM deleted_categories\n)");
    }

    @Test
    void notInWithMixedCase() {
      test("SELECT * FROM users WHERE id Not In (Select id From banned)");
    }

    @Test
    void notInWithExtraSpacesInParens() {
      test("SELECT * FROM users WHERE id NOT IN (  SELECT id FROM banned  )");
    }

    @Test
    void notInWithJoin() {
      test(
          "SELECT u.* FROM users u WHERE u.id NOT IN (SELECT b.user_id FROM bans b JOIN reasons r ON b.reason_id = r.id)");
    }

    @Test
    void notInWithMultipleConditions() {
      test(
          "SELECT * FROM users WHERE active = 1 AND id NOT IN (SELECT user_id FROM deleted_users)");
    }

    @Test
    void notInWithSchemaQualified() {
      test("SELECT * FROM mydb.users WHERE id NOT IN (SELECT user_id FROM mydb.blocked)");
    }
  }

  // -----------------------------------------------------------------------
  // 6. Implicit join subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class ImplicitJoinSubtleVariants {
    private final ImplicitJoinDetector detector = new ImplicitJoinDetector();

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.IMPLICIT_JOIN, sql);
    }

    private void testMustDetect(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.IMPLICIT_JOIN, sql);
      assertThat(issues)
          .as("Should detect implicit join in: %s", sql)
          .anyMatch(i -> i.type() == IssueType.IMPLICIT_JOIN);
    }

    @Test
    void threeTableImplicitJoin() {
      test("SELECT * FROM a, b, c WHERE a.id = b.a_id AND b.id = c.b_id");
    }

    @Test
    void implicitJoinWithAliases() {
      testMustDetect("SELECT * FROM users u, orders o WHERE u.id = o.user_id");
    }

    @Test
    void implicitJoinMixedCase() {
      test("select * from Users, Orders where Users.id = Orders.user_id");
    }

    @Test
    void implicitJoinWithSchemaPrefix() {
      testMustDetect(
          "SELECT * FROM mydb.users, mydb.orders WHERE mydb.users.id = mydb.orders.user_id");
    }

    @Test
    void implicitJoinWithBackticks() {
      testMustDetect("SELECT * FROM `users`, `orders` WHERE `users`.id = `orders`.user_id");
    }

    @Test
    void implicitJoinNoSpaceAfterComma() {
      test("SELECT * FROM users,orders WHERE users.id = orders.user_id");
    }

    @Test
    void implicitJoinWithSubselect() {
      testMustDetect("SELECT * FROM users, (SELECT * FROM orders) o WHERE users.id = o.user_id");
    }

    @Test
    void fourTableImplicitJoin() {
      test("SELECT * FROM a, b, c, d WHERE a.id = b.a_id AND c.id = d.c_id");
    }
  }

  // -----------------------------------------------------------------------
  // 7. ORDER BY RAND() subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class OrderByRandSubtleVariants {
    private final OrderByRandDetector detector = new OrderByRandDetector();

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.ORDER_BY_RAND, sql);
    }

    @Test
    void orderByRandWithSpaces() {
      test("SELECT * FROM users ORDER BY RAND ()");
    }

    @Test
    void orderByRandLowercase() {
      test("SELECT * FROM users ORDER BY rand()");
    }

    @Test
    void orderByRandomPostgres() {
      test("SELECT * FROM users ORDER BY RANDOM()");
    }

    @Test
    void orderByRandWithLimit() {
      test("SELECT * FROM products ORDER BY RAND() LIMIT 10");
    }

    @Test
    void orderByRandMixedCase() {
      test("SELECT * FROM items ORDER  BY  Rand() LIMIT 5");
    }

    @Test
    void orderByNewid() {
      test("SELECT * FROM users ORDER BY NEWID()");
    }

    @Test
    void orderByRandInSubquery() {
      test("SELECT * FROM (SELECT * FROM users ORDER BY RAND() LIMIT 10) t");
    }

    @Test
    void orderByDbmsRandom() {
      test("SELECT * FROM employees ORDER BY DBMS_RANDOM()");
    }

    @Test
    void orderByRandWithWhereClause() {
      test("SELECT * FROM users WHERE active = 1 ORDER BY RAND() LIMIT 3");
    }

    @Test
    void orderByRandNewlines() {
      test("SELECT * FROM users\nORDER\nBY\nRAND()");
    }
  }

  // -----------------------------------------------------------------------
  // 8. LIKE wildcard subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class LikeWildcardSubtleVariants {
    private final LikeWildcardDetector detector = new LikeWildcardDetector();

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.LIKE_LEADING_WILDCARD, sql);
    }

    @Test
    void likeLeadingPercent() {
      test("SELECT * FROM users WHERE name LIKE '%john'");
    }

    @Test
    void likeLeadingPercentBothSides() {
      test("SELECT * FROM users WHERE email LIKE '%@gmail.com%'");
    }

    @Test
    void likeLowercase() {
      test("select * from users where name like '%test'");
    }

    @Test
    void likeWithExtraSpaces() {
      test("SELECT * FROM users WHERE name LIKE  '%test%'");
    }

    @Test
    void likeWithMultipleConditions() {
      test("SELECT * FROM users WHERE active = 1 AND name LIKE '%smith'");
    }

    @Test
    void likeParameterWildcard() {
      // This is a known limitation: LIKE ? cannot detect leading wildcard at SQL analysis time
      String sql = "SELECT * FROM users WHERE name LIKE ?";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      // Intentionally mark as tested but expect miss - this is a design limitation
      totalAntiPatterns++;
      boolean found = issues.stream().anyMatch(i -> i.type() == IssueType.LIKE_LEADING_WILDCARD);
      if (found) {
        detected++;
      } else {
        missed++;
        missedQueries.add(
            "[like-leading-wildcard] " + sql + " (KNOWN LIMITATION: parameterized LIKE)");
      }
    }

    @Test
    void likeConcatWildcard() {
      test("SELECT * FROM users WHERE name LIKE '%' || ?");
    }

    @Test
    void likeInOrCondition() {
      test("SELECT * FROM users WHERE name LIKE '%john' OR name LIKE '%jane'");
    }
  }

  // -----------------------------------------------------------------------
  // 9. Sargability / non-sargable expressions
  // -----------------------------------------------------------------------
  @Nested
  class SargabilitySubtleVariants {
    private final SargabilityDetector detector = new SargabilityDetector();

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.NON_SARGABLE_EXPRESSION, sql);
    }

    @Test
    void colPlusOne() {
      test("SELECT * FROM orders WHERE amount + 1 = 100");
    }

    @Test
    void colMultiplyTwo() {
      test("SELECT * FROM products WHERE price * 2 > 50");
    }

    @Test
    void colDivide() {
      test("SELECT * FROM metrics WHERE value / 10 = 5");
    }

    @Test
    void colMinus() {
      test("SELECT * FROM scores WHERE points - 10 > 0");
    }

    @Test
    void qualifiedColArithmetic() {
      test("SELECT * FROM orders o WHERE o.total + 5 > 100");
    }

    @Test
    void arithmeticWithPlaceholder() {
      test("SELECT * FROM users WHERE age + 1 = ?");
    }

    @Test
    void colArithmeticInAndCondition() {
      test("SELECT * FROM users WHERE active = 1 AND score + 10 > 100");
    }
  }

  // -----------------------------------------------------------------------
  // 10. Cartesian join subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class CartesianJoinSubtleVariants {
    private final CartesianJoinDetector detector = new CartesianJoinDetector();

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.CARTESIAN_JOIN, sql);
    }

    @Test
    void joinWithoutOnClause() {
      test("SELECT * FROM users JOIN orders WHERE users.id = orders.user_id");
    }

    @Test
    void commaJoinWithoutWhere() {
      test("SELECT * FROM users, orders");
    }

    @Test
    void innerJoinWithoutOn() {
      test("SELECT * FROM users INNER JOIN orders WHERE users.id = 1");
    }

    @Test
    void leftJoinWithoutOn() {
      test("SELECT * FROM users LEFT JOIN orders WHERE users.active = 1");
    }

    @Test
    void multipleJoinsOneWithoutOn() {
      test("SELECT * FROM a JOIN b ON a.id = b.a_id JOIN c WHERE a.x = 1");
    }
  }

  // -----------------------------------------------------------------------
  // 11. Null comparison subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class NullComparisonSubtleVariants {
    private final NullComparisonDetector detector = new NullComparisonDetector();

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.NULL_COMPARISON, sql);
    }

    @Test
    void equalsNull() {
      test("SELECT * FROM users WHERE email = NULL");
    }

    @Test
    void notEqualsNull() {
      test("SELECT * FROM users WHERE status != NULL");
    }

    @Test
    void diamondNotEqualsNull() {
      test("SELECT * FROM users WHERE deleted_at <> NULL");
    }

    @Test
    void qualifiedColumnEqualsNull() {
      test("SELECT * FROM users u WHERE u.email = NULL");
    }

    @Test
    void equalsNullWithAnd() {
      test("SELECT * FROM orders WHERE status = 'active' AND deleted_at = NULL");
    }

    @Test
    void equalsNullLowercase() {
      test("select * from users where name = null");
    }
  }

  // -----------------------------------------------------------------------
  // 12. DISTINCT misuse subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class DistinctMisuseSubtleVariants {
    private final DistinctMisuseDetector detector = new DistinctMisuseDetector();

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.DISTINCT_MISUSE, sql);
    }

    @Test
    void distinctWithGroupBy() {
      test("SELECT DISTINCT name, COUNT(*) FROM users GROUP BY name");
    }

    @Test
    void distinctWithJoin() {
      test("SELECT DISTINCT u.name FROM users u JOIN orders o ON u.id = o.user_id");
    }

    @Test
    void distinctWithGroupByLowercase() {
      test("select distinct name from users group by name");
    }

    @Test
    void distinctWithMultipleJoins() {
      test(
          "SELECT DISTINCT u.id FROM users u JOIN orders o ON u.id = o.user_id JOIN items i ON o.id = i.order_id");
    }

    @Test
    void distinctWithGroupByAndHaving() {
      test("SELECT DISTINCT status, COUNT(*) cnt FROM orders GROUP BY status HAVING cnt > 5");
    }
  }

  // -----------------------------------------------------------------------
  // 13. UNION without ALL subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class UnionWithoutAllSubtleVariants {
    private final UnionWithoutAllDetector detector = new UnionWithoutAllDetector();

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.UNION_WITHOUT_ALL, sql);
    }

    @Test
    void simpleUnion() {
      test("SELECT id FROM users UNION SELECT id FROM admins");
    }

    @Test
    void unionLowercase() {
      test("select id from users union select id from admins");
    }

    @Test
    void unionWithWhere() {
      test("SELECT id FROM users WHERE active = 1 UNION SELECT id FROM admins WHERE active = 1");
    }

    @Test
    void unionMultiple() {
      test("SELECT a FROM t1 UNION SELECT b FROM t2 UNION SELECT c FROM t3");
    }

    @Test
    void unionWithOrderBy() {
      test("SELECT id FROM users UNION SELECT id FROM admins ORDER BY id");
    }
  }

  // -----------------------------------------------------------------------
  // 14. COUNT(*) without WHERE subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class CountStarWithoutWhereSubtleVariants {
    private final SelectCountStarWithoutWhereDetector detector =
        new SelectCountStarWithoutWhereDetector();

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.COUNT_STAR_WITHOUT_WHERE, sql);
    }

    @Test
    void countStarSimple() {
      test("SELECT COUNT(*) FROM users");
    }

    @Test
    void countOneWithoutWhere() {
      test("SELECT COUNT(1) FROM orders");
    }

    @Test
    void countStarLowercase() {
      test("select count(*) from users");
    }

    @Test
    void countStarWithSpaces() {
      test("SELECT COUNT( * ) FROM users");
    }

    @Test
    void countStarWithAlias() {
      test("SELECT COUNT(*) AS total FROM products");
    }
  }

  // -----------------------------------------------------------------------
  // 15. Subquery in DML subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class SubqueryInDmlSubtleVariants {
    private final SubqueryInDmlDetector detector = new SubqueryInDmlDetector();

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.SUBQUERY_IN_DML, sql);
    }

    @Test
    void updateWithInSubquery() {
      test("UPDATE orders SET status = 'cancelled' WHERE user_id IN (SELECT id FROM banned_users)");
    }

    @Test
    void deleteWithInSubquery() {
      test("DELETE FROM sessions WHERE user_id IN (SELECT id FROM deleted_users)");
    }

    @Test
    void updateWithInSubqueryMixedCase() {
      test("update orders set status = 'x' where user_id in (select id from banned)");
    }

    @Test
    void updateWithNestedInSubquery() {
      test(
          "UPDATE products SET active = 0 WHERE category_id IN (SELECT id FROM categories WHERE parent_id IN (SELECT id FROM deleted_parents))");
    }

    @Test
    void deleteWithInSubqueryAndCondition() {
      test("DELETE FROM logs WHERE level = 'DEBUG' AND user_id IN (SELECT id FROM test_users)");
    }
  }

  // -----------------------------------------------------------------------
  // 16. OR abuse subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class OrAbuseSubtleVariants {
    private final OrAbuseDetector detector = new OrAbuseDetector(3);

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.OR_ABUSE, sql);
    }

    @Test
    void manyOrOnDifferentColumns() {
      test("SELECT * FROM users WHERE name = 'a' OR email = 'b' OR phone = 'c' OR age = 1");
    }

    @Test
    void orWithMixedCase() {
      test("SELECT * FROM users WHERE name = 'x' Or email = 'y' OR phone = 'z' or status = 1");
    }

    @Test
    void orWithQualifiedColumns() {
      test(
          "SELECT * FROM users u WHERE u.name = 'a' OR u.email = 'b' OR u.phone = 'c' OR u.status = 1");
    }
  }

  // -----------------------------------------------------------------------
  // 17. Offset pagination subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class OffsetPaginationSubtleVariants {
    private final OffsetPaginationDetector detector = new OffsetPaginationDetector(1000);

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.OFFSET_PAGINATION, sql);
    }

    @Test
    void largeOffsetLiteral() {
      test("SELECT * FROM users ORDER BY id LIMIT 20 OFFSET 5000");
    }

    @Test
    void largeOffsetInLimit() {
      test("SELECT * FROM users LIMIT 20 OFFSET 10000");
    }

    @Test
    void parameterizedOffset() {
      test("SELECT * FROM users ORDER BY id LIMIT ? OFFSET ?");
    }

    @Test
    void offsetExactlyAtThreshold() {
      test("SELECT * FROM products ORDER BY id LIMIT 10 OFFSET 1000");
    }

    @Test
    void veryLargeOffset() {
      test("SELECT * FROM logs LIMIT 50 OFFSET 100000");
    }
  }

  // -----------------------------------------------------------------------
  // 18. Repeated single INSERT subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class RepeatedSingleInsertSubtleVariants {
    private final RepeatedSingleInsertDetector detector = new RepeatedSingleInsertDetector(3);

    @Test
    void repeatedInserts() {
      List<QueryRecord> queries =
          List.of(
              record("INSERT INTO users (name, email) VALUES ('Alice', 'alice@test.com')"),
              record("INSERT INTO users (name, email) VALUES ('Bob', 'bob@test.com')"),
              record("INSERT INTO users (name, email) VALUES ('Carol', 'carol@test.com')"),
              record("INSERT INTO users (name, email) VALUES ('Dave', 'dave@test.com')"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      check(issues, IssueType.REPEATED_SINGLE_INSERT, "Repeated INSERT 4 times");
    }

    @Test
    void repeatedInsertsWithPlaceholders() {
      List<QueryRecord> queries =
          List.of(
              record("INSERT INTO orders (user_id, amount) VALUES (?, ?)"),
              record("INSERT INTO orders (user_id, amount) VALUES (?, ?)"),
              record("INSERT INTO orders (user_id, amount) VALUES (?, ?)"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      check(issues, IssueType.REPEATED_SINGLE_INSERT, "Repeated INSERT with ? placeholders");
    }

    @Test
    void repeatedInsertsMixedCase() {
      List<QueryRecord> queries =
          List.of(
              record("insert into items (name) values ('a')"),
              record("insert into items (name) values ('b')"),
              record("insert into items (name) values ('c')"));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      check(issues, IssueType.REPEATED_SINGLE_INSERT, "Repeated INSERT lowercase");
    }
  }

  // -----------------------------------------------------------------------
  // 19. FIND_IN_SET subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class FindInSetSubtleVariants {
    private final FindInSetDetector detector = new FindInSetDetector();

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.FIND_IN_SET_USAGE, sql);
    }

    @Test
    void findInSetSimple() {
      test("SELECT * FROM users WHERE FIND_IN_SET('admin', roles)");
    }

    @Test
    void findInSetLowercase() {
      test("select * from users where find_in_set('editor', roles)");
    }

    @Test
    void findInSetWithSpaces() {
      test("SELECT * FROM users WHERE FIND_IN_SET ('admin', roles) > 0");
    }

    @Test
    void findInSetInJoin() {
      test("SELECT * FROM users u JOIN perms p ON FIND_IN_SET(p.name, u.permissions) > 0");
    }
  }

  // -----------------------------------------------------------------------
  // 20. GROUP BY function subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class GroupByFunctionSubtleVariants {
    private final GroupByFunctionDetector detector = new GroupByFunctionDetector();

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.GROUP_BY_FUNCTION, sql);
    }

    @Test
    void yearInGroupBy() {
      test("SELECT YEAR(created_at), COUNT(*) FROM orders GROUP BY YEAR(created_at)");
    }

    @Test
    void monthInGroupBy() {
      test("SELECT MONTH(created_at), COUNT(*) FROM events GROUP BY MONTH(created_at)");
    }

    @Test
    void dateInGroupBy() {
      test("SELECT DATE(created_at), COUNT(*) FROM logs GROUP BY DATE(created_at)");
    }

    @Test
    void upperInGroupBy() {
      test("SELECT UPPER(name), COUNT(*) FROM users GROUP BY UPPER(name)");
    }

    @Test
    void lowerInGroupBy() {
      test("SELECT LOWER(category), COUNT(*) FROM products GROUP BY LOWER(category)");
    }
  }

  // -----------------------------------------------------------------------
  // 21. REGEXP subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class RegexpSubtleVariants {
    private final RegexpInsteadOfLikeDetector detector = new RegexpInsteadOfLikeDetector();

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.REGEXP_INSTEAD_OF_LIKE, sql);
    }

    @Test
    void regexpSimple() {
      test("SELECT * FROM users WHERE name REGEXP '^John'");
    }

    @Test
    void rlikeSimple() {
      test("SELECT * FROM users WHERE email RLIKE '@gmail\\.com$'");
    }

    @Test
    void regexpLowercase() {
      test("select * from users where name regexp 'test'");
    }

    @Test
    void regexpWithAnd() {
      test("SELECT * FROM users WHERE active = 1 AND name REGEXP '^[A-Z]'");
    }
  }

  // -----------------------------------------------------------------------
  // 22. String concat in WHERE (||) subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class StringConcatInWhereSubtleVariants {
    private final StringConcatInWhereDetector detector = new StringConcatInWhereDetector();

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.STRING_CONCAT_IN_WHERE, sql);
    }

    @Test
    void concatOperator() {
      test("SELECT * FROM users WHERE first_name || ' ' || last_name = 'John Doe'");
    }

    @Test
    void concatOperatorQualified() {
      test("SELECT * FROM users u WHERE u.first_name || u.last_name = 'JohnDoe'");
    }

    @Test
    void concatOperatorLowercase() {
      test("select * from users where name || suffix = 'test'");
    }
  }

  // -----------------------------------------------------------------------
  // 23. INSERT ... SELECT * subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class InsertSelectAllSubtleVariants {
    private final InsertSelectAllDetector detector = new InsertSelectAllDetector();

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.INSERT_SELECT_ALL, sql);
    }

    @Test
    void insertSelectStar() {
      test("INSERT INTO archive SELECT * FROM orders WHERE created_at < '2023-01-01'");
    }

    @Test
    void insertSelectStarWithAlias() {
      test("INSERT INTO backup SELECT o.* FROM orders o WHERE o.status = 'done'");
    }

    @Test
    void insertSelectStarLowercase() {
      test("insert into archive select * from users where deleted = 1");
    }

    @Test
    void insertIntoSelectDistinctStar() {
      test("INSERT INTO temp SELECT DISTINCT * FROM staging");
    }
  }

  // -----------------------------------------------------------------------
  // 24. HAVING misuse subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class HavingMisuseSubtleVariants {
    private final HavingMisuseDetector detector = new HavingMisuseDetector();

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.HAVING_MISUSE, sql);
    }

    @Test
    void havingOnNonAggregateColumn() {
      test(
          "SELECT department, COUNT(*) FROM employees GROUP BY department HAVING department = 'Engineering'");
    }

    @Test
    void havingWithNonAggregateAndAggregate() {
      test(
          "SELECT status, COUNT(*) cnt FROM orders GROUP BY status HAVING status = 'active' AND COUNT(*) > 5");
    }

    @Test
    void havingLowercase() {
      test("select category, count(*) from products group by category having category = 'books'");
    }

    @Test
    void havingWithComparisonOperator() {
      test("SELECT region, SUM(sales) FROM stores GROUP BY region HAVING region != 'test'");
    }
  }

  // -----------------------------------------------------------------------
  // 25. Large IN list subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class LargeInListSubtleVariants {
    private final LargeInListDetector detector = new LargeInListDetector();

    @Test
    void largeInListWith150Values() {
      StringBuilder sb = new StringBuilder("SELECT * FROM users WHERE id IN (");
      for (int i = 0; i < 150; i++) {
        if (i > 0) sb.append(", ");
        sb.append(i);
      }
      sb.append(")");
      String sql = sb.toString();
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.LARGE_IN_LIST, sql);
    }

    @Test
    void largeInListWithPlaceholders() {
      StringBuilder sb = new StringBuilder("SELECT * FROM orders WHERE id IN (");
      for (int i = 0; i < 200; i++) {
        if (i > 0) sb.append(", ");
        sb.append("?");
      }
      sb.append(")");
      String sql = sb.toString();
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.LARGE_IN_LIST, sql);
    }
  }

  // -----------------------------------------------------------------------
  // 26. Correlated subquery subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class CorrelatedSubquerySubtleVariants {
    private final CorrelatedSubqueryDetector detector = new CorrelatedSubqueryDetector();

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.CORRELATED_SUBQUERY, sql);
    }

    @Test
    void correlatedSubqueryInSelect() {
      test(
          "SELECT u.name, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS order_count FROM users u");
    }

    @Test
    void correlatedSubqueryWithAlias() {
      test(
          "SELECT e.name, (SELECT MAX(s.amount) FROM sales s WHERE s.emp_id = e.id) AS max_sale FROM employees e");
    }

    @Test
    void correlatedSubqueryLowercase() {
      test("select u.name, (select count(*) from orders o where o.user_id = u.id) from users u");
    }

    @Test
    void correlatedSubqueryWithSum() {
      test(
          "SELECT d.name, (SELECT SUM(e.salary) FROM employees e WHERE e.dept_id = d.id) AS total FROM departments d");
    }
  }

  // -----------------------------------------------------------------------
  // 27. FOR UPDATE subtle variants (without index metadata)
  // -----------------------------------------------------------------------
  @Nested
  class ForUpdateSubtleVariants {
    private final ForUpdateWithoutIndexDetector detector = new ForUpdateWithoutIndexDetector();

    @Test
    void forUpdateWithoutWhere() {
      String sql = "SELECT * FROM users FOR UPDATE";
      IndexMetadata idx = indexWithTable("users", "id");
      List<Issue> issues = detector.evaluate(List.of(record(sql)), idx);
      check(issues, IssueType.FOR_UPDATE_WITHOUT_INDEX, sql);
    }

    @Test
    void forUpdateOnUnindexedColumn() {
      String sql = "SELECT * FROM users WHERE email = 'test@test.com' FOR UPDATE";
      IndexMetadata idx = indexWithTable("users", "id");
      List<Issue> issues = detector.evaluate(List.of(record(sql)), idx);
      check(issues, IssueType.FOR_UPDATE_WITHOUT_INDEX, sql);
    }

    @Test
    void forShareWithoutIndex() {
      String sql = "SELECT * FROM orders WHERE status = 'pending' FOR SHARE";
      IndexMetadata idx = indexWithTable("orders", "id");
      List<Issue> issues = detector.evaluate(List.of(record(sql)), idx);
      check(issues, IssueType.FOR_UPDATE_WITHOUT_INDEX, sql);
    }

    @Test
    void forUpdateLowercase() {
      String sql = "select * from users where email = 'a@b.com' for update";
      IndexMetadata idx = indexWithTable("users", "id");
      List<Issue> issues = detector.evaluate(List.of(record(sql)), idx);
      check(issues, IssueType.FOR_UPDATE_WITHOUT_INDEX, sql);
    }
  }

  // -----------------------------------------------------------------------
  // 28. Unbounded result set subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class UnboundedResultSetSubtleVariants {
    private final UnboundedResultSetDetector detector = new UnboundedResultSetDetector();

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.UNBOUNDED_RESULT_SET, sql);
    }

    @Test
    void selectWithoutLimit() {
      test("SELECT name, email FROM users WHERE active = 1");
    }

    @Test
    void selectWithJoinNoLimit() {
      test(
          "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE o.status = 'active'");
    }

    @Test
    void selectWithMultipleConditionsNoLimit() {
      test("SELECT * FROM products WHERE category = 'electronics' AND price > 100 AND stock > 0");
    }

    @Test
    void selectWithOrderByNoLimit() {
      test("SELECT name FROM users ORDER BY created_at DESC");
    }

    @Test
    void selectAllColumnsNoLimit() {
      test("SELECT id, name, email, phone, address FROM customers WHERE city = 'Seoul'");
    }
  }

  // -----------------------------------------------------------------------
  // 29. Duplicate query subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class DuplicateQuerySubtleVariants {
    private final DuplicateQueryDetector detector = new DuplicateQueryDetector(3);

    @Test
    void exactDuplicateQueries() {
      String sql = "SELECT name FROM config WHERE key = 'app.version'";
      List<QueryRecord> queries = List.of(record(sql), record(sql));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      check(issues, IssueType.DUPLICATE_QUERY, "Exact duplicate query x2");
    }

    @Test
    void duplicateWithPlaceholders() {
      String sql = "SELECT value FROM settings WHERE name = ?";
      List<QueryRecord> queries = List.of(record(sql), record(sql));
      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      check(issues, IssueType.DUPLICATE_QUERY, "Duplicate with ? placeholder x2");
    }
  }

  // -----------------------------------------------------------------------
  // 30. INSERT ON DUPLICATE KEY subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class InsertOnDuplicateKeySubtleVariants {
    private final InsertOnDuplicateKeyDetector detector = new InsertOnDuplicateKeyDetector();

    private void test(String sql) {
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.INSERT_ON_DUPLICATE_KEY, sql);
    }

    @Test
    void insertOnDuplicateKey() {
      test(
          "INSERT INTO counters (id, count) VALUES (1, 1) ON DUPLICATE KEY UPDATE count = count + 1");
    }

    @Test
    void insertOnDuplicateKeyLowercase() {
      test(
          "insert into stats (key, value) values ('hits', 1) on duplicate key update value = value + 1");
    }
  }

  // -----------------------------------------------------------------------
  // 31. Mixed-case table name edge cases
  // -----------------------------------------------------------------------
  @Nested
  class MixedCaseEdgeCases {
    @Test
    void updateMixedCaseTable() {
      String sql = "UPDATE UserAccounts SET lastLogin = NOW()";
      UpdateWithoutWhereDetector detector = new UpdateWithoutWhereDetector();
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.UPDATE_WITHOUT_WHERE, sql);
    }

    @Test
    void selectAllMixedCaseTable() {
      String sql = "SELECT * FROM UserAccounts WHERE id = 1";
      SelectAllDetector detector = new SelectAllDetector();
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.SELECT_ALL, sql);
    }

    @Test
    void deleteMixedCaseTable() {
      String sql = "DELETE FROM AuditLog";
      UpdateWithoutWhereDetector detector = new UpdateWithoutWhereDetector();
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.UPDATE_WITHOUT_WHERE, sql);
    }
  }

  // -----------------------------------------------------------------------
  // 32. FOR UPDATE with subquery edge case
  // -----------------------------------------------------------------------
  @Nested
  class ForUpdateWithSubquery {
    @Test
    void forUpdateOnDerivedTable() {
      String sql = "SELECT * FROM (SELECT id, name FROM users WHERE active = 1) s FOR UPDATE";
      ForUpdateWithoutIndexDetector detector = new ForUpdateWithoutIndexDetector();
      IndexMetadata idx = indexWithTable("users", "id");
      List<Issue> issues = detector.evaluate(List.of(record(sql)), idx);
      // This is an edge case: FOR UPDATE on derived table
      checkAny(issues, sql, IssueType.FOR_UPDATE_WITHOUT_INDEX);
    }
  }

  // -----------------------------------------------------------------------
  // 33. Implicit type conversion subtle variants
  // -----------------------------------------------------------------------
  @Nested
  class ImplicitTypeConversionSubtleVariants {
    private final ImplicitTypeConversionDetector detector = new ImplicitTypeConversionDetector();

    @Test
    void stringComparedToNumber() {
      // This is a subtle case - comparing varchar column to numeric literal
      String sql = "SELECT * FROM users WHERE phone = 1234567890";
      IndexMetadata idx = indexWithTable("users", "id", "phone");
      List<Issue> issues = detector.evaluate(List.of(record(sql)), idx);
      // May or may not detect depending on column type info availability
      totalAntiPatterns++;
      if (!issues.isEmpty()) {
        detected++;
      } else {
        missed++;
        missedQueries.add("[implicit-type-conversion] " + sql + " (requires column type metadata)");
      }
    }
  }

  // -----------------------------------------------------------------------
  // 34. Correlated subquery with complex patterns
  // -----------------------------------------------------------------------
  @Nested
  class CorrelatedSubqueryComplex {
    private final CorrelatedSubqueryDetector detector = new CorrelatedSubqueryDetector();

    @Test
    void multipleCorrelatedSubqueriesInSelect() {
      String sql =
          "SELECT u.name, "
              + "(SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS orders, "
              + "(SELECT SUM(amount) FROM payments p WHERE p.user_id = u.id) AS total "
              + "FROM users u";
      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      check(issues, IssueType.CORRELATED_SUBQUERY, sql);
    }
  }
}
