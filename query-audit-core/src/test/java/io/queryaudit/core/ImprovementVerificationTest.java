package io.queryaudit.core;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.detector.*;
import io.queryaudit.core.model.*;
import io.queryaudit.core.parser.ColumnReference;
import io.queryaudit.core.parser.FunctionUsage;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive verification tests for all detection improvements. Organized by team (Parser,
 * Detector) and by issue category (false positives, false negatives).
 *
 * <p>Each test documents the specific bug it verifies with a descriptive name.
 */
@DisplayName("Detection Improvement Verification")
class ImprovementVerificationTest {

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private static QueryRecord recordWithTrace(String sql, String stackTrace) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), stackTrace);
  }

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  // ═══════════════════════════════════════════════════════════════════
  //  Team A: SqlParser Improvements
  // ═══════════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("A1: normalize() — Escaped Quote Handling")
  class NormalizeEscapedQuotes {

    @Test
    @DisplayName("SQL-standard escaped quotes ('') are correctly normalized")
    void sqlStandardEscapedQuotes() {
      String sql = "SELECT * FROM users WHERE name = 'it''s a test'";
      String normalized = SqlParser.normalize(sql);
      assertThat(normalized).isEqualTo("select * from users where name = ?");
    }

    @Test
    @DisplayName("MySQL backslash-escaped quotes (\\') are correctly normalized")
    void mysqlBackslashEscapedQuotes() {
      String sql = "SELECT * FROM users WHERE name = 'O\\'Brien'";
      String normalized = SqlParser.normalize(sql);
      assertThat(normalized).isEqualTo("select * from users where name = ?");
    }

    @Test
    @DisplayName("Multiple escaped quotes in one string")
    void multipleEscapedQuotes() {
      String sql = "SELECT * FROM t WHERE val = 'it''s a ''test'''";
      String normalized = SqlParser.normalize(sql);
      assertThat(normalized).isEqualTo("select * from t where val = ?");
    }

    @Test
    @DisplayName("Empty string literal is normalized")
    void emptyStringLiteral() {
      String sql = "SELECT * FROM t WHERE name = ''";
      String normalized = SqlParser.normalize(sql);
      assertThat(normalized).isEqualTo("select * from t where name = ?");
    }
  }

  @Nested
  @DisplayName("A2: normalize() — Numeric Literal Handling")
  class NormalizeNumbers {

    @Test
    @DisplayName("Scientific notation is normalized")
    void scientificNotation() {
      String sql = "SELECT * FROM data WHERE value = 1.5e10";
      String normalized = SqlParser.normalize(sql);
      assertThat(normalized).doesNotContain("1.5e10");
      assertThat(normalized).contains("value = ?");
    }

    @Test
    @DisplayName("Scientific notation with negative exponent")
    void scientificNotationNegativeExponent() {
      String sql = "SELECT * FROM data WHERE value > 3.14e-5";
      String normalized = SqlParser.normalize(sql);
      assertThat(normalized).doesNotContain("3.14");
    }

    @Test
    @DisplayName("Hexadecimal literals are normalized")
    void hexLiterals() {
      String sql = "SELECT * FROM flags WHERE bits = 0xFF";
      String normalized = SqlParser.normalize(sql);
      assertThat(normalized).doesNotContain("0xFF");
      assertThat(normalized).contains("bits = ?");
    }

    @Test
    @DisplayName("Basic integers still normalized")
    void basicIntegers() {
      String sql = "SELECT * FROM users WHERE id = 42";
      String normalized = SqlParser.normalize(sql);
      assertThat(normalized).isEqualTo("select * from users where id = ?");
    }

    @Test
    @DisplayName("Decimal numbers still normalized")
    void decimalNumbers() {
      String sql = "SELECT * FROM products WHERE price < 19.99";
      String normalized = SqlParser.normalize(sql);
      assertThat(normalized).doesNotContain("19.99");
    }
  }

  @Nested
  @DisplayName("A3: extractWhereColumns() — Extended Operators")
  class WhereColumnOperators {

    @Test
    @DisplayName("NOT IN operator extracts column")
    void notInOperator() {
      String sql = "SELECT * FROM t WHERE status NOT IN ('a', 'b')";
      List<ColumnReference> cols = SqlParser.extractWhereColumns(sql);
      assertThat(cols).anyMatch(c -> "status".equals(c.columnName()));
    }

    @Test
    @DisplayName("NOT LIKE operator extracts column")
    void notLikeOperator() {
      String sql = "SELECT * FROM t WHERE name NOT LIKE '%admin%'";
      List<ColumnReference> cols = SqlParser.extractWhereColumns(sql);
      assertThat(cols).anyMatch(c -> "name".equals(c.columnName()));
    }

    @Test
    @DisplayName("IS NOT operator extracts column")
    void isNotOperator() {
      String sql = "SELECT * FROM t WHERE deleted_at IS NOT NULL";
      List<ColumnReference> cols = SqlParser.extractWhereColumns(sql);
      assertThat(cols).anyMatch(c -> "deleted_at".equals(c.columnName()));
    }

    @Test
    @DisplayName("ILIKE operator (PostgreSQL) extracts column")
    void ilikeOperator() {
      String sql = "SELECT * FROM users WHERE email ILIKE '%@gmail.com'";
      List<ColumnReference> cols = SqlParser.extractWhereColumns(sql);
      assertThat(cols).anyMatch(c -> "email".equals(c.columnName()));
    }

    @Test
    @DisplayName("Standard operators still work")
    void standardOperatorsStillWork() {
      String sql =
          "SELECT * FROM t WHERE a = 1 AND b != 2 AND c <> 3 AND d >= 4 AND e BETWEEN 5 AND 10";
      List<ColumnReference> cols = SqlParser.extractWhereColumns(sql);
      assertThat(cols).extracting(ColumnReference::columnName).contains("a", "b", "c", "d", "e");
    }
  }

  @Nested
  @DisplayName("A4: detectWhereFunctions() — Expanded Function List")
  class ExpandedFunctionDetection {

    @Test
    @DisplayName("LENGTH() detected in WHERE")
    void lengthFunction() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions("SELECT * FROM users WHERE LENGTH(name) > 10");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).functionName()).isEqualTo("LENGTH");
      assertThat(funcs.get(0).columnName()).isEqualTo("name");
    }

    @Test
    @DisplayName("COALESCE() detected in WHERE")
    void coalesceFunction() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions("SELECT * FROM orders WHERE COALESCE(discount, 0) > 0");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).functionName()).isEqualTo("COALESCE");
    }

    @Test
    @DisplayName("ABS() detected in WHERE")
    void absFunction() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions("SELECT * FROM t WHERE ABS(balance) > 100");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).functionName()).isEqualTo("ABS");
    }

    @Test
    @DisplayName("JSON_EXTRACT() detected in WHERE")
    void jsonExtractFunction() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions(
              "SELECT * FROM events WHERE JSON_EXTRACT(payload, '$.type') = 'click'");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).functionName()).isEqualTo("JSON_EXTRACT");
      assertThat(funcs.get(0).columnName()).isEqualTo("payload");
    }

    @Test
    @DisplayName("MD5() detected in WHERE")
    void md5Function() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions("SELECT * FROM users WHERE MD5(email) = 'abc123'");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).functionName()).isEqualTo("MD5");
    }

    @Test
    @DisplayName("DAY() detected in WHERE")
    void dayFunction() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions("SELECT * FROM events WHERE DAY(created_at) = 15");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).functionName()).isEqualTo("DAY");
    }

    @Test
    @DisplayName(
        "Original functions still detected (DATE, LOWER, UPPER, YEAR, MONTH, TRIM, SUBSTRING, CAST)")
    void originalFunctionsStillWork() {
      assertThat(SqlParser.detectWhereFunctions("SELECT * FROM t WHERE DATE(c) = '2024-01-01'"))
          .hasSize(1);
      assertThat(SqlParser.detectWhereFunctions("SELECT * FROM t WHERE LOWER(c) = 'x'")).hasSize(1);
      assertThat(SqlParser.detectWhereFunctions("SELECT * FROM t WHERE UPPER(c) = 'X'")).hasSize(1);
      assertThat(SqlParser.detectWhereFunctions("SELECT * FROM t WHERE YEAR(c) = 2024")).hasSize(1);
    }
  }

  @Nested
  @DisplayName("A5: detectJoinFunctions() — Function Detection in JOIN ON")
  class JoinFunctionDetection {

    @Test
    @DisplayName("DATE() in JOIN ON is detected")
    void dateInJoinOn() {
      String sql =
          "SELECT * FROM orders o JOIN users u ON DATE(u.created_at) = '2024-01-01' WHERE o.status = 'active'";
      List<FunctionUsage> funcs = SqlParser.detectJoinFunctions(sql);
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).functionName()).isEqualTo("DATE");
      assertThat(funcs.get(0).columnName()).isEqualTo("created_at");
    }

    @Test
    @DisplayName("LOWER() in JOIN ON is detected")
    void lowerInJoinOn() {
      String sql = "SELECT * FROM a JOIN b ON LOWER(a.code) = LOWER(b.code)";
      List<FunctionUsage> funcs = SqlParser.detectJoinFunctions(sql);
      assertThat(funcs).hasSizeGreaterThanOrEqualTo(1);
    }

    @Test
    @DisplayName("No functions in JOIN ON returns empty")
    void noFunctionsInJoinOn() {
      String sql =
          "SELECT * FROM orders o JOIN users u ON o.user_id = u.id WHERE o.status = 'active'";
      List<FunctionUsage> funcs = SqlParser.detectJoinFunctions(sql);
      assertThat(funcs).isEmpty();
    }

    @Test
    @DisplayName("Function in WHERE but not in JOIN returns empty for detectJoinFunctions")
    void functionInWhereNotJoin() {
      String sql =
          "SELECT * FROM orders o JOIN users u ON o.user_id = u.id WHERE YEAR(o.created_at) = 2024";
      List<FunctionUsage> funcs = SqlParser.detectJoinFunctions(sql);
      assertThat(funcs).isEmpty();
    }
  }

  @Nested
  @DisplayName("A6: extractTableNames() — Schema-Qualified & Quoted Names")
  class TableNameExtraction {

    @Test
    @DisplayName("Schema-qualified table name extracts table part")
    void schemaQualifiedTable() {
      List<String> tables =
          SqlParser.extractTableNames("SELECT * FROM analytics.user_events WHERE id = 1");
      assertThat(tables).contains("user_events");
    }

    @Test
    @DisplayName("Backtick-quoted table name extracted")
    void backtickQuotedTable() {
      List<String> tables = SqlParser.extractTableNames("SELECT * FROM `my_table` WHERE id = 1");
      assertThat(tables).contains("my_table");
    }

    @Test
    @DisplayName("Regular table names still work")
    void regularTableNames() {
      List<String> tables =
          SqlParser.extractTableNames("SELECT * FROM users u JOIN orders o ON u.id = o.user_id");
      assertThat(tables).contains("users", "orders");
    }

    @Test
    @DisplayName("Schema-qualified JOIN table extracted")
    void schemaQualifiedJoinTable() {
      List<String> tables =
          SqlParser.extractTableNames(
              "SELECT * FROM users JOIN schema1.orders ON users.id = orders.user_id");
      assertThat(tables).contains("users", "orders");
    }
  }

  @Nested
  @DisplayName("A7: countOrConditions() — False Positive Prevention")
  class OrCountingFalsePositives {

    @Test
    @DisplayName("OR inside string literal is NOT counted")
    void orInsideStringLiteral() {
      String sql = "SELECT * FROM t WHERE name LIKE '%OR%' AND status = 'active'";
      int count = SqlParser.countOrConditions(sql);
      assertThat(count).isEqualTo(0);
    }

    @Test
    @DisplayName("OR inside string value is NOT counted")
    void orInsideStringValue() {
      String sql = "SELECT * FROM t WHERE description = 'Oregon or Oregon City' AND id = 1";
      int count = SqlParser.countOrConditions(sql);
      assertThat(count).isEqualTo(0);
    }

    @Test
    @DisplayName("Real OR conditions are still counted correctly")
    void realOrConditionsStillCounted() {
      String sql = "SELECT * FROM users WHERE a = 1 OR b = 2 OR c = 3";
      int count = SqlParser.countOrConditions(sql);
      assertThat(count).isEqualTo(2);
    }

    @Test
    @DisplayName("Mix of real OR and string OR counts only real ones")
    void mixedOrAndStringOr() {
      String sql =
          "SELECT * FROM t WHERE name = 'OR logic' OR status = 'active' OR type = 'OR gate'";
      int count = SqlParser.countOrConditions(sql);
      assertThat(count).isEqualTo(2);
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Team B: Detector Improvements
  // ═══════════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("B1: WhereFunctionDetector — JOIN ON Function Detection")
  class WhereFunctionDetectorJoinOn {

    @Test
    @DisplayName("Detects function in JOIN ON condition")
    void detectsFunctionInJoinOn() {
      WhereFunctionDetector detector = new WhereFunctionDetector();
      List<QueryRecord> queries =
          List.of(
              record(
                  "SELECT * FROM orders o JOIN users u ON DATE(u.created_at) = o.order_date WHERE o.status = 'active'"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues)
          .anyMatch(
              i ->
                  i.type() == IssueType.WHERE_FUNCTION && i.detail().contains("JOIN ON condition"));
    }

    @Test
    @DisplayName("JOIN ON function issues have WARNING severity")
    void joinOnFunctionHasWarningSeverity() {
      WhereFunctionDetector detector = new WhereFunctionDetector();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM a JOIN b ON LOWER(a.code) = b.code"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      List<Issue> joinIssues = issues.stream().filter(i -> i.detail().contains("JOIN ON")).toList();
      assertThat(joinIssues).allMatch(i -> i.severity() == Severity.WARNING);
    }

    @Test
    @DisplayName("WHERE function issues still have ERROR severity")
    void whereFunctionStillError() {
      WhereFunctionDetector detector = new WhereFunctionDetector();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM users WHERE LOWER(email) = 'test@test.com'"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).severity()).isEqualTo(Severity.ERROR);
    }

    @Test
    @DisplayName("Detects expanded functions (LENGTH) in WHERE via detector")
    void detectsExpandedFunctionsViaDetector() {
      WhereFunctionDetector detector = new WhereFunctionDetector();
      List<QueryRecord> queries = List.of(record("SELECT * FROM users WHERE LENGTH(name) > 10"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).detail()).contains("LENGTH");
    }

    @Test
    @DisplayName("Query with only JOIN function (no WHERE function) still detected")
    void onlyJoinFunctionDetected() {
      WhereFunctionDetector detector = new WhereFunctionDetector();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM orders o JOIN users u ON YEAR(u.created_at) = 2024"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isNotEmpty();
    }
  }

  @Nested
  @DisplayName("B2: CartesianJoinDetector — USING Clause Handling")
  class CartesianJoinUsingClause {

    @Test
    @DisplayName("JOIN with ON clause is NOT flagged")
    void joinWithOnNotFlagged() {
      CartesianJoinDetector detector = new CartesianJoinDetector();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM orders o JOIN users u ON o.user_id = u.id"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("JOIN with USING clause is NOT flagged as Cartesian")
    void joinWithUsingNotFlagged() {
      CartesianJoinDetector detector = new CartesianJoinDetector();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM orders JOIN users USING (user_id)"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("JOIN without ON or USING IS flagged")
    void joinWithoutConditionFlagged() {
      CartesianJoinDetector detector = new CartesianJoinDetector();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM orders JOIN users WHERE orders.id = 1"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.CARTESIAN_JOIN);
    }

    @Test
    @DisplayName("Implicit join (FROM a, b) without WHERE IS flagged")
    void implicitJoinWithoutWhereFlagged() {
      CartesianJoinDetector detector = new CartesianJoinDetector();
      List<QueryRecord> queries = List.of(record("SELECT * FROM users, orders"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Implicit join (FROM a, b) WITH WHERE is NOT flagged")
    void implicitJoinWithWhereNotFlagged() {
      CartesianJoinDetector detector = new CartesianJoinDetector();
      List<QueryRecord> queries =
          List.of(record("SELECT * FROM users, orders WHERE users.id = orders.user_id"));

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }
  }

  @Nested
  @DisplayName("B3: NPlusOneDetector — MySQL Comma Pagination")
  class NPlusOnePagination {

    @Test
    @DisplayName("LIMIT ?, ? (MySQL comma format) from separate requests is INFO")
    void mysqlCommaPaginationSpreadOut_isInfo() {
      NPlusOneDetector detector = new NPlusOneDetector(3);
      // Simulate 5 separate HTTP requests, each with a pagination query + 20 other queries
      List<QueryRecord> queries = new ArrayList<>();
      String[] sqls = {
        "SELECT * FROM products LIMIT 0, 20",
        "SELECT * FROM products LIMIT 20, 20",
        "SELECT * FROM products LIMIT 40, 20",
        "SELECT * FROM products LIMIT 60, 20",
        "SELECT * FROM products LIMIT 80, 20"
      };
      for (int req = 0; req < sqls.length; req++) {
        queries.add(recordWithTrace(sqls[req], "com.example.Controller.page:10"));
        for (int j = 0; j < 20; j++) {
          queries.add(record("SELECT * FROM filler_" + req + "_" + j + " WHERE id = 1"));
        }
      }

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      // Spread across requests -> medianGap ~20 -> INFO
      assertThat(issues).filteredOn(i -> i.severity() == Severity.ERROR).isEmpty();
    }

    @Test
    @DisplayName("Standard LIMIT OFFSET from separate requests is INFO")
    void standardPaginationSpreadOut_isInfo() {
      NPlusOneDetector detector = new NPlusOneDetector(3);
      // Simulate 5 separate HTTP requests
      List<QueryRecord> queries = new ArrayList<>();
      String[] sqls = {
        "SELECT * FROM products LIMIT 20 OFFSET 0",
        "SELECT * FROM products LIMIT 20 OFFSET 20",
        "SELECT * FROM products LIMIT 20 OFFSET 40",
        "SELECT * FROM products LIMIT 20 OFFSET 60",
        "SELECT * FROM products LIMIT 20 OFFSET 80"
      };
      for (int req = 0; req < sqls.length; req++) {
        queries.add(recordWithTrace(sqls[req], "com.example.Controller.page:10"));
        for (int j = 0; j < 20; j++) {
          queries.add(record("SELECT * FROM filler_" + req + "_" + j + " WHERE id = 1"));
        }
      }

      List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
      // Spread across requests -> INFO
      assertThat(issues).filteredOn(i -> i.severity() == Severity.ERROR).isEmpty();
    }

    @Test
    @DisplayName("Non-pagination repeated queries ARE flagged")
    void nonPaginationQueriesFlagged() {
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
      assertThat(issues.get(0).type()).isEqualTo(IssueType.N_PLUS_ONE_SUSPECT);
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Regression Tests — Ensure existing behavior is preserved
  // ═══════════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Regression: Existing detections still work")
  class RegressionTests {

    @Test
    @DisplayName("SELECT * still detected")
    void selectAllStillDetected() {
      SelectAllDetector detector = new SelectAllDetector();
      List<Issue> issues = detector.evaluate(List.of(record("SELECT * FROM users")), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).type()).isEqualTo(IssueType.SELECT_ALL);
    }

    @Test
    @DisplayName("LIKE leading wildcard still detected")
    void likeWildcardStillDetected() {
      LikeWildcardDetector detector = new LikeWildcardDetector();
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE name LIKE '%test'")), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("LIKE both-side wildcard also detected")
    void likeBothSideWildcardDetected() {
      LikeWildcardDetector detector = new LikeWildcardDetector();
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE name LIKE '%test%'")), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Large OFFSET still detected")
    void largeOffsetStillDetected() {
      OffsetPaginationDetector detector = new OffsetPaginationDetector(1000);
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users LIMIT 10 OFFSET 5000")), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("OR abuse still detected with real ORs")
    void orAbuseStillDetected() {
      OrAbuseDetector detector = new OrAbuseDetector(3);
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM t WHERE a=1 OR b=2 OR c=3 OR d=4")), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("OR abuse NOT triggered by ORs inside strings")
    void orAbuseNotTriggeredByStringOr() {
      OrAbuseDetector detector = new OrAbuseDetector(3);
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM t WHERE name = 'a OR b OR c OR d'")), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("WHERE function DATE() still detected")
    void whereFunctionStillDetected() {
      WhereFunctionDetector detector = new WhereFunctionDetector();
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM t WHERE DATE(created_at) = '2024-01-01'")),
              EMPTY_INDEX);
      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Cartesian JOIN without ON still detected")
    void cartesianJoinStillDetected() {
      CartesianJoinDetector detector = new CartesianJoinDetector();
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users JOIN orders WHERE users.id = 1")), EMPTY_INDEX);
      assertThat(issues).hasSize(1);
    }

    @Test
    @DisplayName("Missing WHERE index still detected with metadata")
    void missingIndexStillDetected() {
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of("users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 100))));

      MissingIndexDetector detector = new MissingIndexDetector();
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE email = 'test@example.com'")), metadata);
      assertThat(issues).anyMatch(i -> i.type() == IssueType.MISSING_WHERE_INDEX);
    }

    @Test
    @DisplayName("SqlParser.normalize null safety")
    void normalizeNullSafety() {
      assertThat(SqlParser.normalize(null)).isNull();
    }

    @Test
    @DisplayName("SqlParser.isSelectQuery works")
    void isSelectQuery() {
      assertThat(SqlParser.isSelectQuery("SELECT * FROM t")).isTrue();
      assertThat(SqlParser.isSelectQuery("INSERT INTO t VALUES (1)")).isFalse();
      assertThat(SqlParser.isSelectQuery(null)).isFalse();
    }
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Edge Case Tests
  // ═══════════════════════════════════════════════════════════════════

  @Nested
  @DisplayName("Edge Cases")
  class EdgeCases {

    @Test
    @DisplayName("Null SQL handled gracefully in all parser methods")
    void nullSqlHandled() {
      assertThat(SqlParser.normalize(null)).isNull();
      assertThat(SqlParser.isSelectQuery(null)).isFalse();
      assertThat(SqlParser.hasSelectAll(null)).isFalse();
      assertThat(SqlParser.extractWhereColumns(null)).isEmpty();
      assertThat(SqlParser.extractJoinColumns(null)).isEmpty();
      assertThat(SqlParser.extractOrderByColumns(null)).isEmpty();
      assertThat(SqlParser.extractGroupByColumns(null)).isEmpty();
      assertThat(SqlParser.detectWhereFunctions(null)).isEmpty();
      assertThat(SqlParser.detectJoinFunctions(null)).isEmpty();
      assertThat(SqlParser.countOrConditions(null)).isEqualTo(0);
      assertThat(SqlParser.extractOffsetValue(null)).isEmpty();
      assertThat(SqlParser.extractTableNames(null)).isEmpty();
    }

    @Test
    @DisplayName("Empty SQL handled gracefully")
    void emptySqlHandled() {
      assertThat(SqlParser.normalize("")).isEqualTo("");
      assertThat(SqlParser.isSelectQuery("")).isFalse();
      assertThat(SqlParser.extractTableNames("")).isEmpty();
      assertThat(SqlParser.countOrConditions("")).isEqualTo(0);
    }

    @Test
    @DisplayName("Complex query with all features combined")
    void complexQueryCombined() {
      String sql =
          "SELECT * FROM analytics.user_events ue "
              + "JOIN `order_items` oi ON LOWER(ue.email) = oi.email "
              + "WHERE YEAR(ue.created_at) = 2024 "
              + "AND ue.status NOT IN ('deleted', 'archived') "
              + "AND ue.name NOT LIKE '%test%' "
              + "ORDER BY ue.created_at DESC "
              + "LIMIT 20 OFFSET 100";

      // Table extraction
      List<String> tables = SqlParser.extractTableNames(sql);
      assertThat(tables).contains("user_events", "order_items");

      // WHERE columns
      List<ColumnReference> whereCols = SqlParser.extractWhereColumns(sql);
      assertThat(whereCols).anyMatch(c -> "status".equals(c.columnName()));
      assertThat(whereCols).anyMatch(c -> "name".equals(c.columnName()));

      // WHERE functions
      List<FunctionUsage> whereFuncs = SqlParser.detectWhereFunctions(sql);
      assertThat(whereFuncs).anyMatch(f -> "YEAR".equals(f.functionName()));

      // JOIN functions
      List<FunctionUsage> joinFuncs = SqlParser.detectJoinFunctions(sql);
      assertThat(joinFuncs).anyMatch(f -> "LOWER".equals(f.functionName()));

      // Offset
      OptionalLong offset = SqlParser.extractOffsetValue(sql);
      assertThat(offset).isPresent();
      assertThat(offset.getAsLong()).isEqualTo(100);
    }

    @Test
    @DisplayName("Multiple backtick-quoted tables in complex JOIN")
    void multipleBacktickTables() {
      String sql =
          "SELECT * FROM `users` u JOIN `orders` o ON u.id = o.user_id JOIN `products` p ON o.product_id = p.id";
      List<String> tables = SqlParser.extractTableNames(sql);
      assertThat(tables).contains("users", "orders", "products");
    }
  }
}
