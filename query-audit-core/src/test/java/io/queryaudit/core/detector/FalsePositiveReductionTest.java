package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.parser.FunctionUsage;
import io.queryaudit.core.parser.SqlParser;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests verifying false positive reductions in query-audit detection rules.
 *
 * <p>These tests cover scenarios that were previously incorrectly flagged:
 *
 * <ul>
 *   <li>Index-safe functions (COALESCE, IFNULL, IF, NULLIF) in WHERE clauses
 *   <li>CTE (WITH clause) queries causing spurious detections
 * </ul>
 */
class FalsePositiveReductionTest {

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());
  private final WhereFunctionDetector detector = new WhereFunctionDetector();

  // ── Task 1: Index-safe functions should not be flagged ──────────────

  @Nested
  @DisplayName("Index-safe functions (COALESCE, IFNULL, IF, NULLIF) should not be flagged")
  class IndexSafeFunctions {

    @Test
    @DisplayName("COALESCE in WHERE clause is not flagged")
    void coalesceInWhere_notFlagged() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM orders WHERE COALESCE(status, 'pending') = 'active'")),
              EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("IFNULL in WHERE clause is not flagged")
    void ifnullInWhere_notFlagged() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE IFNULL(email, '') != ''")),
              EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("IF in WHERE clause is not flagged")
    void ifInWhere_notFlagged() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM products WHERE IF(discount > 0, price * 0.9, price) > 100")),
              EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("NULLIF in WHERE clause is not flagged")
    void nullifInWhere_notFlagged() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM accounts WHERE NULLIF(balance, 0) IS NOT NULL")),
              EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Non-safe functions like LOWER are still flagged")
    void lowerInWhere_stillFlagged() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM users WHERE LOWER(name) = 'john'")),
              EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).column()).isEqualTo("name");
    }

    @Test
    @DisplayName("DATE function is still flagged")
    void dateInWhere_stillFlagged() {
      List<Issue> issues =
          detector.evaluate(
              List.of(record("SELECT * FROM events WHERE DATE(created_at) = '2024-01-01'")),
              EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).column()).isEqualTo("created_at");
    }

    @Test
    @DisplayName("COALESCE in JOIN ON clause is not flagged")
    void coalesceInJoin_notFlagged() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM orders o "
                          + "LEFT JOIN users u ON u.id = COALESCE(o.user_id, o.guest_id)")),
              EMPTY_INDEX);

      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("Mixed safe and unsafe functions - only unsafe is flagged")
    void mixedSafeAndUnsafe_onlyUnsafeFlagged() {
      List<Issue> issues =
          detector.evaluate(
              List.of(
                  record(
                      "SELECT * FROM users "
                          + "WHERE COALESCE(status, 'active') = 'active' "
                          + "AND LOWER(email) = 'test@example.com'")),
              EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).column()).isEqualTo("email");
    }
  }

  // ── Task 2: CTE (WITH clause) handling ─────────────────────────────

  @Nested
  @DisplayName("CTE queries should not cause false positives")
  class CteHandling {

    @Test
    @DisplayName("Simple CTE with WHERE on main query - no false positives from CTE body")
    void simpleCte_whereFunctionsOnlyFromMainQuery() {
      String sql =
          "WITH cte AS (SELECT id, LOWER(name) AS lower_name FROM users) "
              + "SELECT * FROM cte WHERE lower_name = 'john'";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // The main query has no function wrapping - LOWER is inside the CTE definition
      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("CTE with function in main WHERE clause is still flagged")
    void cte_functionInMainWhere_isFlagged() {
      String sql =
          "WITH active_users AS (SELECT * FROM users WHERE active = 1) "
              + "SELECT * FROM active_users WHERE LOWER(name) = 'john'";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      assertThat(issues).hasSize(1);
      assertThat(issues.get(0).column()).isEqualTo("name");
    }

    @Test
    @DisplayName("Multiple CTEs - only main query is analyzed")
    void multipleCtes_onlyMainQueryAnalyzed() {
      String sql =
          "WITH cte1 AS (SELECT id, UPPER(name) AS uname FROM users), "
              + "cte2 AS (SELECT id, DATE(created_at) AS cdate FROM orders) "
              + "SELECT * FROM cte1 JOIN cte2 ON cte1.id = cte2.id WHERE cte1.uname = 'TEST'";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);

      // UPPER and DATE are inside CTE definitions, not in the main WHERE
      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("stripCtePrefix correctly extracts main query body")
    void stripCtePrefix_extractsMainBody() {
      String sql =
          "WITH cte AS (SELECT * FROM users WHERE id > 10) "
              + "SELECT * FROM cte WHERE name = 'test'";

      String mainBody = SqlParser.stripCtePrefix(sql);
      assertThat(mainBody).startsWith("SELECT * FROM cte");
      assertThat(mainBody).doesNotContain("WITH");
    }

    @Test
    @DisplayName("Recursive CTE is handled correctly")
    void recursiveCte_handledCorrectly() {
      String sql =
          "WITH RECURSIVE tree AS ("
              + "SELECT id, parent_id, name FROM categories WHERE parent_id IS NULL "
              + "UNION ALL "
              + "SELECT c.id, c.parent_id, c.name FROM categories c "
              + "JOIN tree t ON c.parent_id = t.id"
              + ") SELECT * FROM tree WHERE name = 'Electronics'";

      List<Issue> issues = detector.evaluate(List.of(record(sql)), EMPTY_INDEX);
      assertThat(issues).isEmpty();
    }

    @Test
    @DisplayName("CTE query - extractTableNames works on main query only")
    void cte_extractTableNames_mainQueryOnly() {
      String sql =
          "WITH temp AS (SELECT * FROM internal_table WHERE LOWER(col) = 'x') "
              + "SELECT * FROM temp WHERE id = 1";

      List<String> tables = SqlParser.extractTableNames(sql);
      // Should extract 'temp' from the main query, not 'internal_table' from CTE
      assertThat(tables).contains("temp");
      assertThat(tables).doesNotContain("internal_table");
    }

    @Test
    @DisplayName("detectWhereFunctions on CTE query only detects main WHERE functions")
    void detectWhereFunctions_cteQuery_onlyMainWhere() {
      String sql =
          "WITH filtered AS (SELECT id, YEAR(created_at) AS yr FROM events) "
              + "SELECT * FROM filtered WHERE yr > 2020";

      List<FunctionUsage> functions = SqlParser.detectWhereFunctions(sql);
      // YEAR is inside CTE definition, not in main WHERE
      assertThat(functions).isEmpty();
    }
  }
}
