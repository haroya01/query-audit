package io.queryaudit.core.parser;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class EnhancedSqlParserTest {

  @Test
  void jsqlParserIsAvailableOnTestClasspath() {
    assertThat(EnhancedSqlParser.isJSqlParserAvailable()).isTrue();
  }

  // ── CTE support (regex cannot handle this) ─────────────────────────

  @Nested
  class CommonTableExpressions {

    @Test
    void extractsTableNamesFromCte() {
      String sql =
          """
                    WITH active_users AS (
                        SELECT id, name FROM users WHERE status = 'active'
                    )
                    SELECT au.name, o.total
                    FROM active_users au
                    JOIN orders o ON au.id = o.user_id
                    WHERE o.total > 100
                    """;

      List<String> tables = EnhancedSqlParser.extractTableNames(sql);
      assertThat(tables).contains("users", "orders");
    }

    @Test
    void extractsWhereColumnsFromCte() {
      String sql =
          """
                    WITH recent_orders AS (
                        SELECT * FROM orders WHERE created_at > '2024-01-01'
                    )
                    SELECT * FROM recent_orders WHERE total > 500
                    """;

      // Should at least parse without error and return results
      List<ColumnReference> columns = EnhancedSqlParser.extractWhereColumns(sql);
      assertThat(columns).isNotNull();
    }
  }

  // ── Window functions ───────────────────────────────────────────────

  @Nested
  class WindowFunctions {

    @Test
    void parsesQueryWithWindowFunction() {
      String sql =
          """
                    SELECT id, name, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rn
                    FROM employees
                    WHERE status = 'active'
                    """;

      List<ColumnReference> columns = EnhancedSqlParser.extractWhereColumns(sql);
      assertThat(columns).isNotEmpty();
      assertThat(columns).anyMatch(c -> c.columnName().equals("status"));
    }
  }

  // ── Fallback to regex ──────────────────────────────────────────────

  @Nested
  class FallbackBehavior {

    @Test
    void simpleSelectFallsBackGracefully() {
      String sql = "SELECT * FROM users WHERE id = 1";

      List<ColumnReference> columns = EnhancedSqlParser.extractWhereColumns(sql);
      assertThat(columns).isNotEmpty();
      assertThat(columns).anyMatch(c -> c.columnName().equals("id"));
    }

    @Test
    void extractJoinColumnsWorks() {
      String sql =
          "SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = 1";

      List<JoinColumnPair> joins = EnhancedSqlParser.extractJoinColumns(sql);
      assertThat(joins).isNotEmpty();
      assertThat(joins.get(0).left().columnName()).isEqualTo("id");
      assertThat(joins.get(0).right().columnName()).isEqualTo("user_id");
    }

    @Test
    void nullInputReturnsEmpty() {
      assertThat(EnhancedSqlParser.extractWhereColumns(null)).isEmpty();
      assertThat(EnhancedSqlParser.extractJoinColumns(null)).isEmpty();
      assertThat(EnhancedSqlParser.extractTableNames(null)).isEmpty();
    }
  }

  // ── Simple delegations (always use regex) ──────────────────────────

  @Nested
  class SimpleDelegations {

    @Test
    void normalizeDelegates() {
      String result = EnhancedSqlParser.normalize("SELECT * FROM users WHERE id = 42");
      assertThat(result).contains("id = ?");
    }

    @Test
    void hasSelectAllDelegates() {
      assertThat(EnhancedSqlParser.hasSelectAll("SELECT * FROM users")).isTrue();
      assertThat(EnhancedSqlParser.hasSelectAll("SELECT id FROM users")).isFalse();
    }

    @Test
    void isSelectQueryDelegates() {
      assertThat(EnhancedSqlParser.isSelectQuery("SELECT * FROM users")).isTrue();
      assertThat(EnhancedSqlParser.isSelectQuery("INSERT INTO users VALUES (1)")).isFalse();
    }

    @Test
    void hasWhereClauseDelegates() {
      assertThat(EnhancedSqlParser.hasWhereClause("SELECT * FROM users WHERE id = 1")).isTrue();
      assertThat(EnhancedSqlParser.hasWhereClause("SELECT * FROM users")).isFalse();
    }
  }

  // ── Complex nested SQL ─────────────────────────────────────────────

  @Nested
  class ComplexSql {

    @Test
    void handlesNestedSubqueries() {
      String sql =
          """
                    SELECT u.name
                    FROM users u
                    WHERE u.id IN (
                        SELECT o.user_id FROM orders o
                        WHERE o.total > (SELECT AVG(total) FROM orders)
                    )
                    """;

      List<String> tables = EnhancedSqlParser.extractTableNames(sql);
      assertThat(tables).contains("users", "orders");
    }

    @Test
    void handlesMultipleJoins() {
      String sql =
          """
                    SELECT u.name, o.total, p.name as product
                    FROM users u
                    JOIN orders o ON u.id = o.user_id
                    JOIN order_items oi ON o.id = oi.order_id
                    JOIN products p ON oi.product_id = p.id
                    WHERE u.status = 'active'
                    """;

      List<JoinColumnPair> joins = EnhancedSqlParser.extractJoinColumns(sql);
      assertThat(joins).hasSize(3);
    }
  }
}
