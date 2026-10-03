package io.queryaudit.core.parser;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for issue #291: Preserve SQL clause boundaries across identifiers, whitespace
 * and nested queries.
 *
 * These tests verify the shared SqlSourceScanner behavior that underpins all clause extraction.
 */
class SqlSourceScannerRegressionTest {

  // ── Test 1: Underscore identifier ─────────────────────────────────────

  @Test
  @DisplayName("Underscore in identifier does not break WHERE boundary")
  void underscoreInIdentifier() {
    String sql = "SELECT * FROM users WHERE credit_limit > ? AND CASE WHEN score > 0 THEN 1 ELSE 0 END = 1";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY", "GROUP BY", "LIMIT");
    assertThat(body).contains("credit_limit > ?");
    assertThat(body).contains("CASE WHEN score > 0 THEN 1 ELSE 0 END = 1");
    // Should NOT be truncated to "credit_"
    assertThat(body).doesNotContain("credit_ >");
  }

  @Test
  @DisplayName("Identifier containing WHERE-like text does not trigger false keyword match")
  void identifierContainingKeyword() {
    String sql = "SELECT * FROM t WHERE some_where_column = 1 AND other_col = 2";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY", "GROUP BY");
    assertThat(body).contains("some_where_column = 1");
    assertThat(body).contains("other_col = 2");
  }

  @Test
  @DisplayName("Identifier containing ORDER BY-like text does not trigger false keyword match")
  void identifierContainingOrderBy() {
    String sql = "SELECT * FROM t WHERE order_by_value = 1 ORDER BY id";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY", "GROUP BY");
    assertThat(body).contains("order_by_value = 1");
    assertThat(body).doesNotContain("ORDER BY");
  }

  // ── Test 2: Normal ORDER BY terminates WHERE ──────────────────────────

  @Test
  @DisplayName("Normal ORDER BY terminates WHERE clause")
  void normalOrderByTerminatesWhere() {
    String sql = "SELECT * FROM users WHERE id > ? ORDER BY id";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY", "GROUP BY");
    assertThat(body).contains("id > ?");
    assertThat(body).doesNotContain("ORDER BY");
  }

  // ── Test 3: Newline between ORDER and BY ──────────────────────────────

  @Test
  @DisplayName("Newline between ORDER and BY still terminates WHERE")
  void newlineBetweenOrderAndBy() {
    String sql = "SELECT * FROM users WHERE id > ?\nORDER\nBY id";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY", "GROUP BY");
    assertThat(body).contains("id > ?");
    assertThat(body).doesNotContain("ORDER");
  }

  // ── Test 4: Arbitrary whitespace ──────────────────────────────────────

  @Test
  @DisplayName("Multiple spaces between ORDER and BY")
  void multipleSpacesOrderBy() {
    String sql = "SELECT * FROM users WHERE id > ? ORDER    BY id";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY", "GROUP BY");
    assertThat(body).contains("id > ?");
    assertThat(body).doesNotContain("ORDER");
  }

  @Test
  @DisplayName("Tab between ORDER and BY")
  void tabOrderBy() {
    String sql = "SELECT * FROM users WHERE id > ? ORDER\tBY id";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY", "GROUP BY");
    assertThat(body).contains("id > ?");
    assertThat(body).doesNotContain("ORDER");
  }

  @Test
  @DisplayName("Carriage return between ORDER and BY")
  void crOrderBy() {
    String sql = "SELECT * FROM users WHERE id > ? ORDER\rBY id";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY", "GROUP BY");
    assertThat(body).contains("id > ?");
    assertThat(body).doesNotContain("ORDER");
  }

  // ── Test 5: Nested ORDER BY in subquery ───────────────────────────────

  @Test
  @DisplayName("Inner ORDER BY in subquery does not terminate outer WHERE")
  void nestedOrderByInSubquery() {
    String sql = "SELECT * FROM users "
        + "WHERE id IN ("
        + "    SELECT user_id FROM permissions WHERE active = 1 ORDER BY created_at"
        + ") "
        + "AND status = ? "
        + "ORDER BY id";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY", "GROUP BY");
    // Outer WHERE should include the subquery and the AND status = ?
    assertThat(body).contains("id IN");
    assertThat(body).contains("status = ?");
    // Should not include the outer ORDER BY
    assertThat(body).doesNotContain("ORDER BY id");
  }

  // ── Test 6: Nested WHERE in subquery ──────────────────────────────────

  @Test
  @DisplayName("Inner WHERE in subquery does not affect outer WHERE boundary")
  void nestedWhereInSubquery() {
    String sql = "SELECT * FROM users "
        + "WHERE id IN (SELECT user_id FROM permissions WHERE active = 1) "
        + "AND status = ? "
        + "ORDER BY id";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY", "GROUP BY");
    assertThat(body).contains("id IN");
    assertThat(body).contains("status = ?");
    assertThat(body).doesNotContain("ORDER BY");
  }

  @Test
  @DisplayName("Multiple nested WHERE clauses")
  void multipleNestedWhere() {
    String sql = "SELECT * FROM users "
        + "WHERE id IN (SELECT user_id FROM permissions WHERE active = 1 AND role = 'admin') "
        + "AND EXISTS (SELECT 1 FROM logs WHERE user_id = users.id AND action = 'login') "
        + "AND status = ?";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY", "GROUP BY");
    assertThat(body).contains("id IN");
    assertThat(body).contains("status = ?");
    assertThat(body).contains("EXISTS");
  }

  // ── Test 7: Quoted identifier ─────────────────────────────────────────

  @Test
  @DisplayName("Double-quoted identifier with keyword text does not trigger clause boundary")
  void doubleQuotedIdentifier() {
    String sql = "SELECT * FROM users WHERE \"order_by\" = ? AND status = ? ORDER BY id";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY", "GROUP BY");
    assertThat(body).contains("\"order_by\" = ?");
    assertThat(body).contains("status = ?");
    assertThat(body).doesNotContain("ORDER BY");
  }

  @Test
  @DisplayName("Backtick-quoted identifier with keyword text does not trigger clause boundary")
  void backtickQuotedIdentifier() {
    String sql = "SELECT * FROM users WHERE `order_by` = ? AND status = ? ORDER BY id";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY", "GROUP BY");
    assertThat(body).contains("`order_by` = ?");
    assertThat(body).contains("status = ?");
    assertThat(body).doesNotContain("ORDER BY");
  }

  // ── Test 8: String literal ────────────────────────────────────────────

  @Test
  @DisplayName("String literal with keyword text does not trigger clause boundary")
  void stringLiteralWithKeyword() {
    String sql = "SELECT * FROM users WHERE note = 'ORDER BY' AND status = ? ORDER BY id";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY", "GROUP BY");
    assertThat(body).contains("note = 'ORDER BY'");
    assertThat(body).contains("status = ?");
    assertThat(body).doesNotContain("ORDER BY id");
  }

  @Test
  @DisplayName("String literal with escaped quotes")
  void stringLiteralWithEscapedQuotes() {
    String sql = "SELECT * FROM users WHERE note = 'it''s ORDER BY here' AND status = ? ORDER BY id";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY", "GROUP BY");
    assertThat(body).contains("note = 'it''s ORDER BY here'");
    assertThat(body).contains("status = ?");
    assertThat(body).doesNotContain("ORDER BY id");
  }

  @Test
  @DisplayName("MySQL backslash escape in string literal")
  void mysqlBackslashEscape() {
    String sql = "SELECT * FROM users WHERE note = 'ORDER\\' BY' AND status = ? ORDER BY id";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY", "GROUP BY");
    assertThat(body).contains("note = 'ORDER\\' BY'");
    assertThat(body).contains("status = ?");
    assertThat(body).doesNotContain("ORDER BY id");
  }

  // ── Test 9: Comments ──────────────────────────────────────────────────

  @Test
  @DisplayName("Line comment with keyword does not trigger clause boundary")
  void lineCommentWithKeyword() {
    String sql = "SELECT * FROM users WHERE status = ? -- ORDER BY comment\nORDER BY id";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY", "GROUP BY");
    assertThat(body).contains("status = ?");
    assertThat(body).doesNotContain("ORDER BY");
  }

  @Test
  @DisplayName("Block comment with keyword does not trigger clause boundary")
  void blockCommentWithKeyword() {
    String sql = "SELECT * FROM users WHERE status = ? /* ORDER BY */ AND active = 1 ORDER BY id";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY", "GROUP BY");
    assertThat(body).contains("status = ?");
    assertThat(body).contains("active = 1");
    assertThat(body).doesNotContain("ORDER BY id");
  }

  @Test
  @DisplayName("Nested block comments")
  void nestedBlockComments() {
    String sql = "SELECT * FROM users WHERE status = ? /* outer /* ORDER BY */ inner */ AND active = 1 ORDER BY id";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY", "GROUP BY");
    assertThat(body).contains("status = ?");
    assertThat(body).contains("active = 1");
    assertThat(body).doesNotContain("ORDER BY id");
  }

  // ── Test 10: Metamorphic tests ────────────────────────────────────────

  @Test
  @DisplayName("Metamorphic: identifier spelling change does not affect WHERE extraction")
  void metamorphicIdentifierSpelling() {
    String sql1 = "SELECT * FROM users WHERE credit > ? AND status = ?";
    String sql2 = "SELECT * FROM users WHERE credit_limit > ? AND status = ?";
    String sql3 = "SELECT * FROM users WHERE credit_limit_max_value > ? AND status = ?";

    String body1 = SqlSourceScanner.clauseBody(sql1, "WHERE", "ORDER BY");
    String body2 = SqlSourceScanner.clauseBody(sql2, "WHERE", "ORDER BY");
    String body3 = SqlSourceScanner.clauseBody(sql3, "WHERE", "ORDER BY");

    // All should extract the full WHERE body up to the end (no ORDER BY present)
    assertThat(body1).contains("credit > ?").contains("status = ?");
    assertThat(body2).contains("credit_limit > ?").contains("status = ?");
    assertThat(body3).contains("credit_limit_max_value > ?").contains("status = ?");
  }

  @Test
  @DisplayName("Metamorphic: whitespace variations produce same WHERE extraction")
  void metamorphicWhitespace() {
    String sql1 = "SELECT * FROM users WHERE id > ? ORDER BY name";
    String sql2 = "SELECT * FROM users WHERE id > ? ORDER    BY name";
    String sql3 = "SELECT * FROM users WHERE id > ? ORDER\nBY name";
    String sql4 = "SELECT * FROM users WHERE id > ? ORDER\tBY name";

    String body1 = SqlSourceScanner.clauseBody(sql1, "WHERE", "ORDER BY");
    String body2 = SqlSourceScanner.clauseBody(sql2, "WHERE", "ORDER BY");
    String body3 = SqlSourceScanner.clauseBody(sql3, "WHERE", "ORDER BY");
    String body4 = SqlSourceScanner.clauseBody(sql4, "WHERE", "ORDER BY");

    // All should extract the same WHERE body
    assertThat(body1).isEqualTo(body2);
    assertThat(body2).isEqualTo(body3);
    assertThat(body3).isEqualTo(body4);
    assertThat(body1).contains("id > ?");
  }

  @Test
  @DisplayName("Metamorphic: comments around clause keywords do not affect extraction")
  void metamorphicComments() {
    String sql1 = "SELECT * FROM users WHERE id > ? ORDER BY name";
    String sql2 = "SELECT * FROM users WHERE id > ? -- comment\nORDER BY name";
    String sql3 = "SELECT * FROM users WHERE id > ? /* comment */ ORDER BY name";

    String body1 = SqlSourceScanner.clauseBody(sql1, "WHERE", "ORDER BY");
    String body2 = SqlSourceScanner.clauseBody(sql2, "WHERE", "ORDER BY");
    String body3 = SqlSourceScanner.clauseBody(sql3, "WHERE", "ORDER BY");

    assertThat(body1).isEqualTo(body2);
    assertThat(body2).isEqualTo(body3);
    assertThat(body1).contains("id > ?");
  }

  @Test
  @DisplayName("Metamorphic: nested clause content change does not affect outer extraction")
  void metamorphicNestedContent() {
    String sql1 = "SELECT * FROM users WHERE id IN (SELECT x FROM y WHERE a = 1) AND b = 2";
    String sql2 = "SELECT * FROM users WHERE id IN (SELECT x FROM y WHERE a = 1 ORDER BY z) AND b = 2";
    String sql3 = "SELECT * FROM users WHERE id IN (SELECT x FROM y WHERE a = 1 GROUP BY w) AND b = 2";

    String body1 = SqlSourceScanner.clauseBody(sql1, "WHERE", "ORDER BY");
    String body2 = SqlSourceScanner.clauseBody(sql2, "WHERE", "ORDER BY");
    String body3 = SqlSourceScanner.clauseBody(sql3, "WHERE", "ORDER BY");

    // All should extract the same outer WHERE body
    assertThat(body1).contains("id IN").contains("b = 2");
    assertThat(body2).contains("id IN").contains("b = 2");
    assertThat(body3).contains("id IN").contains("b = 2");
    // None should include outer ORDER BY (there isn't one, but they should all stop at end)
  }

  // ── Nested GROUP BY / HAVING / LIMIT / FETCH ──────────────────────────

  @Test
  @DisplayName("Inner GROUP BY in subquery does not terminate outer WHERE")
  void nestedGroupByInSubquery() {
    String sql = "SELECT * FROM users "
        + "WHERE id IN (SELECT user_id FROM permissions WHERE active = 1 GROUP BY role) "
        + "AND status = ?";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY", "GROUP BY");
    assertThat(body).contains("id IN").contains("status = ?");
    assertThat(body).doesNotContain("GROUP BY role");
  }

  @Test
  @DisplayName("Inner HAVING in subquery does not terminate outer WHERE")
  void nestedHavingInSubquery() {
    String sql = "SELECT * FROM users "
        + "WHERE id IN (SELECT user_id FROM permissions WHERE active = 1 GROUP BY role HAVING COUNT(*) > 5) "
        + "AND status = ?";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY", "GROUP BY");
    assertThat(body).contains("id IN").contains("status = ?");
    assertThat(body).doesNotContain("HAVING");
  }

  @Test
  @DisplayName("Inner LIMIT in subquery does not terminate outer WHERE")
  void nestedLimitInSubquery() {
    String sql = "SELECT * FROM users "
        + "WHERE id IN (SELECT user_id FROM permissions WHERE active = 1 LIMIT 10) "
        + "AND status = ?";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY", "LIMIT");
    assertThat(body).contains("id IN").contains("status = ?");
    assertThat(body).doesNotContain("LIMIT 10");
  }

  // ── Parenthesis depth tracking ────────────────────────────────────────

  @Test
  @DisplayName("Parentheses in function calls do not affect depth")
  void parenthesesInFunctionCalls() {
    String sql = "SELECT * FROM users WHERE UPPER(name) = 'JOHN' AND id > ?";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY");
    assertThat(body).contains("UPPER(name) = 'JOHN'").contains("id > ?");
  }

  @Test
  @DisplayName("Complex nested parentheses with subqueries")
  void complexNestedParentheses() {
    String sql = "SELECT * FROM users "
        + "WHERE id IN (SELECT a FROM b WHERE x IN (SELECT y FROM z WHERE z = 1)) "
        + "AND status = ?";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "ORDER BY");
    assertThat(body).contains("id IN").contains("status = ?");
  }

  // ── ClauseBodyFrom tests (for JOIN ON extraction) ────────────────────

  @Test
  @DisplayName("clauseBodyFrom extracts JOIN ON body correctly")
  void clauseBodyFromJoinOn() {
    String sql = "SELECT * FROM orders JOIN users ON orders.user_id = users.id WHERE orders.status = 'active'";

    // Find JOIN ON position manually, then extract from there
    int onPos = sql.indexOf("ON ") + 3;
    String body = SqlSourceScanner.clauseBodyFrom(sql, onPos, "JOIN", "WHERE", "GROUP BY");
    assertThat(body).contains("orders.user_id = users.id");
    assertThat(body).doesNotContain("WHERE");
  }

  @Test
  @DisplayName("clauseBodyFrom handles nested subquery in JOIN ON")
  void clauseBodyFromJoinOnNested() {
    String sql = "SELECT * FROM orders "
        + "JOIN users ON orders.user_id IN (SELECT id FROM users WHERE active = 1) "
        + "WHERE orders.status = 'active'";

    int onPos = sql.indexOf("ON ") + 3;
    String body = SqlSourceScanner.clauseBodyFrom(sql, onPos, "JOIN", "WHERE", "GROUP BY");
    assertThat(body).contains("orders.user_id IN");
    assertThat(body).doesNotContain("WHERE orders.status");
  }

  // ── FETCH FIRST support ──────────────────────────────────────────────

  @Test
  @DisplayName("FETCH FIRST terminates WHERE")
  void fetchFirstTerminatesWhere() {
    String sql = "SELECT * FROM users WHERE id > ? FETCH FIRST 10 ROWS ONLY";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "FETCH");
    assertThat(body).contains("id > ?");
    assertThat(body).doesNotContain("FETCH");
  }

  @Test
  @DisplayName("Nested FETCH FIRST in subquery does not terminate outer WHERE")
  void nestedFetchFirstInSubquery() {
    String sql = "SELECT * FROM users "
        + "WHERE id IN (SELECT id FROM logs WHERE created > '2024-01-01' FETCH FIRST 5 ROWS ONLY) "
        + "AND status = ?";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "FETCH");
    assertThat(body).contains("id IN").contains("status = ?");
    assertThat(body).doesNotContain("FETCH FIRST");
  }

  // ── UNION support ────────────────────────────────────────────────────

  @Test
  @DisplayName("UNION terminates WHERE")
  void unionTerminatesWhere() {
    String sql = "SELECT * FROM users WHERE id > ? UNION SELECT * FROM admins WHERE id > ?";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "UNION");
    assertThat(body).contains("id > ?");
    assertThat(body).doesNotContain("UNION");
  }

  @Test
  @DisplayName("Nested UNION in subquery does not terminate outer WHERE")
  void nestedUnionInSubquery() {
    String sql = "SELECT * FROM users "
        + "WHERE id IN (SELECT id FROM a UNION SELECT id FROM b) "
        + "AND status = ?";

    String body = SqlSourceScanner.clauseBody(sql, "WHERE", "UNION");
    assertThat(body).contains("id IN").contains("status = ?");
    assertThat(body).doesNotContain("UNION");
  }
}