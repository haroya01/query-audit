package io.queryaudit.core.parser;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Validates EnhancedSqlParser.removeSubqueries on the #54 reproducer and common subquery shapes.
 * Each test prints the Enhanced + regex output side-by-side so parity and divergences are visible.
 */
class EnhancedRemoveSubqueriesTest {

  private static void compare(String label, String sql) {
    String enhanced = EnhancedSqlParser.removeSubqueries(sql);
    String regex = SqlParser.removeSubqueries(sql);
    System.out.println("[" + label + "] enhanced = " + enhanced);
    System.out.println("[" + label + "] regex    = " + regex);
    System.out.println();
  }

  // ─── #54 — literal containing SQL keywords ────────────────────────
  @Test
  @DisplayName("#54: '(SELECT' inside literal does NOT drop trailing WHERE")
  void issue54_worstCase() {
    String sql =
        "SELECT * FROM logs WHERE note = '(SELECT ' AND status = 'ok)' AND y = 1";
    String enhanced = EnhancedSqlParser.removeSubqueries(sql);
    String regex = SqlParser.removeSubqueries(sql);
    System.out.println("[#54] enhanced = " + enhanced);
    System.out.println("[#54] regex    = " + regex);
    // Enhanced must preserve the full WHERE chain (literal is opaque).
    // Regex drops "AND status = 'ok'" because it treats '(SELECT ' as a real subquery start.
    assertThat(enhanced).containsIgnoringCase("status");
    assertThat(enhanced).containsIgnoringCase("y = 1");
  }

  @Test
  @DisplayName("#54 milder: literal contains '(SELECT' but parens self-balance")
  void issue54_balanced() {
    String sql =
        "SELECT * FROM logs WHERE message = 'Error in (SELECT query failed)' "
            + "AND status = 'active'";
    String enhanced = EnhancedSqlParser.removeSubqueries(sql);
    System.out.println("[#54 mild] enhanced = " + enhanced);
    // Literal content preserved (JSqlParser treats '...' as opaque).
    assertThat(enhanced).containsIgnoringCase("Error in (SELECT query failed)");
    assertThat(enhanced).containsIgnoringCase("AND status = 'active'");
  }

  // ─── Real subquery shapes — parity checks ────────────────────────
  @Test
  @DisplayName("IN (SELECT ...) — replaced with (?)")
  void inSubquery() {
    String sql = "SELECT * FROM t WHERE id IN (SELECT user_id FROM perms WHERE role = 'admin')";
    String enhanced = EnhancedSqlParser.removeSubqueries(sql);
    System.out.println("[IN] enhanced = " + enhanced);
    assertThat(enhanced).doesNotContainIgnoringCase("perms");
    assertThat(enhanced).doesNotContainIgnoringCase("admin");
    assertThat(enhanced).contains("(?)");
  }

  @Test
  @DisplayName("EXISTS (SELECT ...) — replaced with EXISTS (?)")
  void existsSubquery() {
    String sql = "SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM items i WHERE i.order_id = o.id)";
    String enhanced = EnhancedSqlParser.removeSubqueries(sql);
    System.out.println("[EXISTS] enhanced = " + enhanced);
    assertThat(enhanced).doesNotContainIgnoringCase("items");
    assertThat(enhanced).containsIgnoringCase("EXISTS (?)");
  }

  @Test
  @DisplayName("FROM (SELECT ...) t — replaced with (?)")
  void fromSubquery() {
    String sql =
        "SELECT t.id FROM (SELECT id, name FROM users WHERE deleted_at IS NULL) t WHERE t.id > 10";
    String enhanced = EnhancedSqlParser.removeSubqueries(sql);
    System.out.println("[FROM-sub] enhanced = " + enhanced);
    assertThat(enhanced).doesNotContainIgnoringCase("deleted_at");
    assertThat(enhanced).contains("(?)");
    assertThat(enhanced).containsIgnoringCase("t.id > 10");
  }

  @Test
  @DisplayName("Scalar subquery in SELECT list — replaced with (?)")
  void scalarSubquery() {
    String sql =
        "SELECT u.id, (SELECT COUNT(*) FROM orders o WHERE o.user_id = u.id) AS cnt FROM users u";
    String enhanced = EnhancedSqlParser.removeSubqueries(sql);
    System.out.println("[scalar] enhanced = " + enhanced);
    assertThat(enhanced).doesNotContainIgnoringCase("COUNT");
    assertThat(enhanced).contains("(?)");
  }

  @Test
  @DisplayName("Nested subqueries — all replaced")
  void nestedSubqueries() {
    String sql =
        "SELECT * FROM t WHERE id IN (SELECT uid FROM a WHERE gid IN (SELECT g.id FROM g))";
    String enhanced = EnhancedSqlParser.removeSubqueries(sql);
    System.out.println("[nested] enhanced = " + enhanced);
    assertThat(enhanced).doesNotContainIgnoringCase("uid");
    assertThat(enhanced).doesNotContainIgnoringCase("gid");
    assertThat(enhanced).contains("(?)");
  }

  // ─── No subquery — should be effectively unchanged ───────────────
  @Test
  @DisplayName("No subquery — output semantically equivalent to input")
  void noSubquery() {
    String sql = "SELECT id, name FROM users WHERE status = 'active' AND age > 18";
    String enhanced = EnhancedSqlParser.removeSubqueries(sql);
    System.out.println("[no-sub] enhanced = " + enhanced);
    assertThat(enhanced).containsIgnoringCase("users");
    assertThat(enhanced).containsIgnoringCase("status");
    assertThat(enhanced).containsIgnoringCase("age");
  }

  // ─── DML ─────────────────────────────────────────────────────────
  @Test
  @DisplayName("UPDATE with subquery in WHERE")
  void updateWithSubquery() {
    String sql =
        "UPDATE users SET active = 0 WHERE id IN (SELECT user_id FROM banned WHERE reason = 'spam')";
    String enhanced = EnhancedSqlParser.removeSubqueries(sql);
    System.out.println("[UPDATE] enhanced = " + enhanced);
    assertThat(enhanced).doesNotContainIgnoringCase("banned");
    assertThat(enhanced).contains("(?)");
  }

  @Test
  @DisplayName("DELETE with subquery in WHERE")
  void deleteWithSubquery() {
    String sql =
        "DELETE FROM sessions WHERE user_id IN (SELECT id FROM users WHERE last_login < '2020-01-01')";
    String enhanced = EnhancedSqlParser.removeSubqueries(sql);
    System.out.println("[DELETE] enhanced = " + enhanced);
    assertThat(enhanced).doesNotContainIgnoringCase("last_login");
    assertThat(enhanced).contains("(?)");
  }

  // ─── Idempotence ──────────────────────────────────────────────────
  @Test
  @DisplayName("Idempotent — applying twice yields the same output as once")
  void idempotent() {
    String sql = "SELECT * FROM t WHERE id IN (SELECT u.id FROM u WHERE u.active = 1)";
    String once = EnhancedSqlParser.removeSubqueries(sql);
    String twice = EnhancedSqlParser.removeSubqueries(once);
    assertThat(twice).isEqualTo(once);
  }

  // ─── Hibernate-style alias regression ─────────────────────────────
  @Test
  @DisplayName("Hibernate-style u1_0 aliases parse without error")
  void hibernateAliases() {
    String sql =
        "select u1_0.id,u1_0.email from users u1_0 where u1_0.deleted_at is null and u1_0.id=?";
    String enhanced = EnhancedSqlParser.removeSubqueries(sql);
    System.out.println("[hibernate] enhanced = " + enhanced);
    assertThat(enhanced).containsIgnoringCase("u1_0");
  }
}
