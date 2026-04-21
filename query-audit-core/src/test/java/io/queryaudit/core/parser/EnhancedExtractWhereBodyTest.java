package io.queryaudit.core.parser;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Verifies EnhancedSqlParser.extractWhereBody on literal-laden and realistic SQL. */
class EnhancedExtractWhereBodyTest {

  @Test
  @DisplayName("Literal containing ORDER BY does not cut the body")
  void literalOrderByDoesNotCut() {
    String sql = "SELECT * FROM t WHERE note = 'contains ORDER BY keyword' AND x = 1";
    String body = EnhancedSqlParser.extractWhereBody(sql);
    System.out.println("[whereBody] enhanced = " + body);
    // Both clauses preserved, full literal preserved
    assertThat(body).containsIgnoringCase("AND x = 1");
    assertThat(body).contains("contains ORDER BY keyword");
  }

  @Test
  @DisplayName("Plain WHERE body extracted")
  void plainWhere() {
    String sql = "SELECT * FROM t WHERE x = 1 AND y = 2";
    String body = EnhancedSqlParser.extractWhereBody(sql);
    assertThat(body).containsIgnoringCase("x = 1");
    assertThat(body).containsIgnoringCase("y = 2");
  }

  @Test
  @DisplayName("No WHERE clause returns null")
  void noWhere() {
    assertThat(EnhancedSqlParser.extractWhereBody("SELECT * FROM t")).isNull();
  }

  @Test
  @DisplayName("UPDATE WHERE body extracted")
  void updateWhere() {
    String body = EnhancedSqlParser.extractWhereBody("UPDATE t SET x = 1 WHERE id = 5");
    assertThat(body).containsIgnoringCase("id = 5");
  }

  @Test
  @DisplayName("DELETE WHERE body extracted")
  void deleteWhere() {
    String body = EnhancedSqlParser.extractWhereBody("DELETE FROM t WHERE id = 5");
    assertThat(body).containsIgnoringCase("id = 5");
  }

  @Test
  @DisplayName("Hibernate alias style parses cleanly")
  void hibernate() {
    String sql = "select u1_0.id from users u1_0 where u1_0.deleted_at is null and u1_0.id = ?";
    String body = EnhancedSqlParser.extractWhereBody(sql);
    assertThat(body).containsIgnoringCase("u1_0.id");
    assertThat(body).containsIgnoringCase("deleted_at");
  }

  @Test
  @DisplayName("Null input returns null")
  void nullInput() {
    assertThat(EnhancedSqlParser.extractWhereBody(null)).isNull();
  }
}
