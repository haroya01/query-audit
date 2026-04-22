package io.queryaudit.core.parser;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Verifies EnhancedSqlParser.extractWhereColumnsWithOperators parity + literal safety. */
class EnhancedWhereColumnsWithOpsTest {

  @Test
  @DisplayName("Basic equality, inequality, range operators")
  void comparisonOperators() {
    String sql = "SELECT * FROM t WHERE a = 1 AND b > 2 AND c <= 10 AND d != 5";
    List<WhereColumnReference> cols = EnhancedSqlParser.extractWhereColumnsWithOperators(sql);
    assertThat(cols)
        .extracting(WhereColumnReference::columnName)
        .containsExactly("a", "b", "c", "d");
    assertThat(cols)
        .extracting(WhereColumnReference::operator)
        .containsExactly("=", ">", "<=", "!=");
  }

  @Test
  @DisplayName("IS NULL / IS NOT NULL operators")
  void isNull() {
    String sql = "SELECT * FROM t WHERE deleted_at IS NULL AND flagged_at IS NOT NULL";
    List<WhereColumnReference> cols = EnhancedSqlParser.extractWhereColumnsWithOperators(sql);
    assertThat(cols)
        .extracting(WhereColumnReference::columnName)
        .containsExactly("deleted_at", "flagged_at");
    assertThat(cols).extracting(WhereColumnReference::operator).containsExactly("IS", "IS NOT");
  }

  @Test
  @DisplayName("LIKE / NOT LIKE / IN / NOT IN / BETWEEN")
  void moreOperators() {
    String sql =
        "SELECT * FROM t WHERE name LIKE 'a%' AND code NOT LIKE 'x_' "
            + "AND id IN (1,2) AND kind NOT IN (3,4) AND age BETWEEN 18 AND 30";
    List<WhereColumnReference> cols = EnhancedSqlParser.extractWhereColumnsWithOperators(sql);
    assertThat(cols)
        .extracting(WhereColumnReference::columnName)
        .containsExactly("name", "code", "id", "kind", "age");
    assertThat(cols)
        .extracting(WhereColumnReference::operator)
        .containsExactly("LIKE", "NOT LIKE", "IN", "NOT IN", "BETWEEN");
  }

  @Test
  @DisplayName("Literal containing SQL keyword does not leak a column")
  void literalNotConfusedForColumn() {
    String sql = "SELECT * FROM t WHERE note = 'something LIKE something' AND id = 5";
    List<WhereColumnReference> cols = EnhancedSqlParser.extractWhereColumnsWithOperators(sql);
    assertThat(cols).extracting(WhereColumnReference::columnName).containsExactly("note", "id");
  }

  @Test
  @DisplayName("Table-qualified columns preserve alias")
  void qualifiedColumns() {
    String sql =
        "SELECT * FROM users u JOIN orders o ON u.id=o.user_id WHERE u.name = 'X' AND o.status IN (1,2)";
    List<WhereColumnReference> cols = EnhancedSqlParser.extractWhereColumnsWithOperators(sql);
    assertThat(cols).extracting(WhereColumnReference::columnName).containsExactly("name", "status");
    assertThat(cols.get(0).tableOrAlias()).isEqualTo("u");
    assertThat(cols.get(1).tableOrAlias()).isEqualTo("o");
  }

  @Test
  @DisplayName("Hibernate alias style parses cleanly")
  void hibernateAliases() {
    String sql = "select u1_0.id from users u1_0 where u1_0.deleted_at is null and u1_0.id = ?";
    List<WhereColumnReference> cols = EnhancedSqlParser.extractWhereColumnsWithOperators(sql);
    assertThat(cols)
        .extracting(WhereColumnReference::columnName)
        .containsExactly("deleted_at", "id");
    assertThat(cols).extracting(WhereColumnReference::operator).containsExactly("IS", "=");
  }

  @Test
  @DisplayName("Reversed form: literal-first comparison")
  void reversedForm() {
    String sql = "SELECT * FROM t WHERE 5 = id AND 'active' = status";
    List<WhereColumnReference> cols = EnhancedSqlParser.extractWhereColumnsWithOperators(sql);
    assertThat(cols).extracting(WhereColumnReference::columnName).containsExactly("id", "status");
  }
}
