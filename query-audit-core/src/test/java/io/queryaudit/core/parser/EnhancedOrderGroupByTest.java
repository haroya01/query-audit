package io.queryaudit.core.parser;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Verifies EnhancedSqlParser.extractOrderByColumns / extractGroupByColumns against the issue #102
 * literal reproducer and common shapes.
 */
class EnhancedOrderGroupByTest {

  @Test
  @DisplayName("#102: literal comma inside ORDER BY does not leak phantom columns")
  void issue102_orderByLiteral() {
    String sql = "SELECT * FROM t ORDER BY name, 'a,b', created_at";

    List<ColumnReference> regex = SqlParser.extractOrderByColumns(sql);
    List<ColumnReference> enhanced = EnhancedSqlParser.extractOrderByColumns(sql);

    // Both parsers now treat single-quoted literals as opaque.
    assertThat(regex).extracting(ColumnReference::columnName).containsExactly("name", "created_at");
    assertThat(enhanced)
        .extracting(ColumnReference::columnName)
        .containsExactly("name", "created_at");
  }

  @Test
  @DisplayName("Plain ORDER BY: parity with regex")
  void orderByPlain() {
    String sql = "SELECT * FROM t ORDER BY a, t.b DESC, c ASC";
    List<ColumnReference> enhanced = EnhancedSqlParser.extractOrderByColumns(sql);
    assertThat(enhanced).extracting(ColumnReference::columnName).containsExactly("a", "b", "c");
    assertThat(enhanced.get(1).tableOrAlias()).isEqualTo("t");
  }

  @Test
  @DisplayName("ORDER BY with function expressions: skipped (matches regex)")
  void orderByWithFunctions() {
    String sql = "SELECT * FROM t ORDER BY LOWER(name), created_at";
    List<ColumnReference> enhanced = EnhancedSqlParser.extractOrderByColumns(sql);
    // Only the plain column should come back
    assertThat(enhanced).extracting(ColumnReference::columnName).containsExactly("created_at");
  }

  @Test
  @DisplayName("No ORDER BY clause returns empty list")
  void noOrderBy() {
    assertThat(EnhancedSqlParser.extractOrderByColumns("SELECT * FROM t WHERE x = 1")).isEmpty();
  }

  @Test
  @DisplayName("GROUP BY with literal comma — literal-safe")
  void groupByLiteral() {
    String sql = "SELECT name, 'x,y', COUNT(*) FROM t GROUP BY name, 'x,y'";
    List<ColumnReference> enhanced = EnhancedSqlParser.extractGroupByColumns(sql);
    // Literal skipped; only real column extracted.
    assertThat(enhanced).extracting(ColumnReference::columnName).containsExactly("name");
  }

  @Test
  @DisplayName("GROUP BY with table-qualified columns")
  void groupByQualified() {
    String sql = "SELECT * FROM t JOIN u ON t.id = u.tid GROUP BY t.id, u.name";
    List<ColumnReference> enhanced = EnhancedSqlParser.extractGroupByColumns(sql);
    assertThat(enhanced).extracting(ColumnReference::columnName).containsExactly("id", "name");
    assertThat(enhanced.get(0).tableOrAlias()).isEqualTo("t");
    assertThat(enhanced.get(1).tableOrAlias()).isEqualTo("u");
  }

  @Test
  @DisplayName("No GROUP BY returns empty list")
  void noGroupBy() {
    assertThat(EnhancedSqlParser.extractGroupByColumns("SELECT * FROM t")).isEmpty();
  }

  @Test
  @DisplayName("Hibernate-style aliases in ORDER BY")
  void hibernateAliases() {
    String sql = "select u1_0.id,u1_0.created_at from users u1_0 order by u1_0.created_at desc";
    List<ColumnReference> enhanced = EnhancedSqlParser.extractOrderByColumns(sql);
    assertThat(enhanced).extracting(ColumnReference::columnName).containsExactly("created_at");
    assertThat(enhanced.get(0).tableOrAlias()).isEqualTo("u1_0");
  }
}
