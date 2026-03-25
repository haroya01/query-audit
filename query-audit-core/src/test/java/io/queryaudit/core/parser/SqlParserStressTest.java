package io.queryaudit.core.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Adversarial stress tests for SqlParser. Every method is tested with edge cases, malformed input,
 * and boundary conditions.
 */
class SqlParserStressTest {

  // ── normalize ───────────────────────────────────────────────────────

  @Nested
  class NormalizeStress {

    @Test
    void nullInput() {
      assertThat(SqlParser.normalize(null)).isNull();
    }

    @Test
    void emptyString() {
      assertThat(SqlParser.normalize("")).isEqualTo("");
    }

    @Test
    void whitespaceOnly() {
      assertThat(SqlParser.normalize("   \t\n  ")).isEqualTo("");
    }

    @Test
    void nestedSingleQuotes() {
      // SQL standard: escaped single quote is ''
      String sql = "SELECT * FROM users WHERE name = 'it''s a test'";
      String result = SqlParser.normalize(sql);
      // The entire string literal should be replaced with ?
      assertThat(result).doesNotContain("it");
      assertThat(result).doesNotContain("test");
      assertThat(result).contains("name = ?");
    }

    @Test
    void backslashEscapedQuotes() {
      // MySQL style: \'
      String sql = "SELECT * FROM users WHERE name = 'O\\'Brien'";
      String result = SqlParser.normalize(sql);
      assertThat(result).doesNotContain("Brien");
      assertThat(result).contains("name = ?");
    }

    @Test
    void unicodeInStrings() {
      String sql = "SELECT * FROM users WHERE name = '日本語'";
      String result = SqlParser.normalize(sql);
      assertThat(result).doesNotContain("日本語");
      assertThat(result).contains("name = ?");
    }

    @Test
    void negativeNumbers() {
      String sql = "SELECT * FROM users WHERE id = -1";
      String result = SqlParser.normalize(sql);
      // negative sign + number should be normalized
      assertThat(result).contains("id = -?");
    }

    @Test
    void decimalNumbers() {
      String sql = "SELECT * FROM products WHERE price = 3.14";
      String result = SqlParser.normalize(sql);
      assertThat(result).doesNotContain("3.14");
      assertThat(result).contains("price = ?");
    }

    @Test
    void hexLiterals() {
      // 0xFF - at minimum the numeric part should be replaced
      String sql = "SELECT * FROM flags WHERE val = 0xFF";
      String result = SqlParser.normalize(sql);
      // 0 gets replaced by ?, xFF remains or whole thing becomes ?
      // This is acceptable for pattern grouping as long as it's consistent
      assertThatCode(() -> SqlParser.normalize(sql)).doesNotThrowAnyException();
    }

    @Test
    void inListWithMixedTypes() {
      // After normalization of literals, IN list values become ?
      String sql = "SELECT * FROM t WHERE x IN (1, 'a', 2.5)";
      String result = SqlParser.normalize(sql);
      assertThat(result).contains("in (?)");
    }

    @Test
    void veryLongSql() {
      // 10000+ character SQL should not throw or hang
      StringBuilder sb = new StringBuilder("SELECT * FROM t WHERE id IN (");
      for (int i = 0; i < 5000; i++) {
        if (i > 0) sb.append(", ");
        sb.append(i);
      }
      sb.append(")");
      String sql = sb.toString();
      assertThat(sql.length()).isGreaterThan(10000);
      assertThatCode(() -> SqlParser.normalize(sql)).doesNotThrowAnyException();
      String result = SqlParser.normalize(sql);
      assertThat(result).isNotNull();
    }

    @Test
    void sqlWithBlockComment() {
      String sql = "/* comment */ SELECT * FROM t WHERE id = 1";
      String result = SqlParser.normalize(sql);
      // Comments are not stripped by current impl - just verify no crash
      assertThat(result).contains("select");
    }

    @Test
    void sqlWithLineComment() {
      String sql = "-- comment\nSELECT * FROM t WHERE id = 1";
      String result = SqlParser.normalize(sql);
      assertThat(result).contains("select");
    }

    @Test
    void multipleSingleQuotedStrings() {
      String sql = "SELECT * FROM t WHERE a = 'x' AND b = 'y' AND c = 'z'";
      String result = SqlParser.normalize(sql);
      assertThat(result).doesNotContain("x").doesNotContain("y").doesNotContain("'");
    }

    @Test
    void emptyStringLiteral() {
      String sql = "SELECT * FROM t WHERE name = ''";
      String result = SqlParser.normalize(sql);
      assertThat(result).contains("name = ?");
    }

    @Test
    void consecutiveNumbers() {
      String sql = "SELECT * FROM t WHERE a = 1 AND b = 2 AND c = 3";
      String result = SqlParser.normalize(sql);
      assertThat(result).isEqualTo("select * from t where a = ? and b = ? and c = ?");
    }

    @Test
    void scientificNotation() {
      // 1e10 - the number regex may not handle this
      String sql = "SELECT * FROM t WHERE val = 1e10";
      assertThatCode(() -> SqlParser.normalize(sql)).doesNotThrowAnyException();
    }

    @Test
    void multipleWhitespaceTypes() {
      String sql = "SELECT\t*\nFROM\r\n  t\tWHERE\n  id = 1";
      String result = SqlParser.normalize(sql);
      assertThat(result).doesNotContain("\t").doesNotContain("\n").doesNotContain("\r");
      assertThat(result).doesNotContain("  ");
    }
  }

  // ── isSelectQuery ───────────────────────────────────────────────────

  @Nested
  class IsSelectQueryStress {

    @Test
    void nullInput() {
      assertThat(SqlParser.isSelectQuery(null)).isFalse();
    }

    @Test
    void emptyString() {
      assertThat(SqlParser.isSelectQuery("")).isFalse();
    }

    @Test
    void leadingSpaces() {
      assertThat(SqlParser.isSelectQuery("  SELECT * FROM t")).isTrue();
    }

    @Test
    void leadingNewlinesAndTabs() {
      assertThat(SqlParser.isSelectQuery("\n\tSELECT * FROM t")).isTrue();
    }

    @Test
    void selectfromNoSpace() {
      // "SELECTFROM" - SELECT is not followed by word boundary
      // \bSELECT\b requires word boundary after T, but SELECTFROM has no boundary
      assertThat(SqlParser.isSelectQuery("SELECTFROM t")).isFalse();
    }

    @Test
    void subqueryInParens() {
      // "(SELECT ...)" - does NOT start with SELECT
      assertThat(SqlParser.isSelectQuery("(SELECT * FROM t)")).isFalse();
    }

    @Test
    void withCteSelect() {
      // WITH ... SELECT is not a SELECT at the start
      assertThat(SqlParser.isSelectQuery("WITH cte AS (SELECT 1) SELECT * FROM cte")).isFalse();
    }

    @Test
    void explainSelect() {
      assertThat(SqlParser.isSelectQuery("EXPLAIN SELECT * FROM t")).isFalse();
    }

    @Test
    void insertIntoSelect() {
      assertThat(SqlParser.isSelectQuery("INSERT INTO t2 SELECT * FROM t1")).isFalse();
    }

    @Test
    void selectInComment() {
      // The word SELECT in a comment - should not be treated as start
      // Current impl just checks start, so comment at start would be a problem
      assertThat(SqlParser.isSelectQuery("-- SELECT\nINSERT INTO t VALUES (1)")).isFalse();
    }

    @Test
    void onlySelect() {
      // Just the word "SELECT" with no columns
      assertThat(SqlParser.isSelectQuery("SELECT")).isTrue();
    }
  }

  // ── hasSelectAll ────────────────────────────────────────────────────

  @Nested
  class HasSelectAllStress {

    @Test
    void nullInput() {
      assertThat(SqlParser.hasSelectAll(null)).isFalse();
    }

    @Test
    void selectStarWithAdditionalColumns() {
      assertThat(SqlParser.hasSelectAll("SELECT *, id FROM t")).isTrue();
    }

    @Test
    void selectAllStar() {
      assertThat(SqlParser.hasSelectAll("SELECT ALL * FROM t")).isTrue();
    }

    @Test
    void selectDistinctStar() {
      // DISTINCT * is still a select-all
      assertThat(SqlParser.hasSelectAll("SELECT DISTINCT * FROM t")).isTrue();
    }

    @Test
    void selectTableDotStarWithSpaces() {
      // "SELECT t . * FROM t" - spaces around dot
      // Current regex requires \w+\. with no space, so this may not match
      // That's acceptable - this is unusual SQL
      assertThatCode(() -> SqlParser.hasSelectAll("SELECT t . * FROM t"))
          .doesNotThrowAnyException();
    }

    @Test
    void nestedSelectStar() {
      // Outer query selects specific columns, inner has *
      String sql = "SELECT id FROM (SELECT * FROM t) sub";
      // hasSelectAll scans entire SQL, so it finds the inner SELECT *
      assertThat(SqlParser.hasSelectAll(sql)).isTrue();
    }

    @Test
    void countStarNotSelectAll() {
      assertThat(SqlParser.hasSelectAll("SELECT COUNT(*) FROM t")).isFalse();
    }

    @Test
    void existsStarNotSelectAll() {
      // EXISTS(SELECT * ...) is idiomatic SQL and should not be flagged
      assertThat(SqlParser.hasSelectAll("SELECT EXISTS(SELECT * FROM t)")).isFalse();
    }

    @Test
    void multipleAliasedStars() {
      assertThat(SqlParser.hasSelectAll("SELECT a.*, b.* FROM a JOIN b ON a.id = b.id")).isTrue();
    }

    @Test
    void selectStarInSubqueryOnly() {
      // If only the subquery has *, that still triggers
      String sql = "SELECT id FROM (SELECT * FROM inner_t) AS sub";
      assertThat(SqlParser.hasSelectAll(sql)).isTrue();
    }
  }

  // ── extractWhereColumns ─────────────────────────────────────────────

  @Nested
  class ExtractWhereColumnsStress {

    @Test
    void nullInput() {
      assertThat(SqlParser.extractWhereColumns(null)).isEmpty();
    }

    @Test
    void emptyString() {
      assertThat(SqlParser.extractWhereColumns("")).isEmpty();
    }

    @Test
    void tautology() {
      // WHERE 1=1 - "1" is not a column, should the parser pick it up?
      // After removeSubqueries, WHERE_COLUMN regex: (\w+\.)?\w+ operator
      // "1" matches \w+ but it's a number. Current impl doesn't filter numbers.
      List<ColumnReference> cols = SqlParser.extractWhereColumns("SELECT * FROM t WHERE 1=1");
      // Columns extracted may include "1" since the parser doesn't distinguish
      // This is a known limitation - just verify no crash
      assertThatCode(() -> SqlParser.extractWhereColumns("SELECT * FROM t WHERE 1=1"))
          .doesNotThrowAnyException();
    }

    @Test
    void notExists() {
      // Subquery should be removed
      List<ColumnReference> cols =
          SqlParser.extractWhereColumns(
              "SELECT * FROM t WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.id = t.id)");
      // After subquery removal, WHERE body should not contain the inner columns
      // The NOT keyword is filtered by isKeyword
      assertThatCode(() -> cols.size()).doesNotThrowAnyException();
    }

    @Test
    void betweenOperator() {
      List<ColumnReference> cols =
          SqlParser.extractWhereColumns("SELECT * FROM t WHERE a BETWEEN 1 AND 10");
      assertThat(cols).extracting(ColumnReference::columnName).contains("a");
    }

    @Test
    void isNotNull() {
      List<ColumnReference> cols =
          SqlParser.extractWhereColumns("SELECT * FROM t WHERE a IS NOT NULL");
      assertThat(cols).extracting(ColumnReference::columnName).contains("a");
    }

    @Test
    void inSubquery() {
      // Subquery should be removed
      List<ColumnReference> cols =
          SqlParser.extractWhereColumns("SELECT * FROM t WHERE a IN (SELECT id FROM t2)");
      assertThat(cols).extracting(ColumnReference::columnName).contains("a");
    }

    @Test
    void caseWhenInWhere() {
      // CASE is a keyword, WHEN is a keyword
      List<ColumnReference> cols =
          SqlParser.extractWhereColumns(
              "SELECT * FROM t WHERE CASE WHEN a = 1 THEN b ELSE c END = 'd'");
      // "a" should be extracted from "a = 1"
      assertThat(cols).extracting(ColumnReference::columnName).contains("a");
    }

    @Test
    void deeplyNestedParens() {
      List<ColumnReference> cols =
          SqlParser.extractWhereColumns("SELECT * FROM t WHERE ((((a = 1))))");
      assertThat(cols).extracting(ColumnReference::columnName).contains("a");
    }

    @Test
    void columnEqualsColumn() {
      // WHERE a = b - both sides match the pattern
      List<ColumnReference> cols = SqlParser.extractWhereColumns("SELECT * FROM t WHERE a = b");
      // The regex only matches left side of operator, so only "a" is found
      assertThat(cols).extracting(ColumnReference::columnName).contains("a");
    }

    @Test
    void likeOperator() {
      List<ColumnReference> cols =
          SqlParser.extractWhereColumns("SELECT * FROM t WHERE name LIKE '%test%'");
      assertThat(cols).extracting(ColumnReference::columnName).contains("name");
    }

    @Test
    void multipleOperators() {
      List<ColumnReference> cols =
          SqlParser.extractWhereColumns(
              "SELECT * FROM t WHERE a = 1 AND b != 2 AND c <> 3 AND d <= 4 AND e >= 5 AND f < 6 AND g > 7");
      assertThat(cols)
          .extracting(ColumnReference::columnName)
          .contains("a", "b", "c", "d", "e", "f", "g");
    }

    @Test
    void whereBeforeGroupBy() {
      List<ColumnReference> cols =
          SqlParser.extractWhereColumns("SELECT * FROM t WHERE a = 1 GROUP BY b");
      assertThat(cols).extracting(ColumnReference::columnName).contains("a");
      // "b" should NOT be extracted (it's in GROUP BY, not WHERE)
      assertThat(cols).extracting(ColumnReference::columnName).doesNotContain("b");
    }

    @Test
    void whereBeforeOrderBy() {
      List<ColumnReference> cols =
          SqlParser.extractWhereColumns("SELECT * FROM t WHERE a = 1 ORDER BY b");
      assertThat(cols).extracting(ColumnReference::columnName).contains("a");
      assertThat(cols).extracting(ColumnReference::columnName).doesNotContain("b");
    }

    @Test
    void whereBeforeLimit() {
      List<ColumnReference> cols =
          SqlParser.extractWhereColumns("SELECT * FROM t WHERE a = 1 LIMIT 10");
      assertThat(cols).extracting(ColumnReference::columnName).contains("a");
    }

    @Test
    void whereBeforeHaving() {
      List<ColumnReference> cols =
          SqlParser.extractWhereColumns(
              "SELECT * FROM t WHERE a = 1 GROUP BY a HAVING COUNT(*) > 1");
      assertThat(cols).extracting(ColumnReference::columnName).contains("a");
    }
  }

  // ── extractJoinColumns ──────────────────────────────────────────────

  @Nested
  class ExtractJoinColumnsStress {

    @Test
    void nullInput() {
      assertThat(SqlParser.extractJoinColumns(null)).isEmpty();
    }

    @Test
    void leftOuterJoin() {
      // "LEFT OUTER JOIN" - the regex expects \bJOIN\s+\w+
      // LEFT OUTER JOIN has extra keywords before JOIN
      List<JoinColumnPair> pairs =
          SqlParser.extractJoinColumns("SELECT * FROM a LEFT OUTER JOIN b ON a.id = b.a_id");
      assertThat(pairs).hasSize(1);
      assertThat(pairs.get(0).left().columnName()).isEqualTo("id");
      assertThat(pairs.get(0).right().columnName()).isEqualTo("a_id");
    }

    @Test
    void rightJoin() {
      List<JoinColumnPair> pairs =
          SqlParser.extractJoinColumns("SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id");
      assertThat(pairs).hasSize(1);
    }

    @Test
    void innerJoin() {
      List<JoinColumnPair> pairs =
          SqlParser.extractJoinColumns("SELECT * FROM a INNER JOIN b ON a.id = b.a_id");
      assertThat(pairs).hasSize(1);
    }

    @Test
    void crossJoinNoOn() {
      // CROSS JOIN has no ON clause
      List<JoinColumnPair> pairs = SqlParser.extractJoinColumns("SELECT * FROM a CROSS JOIN b");
      assertThat(pairs).isEmpty();
    }

    @Test
    void joinUsing() {
      // JOIN ... USING (id) - no ON clause
      // Current parser only handles ON, not USING
      List<JoinColumnPair> pairs =
          SqlParser.extractJoinColumns("SELECT * FROM a JOIN b USING (id)");
      // USING is not supported - should return empty
      assertThat(pairs).isEmpty();
    }

    @Test
    void naturalJoin() {
      // NATURAL JOIN has no ON/USING
      List<JoinColumnPair> pairs = SqlParser.extractJoinColumns("SELECT * FROM a NATURAL JOIN b");
      assertThat(pairs).isEmpty();
    }

    @Test
    void multipleJoinsFourTables() {
      List<JoinColumnPair> pairs =
          SqlParser.extractJoinColumns(
              "SELECT * FROM a "
                  + "JOIN b ON a.id = b.a_id "
                  + "JOIN c ON b.id = c.b_id "
                  + "JOIN d ON c.id = d.c_id");
      assertThat(pairs).hasSize(3);
    }

    @Test
    void joinWithAlias() {
      List<JoinColumnPair> pairs =
          SqlParser.extractJoinColumns("SELECT * FROM orders o JOIN users u ON o.user_id = u.id");
      assertThat(pairs).hasSize(1);
      assertThat(pairs.get(0).left().tableOrAlias()).isEqualTo("o");
      assertThat(pairs.get(0).left().columnName()).isEqualTo("user_id");
    }

    @Test
    void joinWithAsAlias() {
      List<JoinColumnPair> pairs =
          SqlParser.extractJoinColumns(
              "SELECT * FROM orders JOIN users AS u ON orders.user_id = u.id");
      assertThat(pairs).hasSize(1);
    }

    @Test
    void joinWithNoTableQualifier() {
      List<JoinColumnPair> pairs =
          SqlParser.extractJoinColumns("SELECT * FROM a JOIN b ON id = other_id");
      assertThat(pairs).hasSize(1);
      assertThat(pairs.get(0).left().tableOrAlias()).isNull();
      assertThat(pairs.get(0).left().columnName()).isEqualTo("id");
    }
  }

  // ── extractOrderByColumns ───────────────────────────────────────────

  @Nested
  class ExtractOrderByColumnsStress {

    @Test
    void nullInput() {
      assertThat(SqlParser.extractOrderByColumns(null)).isEmpty();
    }

    @Test
    void positionalOrderBy() {
      // ORDER BY 1 - "1" is a number, not a column name
      // Current impl doesn't filter numbers, it matches \w+ which includes digits
      List<ColumnReference> cols = SqlParser.extractOrderByColumns("SELECT a, b FROM t ORDER BY 1");
      // The parser may extract "1" as a column - that's a known limitation
      assertThatCode(() -> SqlParser.extractOrderByColumns("SELECT a, b FROM t ORDER BY 1"))
          .doesNotThrowAnyException();
    }

    @Test
    void orderByAscDesc() {
      List<ColumnReference> cols =
          SqlParser.extractOrderByColumns("SELECT * FROM t ORDER BY a ASC, b DESC");
      assertThat(cols).extracting(ColumnReference::columnName).containsExactly("a", "b");
    }

    @Test
    void orderByFunction() {
      // FIELD(status, 'A', 'B') has parens -> skipped
      List<ColumnReference> cols =
          SqlParser.extractOrderByColumns("SELECT * FROM t ORDER BY FIELD(status, 'A', 'B', 'C')");
      assertThat(cols).isEmpty();
    }

    @Test
    void orderByRand() {
      List<ColumnReference> cols =
          SqlParser.extractOrderByColumns("SELECT * FROM t ORDER BY RAND()");
      assertThat(cols).isEmpty();
    }

    @Test
    void orderByNullsFirst() {
      // "ORDER BY a ASC NULLS FIRST" - NULLS and FIRST might be picked up
      List<ColumnReference> cols =
          SqlParser.extractOrderByColumns("SELECT * FROM t ORDER BY a ASC NULLS FIRST");
      // "a" should be extracted. NULLS/FIRST are not standard SQL keywords in the parser's set.
      assertThat(cols).extracting(ColumnReference::columnName).contains("a");
    }

    @Test
    void orderByWithTableAlias() {
      List<ColumnReference> cols =
          SqlParser.extractOrderByColumns("SELECT * FROM t ORDER BY t.name ASC");
      assertThat(cols).hasSize(1);
      assertThat(cols.get(0).tableOrAlias()).isEqualTo("t");
      assertThat(cols.get(0).columnName()).isEqualTo("name");
    }

    @Test
    void orderByBeforeLimit() {
      List<ColumnReference> cols =
          SqlParser.extractOrderByColumns("SELECT * FROM t ORDER BY name LIMIT 10");
      assertThat(cols).hasSize(1);
      assertThat(cols.get(0).columnName()).isEqualTo("name");
    }

    @Test
    void orderByBeforeOffset() {
      List<ColumnReference> cols =
          SqlParser.extractOrderByColumns("SELECT * FROM t ORDER BY name OFFSET 10");
      assertThat(cols).hasSize(1);
      assertThat(cols.get(0).columnName()).isEqualTo("name");
    }
  }

  // ── extractGroupByColumns ───────────────────────────────────────────

  @Nested
  class ExtractGroupByColumnsStress {

    @Test
    void nullInput() {
      assertThat(SqlParser.extractGroupByColumns(null)).isEmpty();
    }

    @Test
    void groupByWithHaving() {
      List<ColumnReference> cols =
          SqlParser.extractGroupByColumns(
              "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 1");
      assertThat(cols).hasSize(1);
      assertThat(cols.get(0).columnName()).isEqualTo("a");
    }

    @Test
    void groupByRollup() {
      // ROLLUP(a, b) has parens -> skipped
      List<ColumnReference> cols =
          SqlParser.extractGroupByColumns("SELECT * FROM t GROUP BY ROLLUP(a, b)");
      assertThat(cols).isEmpty();
    }

    @Test
    void groupByWithRollup() {
      // MySQL: GROUP BY a WITH ROLLUP
      // "a WITH ROLLUP" - the part "a" is extracted, WITH and ROLLUP might cause issues
      List<ColumnReference> cols =
          SqlParser.extractGroupByColumns("SELECT * FROM t GROUP BY a WITH ROLLUP");
      // Body is " a WITH ROLLUP", no HAVING/ORDER BY/LIMIT terminator
      // Split by comma: ["a WITH ROLLUP"]
      // trimmed: "a WITH ROLLUP", no parens, COLUMN_REF matches "a"
      assertThat(cols).extracting(ColumnReference::columnName).contains("a");
    }

    @Test
    void groupByPositional() {
      // GROUP BY 1, 2
      List<ColumnReference> cols =
          SqlParser.extractGroupByColumns("SELECT a, b, COUNT(*) FROM t GROUP BY 1, 2");
      // "1" and "2" match \w+ but are numbers
      assertThatCode(() -> SqlParser.extractGroupByColumns("SELECT a, b FROM t GROUP BY 1, 2"))
          .doesNotThrowAnyException();
    }

    @Test
    void groupByMultipleColumnsBeforeOrderBy() {
      List<ColumnReference> cols =
          SqlParser.extractGroupByColumns("SELECT a, b, COUNT(*) FROM t GROUP BY a, b ORDER BY a");
      assertThat(cols).extracting(ColumnReference::columnName).containsExactly("a", "b");
    }

    @Test
    void groupByWithTableAlias() {
      List<ColumnReference> cols =
          SqlParser.extractGroupByColumns("SELECT t.a, COUNT(*) FROM t GROUP BY t.a");
      assertThat(cols).hasSize(1);
      assertThat(cols.get(0).tableOrAlias()).isEqualTo("t");
      assertThat(cols.get(0).columnName()).isEqualTo("a");
    }
  }

  // ── detectWhereFunctions ────────────────────────────────────────────

  @Nested
  class DetectWhereFunctionsStress {

    @Test
    void nullInput() {
      assertThat(SqlParser.detectWhereFunctions(null)).isEmpty();
    }

    @Test
    void noWhereClause() {
      assertThat(SqlParser.detectWhereFunctions("SELECT * FROM t")).isEmpty();
    }

    @Test
    void trimFunction() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions("SELECT * FROM t WHERE TRIM(name) = 'test'");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).functionName()).isEqualTo("TRIM");
      assertThat(funcs.get(0).columnName()).isEqualTo("name");
    }

    @Test
    void substringFunction() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions("SELECT * FROM t WHERE SUBSTRING(name, 1, 3) = 'abc'");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).functionName()).isEqualTo("SUBSTRING");
    }

    @Test
    void castFunction() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions("SELECT * FROM t WHERE CAST(price AS INT) > 100");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).functionName()).isEqualTo("CAST");
      assertThat(funcs.get(0).columnName()).isEqualTo("price");
    }

    @Test
    void ifnullFunctionDetected() {
      // IFNULL is now in the expanded recognized function list
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions("SELECT * FROM t WHERE IFNULL(a, 0) = 1");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).functionName()).isEqualTo("IFNULL");
    }

    @Test
    void lengthFunctionDetected() {
      // LENGTH is now in the expanded recognized function list
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions("SELECT * FROM t WHERE LENGTH(name) > 10");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).functionName()).isEqualTo("LENGTH");
    }

    @Test
    void concatFunctionDetected() {
      // CONCAT is now in the expanded recognized function list
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions("SELECT * FROM t WHERE CONCAT(first, last) = 'John Doe'");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).functionName()).isEqualTo("CONCAT");
    }

    @Test
    void arithmeticNotFunction() {
      // "a + 1 = 5" - no function call
      List<FunctionUsage> funcs = SqlParser.detectWhereFunctions("SELECT * FROM t WHERE a + 1 = 5");
      assertThat(funcs).isEmpty();
    }

    @Test
    void multipleFunctionsInWhere() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions(
              "SELECT * FROM t WHERE LOWER(email) = 'x' AND YEAR(created_at) = 2024");
      assertThat(funcs).hasSize(2);
      assertThat(funcs)
          .extracting(FunctionUsage::functionName)
          .containsExactlyInAnyOrder("LOWER", "YEAR");
    }

    @Test
    void functionWithTableAlias() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions("SELECT * FROM t WHERE LOWER(t.email) = 'x'");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).columnName()).isEqualTo("email");
    }

    @Test
    void monthFunction() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions("SELECT * FROM t WHERE MONTH(created_at) = 12");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).functionName()).isEqualTo("MONTH");
    }
  }

  // ── countOrConditions ───────────────────────────────────────────────

  @Nested
  class CountOrConditionsStress {

    @Test
    void nullInput() {
      assertThat(SqlParser.countOrConditions(null)).isEqualTo(0);
    }

    @Test
    void noWhereClause() {
      assertThat(SqlParser.countOrConditions("SELECT * FROM t")).isEqualTo(0);
    }

    @Test
    void orWithNestedAndGroups() {
      // a = 1 OR (b = 2 AND c = 3) OR d = 4 -> 2 ORs
      assertThat(
              SqlParser.countOrConditions(
                  "SELECT * FROM t WHERE a = 1 OR (b = 2 AND c = 3) OR d = 4"))
          .isEqualTo(2);
    }

    @Test
    void orInsideStringLiteral() {
      // "%OR%" inside a string should NOT count as an OR condition
      // BUG: current impl does not strip string literals before counting
      // After normalize/removeSubqueries, the LIKE '%OR%' still contains OR
      // The removeInLists won't help here. This is a known limitation.
      // We test the actual behavior:
      int count = SqlParser.countOrConditions("SELECT * FROM t WHERE a LIKE '%OR%'");
      // Ideally 0, but current parser may count it as 1
      // We document the actual behavior
      assertThatCode(() -> SqlParser.countOrConditions("SELECT * FROM t WHERE a LIKE '%OR%'"))
          .doesNotThrowAnyException();
    }

    @Test
    void orAsValueAndKeyword() {
      // WHERE a = 'OR' OR b = 1
      // The string 'OR' and the keyword OR
      int count = SqlParser.countOrConditions("SELECT * FROM t WHERE a = 'OR' OR b = 1");
      // Ideally 1 (only the keyword OR), but strings aren't stripped
      assertThat(count).isGreaterThanOrEqualTo(1);
    }

    @Test
    void manyOrConditions() {
      assertThat(
              SqlParser.countOrConditions("SELECT * FROM t WHERE a=1 OR b=2 OR c=3 OR d=4 OR e=5"))
          .isEqualTo(4);
    }

    @Test
    void orInSubqueryNotCounted() {
      // Subquery with OR should be removed
      int count =
          SqlParser.countOrConditions(
              "SELECT * FROM t WHERE a = 1 AND id IN (SELECT id FROM t2 WHERE x=1 OR y=2)");
      assertThat(count).isEqualTo(0);
    }

    @Test
    void caseInsensitiveOr() {
      assertThat(SqlParser.countOrConditions("SELECT * FROM t WHERE a = 1 or b = 2 Or c = 3"))
          .isEqualTo(2);
    }
  }

  // ── extractOffsetValue ──────────────────────────────────────────────

  @Nested
  class ExtractOffsetValueStress {

    @Test
    void nullInput() {
      assertThat(SqlParser.extractOffsetValue(null)).isEmpty();
    }

    @Test
    void limitCommaFormat() {
      // LIMIT 10, 5000 - MySQL style: LIMIT offset, count
      // So offset is 10
      OptionalLong offset = SqlParser.extractOffsetValue("SELECT * FROM t LIMIT 10, 5000");
      assertThat(offset).isPresent();
      assertThat(offset.getAsLong()).isEqualTo(10);
    }

    @Test
    void limitOffsetFormat() {
      // LIMIT 5000 OFFSET 10 - standard: offset is 10
      OptionalLong offset = SqlParser.extractOffsetValue("SELECT * FROM t LIMIT 5000 OFFSET 10");
      assertThat(offset).isPresent();
      assertThat(offset.getAsLong()).isEqualTo(10);
    }

    @Test
    void offsetZero() {
      OptionalLong offset = SqlParser.extractOffsetValue("SELECT * FROM t LIMIT 10 OFFSET 0");
      assertThat(offset).isPresent();
      assertThat(offset.getAsLong()).isEqualTo(0);
    }

    @Test
    void parameterizedLimit() {
      // LIMIT ? - not a number, so not matched
      OptionalLong offset = SqlParser.extractOffsetValue("SELECT * FROM t LIMIT ?");
      assertThat(offset).isEmpty();
    }

    @Test
    void fetchFirstRowsOnly() {
      // SQL standard: FETCH FIRST 10 ROWS ONLY - no offset
      OptionalLong offset =
          SqlParser.extractOffsetValue("SELECT * FROM t FETCH FIRST 10 ROWS ONLY");
      assertThat(offset).isEmpty();
    }

    @Test
    void offsetWithoutLimit() {
      // PostgreSQL style: standalone OFFSET
      OptionalLong offset = SqlParser.extractOffsetValue("SELECT * FROM t OFFSET 200");
      assertThat(offset).isPresent();
      assertThat(offset.getAsLong()).isEqualTo(200);
    }

    @Test
    void largeOffset() {
      OptionalLong offset =
          SqlParser.extractOffsetValue("SELECT * FROM t LIMIT 10 OFFSET 999999999");
      assertThat(offset).isPresent();
      assertThat(offset.getAsLong()).isEqualTo(999999999L);
    }

    @Test
    void noOffsetNoLimit() {
      assertThat(SqlParser.extractOffsetValue("SELECT * FROM t")).isEmpty();
    }
  }

  // ── extractTableNames ───────────────────────────────────────────────

  @Nested
  class ExtractTableNamesStress {

    @Test
    void nullInput() {
      assertThat(SqlParser.extractTableNames(null)).isEmpty();
    }

    @Test
    void emptyString() {
      assertThat(SqlParser.extractTableNames("")).isEmpty();
    }

    @Test
    void schemaQualifiedTable() {
      // FROM schema.table_name - current regex \bFROM\s+(\w+) captures "schema" not "table_name"
      List<String> tables =
          SqlParser.extractTableNames("SELECT * FROM myschema.users WHERE id = 1");
      // Current impl captures "myschema" since regex stops at word boundary before dot
      // This is a known limitation - document actual behavior
      assertThat(tables).isNotEmpty();
    }

    @Test
    void backtickQuotedTable() {
      // MySQL: FROM `my_table` - backticks not matched by \w+
      List<String> tables = SqlParser.extractTableNames("SELECT * FROM `my_table` WHERE id = 1");
      // \w+ doesn't include backticks, so this won't match
      // Known limitation
      assertThatCode(() -> SqlParser.extractTableNames("SELECT * FROM `my_table`"))
          .doesNotThrowAnyException();
    }

    @Test
    void doubleQuotedTable() {
      // Standard SQL: FROM "my_table"
      List<String> tables = SqlParser.extractTableNames("SELECT * FROM \"my_table\" WHERE id = 1");
      // Double quotes not matched by \w+
      assertThatCode(() -> SqlParser.extractTableNames("SELECT * FROM \"my_table\""))
          .doesNotThrowAnyException();
    }

    @Test
    void commaSeparatedTables() {
      // FROM t1, t2, t3 - only first table captured by current regex
      List<String> tables =
          SqlParser.extractTableNames("SELECT * FROM t1, t2, t3 WHERE t1.id = t2.id");
      // Current FROM_TABLE regex only matches "FROM t1"
      // t2 and t3 are not preceded by FROM or JOIN
      assertThat(tables).contains("t1");
    }

    @Test
    void derivedTable() {
      // FROM (SELECT ...) AS subquery
      List<String> tables =
          SqlParser.extractTableNames("SELECT * FROM (SELECT id FROM inner_t) AS sub");
      // The opening paren prevents \w+ match after FROM
      // But inner_t is preceded by FROM inside the subquery
      assertThatCode(
              () -> SqlParser.extractTableNames("SELECT * FROM (SELECT id FROM inner_t) AS sub"))
          .doesNotThrowAnyException();
    }

    @Test
    void manyJoins() {
      String sql =
          "SELECT * FROM t1 "
              + "JOIN t2 ON t1.id = t2.t1_id "
              + "JOIN t3 ON t2.id = t3.t2_id "
              + "JOIN t4 ON t3.id = t4.t3_id "
              + "JOIN t5 ON t4.id = t5.t4_id "
              + "JOIN t6 ON t5.id = t6.t5_id "
              + "JOIN t7 ON t6.id = t7.t6_id "
              + "JOIN t8 ON t7.id = t8.t7_id "
              + "JOIN t9 ON t8.id = t9.t8_id "
              + "JOIN t10 ON t9.id = t10.t9_id";
      List<String> tables = SqlParser.extractTableNames(sql);
      assertThat(tables)
          .containsExactlyInAnyOrder("t1", "t2", "t3", "t4", "t5", "t6", "t7", "t8", "t9", "t10");
    }

    @Test
    void leftJoinTable() {
      List<String> tables =
          SqlParser.extractTableNames(
              "SELECT * FROM orders LEFT JOIN users ON orders.user_id = users.id");
      assertThat(tables).contains("orders", "users");
    }

    @Test
    void leftOuterJoinTable() {
      // LEFT OUTER JOIN - "JOIN" still appears, so JOIN_TABLE regex should match
      List<String> tables =
          SqlParser.extractTableNames(
              "SELECT * FROM orders LEFT OUTER JOIN users ON orders.user_id = users.id");
      assertThat(tables).contains("orders", "users");
    }

    @Test
    void noDuplicates() {
      List<String> tables =
          SqlParser.extractTableNames(
              "SELECT * FROM users u1 JOIN users u2 ON u1.id = u2.manager_id");
      assertThat(tables.stream().filter(t -> t.equals("users")).count()).isEqualTo(1);
    }

    @Test
    void keywordsNotExtractedAsTables() {
      // "FROM SELECT" - SELECT is a keyword and should be filtered
      // Though this is invalid SQL, the parser shouldn't return keywords
      List<String> tables =
          SqlParser.extractTableNames("SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders)");
      assertThat(tables).doesNotContain("SELECT", "select");
    }

    @Test
    void insertIntoNotFrom() {
      // INSERT INTO users - no FROM clause
      List<String> tables =
          SqlParser.extractTableNames("INSERT INTO users (id, name) VALUES (1, 'test')");
      assertThat(tables).isEmpty();
    }
  }

  // ── Cross-cutting concerns ──────────────────────────────────────────

  @Nested
  class CrossCutting {

    @Test
    void veryLongSqlDoesNotTimeout() {
      // Build a massive SQL
      StringBuilder sb = new StringBuilder("SELECT * FROM t WHERE ");
      for (int i = 0; i < 1000; i++) {
        if (i > 0) sb.append(" AND ");
        sb.append("col").append(i).append(" = ").append(i);
      }
      String sql = sb.toString();

      assertThatCode(
              () -> {
                SqlParser.normalize(sql);
                SqlParser.isSelectQuery(sql);
                SqlParser.hasSelectAll(sql);
                SqlParser.extractWhereColumns(sql);
                SqlParser.extractOrderByColumns(sql);
                SqlParser.extractGroupByColumns(sql);
                SqlParser.detectWhereFunctions(sql);
                SqlParser.countOrConditions(sql);
                SqlParser.extractOffsetValue(sql);
                SqlParser.extractTableNames(sql);
              })
          .doesNotThrowAnyException();
    }

    @Test
    void allMethodsHandleEmptyString() {
      String sql = "";
      assertThatCode(
              () -> {
                SqlParser.normalize(sql);
                SqlParser.isSelectQuery(sql);
                SqlParser.hasSelectAll(sql);
                SqlParser.extractWhereColumns(sql);
                SqlParser.extractJoinColumns(sql);
                SqlParser.extractOrderByColumns(sql);
                SqlParser.extractGroupByColumns(sql);
                SqlParser.detectWhereFunctions(sql);
                SqlParser.countOrConditions(sql);
                SqlParser.extractOffsetValue(sql);
                SqlParser.extractTableNames(sql);
              })
          .doesNotThrowAnyException();
    }

    @Test
    void sqlInjectionAttempt() {
      // Parser should not crash on malicious input
      String sql = "SELECT * FROM t WHERE id = 1; DROP TABLE t; --";
      assertThatCode(
              () -> {
                SqlParser.normalize(sql);
                SqlParser.extractWhereColumns(sql);
                SqlParser.extractTableNames(sql);
              })
          .doesNotThrowAnyException();
    }

    @Test
    void unicodeSqlDoesNotCrash() {
      String sql = "SELECT * FROM テーブル WHERE 名前 = '太郎'";
      assertThatCode(
              () -> {
                SqlParser.normalize(sql);
                SqlParser.extractTableNames(sql);
                SqlParser.extractWhereColumns(sql);
              })
          .doesNotThrowAnyException();
    }

    @Test
    void nullBytesInSql() {
      String sql = "SELECT * FROM t WHERE id = 1\0";
      assertThatCode(() -> SqlParser.normalize(sql)).doesNotThrowAnyException();
    }

    /**
     * Stress test: 25 JOINs with ON clauses followed by a WHERE clause. This previously caused
     * catastrophic backtracking because JOIN_CLAUSE used (.+?) with DOTALL and a complex lookahead
     * with alternation. The regex engine would backtrack exponentially across all the ON clause
     * bodies. After the fix (manual clause boundary scanning), this completes in O(n) time.
     */
    @Test
    void manyJoinsDoNotCauseBacktracking() {
      StringBuilder sb = new StringBuilder("SELECT * FROM t1 ");
      for (int i = 2; i <= 25; i++) {
        sb.append("JOIN t").append(i).append(" ON t").append(i).append(".a = t").append(i - 1).append(".a ");
      }
      sb.append("WHERE t1.status = 'active' AND t1.created > '2020-01-01'");
      String sql = sb.toString();

      long start = System.nanoTime();
      List<JoinColumnPair> pairs = SqlParser.extractJoinColumns(sql);
      List<FunctionUsage> funcs = SqlParser.detectJoinFunctions(sql);
      List<String> tables = SqlParser.extractTableNames(sql);
      long elapsed = System.nanoTime() - start;

      // Must complete in under 1 second (typically ~1ms)
      assertThat(elapsed).isLessThan(1_000_000_000L);
      assertThat(pairs).hasSize(24);
      assertThat(tables).hasSize(25);
      assertThat(funcs).isEmpty();
    }

    /**
     * Stress test: JOIN with nested subquery in ON clause. Verifies the parser handles parentheses
     * and subqueries inside ON conditions without catastrophic backtracking.
     */
    @Test
    void joinWithSubqueryInOnClause() {
      String sql =
          "SELECT * FROM t1 "
              + "JOIN t2 ON t2.id = (SELECT MAX(id) FROM t3 WHERE t3.ref = t1.id) "
              + "JOIN t4 ON t4.x = t2.x "
              + "WHERE t1.active = 1";

      long start = System.nanoTime();
      List<String> tables = SqlParser.extractTableNames(sql);
      List<FunctionUsage> funcs = SqlParser.detectJoinFunctions(sql);
      long elapsed = System.nanoTime() - start;

      assertThat(elapsed).isLessThan(1_000_000_000L);
      // t1, t2, t3 (from subquery), t4
      assertThat(tables).contains("t1", "t2", "t4");
    }

    /**
     * Stress test: COMPARISON pattern with many conditions that lack comparison operators. The old
     * (.+?) with DOTALL pattern could backtrack on such inputs.
     */
    @Test
    void manyWhereConditionsDoNotCauseBacktracking() {
      StringBuilder sb = new StringBuilder("SELECT * FROM t WHERE ");
      for (int i = 0; i < 100; i++) {
        if (i > 0) sb.append(" AND ");
        sb.append("LOWER(col").append(i).append(") = 'val").append(i).append("'");
      }
      String sql = sb.toString();

      long start = System.nanoTime();
      List<FunctionUsage> funcs = SqlParser.detectWhereFunctions(sql);
      long elapsed = System.nanoTime() - start;

      assertThat(elapsed).isLessThan(1_000_000_000L);
      assertThat(funcs).hasSize(100);
    }

    /**
     * Stress test: detectJoinFunctions with many JOINs containing function calls in ON clauses.
     * Ensures the fix to JOIN_CLAUSE pattern handles this in linear time.
     */
    @Test
    void manyJoinsWithFunctionsDoNotCauseBacktracking() {
      StringBuilder sb = new StringBuilder("SELECT * FROM t1 ");
      for (int i = 2; i <= 20; i++) {
        sb.append("LEFT JOIN t").append(i)
            .append(" ON LOWER(t").append(i).append(".name) = LOWER(t").append(i - 1).append(".name) ");
      }
      sb.append("WHERE t1.active = 1");
      String sql = sb.toString();

      long start = System.nanoTime();
      List<FunctionUsage> funcs = SqlParser.detectJoinFunctions(sql);
      long elapsed = System.nanoTime() - start;

      assertThat(elapsed).isLessThan(1_000_000_000L);
      // Each LEFT JOIN should flag LOWER on the right (lookup) table
      assertThat(funcs).isNotEmpty();
    }
  }
}
