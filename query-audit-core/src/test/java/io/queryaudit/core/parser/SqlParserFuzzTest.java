package io.queryaudit.core.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Exhaustive fuzz-like tests for SqlParser. Ensures the core parsing engine NEVER throws, hangs, or
 * returns incorrect results on any input - including adversarial, malformed, and boundary inputs.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class SqlParserFuzzTest {

  // ── Shared adversarial inputs ──────────────────────────────────────

  private static final String WHITESPACE_ONLY = "   \t\n\r  ";
  private static final String SPECIAL_CHARS = "!@#$%^&*()";
  private static final String KEYWORDS_ONLY = "SELECT FROM WHERE";
  private static final String NULL_BYTES = "SELECT\0 * FROM\0 users";
  private static final String TABS_NEWLINES = "SELECT\t*\nFROM\rusers\r\nWHERE\tid = 1";

  private static String repeat(String s, int count) {
    StringBuilder sb = new StringBuilder(s.length() * count);
    for (int i = 0; i < count; i++) sb.append(s);
    return sb.toString();
  }

  static String nestedParens(int depth) {
    return repeat("(", depth) + "x" + repeat(")", depth);
  }

  static String deepSubquery(int depth) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < depth; i++) {
      sb.append("SELECT * FROM (");
    }
    sb.append("SELECT 1 FROM dual");
    for (int i = 0; i < depth; i++) {
      sb.append(") t").append(i);
    }
    return sb.toString();
  }

  private static final String FULL_SQL =
      "SELECT u.id, u.name FROM users u "
          + "INNER JOIN orders o ON u.id = o.user_id "
          + "LEFT JOIN payments p ON o.id = p.order_id "
          + "WHERE u.active = 1 AND o.status = 'completed' OR u.role = 'admin' "
          + "GROUP BY u.id, u.name "
          + "HAVING COUNT(o.id) > 5 "
          + "ORDER BY u.name ASC "
          + "LIMIT 10 OFFSET 20";

  private static final String LARGE_100KB;

  static {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT * FROM users WHERE ");
    while (sb.length() < 100_000) {
      sb.append("col").append(sb.length()).append(" = 1 AND ");
    }
    sb.append("id = 1");
    LARGE_100KB = sb.toString();
  }

  // ── normalize ──────────────────────────────────────────────────────

  @Nested
  class NormalizeFuzz {

    @Test
    void nullReturnsNull() {
      assertThat(SqlParser.normalize(null)).isNull();
    }

    @Test
    void emptyReturnsEmpty() {
      assertThat(SqlParser.normalize("")).isEqualTo("");
    }

    @Test
    void whitespaceOnly() {
      assertThat(SqlParser.normalize(WHITESPACE_ONLY)).isEqualTo("");
    }

    @Test
    void specialCharsNoThrow() {
      assertThatCode(() -> SqlParser.normalize(SPECIAL_CHARS)).doesNotThrowAnyException();
    }

    @Test
    void keywordsOnly() {
      assertThatCode(() -> SqlParser.normalize(KEYWORDS_ONLY)).doesNotThrowAnyException();
    }

    @Test
    void largeInput() {
      assertThatCode(() -> SqlParser.normalize(LARGE_100KB)).doesNotThrowAnyException();
    }

    @Test
    void nestedParens100() {
      String input = "SELECT * FROM t WHERE " + nestedParens(100);
      assertThatCode(() -> SqlParser.normalize(input)).doesNotThrowAnyException();
    }

    @Test
    void regexHostileOpenParens() {
      assertThatCode(() -> SqlParser.normalize("((((((((((")).doesNotThrowAnyException();
    }

    @Test
    void regexHostileCloseParens() {
      assertThatCode(() -> SqlParser.normalize("))))))))))))")).doesNotThrowAnyException();
    }

    @Test
    void regexHostilePatterns() {
      assertThatCode(() -> SqlParser.normalize(".*+?{}[]|\\")).doesNotThrowAnyException();
    }

    @Test
    void fullSql() {
      String result = SqlParser.normalize(FULL_SQL);
      assertThat(result).isNotNull();
      assertThat(result).isEqualTo(result.toLowerCase());
    }

    @Test
    void nullBytes() {
      assertThatCode(() -> SqlParser.normalize(NULL_BYTES)).doesNotThrowAnyException();
    }

    @Test
    void tabsNewlines() {
      String result = SqlParser.normalize(TABS_NEWLINES);
      assertThat(result).doesNotContain("\t");
      assertThat(result).doesNotContain("\n");
      assertThat(result).doesNotContain("\r");
    }

    // Property: output is always lowercase
    @Test
    void outputAlwaysLowercase() {
      String[] inputs = {
        "SELECT * FROM USERS", "SeLeCt FrOm WhErE",
        "INSERT INTO Foo", "UPDATE Bar SET X = 1"
      };
      for (String input : inputs) {
        String result = SqlParser.normalize(input);
        assertThat(result).isEqualTo(result.toLowerCase());
      }
    }

    // Property: idempotent - normalizing twice gives same result
    @Test
    void idempotent() {
      String[] inputs = {
        "SELECT * FROM users WHERE id = 42",
        "INSERT INTO t VALUES ('hello', 123)",
        FULL_SQL,
        SPECIAL_CHARS,
        ""
      };
      for (String input : inputs) {
        String once = SqlParser.normalize(input);
        String twice = SqlParser.normalize(once);
        assertThat(twice).isEqualTo(once);
      }
    }

    @Test
    void unterminatedSingleQuote() {
      assertThatCode(() -> SqlParser.normalize("SELECT 'unterminated")).doesNotThrowAnyException();
    }

    @Test
    void unterminatedDoubleQuote() {
      assertThatCode(() -> SqlParser.normalize("SELECT \"unterminated")).doesNotThrowAnyException();
    }

    @Test
    void manyEscapedQuotes() {
      String input = "SELECT * FROM t WHERE x = '" + repeat("\\'", 500) + "'";
      assertThatCode(() -> SqlParser.normalize(input)).doesNotThrowAnyException();
    }

    @Test
    void sqlStandardEscapedQuotes() {
      String input = "SELECT * FROM t WHERE x = '" + repeat("''", 500) + "'";
      assertThatCode(() -> SqlParser.normalize(input)).doesNotThrowAnyException();
    }

    @Test
    void replacesNumbers() {
      String result = SqlParser.normalize("SELECT * FROM t WHERE id = 42 AND price = 3.14");
      assertThat(result).contains("id = ?");
      assertThat(result).contains("price = ?");
    }

    @Test
    void collapsesInList() {
      String result = SqlParser.normalize("SELECT * FROM t WHERE id IN (1, 2, 3)");
      assertThat(result).contains("in (?)");
    }

    @Test
    void hexLiterals() {
      String result = SqlParser.normalize("SELECT * FROM t WHERE id = 0xDEADBEEF");
      assertThat(result).contains("id = ?");
    }

    @Test
    void scientificNotation() {
      String result = SqlParser.normalize("SELECT * FROM t WHERE val = 1.5e10");
      assertThat(result).contains("val = ?");
    }
  }

  // ── isSelectQuery ──────────────────────────────────────────────────

  @Nested
  class IsSelectQueryFuzz {

    @Test
    void nullReturnsFalse() {
      assertThat(SqlParser.isSelectQuery(null)).isFalse();
    }

    @Test
    void emptyReturnsFalse() {
      assertThat(SqlParser.isSelectQuery("")).isFalse();
    }

    @Test
    void whitespaceOnly() {
      assertThat(SqlParser.isSelectQuery(WHITESPACE_ONLY)).isFalse();
    }

    @Test
    void specialChars() {
      assertThat(SqlParser.isSelectQuery(SPECIAL_CHARS)).isFalse();
    }

    @Test
    void startsWithSelect() {
      assertThat(SqlParser.isSelectQuery("SELECT 1")).isTrue();
    }

    @Test
    void caseInsensitive() {
      assertThat(SqlParser.isSelectQuery("select 1")).isTrue();
      assertThat(SqlParser.isSelectQuery("SeLeCt 1")).isTrue();
    }

    @Test
    void leadingWhitespace() {
      assertThat(SqlParser.isSelectQuery("   SELECT 1")).isTrue();
      assertThat(SqlParser.isSelectQuery("\n\tSELECT 1")).isTrue();
    }

    @Test
    void notSelect() {
      assertThat(SqlParser.isSelectQuery("INSERT INTO t VALUES (1)")).isFalse();
      assertThat(SqlParser.isSelectQuery("UPDATE t SET x = 1")).isFalse();
      assertThat(SqlParser.isSelectQuery("DELETE FROM t")).isFalse();
    }

    @Test
    void largeInput() {
      assertThatCode(() -> SqlParser.isSelectQuery(LARGE_100KB)).doesNotThrowAnyException();
    }

    @Test
    void regexHostile() {
      assertThat(SqlParser.isSelectQuery("((((((((((")).isFalse();
      assertThat(SqlParser.isSelectQuery(".*+?{}[]|\\")).isFalse();
    }

    @Test
    void nullBytes() {
      assertThatCode(() -> SqlParser.isSelectQuery(NULL_BYTES)).doesNotThrowAnyException();
    }

    @Test
    void selectEmbeddedNotAtStart() {
      assertThat(SqlParser.isSelectQuery("NOT A SELECT")).isFalse();
    }
  }

  // ── isInsertQuery / isUpdateQuery / isDeleteQuery / isDmlQuery ──────

  @Nested
  class DmlQueryTypeFuzz {

    @Test
    void nullReturnsFalse() {
      assertThat(SqlParser.isInsertQuery(null)).isFalse();
      assertThat(SqlParser.isUpdateQuery(null)).isFalse();
      assertThat(SqlParser.isDeleteQuery(null)).isFalse();
      assertThat(SqlParser.isDmlQuery(null)).isFalse();
    }

    @Test
    void emptyReturnsFalse() {
      assertThat(SqlParser.isInsertQuery("")).isFalse();
      assertThat(SqlParser.isUpdateQuery("")).isFalse();
      assertThat(SqlParser.isDeleteQuery("")).isFalse();
      assertThat(SqlParser.isDmlQuery("")).isFalse();
    }

    @Test
    void whitespaceOnly() {
      assertThat(SqlParser.isInsertQuery(WHITESPACE_ONLY)).isFalse();
      assertThat(SqlParser.isUpdateQuery(WHITESPACE_ONLY)).isFalse();
      assertThat(SqlParser.isDeleteQuery(WHITESPACE_ONLY)).isFalse();
      assertThat(SqlParser.isDmlQuery(WHITESPACE_ONLY)).isFalse();
    }

    @Test
    void specialCharsNoThrow() {
      assertThatCode(() -> SqlParser.isInsertQuery(SPECIAL_CHARS)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.isUpdateQuery(SPECIAL_CHARS)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.isDeleteQuery(SPECIAL_CHARS)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.isDmlQuery(SPECIAL_CHARS)).doesNotThrowAnyException();
    }

    @Test
    void correctDetection() {
      assertThat(SqlParser.isInsertQuery("INSERT INTO t VALUES (1)")).isTrue();
      assertThat(SqlParser.isUpdateQuery("UPDATE t SET x = 1")).isTrue();
      assertThat(SqlParser.isDeleteQuery("DELETE FROM t WHERE id = 1")).isTrue();
    }

    @Test
    void caseInsensitive() {
      assertThat(SqlParser.isInsertQuery("insert into t values (1)")).isTrue();
      assertThat(SqlParser.isUpdateQuery("update t set x = 1")).isTrue();
      assertThat(SqlParser.isDeleteQuery("delete from t where id = 1")).isTrue();
    }

    @Test
    void isDmlAggregates() {
      assertThat(SqlParser.isDmlQuery("INSERT INTO t VALUES (1)")).isTrue();
      assertThat(SqlParser.isDmlQuery("UPDATE t SET x = 1")).isTrue();
      assertThat(SqlParser.isDmlQuery("DELETE FROM t")).isTrue();
      assertThat(SqlParser.isDmlQuery("SELECT * FROM t")).isFalse();
    }

    @Test
    void leadingWhitespace() {
      assertThat(SqlParser.isInsertQuery("  \n INSERT INTO t VALUES (1)")).isTrue();
      assertThat(SqlParser.isUpdateQuery("  \t UPDATE t SET x = 1")).isTrue();
      assertThat(SqlParser.isDeleteQuery("  \r\n DELETE FROM t")).isTrue();
    }

    @Test
    void largeInput() {
      assertThatCode(() -> SqlParser.isInsertQuery(LARGE_100KB)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.isUpdateQuery(LARGE_100KB)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.isDeleteQuery(LARGE_100KB)).doesNotThrowAnyException();
    }

    @Test
    void regexHostile() {
      assertThat(SqlParser.isInsertQuery(".*+?{}[]|\\")).isFalse();
      assertThat(SqlParser.isUpdateQuery("((((((((((")).isFalse();
      assertThat(SqlParser.isDeleteQuery("))))))))))))")).isFalse();
    }

    @Test
    void nullBytesInInput() {
      assertThatCode(() -> SqlParser.isDmlQuery(NULL_BYTES)).doesNotThrowAnyException();
    }
  }

  // ── hasSelectAll ───────────────────────────────────────────────────

  @Nested
  class HasSelectAllFuzz {

    @Test
    void nullReturnsFalse() {
      assertThat(SqlParser.hasSelectAll(null)).isFalse();
    }

    @Test
    void emptyReturnsFalse() {
      assertThat(SqlParser.hasSelectAll("")).isFalse();
    }

    @Test
    void whitespaceOnly() {
      assertThat(SqlParser.hasSelectAll(WHITESPACE_ONLY)).isFalse();
    }

    @Test
    void specialChars() {
      assertThat(SqlParser.hasSelectAll(SPECIAL_CHARS)).isFalse();
    }

    @Test
    void detectsSelectStar() {
      assertThat(SqlParser.hasSelectAll("SELECT * FROM users")).isTrue();
    }

    @Test
    void detectsTableQualifiedStar() {
      assertThat(SqlParser.hasSelectAll("SELECT u.* FROM users u")).isTrue();
    }

    @Test
    void doesNotMatchCountStar() {
      assertThat(SqlParser.hasSelectAll("SELECT COUNT(*) FROM users")).isFalse();
    }

    @Test
    void doesNotMatchExistsSelectStar() {
      assertThat(SqlParser.hasSelectAll("SELECT 1 FROM t WHERE EXISTS (SELECT * FROM u)"))
          .isFalse();
    }

    @Test
    void largeInput() {
      assertThatCode(() -> SqlParser.hasSelectAll(LARGE_100KB)).doesNotThrowAnyException();
    }

    @Test
    void regexHostile() {
      assertThatCode(() -> SqlParser.hasSelectAll(".*+?{}[]|\\")).doesNotThrowAnyException();
    }

    @Test
    void nullBytes() {
      assertThatCode(() -> SqlParser.hasSelectAll(NULL_BYTES)).doesNotThrowAnyException();
    }

    @Test
    void keywordsOnly() {
      assertThatCode(() -> SqlParser.hasSelectAll(KEYWORDS_ONLY)).doesNotThrowAnyException();
    }
  }

  // ── hasWhereClause ─────────────────────────────────────────────────

  @Nested
  class HasWhereClauseFuzz {

    @Test
    void nullReturnsFalse() {
      assertThat(SqlParser.hasWhereClause(null)).isFalse();
    }

    @Test
    void emptyReturnsFalse() {
      assertThat(SqlParser.hasWhereClause("")).isFalse();
    }

    @Test
    void whitespaceOnly() {
      assertThat(SqlParser.hasWhereClause(WHITESPACE_ONLY)).isFalse();
    }

    @Test
    void specialChars() {
      assertThat(SqlParser.hasWhereClause(SPECIAL_CHARS)).isFalse();
    }

    @Test
    void detectsWhere() {
      assertThat(SqlParser.hasWhereClause("SELECT * FROM t WHERE id = 1")).isTrue();
    }

    @Test
    void caseInsensitive() {
      assertThat(SqlParser.hasWhereClause("SELECT * FROM t where id = 1")).isTrue();
    }

    @Test
    void noWhere() {
      assertThat(SqlParser.hasWhereClause("SELECT * FROM t")).isFalse();
    }

    @Test
    void largeInput() {
      assertThatCode(() -> SqlParser.hasWhereClause(LARGE_100KB)).doesNotThrowAnyException();
    }

    @Test
    void regexHostile() {
      assertThatCode(() -> SqlParser.hasWhereClause("((((((((((")).doesNotThrowAnyException();
    }
  }

  // ── extractWhereBody ───────────────────────────────────────────────

  @Nested
  class ExtractWhereBodyFuzz {

    @Test
    void nullReturnsNull() {
      assertThat(SqlParser.extractWhereBody(null)).isNull();
    }

    @Test
    void emptyReturnsNull() {
      assertThat(SqlParser.extractWhereBody("")).isNull();
    }

    @Test
    void whitespaceOnly() {
      assertThat(SqlParser.extractWhereBody(WHITESPACE_ONLY)).isNull();
    }

    @Test
    void specialChars() {
      assertThat(SqlParser.extractWhereBody(SPECIAL_CHARS)).isNull();
    }

    @Test
    void noWhere() {
      assertThat(SqlParser.extractWhereBody("SELECT * FROM t")).isNull();
    }

    @Test
    void simpleWhere() {
      String result = SqlParser.extractWhereBody("SELECT * FROM t WHERE id = 1");
      assertThat(result).isNotNull();
      assertThat(result).contains("id = 1");
    }

    @Test
    void whereTerminatedByGroupBy() {
      String result = SqlParser.extractWhereBody("SELECT * FROM t WHERE id = 1 GROUP BY name");
      assertThat(result).isNotNull();
      assertThat(result).doesNotContain("GROUP BY");
    }

    @Test
    void whereTerminatedByOrderBy() {
      String result = SqlParser.extractWhereBody("SELECT * FROM t WHERE id = 1 ORDER BY name");
      assertThat(result).isNotNull();
      assertThat(result).doesNotContain("ORDER BY");
    }

    @Test
    void whereTerminatedByLimit() {
      String result = SqlParser.extractWhereBody("SELECT * FROM t WHERE id = 1 LIMIT 10");
      assertThat(result).isNotNull();
      assertThat(result).doesNotContain("LIMIT");
    }

    @Test
    void fullSql() {
      String result = SqlParser.extractWhereBody(FULL_SQL);
      assertThat(result).isNotNull();
    }

    @Test
    void largeInput() {
      assertThatCode(() -> SqlParser.extractWhereBody(LARGE_100KB)).doesNotThrowAnyException();
    }

    @Test
    void regexHostile() {
      assertThatCode(() -> SqlParser.extractWhereBody(".*+?{}[]|\\")).doesNotThrowAnyException();
    }

    @Test
    void deeplyNestedParens() {
      String input = "SELECT * FROM t WHERE " + nestedParens(100);
      assertThatCode(() -> SqlParser.extractWhereBody(input)).doesNotThrowAnyException();
    }

    @Test
    void keywordsOnly() {
      // WHERE keyword found but no real body after terminators
      assertThatCode(() -> SqlParser.extractWhereBody(KEYWORDS_ONLY)).doesNotThrowAnyException();
    }

    @Test
    void nullBytes() {
      assertThatCode(() -> SqlParser.extractWhereBody(NULL_BYTES)).doesNotThrowAnyException();
    }
  }

  // ── extractWhereColumns ────────────────────────────────────────────

  @Nested
  class ExtractWhereColumnsFuzz {

    @Test
    void nullReturnsEmpty() {
      assertThat(SqlParser.extractWhereColumns(null)).isEmpty();
    }

    @Test
    void emptyReturnsEmpty() {
      assertThat(SqlParser.extractWhereColumns("")).isEmpty();
    }

    @Test
    void whitespaceOnly() {
      assertThat(SqlParser.extractWhereColumns(WHITESPACE_ONLY)).isEmpty();
    }

    @Test
    void specialChars() {
      assertThat(SqlParser.extractWhereColumns(SPECIAL_CHARS)).isEmpty();
    }

    @Test
    void noWhere() {
      assertThat(SqlParser.extractWhereColumns("SELECT * FROM t")).isEmpty();
    }

    @Test
    void simpleEquality() {
      List<ColumnReference> cols = SqlParser.extractWhereColumns("SELECT * FROM t WHERE id = 1");
      assertThat(cols).isNotEmpty();
      assertThat(cols.get(0).columnName()).isEqualTo("id");
    }

    @Test
    void resultNeverContainsSqlKeywords() {
      List<ColumnReference> cols = SqlParser.extractWhereColumns(FULL_SQL);
      for (ColumnReference col : cols) {
        String name = col.columnName().toLowerCase();
        assertThat(name)
            .isNotIn(
                "select", "from", "where", "and", "or", "not", "in", "is", "null", "between",
                "like", "join", "on", "order", "by", "group", "having", "limit", "offset");
      }
    }

    @Test
    void largeInput() {
      assertThatCode(() -> SqlParser.extractWhereColumns(LARGE_100KB)).doesNotThrowAnyException();
    }

    @Test
    void regexHostile() {
      assertThatCode(() -> SqlParser.extractWhereColumns("((((((((((")).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractWhereColumns(".*+?{}[]|\\")).doesNotThrowAnyException();
    }

    @Test
    void nestedParensInput() {
      String input = "SELECT * FROM t WHERE " + SqlParserFuzzTest.nestedParens(100);
      assertThatCode(() -> SqlParser.extractWhereColumns(input)).doesNotThrowAnyException();
    }

    @Test
    void nullBytes() {
      assertThatCode(() -> SqlParser.extractWhereColumns(NULL_BYTES)).doesNotThrowAnyException();
    }

    @Test
    void keywordsOnly() {
      assertThat(SqlParser.extractWhereColumns(KEYWORDS_ONLY)).isEmpty();
    }
  }

  // ── extractWhereColumnsWithOperators ────────────────────────────────

  @Nested
  class ExtractWhereColumnsWithOperatorsFuzz {

    @Test
    void nullReturnsEmpty() {
      assertThat(SqlParser.extractWhereColumnsWithOperators(null)).isEmpty();
    }

    @Test
    void emptyReturnsEmpty() {
      assertThat(SqlParser.extractWhereColumnsWithOperators("")).isEmpty();
    }

    @Test
    void whitespaceOnly() {
      assertThat(SqlParser.extractWhereColumnsWithOperators(WHITESPACE_ONLY)).isEmpty();
    }

    @Test
    void specialChars() {
      assertThat(SqlParser.extractWhereColumnsWithOperators(SPECIAL_CHARS)).isEmpty();
    }

    @Test
    void multipleOperators() {
      String sql =
          "SELECT * FROM t WHERE a = 1 AND b != 2 AND c LIKE '%foo' AND d IS NULL AND e IN (1,2)";
      List<WhereColumnReference> cols = SqlParser.extractWhereColumnsWithOperators(sql);
      assertThat(cols).isNotEmpty();
      for (WhereColumnReference ref : cols) {
        assertThat(ref.operator()).isNotNull();
        assertThat(ref.columnName()).isNotNull();
      }
    }

    @Test
    void largeInput() {
      assertThatCode(() -> SqlParser.extractWhereColumnsWithOperators(LARGE_100KB))
          .doesNotThrowAnyException();
    }

    @Test
    void regexHostile() {
      assertThatCode(() -> SqlParser.extractWhereColumnsWithOperators(".*+?{}[]|\\"))
          .doesNotThrowAnyException();
    }

    @Test
    void fullSql() {
      List<WhereColumnReference> cols = SqlParser.extractWhereColumnsWithOperators(FULL_SQL);
      assertThat(cols).isNotEmpty();
    }

    @Test
    void nullBytes() {
      assertThatCode(() -> SqlParser.extractWhereColumnsWithOperators(NULL_BYTES))
          .doesNotThrowAnyException();
    }
  }

  // ── extractJoinColumns ─────────────────────────────────────────────

  @Nested
  class ExtractJoinColumnsFuzz {

    @Test
    void nullReturnsEmpty() {
      assertThat(SqlParser.extractJoinColumns(null)).isEmpty();
    }

    @Test
    void emptyReturnsEmpty() {
      assertThat(SqlParser.extractJoinColumns("")).isEmpty();
    }

    @Test
    void whitespaceOnly() {
      assertThat(SqlParser.extractJoinColumns(WHITESPACE_ONLY)).isEmpty();
    }

    @Test
    void specialChars() {
      assertThat(SqlParser.extractJoinColumns(SPECIAL_CHARS)).isEmpty();
    }

    @Test
    void noJoin() {
      assertThat(SqlParser.extractJoinColumns("SELECT * FROM t WHERE id = 1")).isEmpty();
    }

    @Test
    void simpleJoin() {
      List<JoinColumnPair> pairs =
          SqlParser.extractJoinColumns("SELECT * FROM a JOIN b ON a.id = b.a_id");
      assertThat(pairs).hasSize(1);
    }

    @Test
    void multipleJoins() {
      List<JoinColumnPair> pairs = SqlParser.extractJoinColumns(FULL_SQL);
      assertThat(pairs).isNotEmpty();
    }

    @Test
    void largeInput() {
      assertThatCode(() -> SqlParser.extractJoinColumns(LARGE_100KB)).doesNotThrowAnyException();
    }

    @Test
    void regexHostile() {
      assertThatCode(() -> SqlParser.extractJoinColumns("((((((((((")).doesNotThrowAnyException();
    }

    @Test
    void keywordsOnly() {
      assertThat(SqlParser.extractJoinColumns(KEYWORDS_ONLY)).isEmpty();
    }

    @Test
    void nullBytes() {
      assertThatCode(() -> SqlParser.extractJoinColumns(NULL_BYTES)).doesNotThrowAnyException();
    }
  }

  // ── extractOrderByColumns ──────────────────────────────────────────

  @Nested
  class ExtractOrderByColumnsFuzz {

    @Test
    void nullReturnsEmpty() {
      assertThat(SqlParser.extractOrderByColumns(null)).isEmpty();
    }

    @Test
    void emptyReturnsEmpty() {
      assertThat(SqlParser.extractOrderByColumns("")).isEmpty();
    }

    @Test
    void whitespaceOnly() {
      assertThat(SqlParser.extractOrderByColumns(WHITESPACE_ONLY)).isEmpty();
    }

    @Test
    void specialChars() {
      assertThat(SqlParser.extractOrderByColumns(SPECIAL_CHARS)).isEmpty();
    }

    @Test
    void noOrderBy() {
      assertThat(SqlParser.extractOrderByColumns("SELECT * FROM t")).isEmpty();
    }

    @Test
    void simpleOrderBy() {
      List<ColumnReference> cols =
          SqlParser.extractOrderByColumns("SELECT * FROM t ORDER BY name ASC");
      assertThat(cols).isNotEmpty();
      assertThat(cols.get(0).columnName()).isEqualTo("name");
    }

    @Test
    void multipleColumns() {
      List<ColumnReference> cols =
          SqlParser.extractOrderByColumns("SELECT * FROM t ORDER BY a, b DESC, c ASC");
      assertThat(cols).hasSizeGreaterThanOrEqualTo(2);
    }

    @Test
    void fullSql() {
      List<ColumnReference> cols = SqlParser.extractOrderByColumns(FULL_SQL);
      assertThat(cols).isNotEmpty();
    }

    @Test
    void largeInput() {
      assertThatCode(() -> SqlParser.extractOrderByColumns(LARGE_100KB)).doesNotThrowAnyException();
    }

    @Test
    void regexHostile() {
      assertThatCode(() -> SqlParser.extractOrderByColumns(".*+?{}[]|\\"))
          .doesNotThrowAnyException();
    }

    @Test
    void keywordsOnly() {
      assertThat(SqlParser.extractOrderByColumns(KEYWORDS_ONLY)).isEmpty();
    }

    @Test
    void nullBytes() {
      assertThatCode(() -> SqlParser.extractOrderByColumns(NULL_BYTES)).doesNotThrowAnyException();
    }

    @Test
    void terminatedByLimit() {
      List<ColumnReference> cols =
          SqlParser.extractOrderByColumns("SELECT * FROM t ORDER BY name LIMIT 10");
      assertThat(cols).isNotEmpty();
    }
  }

  // ── extractGroupByColumns ──────────────────────────────────────────

  @Nested
  class ExtractGroupByColumnsFuzz {

    @Test
    void nullReturnsEmpty() {
      assertThat(SqlParser.extractGroupByColumns(null)).isEmpty();
    }

    @Test
    void emptyReturnsEmpty() {
      assertThat(SqlParser.extractGroupByColumns("")).isEmpty();
    }

    @Test
    void whitespaceOnly() {
      assertThat(SqlParser.extractGroupByColumns(WHITESPACE_ONLY)).isEmpty();
    }

    @Test
    void specialChars() {
      assertThat(SqlParser.extractGroupByColumns(SPECIAL_CHARS)).isEmpty();
    }

    @Test
    void noGroupBy() {
      assertThat(SqlParser.extractGroupByColumns("SELECT * FROM t")).isEmpty();
    }

    @Test
    void simpleGroupBy() {
      List<ColumnReference> cols =
          SqlParser.extractGroupByColumns("SELECT name, COUNT(*) FROM t GROUP BY name");
      assertThat(cols).isNotEmpty();
    }

    @Test
    void fullSql() {
      List<ColumnReference> cols = SqlParser.extractGroupByColumns(FULL_SQL);
      assertThat(cols).isNotEmpty();
    }

    @Test
    void largeInput() {
      assertThatCode(() -> SqlParser.extractGroupByColumns(LARGE_100KB)).doesNotThrowAnyException();
    }

    @Test
    void regexHostile() {
      assertThatCode(() -> SqlParser.extractGroupByColumns(".*+?{}[]|\\"))
          .doesNotThrowAnyException();
    }

    @Test
    void keywordsOnly() {
      assertThat(SqlParser.extractGroupByColumns(KEYWORDS_ONLY)).isEmpty();
    }

    @Test
    void nullBytes() {
      assertThatCode(() -> SqlParser.extractGroupByColumns(NULL_BYTES)).doesNotThrowAnyException();
    }

    @Test
    void terminatedByHaving() {
      List<ColumnReference> cols =
          SqlParser.extractGroupByColumns("SELECT name FROM t GROUP BY name HAVING COUNT(*) > 1");
      assertThat(cols).isNotEmpty();
    }
  }

  // ── extractHavingClause ────────────────────────────────────────────

  @Nested
  class ExtractHavingClauseFuzz {

    @Test
    void nullReturnsNull() {
      assertThat(SqlParser.extractHavingClause(null)).isNull();
    }

    @Test
    void emptyReturnsNull() {
      assertThat(SqlParser.extractHavingClause("")).isNull();
    }

    @Test
    void whitespaceOnly() {
      assertThat(SqlParser.extractHavingClause(WHITESPACE_ONLY)).isNull();
    }

    @Test
    void specialChars() {
      assertThat(SqlParser.extractHavingClause(SPECIAL_CHARS)).isNull();
    }

    @Test
    void noHaving() {
      assertThat(SqlParser.extractHavingClause("SELECT * FROM t")).isNull();
    }

    @Test
    void simpleHaving() {
      String result =
          SqlParser.extractHavingClause(
              "SELECT name, COUNT(*) FROM t GROUP BY name HAVING COUNT(*) > 5");
      assertThat(result).isNotNull();
      assertThat(result).contains("COUNT");
    }

    @Test
    void fullSql() {
      String result = SqlParser.extractHavingClause(FULL_SQL);
      assertThat(result).isNotNull();
    }

    @Test
    void largeInput() {
      assertThatCode(() -> SqlParser.extractHavingClause(LARGE_100KB)).doesNotThrowAnyException();
    }

    @Test
    void regexHostile() {
      assertThatCode(() -> SqlParser.extractHavingClause(".*+?{}[]|\\")).doesNotThrowAnyException();
    }

    @Test
    void keywordsOnly() {
      assertThatCode(() -> SqlParser.extractHavingClause(KEYWORDS_ONLY)).doesNotThrowAnyException();
    }

    @Test
    void nullBytes() {
      assertThatCode(() -> SqlParser.extractHavingClause(NULL_BYTES)).doesNotThrowAnyException();
    }

    @Test
    void terminatedByOrderBy() {
      String result =
          SqlParser.extractHavingClause(
              "SELECT name FROM t GROUP BY name HAVING COUNT(*) > 1 ORDER BY name");
      assertThat(result).isNotNull();
      assertThat(result).doesNotContain("ORDER BY");
    }
  }

  // ── detectWhereFunctions ───────────────────────────────────────────

  @Nested
  class DetectWhereFunctionsFuzz {

    @Test
    void nullReturnsEmpty() {
      assertThat(SqlParser.detectWhereFunctions(null)).isEmpty();
    }

    @Test
    void emptyReturnsEmpty() {
      assertThat(SqlParser.detectWhereFunctions("")).isEmpty();
    }

    @Test
    void whitespaceOnly() {
      assertThat(SqlParser.detectWhereFunctions(WHITESPACE_ONLY)).isEmpty();
    }

    @Test
    void specialChars() {
      assertThat(SqlParser.detectWhereFunctions(SPECIAL_CHARS)).isEmpty();
    }

    @Test
    void noFunctions() {
      assertThat(SqlParser.detectWhereFunctions("SELECT * FROM t WHERE id = 1")).isEmpty();
    }

    @Test
    void detectsFunction() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions("SELECT * FROM t WHERE LOWER(name) = 'foo'");
      assertThat(funcs).isNotEmpty();
      assertThat(funcs.get(0).functionName()).isEqualTo("LOWER");
    }

    @Test
    void functionOnRightSideSkipped() {
      // Function on comparison-value side should not be flagged
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions("SELECT * FROM t WHERE id > COALESCE(other_id, 0)");
      assertThat(funcs).isEmpty();
    }

    @Test
    void largeInput() {
      assertThatCode(() -> SqlParser.detectWhereFunctions(LARGE_100KB)).doesNotThrowAnyException();
    }

    @Test
    void regexHostile() {
      assertThatCode(() -> SqlParser.detectWhereFunctions(".*+?{}[]|\\"))
          .doesNotThrowAnyException();
    }

    @Test
    void keywordsOnly() {
      assertThat(SqlParser.detectWhereFunctions(KEYWORDS_ONLY)).isEmpty();
    }

    @Test
    void nullBytes() {
      assertThatCode(() -> SqlParser.detectWhereFunctions(NULL_BYTES)).doesNotThrowAnyException();
    }

    @Test
    void nestedParensInput() {
      String input = "SELECT * FROM t WHERE " + SqlParserFuzzTest.nestedParens(100);
      assertThatCode(() -> SqlParser.detectWhereFunctions(input)).doesNotThrowAnyException();
    }
  }

  // ── detectJoinFunctions ────────────────────────────────────────────

  @Nested
  class DetectJoinFunctionsFuzz {

    @Test
    void nullReturnsEmpty() {
      assertThat(SqlParser.detectJoinFunctions(null)).isEmpty();
    }

    @Test
    void emptyReturnsEmpty() {
      assertThat(SqlParser.detectJoinFunctions("")).isEmpty();
    }

    @Test
    void whitespaceOnly() {
      assertThat(SqlParser.detectJoinFunctions(WHITESPACE_ONLY)).isEmpty();
    }

    @Test
    void specialChars() {
      assertThat(SqlParser.detectJoinFunctions(SPECIAL_CHARS)).isEmpty();
    }

    @Test
    void noJoin() {
      assertThat(SqlParser.detectJoinFunctions("SELECT * FROM t WHERE id = 1")).isEmpty();
    }

    @Test
    void detectsFunctionInJoin() {
      List<FunctionUsage> funcs =
          SqlParser.detectJoinFunctions("SELECT * FROM a INNER JOIN b ON LOWER(b.name) = a.name");
      assertThat(funcs).isNotEmpty();
    }

    @Test
    void largeInput() {
      assertThatCode(() -> SqlParser.detectJoinFunctions(LARGE_100KB)).doesNotThrowAnyException();
    }

    @Test
    void regexHostile() {
      assertThatCode(() -> SqlParser.detectJoinFunctions("((((((((((")).doesNotThrowAnyException();
    }

    @Test
    void keywordsOnly() {
      assertThat(SqlParser.detectJoinFunctions(KEYWORDS_ONLY)).isEmpty();
    }

    @Test
    void nullBytes() {
      assertThatCode(() -> SqlParser.detectJoinFunctions(NULL_BYTES)).doesNotThrowAnyException();
    }
  }

  // ── countOrConditions ──────────────────────────────────────────────

  @Nested
  class CountOrConditionsFuzz {

    @Test
    void nullReturnsZero() {
      assertThat(SqlParser.countOrConditions(null)).isEqualTo(0);
    }

    @Test
    void emptyReturnsZero() {
      assertThat(SqlParser.countOrConditions("")).isEqualTo(0);
    }

    @Test
    void whitespaceOnly() {
      assertThat(SqlParser.countOrConditions(WHITESPACE_ONLY)).isEqualTo(0);
    }

    @Test
    void specialChars() {
      assertThat(SqlParser.countOrConditions(SPECIAL_CHARS)).isEqualTo(0);
    }

    @Test
    void noOr() {
      assertThat(SqlParser.countOrConditions("SELECT * FROM t WHERE id = 1 AND name = 'foo'"))
          .isEqualTo(0);
    }

    @Test
    void singleOr() {
      assertThat(SqlParser.countOrConditions("SELECT * FROM t WHERE id = 1 OR name = 'foo'"))
          .isEqualTo(1);
    }

    @Test
    void multipleOr() {
      assertThat(SqlParser.countOrConditions("SELECT * FROM t WHERE a = 1 OR b = 2 OR c = 3"))
          .isEqualTo(2);
    }

    // Property: result is always >= 0
    @Test
    void resultAlwaysNonNegative() {
      String[] inputs = {"", SPECIAL_CHARS, KEYWORDS_ONLY, FULL_SQL, WHITESPACE_ONLY};
      for (String input : inputs) {
        assertThat(SqlParser.countOrConditions(input)).isGreaterThanOrEqualTo(0);
      }
    }

    @Test
    void largeInput() {
      assertThatCode(() -> SqlParser.countOrConditions(LARGE_100KB)).doesNotThrowAnyException();
    }

    @Test
    void regexHostile() {
      assertThatCode(() -> SqlParser.countOrConditions(".*+?{}[]|\\")).doesNotThrowAnyException();
    }

    @Test
    void keywordsOnly() {
      assertThat(SqlParser.countOrConditions(KEYWORDS_ONLY)).isEqualTo(0);
    }

    @Test
    void nullBytes() {
      assertThatCode(() -> SqlParser.countOrConditions(NULL_BYTES)).doesNotThrowAnyException();
    }
  }

  // ── countEffectiveOrConditions ──────────────────────────────────────

  @Nested
  class CountEffectiveOrConditionsFuzz {

    @Test
    void nullReturnsZero() {
      assertThat(SqlParser.countEffectiveOrConditions(null)).isEqualTo(0);
    }

    @Test
    void emptyReturnsZero() {
      assertThat(SqlParser.countEffectiveOrConditions("")).isEqualTo(0);
    }

    @Test
    void whitespaceOnly() {
      assertThat(SqlParser.countEffectiveOrConditions(WHITESPACE_ONLY)).isEqualTo(0);
    }

    @Test
    void specialChars() {
      assertThat(SqlParser.countEffectiveOrConditions(SPECIAL_CHARS)).isEqualTo(0);
    }

    @Test
    void excludesOptionalParamPattern() {
      String sql = "SELECT * FROM t WHERE (? IS NULL OR name = ?) AND status = 'active'";
      int effective = SqlParser.countEffectiveOrConditions(sql);
      int total = SqlParser.countOrConditions(sql);
      assertThat(effective).isLessThanOrEqualTo(total);
    }

    @Test
    void resultAlwaysNonNegative() {
      String[] inputs = {"", SPECIAL_CHARS, KEYWORDS_ONLY, FULL_SQL, WHITESPACE_ONLY};
      for (String input : inputs) {
        assertThat(SqlParser.countEffectiveOrConditions(input)).isGreaterThanOrEqualTo(0);
      }
    }

    @Test
    void largeInput() {
      assertThatCode(() -> SqlParser.countEffectiveOrConditions(LARGE_100KB))
          .doesNotThrowAnyException();
    }

    @Test
    void regexHostile() {
      assertThatCode(() -> SqlParser.countEffectiveOrConditions("(((((((((("))
          .doesNotThrowAnyException();
    }

    @Test
    void nullBytes() {
      assertThatCode(() -> SqlParser.countEffectiveOrConditions(NULL_BYTES))
          .doesNotThrowAnyException();
    }
  }

  // ── allOrConditionsOnSameColumn ─────────────────────────────────────

  @Nested
  class AllOrConditionsOnSameColumnFuzz {

    @Test
    void nullReturnsFalse() {
      assertThat(SqlParser.allOrConditionsOnSameColumn(null)).isFalse();
    }

    @Test
    void emptyReturnsFalse() {
      assertThat(SqlParser.allOrConditionsOnSameColumn("")).isFalse();
    }

    @Test
    void whitespaceOnly() {
      assertThat(SqlParser.allOrConditionsOnSameColumn(WHITESPACE_ONLY)).isFalse();
    }

    @Test
    void specialChars() {
      assertThat(SqlParser.allOrConditionsOnSameColumn(SPECIAL_CHARS)).isFalse();
    }

    @Test
    void sameColumn() {
      assertThat(
              SqlParser.allOrConditionsOnSameColumn(
                  "SELECT * FROM t WHERE type = 'A' OR type = 'B'"))
          .isTrue();
    }

    @Test
    void differentColumns() {
      assertThat(
              SqlParser.allOrConditionsOnSameColumn(
                  "SELECT * FROM t WHERE type = 'A' OR name = 'B'"))
          .isFalse();
    }

    @Test
    void noOr() {
      assertThat(SqlParser.allOrConditionsOnSameColumn("SELECT * FROM t WHERE type = 'A'"))
          .isFalse();
    }

    @Test
    void largeInput() {
      assertThatCode(() -> SqlParser.allOrConditionsOnSameColumn(LARGE_100KB))
          .doesNotThrowAnyException();
    }

    @Test
    void regexHostile() {
      assertThatCode(() -> SqlParser.allOrConditionsOnSameColumn(".*+?{}[]|\\"))
          .doesNotThrowAnyException();
    }

    @Test
    void keywordsOnly() {
      assertThat(SqlParser.allOrConditionsOnSameColumn(KEYWORDS_ONLY)).isFalse();
    }

    @Test
    void nullBytes() {
      assertThatCode(() -> SqlParser.allOrConditionsOnSameColumn(NULL_BYTES))
          .doesNotThrowAnyException();
    }
  }

  // ── extractOffsetValue ─────────────────────────────────────────────

  @Nested
  class ExtractOffsetValueFuzz {

    @Test
    void nullReturnsEmpty() {
      assertThat(SqlParser.extractOffsetValue(null)).isEmpty();
    }

    @Test
    void emptyReturnsEmpty() {
      assertThat(SqlParser.extractOffsetValue("")).isEmpty();
    }

    @Test
    void whitespaceOnly() {
      assertThat(SqlParser.extractOffsetValue(WHITESPACE_ONLY)).isEmpty();
    }

    @Test
    void specialChars() {
      assertThat(SqlParser.extractOffsetValue(SPECIAL_CHARS)).isEmpty();
    }

    @Test
    void limitOffset() {
      OptionalLong result = SqlParser.extractOffsetValue("SELECT * FROM t LIMIT 10 OFFSET 20");
      assertThat(result).isPresent();
      assertThat(result.getAsLong()).isEqualTo(20);
    }

    @Test
    void mysqlStyle() {
      OptionalLong result = SqlParser.extractOffsetValue("SELECT * FROM t LIMIT 20, 10");
      assertThat(result).isPresent();
      assertThat(result.getAsLong()).isEqualTo(20);
    }

    @Test
    void standaloneOffset() {
      OptionalLong result = SqlParser.extractOffsetValue("SELECT * FROM t OFFSET 50");
      assertThat(result).isPresent();
      assertThat(result.getAsLong()).isEqualTo(50);
    }

    @Test
    void noOffset() {
      assertThat(SqlParser.extractOffsetValue("SELECT * FROM t LIMIT 10")).isEmpty();
    }

    // Property: extracted offset should be >= 0
    @Test
    void offsetAlwaysNonNegative() {
      OptionalLong result = SqlParser.extractOffsetValue(FULL_SQL);
      if (result.isPresent()) {
        assertThat(result.getAsLong()).isGreaterThanOrEqualTo(0);
      }
    }

    @Test
    void largeInput() {
      assertThatCode(() -> SqlParser.extractOffsetValue(LARGE_100KB)).doesNotThrowAnyException();
    }

    @Test
    void regexHostile() {
      assertThatCode(() -> SqlParser.extractOffsetValue(".*+?{}[]|\\")).doesNotThrowAnyException();
    }

    @Test
    void nullBytes() {
      assertThatCode(() -> SqlParser.extractOffsetValue(NULL_BYTES)).doesNotThrowAnyException();
    }

    @Test
    void zeroOffset() {
      OptionalLong result = SqlParser.extractOffsetValue("SELECT * FROM t LIMIT 10 OFFSET 0");
      assertThat(result).isPresent();
      assertThat(result.getAsLong()).isEqualTo(0);
    }

    @Test
    void largeOffsetValue() {
      OptionalLong result =
          SqlParser.extractOffsetValue("SELECT * FROM t LIMIT 10 OFFSET 999999999");
      assertThat(result).isPresent();
      assertThat(result.getAsLong()).isEqualTo(999999999L);
    }
  }

  // ── hasOffsetClause ────────────────────────────────────────────────

  @Nested
  class HasOffsetClauseFuzz {

    @Test
    void nullReturnsFalse() {
      assertThat(SqlParser.hasOffsetClause(null)).isFalse();
    }

    @Test
    void emptyReturnsFalse() {
      assertThat(SqlParser.hasOffsetClause("")).isFalse();
    }

    @Test
    void whitespaceOnly() {
      assertThat(SqlParser.hasOffsetClause(WHITESPACE_ONLY)).isFalse();
    }

    @Test
    void specialChars() {
      assertThat(SqlParser.hasOffsetClause(SPECIAL_CHARS)).isFalse();
    }

    @Test
    void withOffset() {
      assertThat(SqlParser.hasOffsetClause("SELECT * FROM t LIMIT 10 OFFSET 20")).isTrue();
    }

    @Test
    void withParameterizedOffset() {
      assertThat(SqlParser.hasOffsetClause("SELECT * FROM t LIMIT 10 OFFSET ?")).isTrue();
    }

    @Test
    void mysqlStyle() {
      assertThat(SqlParser.hasOffsetClause("SELECT * FROM t LIMIT 20, 10")).isTrue();
    }

    @Test
    void noOffset() {
      assertThat(SqlParser.hasOffsetClause("SELECT * FROM t LIMIT 10")).isFalse();
    }

    @Test
    void largeInput() {
      assertThatCode(() -> SqlParser.hasOffsetClause(LARGE_100KB)).doesNotThrowAnyException();
    }

    @Test
    void regexHostile() {
      assertThatCode(() -> SqlParser.hasOffsetClause("((((((((((")).doesNotThrowAnyException();
    }

    @Test
    void nullBytes() {
      assertThatCode(() -> SqlParser.hasOffsetClause(NULL_BYTES)).doesNotThrowAnyException();
    }
  }

  // ── extractTableNames ──────────────────────────────────────────────

  @Nested
  class ExtractTableNamesFuzz {

    @Test
    void nullReturnsEmpty() {
      assertThat(SqlParser.extractTableNames(null)).isEmpty();
    }

    @Test
    void emptyReturnsEmpty() {
      assertThat(SqlParser.extractTableNames("")).isEmpty();
    }

    @Test
    void whitespaceOnly() {
      assertThat(SqlParser.extractTableNames(WHITESPACE_ONLY)).isEmpty();
    }

    @Test
    void specialChars() {
      assertThat(SqlParser.extractTableNames(SPECIAL_CHARS)).isEmpty();
    }

    @Test
    void simpleFrom() {
      List<String> tables = SqlParser.extractTableNames("SELECT * FROM users");
      assertThat(tables).containsExactly("users");
    }

    @Test
    void fromAndJoin() {
      List<String> tables =
          SqlParser.extractTableNames("SELECT * FROM users u JOIN orders o ON u.id = o.user_id");
      assertThat(tables).contains("users", "orders");
    }

    @Test
    void backtickQuoted() {
      List<String> tables =
          SqlParser.extractTableNames(
              "SELECT * FROM `users` JOIN `orders` ON `users`.id = `orders`.user_id");
      assertThat(tables).contains("users", "orders");
    }

    @Test
    void schemaQualified() {
      List<String> tables = SqlParser.extractTableNames("SELECT * FROM myschema.users");
      assertThat(tables).contains("users");
    }

    // Property: result should never contain SQL keywords
    @Test
    void neverContainsSqlKeywords() {
      List<String> tables = SqlParser.extractTableNames(FULL_SQL);
      for (String table : tables) {
        String lower = table.toLowerCase();
        assertThat(lower)
            .isNotIn(
                "select", "from", "where", "and", "or", "join", "inner", "left", "right", "on",
                "order", "by", "group", "having", "limit", "offset");
      }
    }

    @Test
    void noDuplicates() {
      List<String> tables =
          SqlParser.extractTableNames("SELECT * FROM users JOIN users ON users.id = users.id");
      // Should deduplicate
      long distinctCount = tables.stream().distinct().count();
      assertThat(distinctCount).isEqualTo(tables.size());
    }

    @Test
    void largeInput() {
      assertThatCode(() -> SqlParser.extractTableNames(LARGE_100KB)).doesNotThrowAnyException();
    }

    @Test
    void regexHostile() {
      assertThatCode(() -> SqlParser.extractTableNames(".*+?{}[]|\\")).doesNotThrowAnyException();
    }

    @Test
    void keywordsOnly() {
      // "SELECT FROM WHERE" - FROM is followed by WHERE which is a keyword -> should be filtered
      assertThatCode(() -> SqlParser.extractTableNames(KEYWORDS_ONLY)).doesNotThrowAnyException();
    }

    @Test
    void nullBytes() {
      assertThatCode(() -> SqlParser.extractTableNames(NULL_BYTES)).doesNotThrowAnyException();
    }
  }

  // ── extractInsertTable / extractUpdateTable / extractDeleteTable ────

  @Nested
  class ExtractDmlTableFuzz {

    @Test
    void nullReturnsNull() {
      assertThat(SqlParser.extractInsertTable(null)).isNull();
      assertThat(SqlParser.extractUpdateTable(null)).isNull();
      assertThat(SqlParser.extractDeleteTable(null)).isNull();
    }

    @Test
    void emptyReturnsNull() {
      assertThat(SqlParser.extractInsertTable("")).isNull();
      assertThat(SqlParser.extractUpdateTable("")).isNull();
      assertThat(SqlParser.extractDeleteTable("")).isNull();
    }

    @Test
    void whitespaceOnly() {
      assertThat(SqlParser.extractInsertTable(WHITESPACE_ONLY)).isNull();
      assertThat(SqlParser.extractUpdateTable(WHITESPACE_ONLY)).isNull();
      assertThat(SqlParser.extractDeleteTable(WHITESPACE_ONLY)).isNull();
    }

    @Test
    void specialChars() {
      assertThat(SqlParser.extractInsertTable(SPECIAL_CHARS)).isNull();
      assertThat(SqlParser.extractUpdateTable(SPECIAL_CHARS)).isNull();
      assertThat(SqlParser.extractDeleteTable(SPECIAL_CHARS)).isNull();
    }

    @Test
    void correctExtraction() {
      assertThat(SqlParser.extractInsertTable("INSERT INTO users (name) VALUES ('foo')"))
          .isEqualTo("users");
      assertThat(SqlParser.extractUpdateTable("UPDATE users SET name = 'foo'")).isEqualTo("users");
      assertThat(SqlParser.extractDeleteTable("DELETE FROM users WHERE id = 1")).isEqualTo("users");
    }

    @Test
    void backtickQuoted() {
      assertThat(SqlParser.extractInsertTable("INSERT INTO `users` (name) VALUES ('foo')"))
          .isEqualTo("users");
      assertThat(SqlParser.extractUpdateTable("UPDATE `users` SET name = 'foo'"))
          .isEqualTo("users");
      assertThat(SqlParser.extractDeleteTable("DELETE FROM `users` WHERE id = 1"))
          .isEqualTo("users");
    }

    @Test
    void caseInsensitive() {
      assertThat(SqlParser.extractInsertTable("insert into users (name) values ('foo')"))
          .isEqualTo("users");
      assertThat(SqlParser.extractUpdateTable("update users set name = 'foo'")).isEqualTo("users");
      assertThat(SqlParser.extractDeleteTable("delete from users where id = 1")).isEqualTo("users");
    }

    @Test
    void wrongQueryType() {
      assertThat(SqlParser.extractInsertTable("SELECT * FROM users")).isNull();
      assertThat(SqlParser.extractUpdateTable("SELECT * FROM users")).isNull();
      assertThat(SqlParser.extractDeleteTable("SELECT * FROM users")).isNull();
    }

    @Test
    void largeInput() {
      assertThatCode(() -> SqlParser.extractInsertTable(LARGE_100KB)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractUpdateTable(LARGE_100KB)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractDeleteTable(LARGE_100KB)).doesNotThrowAnyException();
    }

    @Test
    void regexHostile() {
      assertThatCode(() -> SqlParser.extractInsertTable(".*+?{}[]|\\")).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractUpdateTable("((((((((((")).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractDeleteTable("))))))))))))")).doesNotThrowAnyException();
    }

    @Test
    void nullBytes() {
      assertThatCode(() -> SqlParser.extractInsertTable(NULL_BYTES)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractUpdateTable(NULL_BYTES)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractDeleteTable(NULL_BYTES)).doesNotThrowAnyException();
    }
  }

  // ── Cross-method adversarial sweep ─────────────────────────────────

  @Nested
  class CrossMethodAdversarialSweep {

    /** Run ALL methods against every adversarial input category. None should throw. */
    @ParameterizedTest
    @NullSource
    @ValueSource(
        strings = {
          "",
          "   \t\n",
          "!@#$%^&*()",
          "SELECT FROM WHERE",
          ".*+?{}[]|\\",
          "((((((((((",
          "))))))))))",
          "SELECT\0*\0FROM\0t",
          "SELECT\t*\nFROM\rusers\r\nWHERE\tid = 1"
        })
    void allMethodsSurviveAdversarialInput(String input) {
      assertThatCode(() -> SqlParser.normalize(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.isSelectQuery(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.isInsertQuery(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.isUpdateQuery(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.isDeleteQuery(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.isDmlQuery(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.hasSelectAll(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.hasWhereClause(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractWhereBody(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractWhereColumns(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractWhereColumnsWithOperators(input))
          .doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractJoinColumns(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractOrderByColumns(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractGroupByColumns(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractHavingClause(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.detectWhereFunctions(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.detectJoinFunctions(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.countOrConditions(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.countEffectiveOrConditions(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.allOrConditionsOnSameColumn(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractOffsetValue(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.hasOffsetClause(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractTableNames(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractInsertTable(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractUpdateTable(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractDeleteTable(input)).doesNotThrowAnyException();
    }

    @Test
    void deeplyNestedParens100LevelsAllMethods() {
      String input = "SELECT * FROM t WHERE " + nestedParens(100);
      assertThatCode(() -> SqlParser.normalize(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractWhereBody(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractWhereColumns(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractWhereColumnsWithOperators(input))
          .doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.detectWhereFunctions(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.countOrConditions(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.countEffectiveOrConditions(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.allOrConditionsOnSameColumn(input)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractTableNames(input)).doesNotThrowAnyException();
    }

    @Test
    void fullSqlAllMethods() {
      assertThat(SqlParser.normalize(FULL_SQL)).isNotNull();
      assertThat(SqlParser.isSelectQuery(FULL_SQL)).isTrue();
      assertThat(SqlParser.isInsertQuery(FULL_SQL)).isFalse();
      assertThat(SqlParser.isUpdateQuery(FULL_SQL)).isFalse();
      assertThat(SqlParser.isDeleteQuery(FULL_SQL)).isFalse();
      assertThat(SqlParser.isDmlQuery(FULL_SQL)).isFalse();
      assertThat(SqlParser.hasSelectAll(FULL_SQL)).isFalse();
      assertThat(SqlParser.hasWhereClause(FULL_SQL)).isTrue();
      assertThat(SqlParser.extractWhereBody(FULL_SQL)).isNotNull();
      assertThat(SqlParser.extractWhereColumns(FULL_SQL)).isNotEmpty();
      assertThat(SqlParser.extractWhereColumnsWithOperators(FULL_SQL)).isNotEmpty();
      assertThat(SqlParser.extractJoinColumns(FULL_SQL)).isNotEmpty();
      assertThat(SqlParser.extractOrderByColumns(FULL_SQL)).isNotEmpty();
      assertThat(SqlParser.extractGroupByColumns(FULL_SQL)).isNotEmpty();
      assertThat(SqlParser.extractHavingClause(FULL_SQL)).isNotNull();
      assertThat(SqlParser.countOrConditions(FULL_SQL)).isGreaterThanOrEqualTo(0);
      assertThat(SqlParser.countEffectiveOrConditions(FULL_SQL)).isGreaterThanOrEqualTo(0);
      assertThat(SqlParser.extractOffsetValue(FULL_SQL)).isPresent();
      assertThat(SqlParser.hasOffsetClause(FULL_SQL)).isTrue();
      assertThat(SqlParser.extractTableNames(FULL_SQL)).isNotEmpty();
    }
  }

  // ── Performance tests ──────────────────────────────────────────────

  @Nested
  class PerformanceTests {

    @Test
    @Timeout(value = 1, unit = TimeUnit.SECONDS)
    void normalize1MBCompletesInOneSecond() {
      StringBuilder sb = new StringBuilder();
      sb.append("SELECT * FROM users WHERE ");
      while (sb.length() < 1_000_000) {
        sb.append("name = 'value").append(sb.length()).append("' AND ");
      }
      sb.append("id = 1");
      String megaSql = sb.toString();
      assertThatCode(() -> SqlParser.normalize(megaSql)).doesNotThrowAnyException();
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void tenThousandShortSqlsAllExtractors() {
      for (int i = 0; i < 10_000; i++) {
        String sql =
            "SELECT * FROM t"
                + i
                + " WHERE c"
                + i
                + " = "
                + i
                + " ORDER BY c"
                + i
                + " LIMIT 10 OFFSET "
                + i;
        SqlParser.normalize(sql);
        SqlParser.isSelectQuery(sql);
        SqlParser.hasWhereClause(sql);
        SqlParser.extractWhereBody(sql);
        SqlParser.extractWhereColumns(sql);
        SqlParser.extractOrderByColumns(sql);
        SqlParser.extractTableNames(sql);
        SqlParser.extractOffsetValue(sql);
        SqlParser.hasOffsetClause(sql);
        SqlParser.countOrConditions(sql);
      }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void deeplyNestedSubquery50Levels() {
      String sql = deepSubquery(50);
      assertThatCode(
              () -> {
                SqlParser.normalize(sql);
                SqlParser.isSelectQuery(sql);
                SqlParser.extractTableNames(sql);
                SqlParser.extractWhereColumns(sql);
                SqlParser.countOrConditions(sql);
              })
          .doesNotThrowAnyException();
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    void manyOrConditions() {
      StringBuilder sb = new StringBuilder("SELECT * FROM t WHERE ");
      for (int i = 0; i < 1000; i++) {
        if (i > 0) sb.append(" OR ");
        sb.append("col").append(i).append(" = ").append(i);
      }
      String sql = sb.toString();
      assertThatCode(
              () -> {
                SqlParser.countOrConditions(sql);
                SqlParser.countEffectiveOrConditions(sql);
                SqlParser.allOrConditionsOnSameColumn(sql);
              })
          .doesNotThrowAnyException();
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.SECONDS)
    void manyJoins() {
      StringBuilder sb = new StringBuilder("SELECT * FROM t0");
      for (int i = 1; i < 100; i++) {
        sb.append(" JOIN t").append(i).append(" ON t0.id = t").append(i).append(".t0_id");
      }
      String sql = sb.toString();
      assertThatCode(
              () -> {
                SqlParser.extractJoinColumns(sql);
                SqlParser.extractTableNames(sql);
                SqlParser.detectJoinFunctions(sql);
              })
          .doesNotThrowAnyException();
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.SECONDS)
    void repeatedBackslashes() {
      // Regex-hostile: many backslashes
      String hostile = "SELECT * FROM t WHERE x = '" + repeat("\\", 10000) + "'";
      assertThatCode(() -> SqlParser.normalize(hostile)).doesNotThrowAnyException();
    }
  }

  // ── Additional edge cases ──────────────────────────────────────────

  @Nested
  class AdditionalEdgeCases {

    @Test
    void unicodeInput() {
      String sql = "SELECT * FROM users WHERE name = '\u00e9\u00e0\u00fc\u00f1\u00f8'";
      assertThatCode(() -> SqlParser.normalize(sql)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractWhereColumns(sql)).doesNotThrowAnyException();
    }

    @Test
    void emojiInString() {
      String sql = "SELECT * FROM t WHERE val = '\ud83d\ude00\ud83d\ude80'";
      assertThatCode(() -> SqlParser.normalize(sql)).doesNotThrowAnyException();
    }

    @Test
    void onlySemicolons() {
      assertThatCode(() -> SqlParser.normalize(";;;")).doesNotThrowAnyException();
    }

    @Test
    void sqlInjectionAttempts() {
      String[] injections = {
        "'; DROP TABLE users; --",
        "1 OR 1=1",
        "' UNION SELECT * FROM passwords --",
        "1; DELETE FROM users",
        "' OR ''='"
      };
      for (String injection : injections) {
        assertThatCode(() -> SqlParser.normalize(injection)).doesNotThrowAnyException();
        assertThatCode(() -> SqlParser.extractTableNames(injection)).doesNotThrowAnyException();
        assertThatCode(() -> SqlParser.extractWhereColumns(injection)).doesNotThrowAnyException();
      }
    }

    @Test
    void multilineQuery() {
      String sql =
          "SELECT\n  u.id,\n  u.name\nFROM\n  users u\nWHERE\n  u.active = 1\nORDER BY\n  u.name";
      assertThat(SqlParser.isSelectQuery(sql)).isTrue();
      assertThat(SqlParser.hasWhereClause(sql)).isTrue();
      assertThat(SqlParser.extractWhereColumns(sql)).isNotEmpty();
      assertThat(SqlParser.extractOrderByColumns(sql)).isNotEmpty();
      assertThat(SqlParser.extractTableNames(sql)).isNotEmpty();
    }

    @Test
    void commentLikeContent() {
      String sql = "SELECT * FROM t WHERE id = 1 -- this is a comment";
      assertThatCode(() -> SqlParser.extractWhereColumns(sql)).doesNotThrowAnyException();
    }

    @Test
    void blockCommentLikeContent() {
      String sql = "SELECT /* comment */ * FROM t WHERE id = 1";
      assertThatCode(() -> SqlParser.extractTableNames(sql)).doesNotThrowAnyException();
    }

    @Test
    void emptyWhereBody() {
      // WHERE immediately followed by terminator
      String sql = "SELECT * FROM t WHERE GROUP BY name";
      assertThatCode(() -> SqlParser.extractWhereBody(sql)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.extractWhereColumns(sql)).doesNotThrowAnyException();
    }

    @Test
    void whereWithOnlyKeywords() {
      String sql = "SELECT * FROM t WHERE AND OR NOT";
      assertThatCode(() -> SqlParser.extractWhereColumns(sql)).doesNotThrowAnyException();
      assertThatCode(() -> SqlParser.countOrConditions(sql)).doesNotThrowAnyException();
    }

    @Test
    void singleCharacterInput() {
      for (char c = 0; c < 128; c++) {
        String input = String.valueOf(c);
        assertThatCode(() -> SqlParser.normalize(input)).doesNotThrowAnyException();
        assertThatCode(() -> SqlParser.isSelectQuery(input)).doesNotThrowAnyException();
        assertThatCode(() -> SqlParser.extractTableNames(input)).doesNotThrowAnyException();
      }
    }

    @Test
    void veryLongTableName() {
      String longName = repeat("a", 10000);
      String sql = "SELECT * FROM " + longName + " WHERE id = 1";
      assertThatCode(() -> SqlParser.extractTableNames(sql)).doesNotThrowAnyException();
    }

    @Test
    void manyCommasInGroupBy() {
      StringBuilder sb = new StringBuilder("SELECT * FROM t GROUP BY ");
      for (int i = 0; i < 500; i++) {
        if (i > 0) sb.append(", ");
        sb.append("col").append(i);
      }
      String sql = sb.toString();
      List<ColumnReference> cols = SqlParser.extractGroupByColumns(sql);
      assertThat(cols).hasSizeGreaterThanOrEqualTo(100);
    }

    @Test
    void trailingWhitespaceInNormalize() {
      assertThat(SqlParser.normalize("SELECT 1   ")).isEqualTo("select ?");
    }

    @Test
    void leadingWhitespaceInNormalize() {
      assertThat(SqlParser.normalize("   SELECT 1")).isEqualTo("select ?");
    }

    @Test
    void allClauseTypesPresent() {
      // Ensure all extraction methods work on a query with every clause type
      String sql =
          "SELECT u.id, u.name "
              + "FROM users u "
              + "INNER JOIN orders o ON u.id = o.user_id "
              + "WHERE u.status = 'active' AND o.amount > 100 "
              + "GROUP BY u.id, u.name "
              + "HAVING COUNT(o.id) > 2 "
              + "ORDER BY u.name DESC "
              + "LIMIT 50 OFFSET 100";

      assertThat(SqlParser.normalize(sql)).isNotNull();
      assertThat(SqlParser.isSelectQuery(sql)).isTrue();
      assertThat(SqlParser.hasWhereClause(sql)).isTrue();
      assertThat(SqlParser.extractWhereBody(sql)).isNotNull();
      assertThat(SqlParser.extractWhereColumns(sql)).isNotEmpty();
      assertThat(SqlParser.extractJoinColumns(sql)).isNotEmpty();
      assertThat(SqlParser.extractOrderByColumns(sql)).isNotEmpty();
      assertThat(SqlParser.extractGroupByColumns(sql)).isNotEmpty();
      assertThat(SqlParser.extractHavingClause(sql)).isNotNull();
      assertThat(SqlParser.extractTableNames(sql)).isNotEmpty();
      assertThat(SqlParser.extractOffsetValue(sql)).isPresent();
      assertThat(SqlParser.extractOffsetValue(sql).getAsLong()).isEqualTo(100);
      assertThat(SqlParser.hasOffsetClause(sql)).isTrue();
    }

    @Test
    void subqueryInWhere() {
      String sql =
          "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 100)";
      assertThatCode(
              () -> {
                SqlParser.extractWhereColumns(sql);
                SqlParser.countOrConditions(sql);
                SqlParser.extractTableNames(sql);
              })
          .doesNotThrowAnyException();
    }

    @Test
    void correlatedSubquery() {
      String sql =
          "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders o WHERE o.user_id = u.id)";
      assertThatCode(
              () -> {
                SqlParser.extractWhereColumns(sql);
                SqlParser.extractTableNames(sql);
              })
          .doesNotThrowAnyException();
    }

    @Test
    void unionQuery() {
      String sql =
          "SELECT id FROM users WHERE active = 1 UNION SELECT id FROM admins WHERE active = 1";
      assertThatCode(
              () -> {
                SqlParser.normalize(sql);
                SqlParser.extractTableNames(sql);
                SqlParser.extractWhereColumns(sql);
              })
          .doesNotThrowAnyException();
    }

    @Test
    void cteQuery() {
      String sql =
          "WITH active_users AS (SELECT * FROM users WHERE active = 1) "
              + "SELECT * FROM active_users WHERE name = 'foo'";
      assertThatCode(
              () -> {
                SqlParser.normalize(sql);
                SqlParser.extractTableNames(sql);
                SqlParser.extractWhereColumns(sql);
              })
          .doesNotThrowAnyException();
    }
  }
}
