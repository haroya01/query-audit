package io.queryaudit.core.parser;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SqlParserTest {

  // ── normalize ───────────────────────────────────────────────────────

  @Nested
  class Normalize {

    @Test
    void replacesStringLiterals() {
      String result = SqlParser.normalize("SELECT * FROM users WHERE name = 'John'");
      assertThat(result).contains("name = ?");
      assertThat(result).doesNotContain("John");
    }

    @Test
    void preservesDoubleQuotedIdentifiers() {
      // SQL standard: double quotes are delimited identifiers, not string literals
      String result = SqlParser.normalize("SELECT * FROM users WHERE \"name\" = 'John'");
      assertThat(result).contains("\"name\" = ?");
      assertThat(result).doesNotContain("John");
    }

    @Test
    void replacesNumbers() {
      String result = SqlParser.normalize("SELECT * FROM users WHERE id = 42");
      assertThat(result).contains("id = ?");
      assertThat(result).doesNotContain("42");
    }

    @Test
    void replacesDecimalNumbers() {
      String result = SqlParser.normalize("SELECT * FROM products WHERE price > 19.99");
      assertThat(result).contains("price > ?");
      assertThat(result).doesNotContain("19.99");
    }

    @Test
    void collapsesInLists() {
      String result = SqlParser.normalize("SELECT * FROM users WHERE id IN (?, ?, ?)");
      assertThat(result).contains("in (?)");
    }

    @Test
    void collapsesWhitespace() {
      String result = SqlParser.normalize("SELECT  *  FROM   users   WHERE   id = 1");
      assertThat(result).doesNotContain("  ");
    }

    @Test
    void lowercases() {
      String result = SqlParser.normalize("SELECT * FROM Users WHERE Id = 1");
      assertThat(result).isEqualTo(result.toLowerCase());
    }

    @Test
    void returnsNullForNull() {
      assertThat(SqlParser.normalize(null)).isNull();
    }

    // ── PostgreSQL double-quoted identifiers (issue #52) ──────────────

    @Test
    void preservesDoubleQuotedColumnIdentifiers() {
      String result =
          SqlParser.normalize(
              "SELECT \"userId\" FROM \"User\" WHERE \"deletedAt\" IS NULL");
      assertThat(result)
          .isEqualTo("select \"userid\" from \"user\" where \"deletedat\" is null");
    }

    @Test
    void preservesDoubleQuotedIdentifiersInComplexQuery() {
      String result =
          SqlParser.normalize(
              "SELECT \"userId\", \"userName\" FROM \"User\" WHERE \"deletedAt\" IS NULL AND \"status\" = 'active'");
      assertThat(result)
          .isEqualTo(
              "select \"userid\", \"username\" from \"user\" where \"deletedat\" is null and \"status\" = ?");
    }

    @Test
    void normalizeGroupsIdenticalPostgresQueries() {
      String q1 =
          SqlParser.normalize(
              "SELECT \"userId\" FROM \"User\" WHERE \"deletedAt\" IS NULL AND \"id\" = 1");
      String q2 =
          SqlParser.normalize(
              "SELECT \"userId\" FROM \"User\" WHERE \"deletedAt\" IS NULL AND \"id\" = 42");
      // Verify structure is correct first
      assertThat(q1)
          .isEqualTo(
              "select \"userid\" from \"user\" where \"deletedat\" is null and \"id\" = ?");
      // Then verify grouping
      assertThat(q1).isEqualTo(q2);
    }

    @Test
    void preservesEscapedQuoteInsideIdentifier() {
      String result = SqlParser.normalize("SELECT \"col\"\"name\" FROM \"User\"");
      assertThat(result).isEqualTo("select \"col\"\"name\" from \"user\"");
    }

    @Test
    void mixedSingleAndDoubleQuotesHandledCorrectly() {
      String result =
          SqlParser.normalize(
              "SELECT \"userId\" FROM \"User\" WHERE \"name\" = 'John' AND \"age\" > 30");
      assertThat(result)
          .isEqualTo(
              "select \"userid\" from \"user\" where \"name\" = ? and \"age\" > ?");
    }
  }

  // ── isSelectQuery ───────────────────────────────────────────────────

  @Nested
  class IsSelectQuery {

    @Test
    void uppercaseSelect() {
      assertThat(SqlParser.isSelectQuery("SELECT * FROM users")).isTrue();
    }

    @Test
    void lowercaseSelect() {
      assertThat(SqlParser.isSelectQuery("select * from users")).isTrue();
    }

    @Test
    void mixedCaseSelect() {
      assertThat(SqlParser.isSelectQuery("Select id From users")).isTrue();
    }

    @Test
    void insertIsNotSelect() {
      assertThat(SqlParser.isSelectQuery("INSERT INTO users VALUES (1, 'John')")).isFalse();
    }

    @Test
    void updateIsNotSelect() {
      assertThat(SqlParser.isSelectQuery("UPDATE users SET name = 'John'")).isFalse();
    }

    @Test
    void deleteIsNotSelect() {
      assertThat(SqlParser.isSelectQuery("DELETE FROM users WHERE id = 1")).isFalse();
    }

    @Test
    void nullReturnsFalse() {
      assertThat(SqlParser.isSelectQuery(null)).isFalse();
    }
  }

  // ── hasSelectAll ────────────────────────────────────────────────────

  @Nested
  class HasSelectAll {

    @Test
    void detectsSelectStar() {
      assertThat(SqlParser.hasSelectAll("SELECT * FROM users")).isTrue();
    }

    @Test
    void detectsAliasedSelectStar() {
      assertThat(SqlParser.hasSelectAll("SELECT t.* FROM users t")).isTrue();
    }

    @Test
    void countStarIsNotSelectAll() {
      assertThat(SqlParser.hasSelectAll("SELECT COUNT(*) FROM users")).isFalse();
    }

    @Test
    void existsStarIsNotSelectAll() {
      assertThat(SqlParser.hasSelectAll("SELECT EXISTS(SELECT 1 FROM users)")).isFalse();
    }

    @Test
    void specificColumnsIsNotSelectAll() {
      assertThat(SqlParser.hasSelectAll("SELECT id, name FROM users")).isFalse();
    }

    @Test
    void nullReturnsFalse() {
      assertThat(SqlParser.hasSelectAll(null)).isFalse();
    }
  }

  // ── extractWhereColumns ─────────────────────────────────────────────

  @Nested
  class ExtractWhereColumns {

    @Test
    void simpleWhereEquality() {
      List<ColumnReference> cols =
          SqlParser.extractWhereColumns("SELECT * FROM users WHERE id = 1");
      assertThat(cols).hasSize(1);
      assertThat(cols.get(0).columnName()).isEqualTo("id");
    }

    @Test
    void aliasedColumn() {
      List<ColumnReference> cols =
          SqlParser.extractWhereColumns("SELECT * FROM users u WHERE u.id = 1");
      assertThat(cols).hasSize(1);
      assertThat(cols.get(0).tableOrAlias()).isEqualTo("u");
      assertThat(cols.get(0).columnName()).isEqualTo("id");
    }

    @Test
    void multipleConditionsWithAnd() {
      List<ColumnReference> cols =
          SqlParser.extractWhereColumns("SELECT * FROM users WHERE name = 'John' AND age > 30");
      assertThat(cols).extracting(ColumnReference::columnName).contains("name", "age");
    }

    @Test
    void multipleConditionsWithOr() {
      List<ColumnReference> cols =
          SqlParser.extractWhereColumns(
              "SELECT * FROM users WHERE status = 'active' OR role = 'admin'");
      assertThat(cols).extracting(ColumnReference::columnName).contains("status", "role");
    }

    @Test
    void noWhereClauseReturnsEmpty() {
      List<ColumnReference> cols = SqlParser.extractWhereColumns("SELECT * FROM users");
      assertThat(cols).isEmpty();
    }

    @Test
    void nullReturnsEmpty() {
      assertThat(SqlParser.extractWhereColumns(null)).isEmpty();
    }

    @Test
    void literalNumberNotExtractedAsColumn() {
      List<ColumnReference> cols =
          SqlParser.extractWhereColumns("SELECT * FROM room_members WHERE is_pinned = 1");
      assertThat(cols).hasSize(1);
      assertThat(cols.get(0).columnName()).isEqualTo("is_pinned");
      // '1' should NOT appear as a column name
      assertThat(cols).extracting(ColumnReference::columnName).doesNotContain("1");
    }

    @Test
    void literalPlaceholderNotExtractedAsColumn() {
      List<ColumnReference> cols =
          SqlParser.extractWhereColumns("SELECT * FROM users WHERE status = ? AND role = ?");
      assertThat(cols).extracting(ColumnReference::columnName).containsExactly("status", "role");
    }

    @Test
    void booleanLiteralNotExtractedAsColumn() {
      List<ColumnReference> cols =
          SqlParser.extractWhereColumns(
              "SELECT * FROM users WHERE active = true AND deleted = false");
      assertThat(cols).extracting(ColumnReference::columnName).containsExactly("active", "deleted");
    }
  }

  // ── subquery removal (tested via extractWhereColumns) ─────────────

  @Nested
  class SubqueryRemoval {

    @Test
    void subqueryReplacedWithPlaceholder() {
      List<ColumnReference> cols =
          SqlParser.extractWhereColumns(
              "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders) AND status = 'active'");
      assertThat(cols).extracting(ColumnReference::columnName).contains("id", "status");
    }

    @Test
    void subqueryDoesNotProduceMalformedParens() {
      // Ensures opening '(' is preserved: result should be WHERE id IN (?) AND ...
      // not WHERE id IN ?) AND ...
      List<ColumnReference> cols =
          SqlParser.extractWhereColumns(
              "SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)");
      assertThat(cols).extracting(ColumnReference::columnName).containsExactly("id");
    }
  }

  // ── extractJoinColumns ──────────────────────────────────────────────

  @Nested
  class ExtractJoinColumns {

    @Test
    void simpleJoinOn() {
      List<JoinColumnPair> pairs =
          SqlParser.extractJoinColumns(
              "SELECT * FROM orders JOIN users ON orders.user_id = users.id");
      assertThat(pairs).hasSize(1);
      assertThat(pairs.get(0).left().columnName()).isEqualTo("user_id");
      assertThat(pairs.get(0).right().columnName()).isEqualTo("id");
    }

    @Test
    void multipleJoins() {
      List<JoinColumnPair> pairs =
          SqlParser.extractJoinColumns(
              "SELECT * FROM orders "
                  + "JOIN users ON orders.user_id = users.id "
                  + "JOIN products ON orders.product_id = products.id");
      assertThat(pairs).hasSize(2);
    }

    @Test
    void noJoinReturnsEmpty() {
      List<JoinColumnPair> pairs = SqlParser.extractJoinColumns("SELECT * FROM users WHERE id = 1");
      assertThat(pairs).isEmpty();
    }

    @Test
    void nullReturnsEmpty() {
      assertThat(SqlParser.extractJoinColumns(null)).isEmpty();
    }
  }

  // ── extractOrderByColumns ───────────────────────────────────────────

  @Nested
  class ExtractOrderByColumns {

    @Test
    void simpleOrderBy() {
      List<ColumnReference> cols =
          SqlParser.extractOrderByColumns("SELECT * FROM users ORDER BY name");
      assertThat(cols).hasSize(1);
      assertThat(cols.get(0).columnName()).isEqualTo("name");
    }

    @Test
    void orderByMultipleColumns() {
      List<ColumnReference> cols =
          SqlParser.extractOrderByColumns("SELECT * FROM users ORDER BY name ASC, age DESC");
      assertThat(cols).extracting(ColumnReference::columnName).containsExactly("name", "age");
    }

    @Test
    void noOrderByReturnsEmpty() {
      assertThat(SqlParser.extractOrderByColumns("SELECT * FROM users")).isEmpty();
    }

    @Test
    void nullReturnsEmpty() {
      assertThat(SqlParser.extractOrderByColumns(null)).isEmpty();
    }
  }

  // ── extractGroupByColumns ───────────────────────────────────────────

  @Nested
  class ExtractGroupByColumns {

    @Test
    void simpleGroupBy() {
      List<ColumnReference> cols =
          SqlParser.extractGroupByColumns("SELECT status, COUNT(*) FROM users GROUP BY status");
      assertThat(cols).hasSize(1);
      assertThat(cols.get(0).columnName()).isEqualTo("status");
    }

    @Test
    void groupByMultipleColumns() {
      List<ColumnReference> cols =
          SqlParser.extractGroupByColumns(
              "SELECT status, role, COUNT(*) FROM users GROUP BY status, role");
      assertThat(cols).extracting(ColumnReference::columnName).containsExactly("status", "role");
    }

    @Test
    void noGroupByReturnsEmpty() {
      assertThat(SqlParser.extractGroupByColumns("SELECT * FROM users")).isEmpty();
    }

    @Test
    void nullReturnsEmpty() {
      assertThat(SqlParser.extractGroupByColumns(null)).isEmpty();
    }
  }

  // ── detectWhereFunctions ────────────────────────────────────────────

  @Nested
  class DetectWhereFunctions {

    @Test
    void detectsDateFunction() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions(
              "SELECT * FROM orders WHERE DATE(created_at) = '2024-01-01'");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).functionName()).isEqualTo("DATE");
      assertThat(funcs.get(0).columnName()).isEqualTo("created_at");
    }

    @Test
    void detectsLowerFunction() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions(
              "SELECT * FROM users WHERE LOWER(email) = 'test@example.com'");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).functionName()).isEqualTo("LOWER");
    }

    @Test
    void detectsUpperFunction() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions("SELECT * FROM users WHERE UPPER(name) = 'JOHN'");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).functionName()).isEqualTo("UPPER");
    }

    @Test
    void detectsYearFunction() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions("SELECT * FROM orders WHERE YEAR(created_at) = 2024");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).functionName()).isEqualTo("YEAR");
    }

    @Test
    void noFunctionReturnsEmpty() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions("SELECT * FROM users WHERE id = 1");
      assertThat(funcs).isEmpty();
    }

    @Test
    void noWhereReturnsEmpty() {
      assertThat(SqlParser.detectWhereFunctions("SELECT * FROM users")).isEmpty();
    }

    @Test
    void nullReturnsEmpty() {
      assertThat(SqlParser.detectWhereFunctions(null)).isEmpty();
    }
  }

  // ── detectWhereFunctions: comparison-value side ─────────────────────

  @Nested
  class DetectWhereFunctionsComparisonSide {

    @Test
    void functionOnRightSideWithPlainColumnOnLeft_skipped() {
      // m.id > COALESCE(rm.last_read, 0): COALESCE is on comparison value side
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions(
              "SELECT * FROM messages m WHERE m.id > COALESCE(rm.last_read, 0)");
      assertThat(funcs).isEmpty();
    }

    @Test
    void functionOnLeftSideWithLiteralOnRight_flagged() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions(
              "SELECT * FROM orders WHERE DATE(created_at) = '2024-01-01'");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).functionName()).isEqualTo("DATE");
    }

    @Test
    void functionsOnBothSides_bothFlagged() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions("SELECT * FROM users WHERE LOWER(email) = LOWER(name)");
      assertThat(funcs).hasSize(2);
    }

    @Test
    void multipleMixedConditions() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions(
              "SELECT * FROM t WHERE a > COALESCE(b, 0) AND LOWER(c) = 'x'");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).functionName()).isEqualTo("LOWER");
      assertThat(funcs.get(0).columnName()).isEqualTo("c");
    }
  }

  // ── detectJoinFunctions: driving vs lookup table ─────────────────

  @Nested
  class DetectJoinFunctionsLookupTable {

    @Test
    void leftJoin_functionOnDrivingTable_notFlagged() {
      List<FunctionUsage> funcs =
          SqlParser.detectJoinFunctions(
              "SELECT * FROM room_members rm "
                  + "LEFT JOIN messages m ON m.id > COALESCE(rm.last_read_message_id, 0)");
      assertThat(funcs).isEmpty();
    }

    @Test
    void leftJoin_functionOnLookupTable_flagged() {
      List<FunctionUsage> funcs =
          SqlParser.detectJoinFunctions(
              "SELECT * FROM users u " + "LEFT JOIN orders o ON DATE(o.created_at) = u.join_date");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).functionName()).isEqualTo("DATE");
      assertThat(funcs.get(0).columnName()).isEqualTo("created_at");
    }

    @Test
    void rightJoin_functionOnFromTable_flagged() {
      // RIGHT JOIN: FROM table (orders) is the lookup table
      List<FunctionUsage> funcs =
          SqlParser.detectJoinFunctions(
              "SELECT * FROM orders o " + "RIGHT JOIN users u ON YEAR(o.created_at) = u.join_year");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).columnName()).isEqualTo("created_at");
    }

    @Test
    void rightJoin_functionOnJoinedTable_notFlagged() {
      // RIGHT JOIN: joined table (users) is the driving table
      List<FunctionUsage> funcs =
          SqlParser.detectJoinFunctions(
              "SELECT * FROM orders o " + "RIGHT JOIN users u ON o.user_id = LOWER(u.name)");
      assertThat(funcs).isEmpty();
    }

    @Test
    void innerJoin_functionOnEitherSide_allFlagged() {
      List<FunctionUsage> funcs =
          SqlParser.detectJoinFunctions(
              "SELECT * FROM orders o " + "INNER JOIN users u ON LOWER(o.email) = LOWER(u.email)");
      assertThat(funcs).hasSize(2);
    }

    @Test
    void plainJoin_treatedAsInner_flagged() {
      List<FunctionUsage> funcs =
          SqlParser.detectJoinFunctions(
              "SELECT * FROM orders o " + "JOIN users u ON LOWER(u.email) = o.email");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).columnName()).isEqualTo("email");
    }

    @Test
    void nullReturnsEmpty() {
      assertThat(SqlParser.detectJoinFunctions(null)).isEmpty();
    }
  }

  // ── FunctionUsage tableOrAlias ───────────────────────────────────

  @Nested
  class FunctionUsageTableOrAlias {

    @Test
    void capturesTableAlias() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions(
              "SELECT * FROM orders WHERE DATE(o.created_at) = '2024-01-01'");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).tableOrAlias()).isEqualTo("o");
    }

    @Test
    void nullTableAliasWhenNotQualified() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions(
              "SELECT * FROM orders WHERE DATE(created_at) = '2024-01-01'");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).tableOrAlias()).isNull();
    }
  }

  // ── countOrConditions ───────────────────────────────────────────────

  @Nested
  class CountOrConditions {

    @Test
    void zeroOrs() {
      assertThat(SqlParser.countOrConditions("SELECT * FROM users WHERE id = 1 AND name = 'John'"))
          .isEqualTo(0);
    }

    @Test
    void oneOr() {
      assertThat(SqlParser.countOrConditions("SELECT * FROM users WHERE id = 1 OR name = 'John'"))
          .isEqualTo(1);
    }

    @Test
    void threeOrs() {
      assertThat(
              SqlParser.countOrConditions(
                  "SELECT * FROM users WHERE a = 1 OR b = 2 OR c = 3 OR d = 4"))
          .isEqualTo(3);
    }

    @Test
    void orInsideSingleQuotedStringNotCounted() {
      assertThat(SqlParser.countOrConditions("SELECT * FROM users WHERE name LIKE '%OR%'"))
          .isEqualTo(0);
    }

    @Test
    void orInsideDoubleQuotedIdentifierNotCounted() {
      // Double quotes are identifiers per SQL standard; "OR" as column name should not count
      assertThat(SqlParser.countOrConditions("SELECT * FROM users WHERE \"OR_flag\" = 1"))
          .isEqualTo(0);
    }

    @Test
    void noWhereReturnsZero() {
      assertThat(SqlParser.countOrConditions("SELECT * FROM users")).isEqualTo(0);
    }

    @Test
    void nullReturnsZero() {
      assertThat(SqlParser.countOrConditions(null)).isEqualTo(0);
    }
  }

  // ── countEffectiveOrConditions ─────────────────────────────────────

  @Nested
  class CountEffectiveOrConditions {

    @Test
    void excludesOptionalParameterPatterns() {
      // (? IS NULL OR column = ?) patterns should not be counted as OR
      assertThat(
              SqlParser.countEffectiveOrConditions(
                  "SELECT * FROM users WHERE (? IS NULL OR name = ?) AND (? IS NULL OR status = ?)"))
          .isEqualTo(0);
    }

    @Test
    void countsRealOrsExcludingOptionalParams() {
      // 1 optional param pattern + 3 real ORs = 3 effective ORs
      assertThat(
              SqlParser.countEffectiveOrConditions(
                  "SELECT * FROM users WHERE (? IS NULL OR name = ?) AND (a = 1 OR b = 2 OR c = 3 OR d = 4)"))
          .isEqualTo(3);
    }

    @Test
    void noOptionalParamsCountsAllOrs() {
      assertThat(
              SqlParser.countEffectiveOrConditions(
                  "SELECT * FROM users WHERE a = 1 OR b = 2 OR c = 3"))
          .isEqualTo(2);
    }

    @Test
    void nullReturnsZero() {
      assertThat(SqlParser.countEffectiveOrConditions(null)).isEqualTo(0);
    }

    @Test
    void noWhereReturnsZero() {
      assertThat(SqlParser.countEffectiveOrConditions("SELECT * FROM users")).isEqualTo(0);
    }

    @Test
    void multipleOptionalParamsAllExcluded() {
      assertThat(
              SqlParser.countEffectiveOrConditions(
                  "SELECT * FROM users WHERE (? IS NULL OR a = ?) AND (? IS NULL OR b = ?) AND (? IS NULL OR c = ?) AND (? IS NULL OR d = ?)"))
          .isEqualTo(0);
    }

    @Test
    void caseInsensitiveOptionalParam() {
      assertThat(
              SqlParser.countEffectiveOrConditions(
                  "SELECT * FROM users WHERE (? is null or name = ?) AND (? IS NULL OR status = ?)"))
          .isEqualTo(0);
    }
  }

  // ── extractOffsetValue ──────────────────────────────────────────────

  @Nested
  class ExtractOffsetValue {

    @Test
    void limitCountOffsetFormat() {
      OptionalLong offset =
          SqlParser.extractOffsetValue("SELECT * FROM users LIMIT 10 OFFSET 5000");
      assertThat(offset).isPresent();
      assertThat(offset.getAsLong()).isEqualTo(5000);
    }

    @Test
    void limitOffsetCommaFormat() {
      OptionalLong offset = SqlParser.extractOffsetValue("SELECT * FROM users LIMIT 100, 10");
      assertThat(offset).isPresent();
      assertThat(offset.getAsLong()).isEqualTo(100);
    }

    @Test
    void standaloneOffset() {
      OptionalLong offset = SqlParser.extractOffsetValue("SELECT * FROM users OFFSET 200");
      assertThat(offset).isPresent();
      assertThat(offset.getAsLong()).isEqualTo(200);
    }

    @Test
    void noOffsetReturnsEmpty() {
      OptionalLong offset = SqlParser.extractOffsetValue("SELECT * FROM users LIMIT 10");
      assertThat(offset).isEmpty();
    }

    @Test
    void nullReturnsEmpty() {
      assertThat(SqlParser.extractOffsetValue(null)).isEmpty();
    }
  }

  // ── extractTableNames ───────────────────────────────────────────────

  @Nested
  class ExtractTableNames {

    @Test
    void fromClause() {
      List<String> tables = SqlParser.extractTableNames("SELECT * FROM users WHERE id = 1");
      assertThat(tables).containsExactly("users");
    }

    @Test
    void fromWithAlias() {
      List<String> tables = SqlParser.extractTableNames("SELECT * FROM users u WHERE u.id = 1");
      assertThat(tables).containsExactly("users");
    }

    @Test
    void joinTable() {
      List<String> tables =
          SqlParser.extractTableNames(
              "SELECT * FROM orders JOIN users ON orders.user_id = users.id");
      assertThat(tables).contains("orders", "users");
    }

    @Test
    void multipleJoinTables() {
      List<String> tables =
          SqlParser.extractTableNames(
              "SELECT * FROM orders "
                  + "JOIN users ON orders.user_id = users.id "
                  + "JOIN products ON orders.product_id = products.id");
      assertThat(tables).contains("orders", "users", "products");
    }

    @Test
    void noDuplicates() {
      List<String> tables =
          SqlParser.extractTableNames(
              "SELECT * FROM users JOIN users ON users.id = users.manager_id");
      // "users" should appear only once
      assertThat(tables.stream().filter(t -> t.equals("users")).count()).isEqualTo(1);
    }

    @Test
    void nullReturnsEmpty() {
      assertThat(SqlParser.extractTableNames(null)).isEmpty();
    }

    // ── PostgreSQL double-quoted identifiers (issue #52) ──────────────

    @Test
    void doubleQuotedTableName() {
      List<String> tables =
          SqlParser.extractTableNames(
              "SELECT \"userId\" FROM \"User\" WHERE \"deletedAt\" IS NULL");
      assertThat(tables).containsExactly("User");
    }

    @Test
    void doubleQuotedTableNameWithJoin() {
      List<String> tables =
          SqlParser.extractTableNames(
              "SELECT * FROM \"User\" u JOIN \"UserRole\" ur ON u.\"id\" = ur.\"userId\"");
      assertThat(tables).containsExactlyInAnyOrder("User", "UserRole");
    }
  }

  // ── PostgreSQL double-quoted identifiers: column extraction (issue #52) ──

  @Nested
  class PostgresDoubleQuotedColumns {

    @Test
    void extractWhereColumnsWithDoubleQuotedIdentifiers() {
      List<ColumnReference> cols =
          SqlParser.extractWhereColumns(
              "SELECT * FROM \"User\" WHERE \"userId\" = 1 AND \"deletedAt\" IS NULL");
      assertThat(cols)
          .extracting(ColumnReference::columnName)
          .containsExactly("userId", "deletedAt");
    }

    @Test
    void extractWhereColumnsWithTableQualifiedDoubleQuote() {
      List<ColumnReference> cols =
          SqlParser.extractWhereColumns(
              "SELECT * FROM \"User\" u WHERE u.\"status\" = 'active'");
      assertThat(cols).extracting(ColumnReference::columnName).containsExactly("status");
    }

    @Test
    void extractJoinColumnsWithDoubleQuotedIdentifiers() {
      List<JoinColumnPair> pairs =
          SqlParser.extractJoinColumns(
              "SELECT * FROM \"User\" u JOIN \"Order\" o ON u.\"id\" = o.\"userId\"");
      assertThat(pairs).hasSize(1);
      assertThat(pairs.get(0).left().columnName()).isEqualTo("id");
      assertThat(pairs.get(0).right().columnName()).isEqualTo("userId");
    }

    @Test
    void extractOrderByWithDoubleQuotedIdentifiers() {
      List<ColumnReference> cols =
          SqlParser.extractOrderByColumns(
              "SELECT * FROM \"User\" ORDER BY \"userName\" ASC, \"createdAt\" DESC");
      assertThat(cols).extracting(ColumnReference::columnName)
          .containsExactly("userName", "createdAt");
    }

    @Test
    void extractGroupByWithDoubleQuotedIdentifiers() {
      List<ColumnReference> cols =
          SqlParser.extractGroupByColumns(
              "SELECT \"status\", COUNT(*) FROM \"User\" GROUP BY \"status\"");
      assertThat(cols).extracting(ColumnReference::columnName).containsExactly("status");
    }

    @Test
    void detectWhereFunctionsWithDoubleQuotedColumn() {
      List<FunctionUsage> funcs =
          SqlParser.detectWhereFunctions(
              "SELECT * FROM \"User\" WHERE LOWER(\"email\") = 'test@example.com'");
      assertThat(funcs).hasSize(1);
      assertThat(funcs.get(0).functionName()).isEqualTo("LOWER");
      assertThat(funcs.get(0).columnName()).isEqualTo("email");
    }
  }

  // ── PostgreSQL double-quoted identifiers: DML table extraction (issue #52) ──

  @Nested
  class PostgresDoubleQuotedDml {

    @Test
    void extractInsertTableWithDoubleQuote() {
      assertThat(SqlParser.extractInsertTable("INSERT INTO \"User\" (\"name\") VALUES ('test')"))
          .isEqualTo("User");
    }

    @Test
    void extractUpdateTableWithDoubleQuote() {
      assertThat(SqlParser.extractUpdateTable("UPDATE \"User\" SET \"name\" = 'test'"))
          .isEqualTo("User");
    }

    @Test
    void extractDeleteTableWithDoubleQuote() {
      assertThat(SqlParser.extractDeleteTable("DELETE FROM \"User\" WHERE \"id\" = 1"))
          .isEqualTo("User");
    }
  }

  // ── DML query type detection ────────────────────────────────────────

  @Nested
  class DmlQueryTypeDetection {

    @Test
    void isInsertQuery() {
      assertThat(SqlParser.isInsertQuery("INSERT INTO users (name) VALUES ('test')")).isTrue();
      assertThat(SqlParser.isInsertQuery("  insert into users (name) values ('test')")).isTrue();
      assertThat(SqlParser.isInsertQuery("SELECT * FROM users")).isFalse();
      assertThat(SqlParser.isInsertQuery("UPDATE users SET name = 'test'")).isFalse();
      assertThat(SqlParser.isInsertQuery(null)).isFalse();
    }

    @Test
    void isUpdateQuery() {
      assertThat(SqlParser.isUpdateQuery("UPDATE users SET name = 'test'")).isTrue();
      assertThat(SqlParser.isUpdateQuery("  update users set name = 'test'")).isTrue();
      assertThat(SqlParser.isUpdateQuery("SELECT * FROM users")).isFalse();
      assertThat(SqlParser.isUpdateQuery("INSERT INTO users (name) VALUES ('test')")).isFalse();
      assertThat(SqlParser.isUpdateQuery(null)).isFalse();
    }

    @Test
    void isDeleteQuery() {
      assertThat(SqlParser.isDeleteQuery("DELETE FROM users WHERE id = 1")).isTrue();
      assertThat(SqlParser.isDeleteQuery("  delete from users where id = 1")).isTrue();
      assertThat(SqlParser.isDeleteQuery("SELECT * FROM users")).isFalse();
      assertThat(SqlParser.isDeleteQuery("UPDATE users SET name = 'test'")).isFalse();
      assertThat(SqlParser.isDeleteQuery(null)).isFalse();
    }

    @Test
    void isDmlQuery() {
      assertThat(SqlParser.isDmlQuery("INSERT INTO users (name) VALUES ('test')")).isTrue();
      assertThat(SqlParser.isDmlQuery("UPDATE users SET name = 'test'")).isTrue();
      assertThat(SqlParser.isDmlQuery("DELETE FROM users WHERE id = 1")).isTrue();
      assertThat(SqlParser.isDmlQuery("SELECT * FROM users")).isFalse();
      assertThat(SqlParser.isDmlQuery(null)).isFalse();
    }
  }

  // ── DML table extraction ────────────────────────────────────────────

  @Nested
  class DmlTableExtraction {

    @Test
    void extractInsertTable() {
      assertThat(SqlParser.extractInsertTable("INSERT INTO users (name) VALUES ('test')"))
          .isEqualTo("users");
      assertThat(SqlParser.extractInsertTable("INSERT INTO `order_items` (id) VALUES (1)"))
          .isEqualTo("order_items");
      assertThat(SqlParser.extractInsertTable("  insert into users (name) values ('test')"))
          .isEqualTo("users");
      assertThat(SqlParser.extractInsertTable("SELECT * FROM users")).isNull();
      assertThat(SqlParser.extractInsertTable(null)).isNull();
    }

    @Test
    void extractUpdateTable() {
      assertThat(SqlParser.extractUpdateTable("UPDATE users SET name = 'test'")).isEqualTo("users");
      assertThat(SqlParser.extractUpdateTable("UPDATE `order_items` SET qty = 1"))
          .isEqualTo("order_items");
      assertThat(SqlParser.extractUpdateTable("  update users set name = 'test'"))
          .isEqualTo("users");
      assertThat(SqlParser.extractUpdateTable("SELECT * FROM users")).isNull();
      assertThat(SqlParser.extractUpdateTable(null)).isNull();
    }

    @Test
    void extractDeleteTable() {
      assertThat(SqlParser.extractDeleteTable("DELETE FROM users WHERE id = 1")).isEqualTo("users");
      assertThat(SqlParser.extractDeleteTable("DELETE FROM `order_items` WHERE id = 1"))
          .isEqualTo("order_items");
      assertThat(SqlParser.extractDeleteTable("  delete from users where id = 1"))
          .isEqualTo("users");
      assertThat(SqlParser.extractDeleteTable("SELECT * FROM users")).isNull();
      assertThat(SqlParser.extractDeleteTable(null)).isNull();
    }
  }

  // ── hasWhereClause ──────────────────────────────────────────────────

  @Nested
  class HasWhereClause {

    @Test
    void detectsWhereClause() {
      assertThat(SqlParser.hasWhereClause("SELECT * FROM users WHERE id = 1")).isTrue();
      assertThat(SqlParser.hasWhereClause("UPDATE users SET name = 'x' WHERE id = 1")).isTrue();
      assertThat(SqlParser.hasWhereClause("DELETE FROM users WHERE id = 1")).isTrue();
    }

    @Test
    void returnsFalseWithoutWhere() {
      assertThat(SqlParser.hasWhereClause("SELECT * FROM users")).isFalse();
      assertThat(SqlParser.hasWhereClause("UPDATE users SET name = 'x'")).isFalse();
      assertThat(SqlParser.hasWhereClause("DELETE FROM users")).isFalse();
    }

    @Test
    void handlesNull() {
      assertThat(SqlParser.hasWhereClause(null)).isFalse();
    }
  }
}
