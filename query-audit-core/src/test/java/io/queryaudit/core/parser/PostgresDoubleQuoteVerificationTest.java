package io.queryaudit.core.parser;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Verification tests for PostgreSQL double-quoted identifier handling.
 *
 * <p>PostgreSQL uses double quotes to delimit identifiers (table/column names), NOT string literals.
 * For example: {@code SELECT "userId" FROM "User"} — here "userId" and "User" are identifiers.
 *
 * <p>The current SqlParser.normalize() treats double-quoted strings as literals and replaces them
 * with "?", which loses table and column names. These tests demonstrate this bug.
 */
class PostgresDoubleQuoteVerificationTest {

  // ── normalize: double-quoted identifiers incorrectly replaced ─────

  @Nested
  @DisplayName("normalize() with PostgreSQL double-quoted identifiers")
  class NormalizeDoubleQuotedIdentifiers {

    @Test
    @DisplayName("BUG: double-quoted column and table names are replaced with '?'")
    void doubleQuotedColumnAndTableReplacedWithPlaceholder() {
      String sql = "SELECT \"userId\" FROM \"User\" WHERE \"deletedAt\" IS NULL";
      String result = SqlParser.normalize(sql);

      // ACTUAL (buggy) behavior: double-quoted identifiers become '?'
      // Result is something like: select ? from ? where ? is null
      assertThat(result).contains("?");

      // EXPECTED behavior: identifiers should be preserved (unquoted or quoted)
      // Expected: select "userid" from "user" where "deletedat" is null
      //       or: select userid from user where deletedat is null
      assertThat(result)
          .as(
              "normalize() should preserve double-quoted identifiers, but got: \"%s\"",
              result)
          .doesNotContain("select ?")
          .contains("user"); // table name "User" should survive in some form

      // This assertion will FAIL, demonstrating the bug
    }

    @Test
    @DisplayName("BUG: multi-column query loses all column names")
    void multiColumnQueryLosesAllColumnNames() {
      String sql = "SELECT \"order_id\", \"total_amount\" FROM \"Order\" WHERE \"user_id\" = 1";
      String result = SqlParser.normalize(sql);

      // ACTUAL: select ?, ? from ? where ? = ?
      // EXPECTED: select "order_id", "total_amount" from "order" where "user_id" = ?
      assertThat(result)
          .as("Actual normalized output: \"%s\"", result)
          .contains("order_id")
          .contains("total_amount")
          .contains("order"); // table name should be preserved
    }

    @Test
    @DisplayName("BUG: JOIN query with double-quoted identifiers loses table names")
    void joinQueryLosesTableNames() {
      String sql =
          "SELECT \"User\".\"name\", \"Order\".\"total\" "
              + "FROM \"User\" "
              + "JOIN \"Order\" ON \"User\".\"id\" = \"Order\".\"userId\"";
      String result = SqlParser.normalize(sql);

      // ACTUAL: select ?.?, ?.? from ? join ? on ?.? = ?.?
      // EXPECTED: should preserve table/column identifiers
      assertThat(result)
          .as("Actual normalized output: \"%s\"", result)
          .contains("user")
          .contains("order");
    }

    @Test
    @DisplayName("BUG: mixed single-quoted literals and double-quoted identifiers")
    void mixedSingleAndDoubleQuotes() {
      String sql = "SELECT \"userName\" FROM \"User\" WHERE \"status\" = 'active'";
      String result = SqlParser.normalize(sql);

      // Single-quoted 'active' SHOULD be replaced with ? (it's a literal) — correct
      // Double-quoted "userName", "User", "status" should NOT be replaced — bug
      assertThat(result).doesNotContain("active"); // single-quote literal correctly replaced
      assertThat(result)
          .as(
              "Double-quoted identifiers should be preserved, but got: \"%s\"",
              result)
          .contains("username")  // column name should survive
          .contains("user")      // table name should survive
          .contains("status");   // WHERE column should survive
    }
  }

  // ── extractTableNames: double-quoted table names not recognized ────

  @Nested
  @DisplayName("extractTableNames() with PostgreSQL double-quoted tables")
  class ExtractTableNamesDoubleQuoted {

    @Test
    @DisplayName("BUG: double-quoted table name in FROM clause not extracted")
    void doubleQuotedTableInFromClause() {
      String sql = "SELECT \"userId\" FROM \"User\" WHERE \"deletedAt\" IS NULL";
      List<String> tables = SqlParser.extractTableNames(sql);

      // EXPECTED: should contain "User" (the table name)
      assertThat(tables)
          .as("extractTableNames should recognize double-quoted table names, got: %s", tables)
          .isNotEmpty()
          .anyMatch(t -> t.equalsIgnoreCase("User"));
    }

    @Test
    @DisplayName("BUG: double-quoted table name in JOIN clause not extracted")
    void doubleQuotedTableInJoinClause() {
      String sql =
          "SELECT * FROM \"User\" JOIN \"Order\" ON \"User\".\"id\" = \"Order\".\"userId\"";
      List<String> tables = SqlParser.extractTableNames(sql);

      assertThat(tables)
          .as("extractTableNames should find both double-quoted tables, got: %s", tables)
          .anyMatch(t -> t.equalsIgnoreCase("User"))
          .anyMatch(t -> t.equalsIgnoreCase("Order"));
    }

    @Test
    @DisplayName("BUG: double-quoted schema-qualified table name not extracted")
    void doubleQuotedSchemaQualifiedTable() {
      String sql = "SELECT * FROM \"public\".\"User\" WHERE \"id\" = 1";
      List<String> tables = SqlParser.extractTableNames(sql);

      assertThat(tables)
          .as("Should extract 'User' from schema-qualified double-quoted name, got: %s", tables)
          .anyMatch(t -> t.equalsIgnoreCase("User"));
    }
  }
}
