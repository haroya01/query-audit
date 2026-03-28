package io.queryaudit.core.parser;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Verifies that SqlParser.removeSubqueries() incorrectly treats "(SELECT" inside string literals as
 * real subqueries. These tests DEMONSTRATE the bug — assertions are written to match the current
 * (broken) behavior.
 */
class RemoveSubqueriesLiteralVerificationTest {

  @Test
  @DisplayName("BUG: string literal containing '(SELECT ...' is mangled by removeSubqueries")
  void stringLiteralWithSelectIsIncorrectlyStripped() {
    String sql =
        "SELECT * FROM logs WHERE message = 'Error in (SELECT query failed)' AND status = 'active'";

    String result = SqlParser.removeSubqueries(sql);

    // EXPECTED (correct) behavior — the string literal should be preserved:
    //   "SELECT * FROM logs WHERE message = 'Error in (SELECT query failed)' AND status = 'active'"
    //
    // ACTUAL (buggy) behavior — the method treats (SELECT inside the literal as a subquery
    // and replaces it, destroying the literal content:
    assertThat(result)
        .as("Bug: the literal content '(SELECT query failed)' should be preserved but is removed")
        .doesNotContain("(SELECT query failed)");

    // The buggy output contains the subquery placeholder instead
    assertThat(result).contains("(?)");
  }

  @Test
  @DisplayName("BUG: string literal with full subquery-like text is mangled")
  void stringLiteralWithFullSubqueryPatternIsIncorrectlyStripped() {
    String sql =
        "SELECT * FROM events WHERE description = '(SELECT * FROM users)' AND type = 'error'";

    String result = SqlParser.removeSubqueries(sql);

    // EXPECTED: the string literal should be untouched
    // ACTUAL: the method strips the "(SELECT * FROM users)" from inside the literal
    assertThat(result)
        .as("Bug: literal '(SELECT * FROM users)' should be preserved but is removed")
        .doesNotContain("(SELECT * FROM users)");

    assertThat(result).contains("(?)");
  }

  @Test
  @DisplayName("CONTROL: a real subquery IS correctly removed")
  void realSubqueryIsRemoved() {
    String sql = "SELECT * FROM orders WHERE customer_id IN (SELECT id FROM customers)";

    String result = SqlParser.removeSubqueries(sql);

    // Real subqueries should be replaced — this works correctly
    assertThat(result).doesNotContain("SELECT id FROM customers");
    assertThat(result).contains("(?)");
  }
}
