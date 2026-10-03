package io.queryaudit.core.parser;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SqlTextTest {

    // --- 1. stripComments -----------------------------------------------------

    @Test
    void shouldStripLineAndBlockComments() {
        String sql = "SELECT * -- line comment\nFROM users /* block comment */ WHERE id = 1";
        String cleaned = SqlText.stripComments(sql);

        assertThat(cleaned).doesNotContain("line comment");
        assertThat(cleaned).doesNotContain("block comment");
        assertThat(cleaned).contains("SELECT *");
        assertThat(cleaned).contains("FROM users");
    }

    @Test
    void shouldHandleNestedBlockComments() {
        String sql = "SELECT /* outer /* nested */ comment */ id FROM users";
        String cleaned = SqlText.stripComments(sql);

        assertThat(cleaned).isEqualTo("SELECT   id FROM users");
    }

    @Test
    void shouldPreserveCommentMarkersInsideStringLiterals() {
        String sql = "SELECT '--not a comment', '/*also not a comment*/' FROM orders";
        String cleaned = SqlText.stripComments(sql);

        assertThat(cleaned).isEqualTo(sql);
    }

    @Test
    void shouldPreserveCommentMarkersInsideDoubleQuotedIdentifiers() {
        String sql = "SELECT \"--not a comment\" FROM orders";
        String cleaned = SqlText.stripComments(sql);

        assertThat(cleaned).isEqualTo(sql);
    }

    // --- 2. replaceStringLiterals ---------------------------------------------

    @Test
    void shouldReplaceSimpleStringLiteral() {
        String sql = "SELECT 'hello world' FROM t";
        String replaced = SqlText.replaceStringLiterals(sql);

        assertThat(replaced).isEqualTo("SELECT ? FROM t");
    }

    @Test
    void shouldHandleEscapedSingleQuotes() {
        String sql = "SELECT 'it''s fine' FROM t";
        String replaced = SqlText.replaceStringLiterals(sql);

        assertThat(replaced).isEqualTo("SELECT ? FROM t");
    }

    @Test
    void shouldHandleBackslashEscapedQuotes() {
        String sql = "SELECT 'it\\'s fine' FROM t";
        String replaced = SqlText.replaceStringLiterals(sql);

        assertThat(replaced).isEqualTo("SELECT ? FROM t");
    }

    @Test
    void shouldPreserveDoubleQuotedIdentifiersDuringLiteralReplacement() {
        String sql = "SELECT \"user_name\" FROM \"users\" WHERE col = 'val'";
        String replaced = SqlText.replaceStringLiterals(sql);

        assertThat(replaced).isEqualTo("SELECT \"user_name\" FROM \"users\" WHERE col = ?");
    }

    // --- 3. normalize ---------------------------------------------------------

    @Test
    void shouldNormalizeNumbersToPlaceholdersAndLowercase() {
        String sql = "SELECT * FROM orders WHERE id = 42 AND total > 99.5";
        String normalized = SqlText.normalize(sql);

        assertThat(normalized).isEqualTo("select * from orders where id = ? and total > ?");
    }

    @Test
    void shouldCollapseInLists() {
        String sql = "SELECT * FROM users WHERE id IN (1, 2, 3, 4)";
        String normalized = SqlText.normalize(sql);

        assertThat(normalized).isEqualTo("select * from users where id in (?)");
    }

    @Test
    void shouldFoldWhitespaceAndMatchNormalizedShapes() {
        String query1 = "SELECT  id,   name  FROM  users   WHERE  id = 10";
        String query2 = "SELECT id, name FROM users WHERE id = 999";

        assertThat(SqlText.normalize(query1)).isEqualTo(SqlText.normalize(query2));
    }

    // --- 4. splitByTopLevelCommas ---------------------------------------------

    @Test
    void shouldSplitByTopLevelCommasIgnoringNestedParentheses() {
        String sql = "col1, coalesce(col2, col3), col4";
        List<String> parts = SqlText.splitByTopLevelCommas(sql);

        assertThat(parts).containsExactly("col1", " coalesce(col2, col3)", " col4");
    }

    @Test
    void shouldHandleNoCommas() {
        List<String> parts = SqlText.splitByTopLevelCommas("single_column");

        assertThat(parts).containsExactly("single_column");
    }

    // --- 5. null handling -----------------------------------------------------

    @Test
    void shouldHandleNullInputsGracefully() {
        assertThat(SqlText.stripComments(null)).isNull();
        assertThat(SqlText.normalize(null)).isNull();
    }
}