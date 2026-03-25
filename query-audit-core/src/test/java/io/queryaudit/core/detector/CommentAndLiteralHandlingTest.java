package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.parser.SqlParser;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Tests that SQL comments and string literals are properly handled so that detectors do not produce
 * false positives on content inside comments or string literals.
 */
class CommentAndLiteralHandlingTest {

  private final IndexMetadata emptyIndex = new IndexMetadata(Map.of());

  private static QueryRecord q(String sql) {
    return new QueryRecord(sql, 1000L, System.currentTimeMillis(), null);
  }

  // ── stripComments unit tests ─────────────────────────────────────────

  @Nested
  class StripCommentsTest {

    @Test
    void removesLineComment() {
      String sql = "SELECT id FROM users -- this is a comment\nWHERE id = 1";
      String stripped = SqlParser.stripComments(sql);
      assertThat(stripped).doesNotContain("--");
      assertThat(stripped).contains("SELECT id FROM users");
      assertThat(stripped).contains("WHERE id = 1");
    }

    @Test
    void removesBlockComment() {
      String sql = "SELECT id /* all columns */ FROM users WHERE id = 1";
      String stripped = SqlParser.stripComments(sql);
      assertThat(stripped).doesNotContain("/*");
      assertThat(stripped).doesNotContain("*/");
      assertThat(stripped).contains("SELECT id");
      assertThat(stripped).contains("FROM users WHERE id = 1");
    }

    @Test
    void handlesNestedBlockComments() {
      String sql = "SELECT id /* outer /* inner */ still comment */ FROM users";
      String stripped = SqlParser.stripComments(sql);
      assertThat(stripped).doesNotContain("/*");
      assertThat(stripped).doesNotContain("*/");
      assertThat(stripped).contains("SELECT id");
      assertThat(stripped).contains("FROM users");
    }

    @Test
    void preservesCommentLikeContentInSingleQuotedStrings() {
      String sql = "SELECT id FROM users WHERE name = 'value -- not a comment'";
      String stripped = SqlParser.stripComments(sql);
      assertThat(stripped).contains("'value -- not a comment'");
    }

    @Test
    void preservesCommentLikeContentInDoubleQuotedStrings() {
      String sql = "SELECT id FROM users WHERE name = \"value /* not a comment */\"";
      String stripped = SqlParser.stripComments(sql);
      assertThat(stripped).contains("\"value /* not a comment */\"");
    }

    @Test
    void handlesMultiLineBlockComment() {
      String sql = "SELECT id\n/* this is a\nmulti-line\ncomment */\nFROM users";
      String stripped = SqlParser.stripComments(sql);
      assertThat(stripped).doesNotContain("multi-line");
      assertThat(stripped).contains("SELECT id");
      assertThat(stripped).contains("FROM users");
    }

    @Test
    void returnsNullForNullInput() {
      assertThat(SqlParser.stripComments(null)).isNull();
    }
  }

  // ── String literal handling tests ──────────────────────────────────

  @Nested
  class StringLiteralHandlingTest {

    @Test
    void handlesSqlStandardEscapedSingleQuotes() {
      // 'It''s a test' should be treated as a single string literal
      String sql = "SELECT id FROM users WHERE name = 'It''s a test'";
      String normalized = SqlParser.normalize(sql);
      assertThat(normalized).contains("name = ?");
      assertThat(normalized).doesNotContain("It");
    }

    @Test
    void handlesSqlStandardEscapedDoubleQuotes() {
      // "She said ""hello""" should be treated as a single string literal
      String sql = "SELECT id FROM users WHERE name = \"She said \"\"hello\"\"\"";
      String normalized = SqlParser.normalize(sql);
      assertThat(normalized).contains("name = ?");
      assertThat(normalized).doesNotContain("hello");
    }

    @Test
    void handlesMySqlBackslashEscapedQuotes() {
      // 'It\'s' should be treated as a single string literal
      String sql = "SELECT id FROM users WHERE name = 'It\\'s'";
      String normalized = SqlParser.normalize(sql);
      assertThat(normalized).contains("name = ?");
      assertThat(normalized).doesNotContain("It");
    }

    @Test
    void handlesMixedQuoteStyles() {
      String sql = "SELECT id FROM users WHERE name = 'foo' AND label = \"bar\"";
      String normalized = SqlParser.normalize(sql);
      assertThat(normalized).contains("name = ?");
      assertThat(normalized).contains("label = ?");
    }

    @Test
    void likeWithEscapedQuotesDoesNotBreakParsing() {
      // LIKE '%test''' — the escaped quote should not break parsing
      String sql = "SELECT id FROM users WHERE name LIKE '%test'''";
      String normalized = SqlParser.normalize(sql);
      assertThat(normalized).contains("name like ?");
      assertThat(normalized).doesNotContain("test");
    }
  }

  // ── False positive prevention: comments should not trigger detectors ──

  @Nested
  class CommentFalsePositivePreventionTest {

    @Test
    void lineCommentCaseShouldNotTriggerCaseInWhereDetector() {
      // The CASE keyword is in a line comment, not in actual SQL
      String sql =
          "SELECT id FROM users WHERE status = 'active' -- CASE WHEN status = 'A' THEN 1 END";
      CaseInWhereDetector detector = new CaseInWhereDetector();
      List<Issue> issues = detector.evaluate(List.of(q(sql)), emptyIndex);
      assertThat(issues).isEmpty();
    }

    @Test
    void blockCommentSelectStarShouldNotTriggerSelectAllDetector() {
      // SELECT * is inside a block comment
      String sql = "SELECT id /* SELECT * */ FROM users WHERE id = 1";
      SelectAllDetector detector = new SelectAllDetector();
      List<Issue> issues = detector.evaluate(List.of(q(sql)), emptyIndex);
      assertThat(issues).isEmpty();
    }

    @Test
    void stringLiteralOrShouldNotTriggerOrAbuseDetector() {
      // The OR conditions are inside a string literal, not actual SQL
      String sql =
          "SELECT id FROM users WHERE description = 'status = 1 OR status = 2 OR status = 3 OR status = 4'";
      OrAbuseDetector detector = new OrAbuseDetector();
      List<Issue> issues = detector.evaluate(List.of(q(sql)), emptyIndex);
      assertThat(issues).isEmpty();
    }
  }

  // ── Normal detection still works after comment/literal stripping ──

  @Nested
  class NormalDetectionStillWorksTest {

    @Test
    void caseInWhereStillDetectedAfterCommentStripping() {
      String sql =
          "SELECT id FROM users /* get users */ WHERE CASE WHEN status = 'A' THEN 1 ELSE 0 END = 1";
      CaseInWhereDetector detector = new CaseInWhereDetector();
      List<Issue> issues = detector.evaluate(List.of(q(sql)), emptyIndex);
      assertThat(issues).hasSize(1);
    }

    @Test
    void selectAllStillDetectedAfterCommentStripping() {
      String sql = "SELECT * /* fetch all */ FROM users WHERE id = 1";
      SelectAllDetector detector = new SelectAllDetector();
      List<Issue> issues = detector.evaluate(List.of(q(sql)), emptyIndex);
      assertThat(issues).hasSize(1);
    }

    @Test
    void orAbuseStillDetectedAfterCommentStripping() {
      String sql =
          "SELECT id FROM users /* complex query */ WHERE a = 1 OR b = 2 OR c = 3 OR d = 4";
      OrAbuseDetector detector = new OrAbuseDetector();
      List<Issue> issues = detector.evaluate(List.of(q(sql)), emptyIndex);
      assertThat(issues).hasSize(1);
    }

    @Test
    void normalizePreservesQueryStructureAfterCommentStripping() {
      String sql = "SELECT id FROM users -- comment\nWHERE id = 1";
      String normalized = SqlParser.normalize(sql);
      assertThat(normalized).contains("select id from users");
      assertThat(normalized).contains("where id = ?");
    }
  }
}
