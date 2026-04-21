package io.queryaudit.core.parser;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Compares EnhancedSqlParser (JSqlParser-backed) to the regex SqlParser on the exact bugs
 * currently filed as open issues (#102, #103, #54). If Enhanced covers them, migrating the
 * remaining detectors to EnhancedSqlParser resolves those issues without new parser work.
 */
class EnhancedVsRegexSpotCheckTest {

  @Test
  @DisplayName("JSqlParser is on classpath in test context")
  void jsqlparserAvailable() {
    assertThat(EnhancedSqlParser.isJSqlParserAvailable()).isTrue();
  }

  // #103 — quoted schema-qualified identifier
  @Test
  @DisplayName("#103 spot: extractTableNames on \"schema\".\"table\"")
  void issue103() {
    String sql = "SELECT * FROM \"myschema\".\"mytable\" WHERE id = 1";

    List<String> regex = SqlParser.extractTableNames(sql);
    List<String> enhanced = EnhancedSqlParser.extractTableNames(sql);

    System.out.println("[#103] regex    = " + regex);
    System.out.println("[#103] enhanced = " + enhanced);
    // Question: does enhanced yield [mytable] instead of [myschema]?
  }

  // #102 — ORDER BY column extraction around a literal with comma.
  // EnhancedSqlParser currently has no extractOrderByColumns. Pin that gap.
  @Test
  @DisplayName("#102 coverage gap: EnhancedSqlParser has no extractOrderByColumns")
  void issue102GapCheck() {
    // Nothing to call on EnhancedSqlParser for ORDER BY — it's a regex-only path today.
    String sql = "SELECT * FROM t ORDER BY name, 'a,b', created_at";
    List<ColumnReference> cols = SqlParser.extractOrderByColumns(sql);
    System.out.println("[#102] regex order-by = " + cols);
    // Pin: phantom columns come from regex. Migration target: add Enhanced.extractOrderByColumns.
  }

  // #54 worst case — literal containing '(SELECT' + a later ')' in another literal
  @Test
  @DisplayName("#54 coverage gap: EnhancedSqlParser has no removeSubqueries")
  void issue54GapCheck() {
    String sql =
        "SELECT * FROM logs WHERE note = '(SELECT ' AND status = 'ok)' AND y = 1";
    String regex = SqlParser.removeSubqueries(sql);
    System.out.println("[#54] regex = " + regex);
    // Pin: removeSubqueries has no Enhanced replacement. Migration target: add it.
  }
}
