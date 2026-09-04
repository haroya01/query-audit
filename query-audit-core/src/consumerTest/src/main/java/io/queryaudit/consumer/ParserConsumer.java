package io.queryaudit.consumer;

import io.queryaudit.core.parser.ColumnReference;
import io.queryaudit.core.parser.EnhancedSqlParser;
import java.util.List;

/** Exercises the core artifact using only dependencies declared by its published POM. */
public final class ParserConsumer {

  private ParserConsumer() {}

  public static void main(String[] args) {
    String location =
        EnhancedSqlParser.class.getProtectionDomain().getCodeSource().getLocation().getPath();
    require(location.endsWith(".jar"), "The consumer must load the published core JAR");
    require(EnhancedSqlParser.parserName().equals("JSqlParser"), "Unexpected parser name");
    require(EnhancedSqlParser.parserVersion().equals("5.3"), "Unexpected parser version");

    String sql = "SELECT id FROM accounts WHERE COALESCE(closed_at, created_at) > ?";
    List<String> columns =
        EnhancedSqlParser.extractWhereColumns(sql).stream()
            .map(ColumnReference::columnName)
            .toList();
    require(columns.equals(List.of("closed_at", "created_at")), "AST extraction was not used");

    String unsupported = "SELECT id FROM accounts WHERE id = 1 AND";
    require(
        EnhancedSqlParser.extractWhereColumns(unsupported).stream()
            .map(ColumnReference::columnName)
            .toList()
            .equals(List.of("id")),
        "Unsupported statements must retain the regex fallback");
    System.out.println(
        "Published core artifact supplies JSqlParser and preserves statement fallback");
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new AssertionError(message);
    }
  }
}
