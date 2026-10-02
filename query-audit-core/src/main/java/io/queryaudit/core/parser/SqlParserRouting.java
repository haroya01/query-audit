package io.queryaudit.core.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/** One fallback policy for structural operations; this is not a public parser SPI. */
final class SqlParserRouting {
  static final int AST_LENGTH_LIMIT = 10_000;
  private static final System.Logger LOG = System.getLogger(EnhancedSqlParser.class.getName());

  private SqlParserRouting() {}

  @FunctionalInterface
  interface Extraction<T> {
    T extract(String sql) throws Exception;
  }

  static <E> List<E> list(String sql, Extraction<List<E>> ast, Function<String, List<E>> fallback) {
    return extract(sql, ArrayList::new, ast, fallback);
  }

  static String text(String sql, Extraction<String> ast, Function<String, String> fallback) {
    return extract(sql, () -> null, ast, fallback);
  }

  /**
   * A structural predicate whose false result must stay conservative. There is no text fallback:
   * when the parser cannot prove the structure, the predicate reports {@code false} so callers keep
   * treating the SQL as unbounded rather than guessing from keyword text.
   */
  static boolean flag(String sql, Extraction<Boolean> ast) {
    return extract(sql, () -> Boolean.FALSE, ast, ignored -> Boolean.FALSE);
  }

  static String rewriteSubqueries(String sql) {
    if (sql != null && !SqlSourceScanner.containsNestedSelect(sql)) {
      return sql;
    }
    return text(sql, SqlAstSubqueries::removeSubqueries, SqlStatementScope::removeSubqueries);
  }

  private static <T> T extract(
      String sql, Supplier<T> nullResult, Extraction<T> ast, Function<String, T> fallback) {
    if (sql == null) {
      return nullResult.get();
    }
    if (sql.length() <= AST_LENGTH_LIMIT) {
      try {
        return ast.extract(sql);
      } catch (Exception | StackOverflowError failure) {
        // Preserve the historical SQL-failure contract, including AST traversal failures. Linkage,
        // initialization and other VM errors are deliberately NOT hidden by regex fallback.
        LOG.log(
            System.Logger.Level.DEBUG,
            "JSqlParser failed, applying operation fallback: {0}",
            failure.getMessage());
      }
    }
    return fallback.apply(sql);
  }

  static Exception unsupportedStatement() {
    return UnsupportedStatement.INSTANCE;
  }

  /**
   * An AST operation can decline a supported parser statement without choosing its own fallback.
   */
  private static final class UnsupportedStatement extends Exception {
    static final UnsupportedStatement INSTANCE = new UnsupportedStatement();
    private static final long serialVersionUID = 1L;

    private UnsupportedStatement() {
      super("unsupported statement shape", null, false, false);
    }
  }
}
