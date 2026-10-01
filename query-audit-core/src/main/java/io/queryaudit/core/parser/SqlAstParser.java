package io.queryaudit.core.parser;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

/**
 * Owns required parser identity and the bounded success/failure parse caches, not tree traversal.
 */
final class SqlAstParser {
  private static final String VERSION = readParserVersion();

  static String version() {
    return VERSION;
  }

  private static String readParserVersion() {
    String metadata = "/META-INF/maven/com.github.jsqlparser/jsqlparser/pom.properties";
    try (InputStream input = CCJSqlParserUtil.class.getResourceAsStream(metadata)) {
      if (input == null) {
        throw new IllegalStateException("JSqlParser version metadata is missing");
      }
      Properties properties = new Properties();
      properties.load(input);
      String version = properties.getProperty("version");
      if (version == null || version.isBlank()) {
        throw new IllegalStateException("JSqlParser version metadata contains no version");
      }
      return version.trim();
    } catch (IOException e) {
      throw new IllegalStateException("Could not read JSqlParser version metadata", e);
    }
  }

  private SqlAstParser() {}

  /** Bounded parse cache; both successful and failed parses are memoised per SQL string. */
  private static final int PARSE_CACHE_MAX = 4096;

  private static final ConcurrentMap<String, Statement> PARSE_CACHE = new ConcurrentHashMap<>();

  private static final ConcurrentMap<String, Boolean> PARSE_FAILED = new ConcurrentHashMap<>();

  static Statement parse(String sql) throws Exception {
    if (PARSE_FAILED.containsKey(sql)) {
      throw CachedParseFailure.INSTANCE;
    }
    Statement cached = PARSE_CACHE.get(sql);
    if (cached != null) {
      return cached;
    }
    try {
      Statement stmt = CCJSqlParserUtil.parse(sql);
      if (PARSE_CACHE.size() < PARSE_CACHE_MAX) {
        PARSE_CACHE.putIfAbsent(sql, stmt);
      }
      return stmt;
    } catch (Exception e) {
      if (PARSE_FAILED.size() < PARSE_CACHE_MAX) {
        PARSE_FAILED.putIfAbsent(sql, Boolean.TRUE);
      }
      throw e;
    }
  }

  /** Stackless marker re-thrown for an SQL we've already seen JSqlParser reject. */
  private static final class CachedParseFailure extends Exception {
    private static final long serialVersionUID = 1L;
    static final CachedParseFailure INSTANCE = new CachedParseFailure();

    private CachedParseFailure() {
      super("cached parse failure", null, false, false);
    }
  }
}
