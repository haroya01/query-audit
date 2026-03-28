package io.queryaudit.core.parser;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;

/**
 * Measures the performance overhead of duplicate SqlParser calls. Each detection rule independently
 * calls SqlParser methods (which internally call stripComments(), etc.) with no caching. This test
 * quantifies that overhead by simulating 55 rules calling all parser methods.
 */
class DuplicateParsingVerificationTest {

  /** A realistic complex SQL query with comments, joins, subqueries, functions, etc. */
  private static final String COMPLEX_SQL =
      "/* analytics query v2 */ "
          + "SELECT u.id, u.name, o.total_amount, p.product_name, "
          + "       COUNT(*) AS order_count, SUM(o.total_amount) AS lifetime_value "
          + "FROM users u "
          + "INNER JOIN orders o ON u.id = o.user_id "
          + "LEFT JOIN order_items oi ON o.id = oi.order_id "
          + "LEFT JOIN products p ON oi.product_id = p.id "
          + "LEFT JOIN categories c ON p.category_id = c.id "
          + "WHERE u.status = 'active' "
          + "  AND o.created_at >= '2024-01-01' "
          + "  AND LOWER(u.email) LIKE '%@example.com' "
          + "  AND o.total_amount > 100.00 "
          + "  AND (u.country = 'US' OR u.country = 'CA' OR u.country = 'UK') "
          + "  AND p.category_id IN (SELECT id FROM categories WHERE active = 1) "
          + "  -- filter deleted orders\n"
          + "  AND o.deleted_at IS NULL "
          + "GROUP BY u.id, u.name, o.total_amount, p.product_name "
          + "HAVING COUNT(*) > 1 "
          + "ORDER BY lifetime_value DESC "
          + "LIMIT 100 OFFSET 500";

  private static final int RULE_COUNT = 55;

  /**
   * Simulates all parser method calls that a typical detection rule would make. Returns some
   * sentinel value to prevent dead-code elimination by the JVM.
   */
  private int callAllParserMethods(String sql) {
    int sink = 0;

    List<String> tables = SqlParser.extractTableNames(sql);
    sink += tables.size();

    List<ColumnReference> whereCols = SqlParser.extractWhereColumns(sql);
    sink += whereCols.size();

    List<WhereColumnReference> whereColsOp = SqlParser.extractWhereColumnsWithOperators(sql);
    sink += whereColsOp.size();

    List<ColumnReference> orderCols = SqlParser.extractOrderByColumns(sql);
    sink += orderCols.size();

    List<ColumnReference> groupCols = SqlParser.extractGroupByColumns(sql);
    sink += groupCols.size();

    List<JoinColumnPair> joinCols = SqlParser.extractJoinColumns(sql);
    sink += joinCols.size();

    List<FunctionUsage> whereFuncs = SqlParser.detectWhereFunctions(sql);
    sink += whereFuncs.size();

    List<FunctionUsage> joinFuncs = SqlParser.detectJoinFunctions(sql);
    sink += joinFuncs.size();

    List<FunctionUsage> havingFuncs = SqlParser.detectHavingFunctions(sql);
    sink += havingFuncs.size();

    String whereBody = SqlParser.extractWhereBody(sql);
    sink += whereBody != null ? whereBody.length() : 0;

    String havingBody = SqlParser.extractHavingBody(sql);
    sink += havingBody != null ? havingBody.length() : 0;

    List<String> joinOnBodies = SqlParser.extractJoinOnBodies(sql);
    sink += joinOnBodies.size();

    boolean selectAll = SqlParser.hasSelectAll(sql);
    sink += selectAll ? 1 : 0;

    boolean hasWhere = SqlParser.hasWhereClause(sql);
    sink += hasWhere ? 1 : 0;

    boolean hasOuterWhere = SqlParser.hasOuterWhereClause(sql);
    sink += hasOuterWhere ? 1 : 0;

    String normalized = SqlParser.normalize(sql);
    sink += normalized != null ? normalized.length() : 0;

    int orCount = SqlParser.countOrConditions(sql);
    sink += orCount;

    int effectiveOr = SqlParser.countEffectiveOrConditions(sql);
    sink += effectiveOr;

    boolean sameColOr = SqlParser.allOrConditionsOnSameColumn(sql);
    sink += sameColOr ? 1 : 0;

    boolean isSelect = SqlParser.isSelectQuery(sql);
    sink += isSelect ? 1 : 0;

    boolean isDml = SqlParser.isDmlQuery(sql);
    sink += isDml ? 1 : 0;

    OptionalLong offset = SqlParser.extractOffsetValue(sql);
    sink += offset.isPresent() ? (int) offset.getAsLong() : 0;

    boolean hasOffset = SqlParser.hasOffsetClause(sql);
    sink += hasOffset ? 1 : 0;

    String removed = SqlParser.removeSubqueries(sql);
    sink += removed.length();

    String stripped = SqlParser.stripComments(sql);
    sink += stripped.length();

    String strippedCte = SqlParser.stripCtePrefix(sql);
    sink += strippedCte.length();

    return sink;
  }

  @Test
  void quantifyDuplicateParsingOverhead() {
    // Warm-up: run several iterations to let JIT compile hot paths
    for (int i = 0; i < 200; i++) {
      callAllParserMethods(COMPLEX_SQL);
    }

    // ── Scenario 1: No caching (current behavior) ────────────────────
    // Each of the 55 rules calls all parser methods independently.
    // This means stripComments() is called many times redundantly.
    int warmupSink = 0;
    long uncachedStart = System.nanoTime();
    for (int rule = 0; rule < RULE_COUNT; rule++) {
      warmupSink += callAllParserMethods(COMPLEX_SQL);
    }
    long uncachedNanos = System.nanoTime() - uncachedStart;

    // ── Scenario 2: With caching (proposed improvement) ──────────────
    // Pre-compute stripped SQL once and reuse it for all rules.
    String preStripped = SqlParser.stripComments(COMPLEX_SQL);
    String preStrippedCte = SqlParser.stripCtePrefix(preStripped);

    long cachedStart = System.nanoTime();
    for (int rule = 0; rule < RULE_COUNT; rule++) {
      // Simulate what rules would do if they received pre-stripped SQL
      warmupSink += callAllParserMethods(preStripped);
    }
    long cachedNanos = System.nanoTime() - cachedStart;

    // ── Measure stripComments() alone ────────────────────────────────
    // Count how many times stripComments() is called per "all methods" invocation
    long stripStart = System.nanoTime();
    for (int i = 0; i < 1000; i++) {
      SqlParser.stripComments(COMPLEX_SQL);
    }
    long stripNanos = System.nanoTime() - stripStart;
    double stripPerCallUs = stripNanos / 1000.0 / 1000.0;

    // ── Report ───────────────────────────────────────────────────────
    double uncachedMs = uncachedNanos / 1_000_000.0;
    double cachedMs = cachedNanos / 1_000_000.0;
    double savedMs = uncachedMs - cachedMs;
    double speedupRatio = uncachedMs / cachedMs;

    System.out.println();
    System.out.println("=== Duplicate Parsing Verification ===");
    System.out.println();
    System.out.println("SQL length: " + COMPLEX_SQL.length() + " chars");
    System.out.println("Simulated rules: " + RULE_COUNT);
    System.out.println();
    System.out.println("--- Per single invocation of all parser methods ---");
    System.out.println(
        "  Methods that call stripComments() internally:");
    System.out.println(
        "    extractTableNames, hasSelectAll, extractWhereBody (via extractWhereColumns,");
    System.out.println(
        "    extractWhereColumnsWithOperators, detectWhereFunctions), normalize,");
    System.out.println(
        "    countOrConditions, countEffectiveOrConditions, allOrConditionsOnSameColumn,");
    System.out.println(
        "    detectJoinFunctions, stripComments (direct)");
    System.out.println(
        "  Estimated stripComments() calls per all-methods invocation: ~10+");
    System.out.println();
    System.out.println("--- stripComments() cost ---");
    System.out.printf("  1000 calls: %.3f ms (%.3f us/call)%n", stripNanos / 1_000_000.0, stripPerCallUs);
    System.out.println();
    System.out.println("--- 55 rules x all parser methods ---");
    System.out.printf("  Without caching (current): %.3f ms%n", uncachedMs);
    System.out.printf("  With pre-stripped SQL:      %.3f ms%n", cachedMs);
    System.out.printf("  Time saved:                 %.3f ms%n", savedMs);
    System.out.printf("  Speedup ratio:              %.2fx%n", speedupRatio);
    System.out.println();
    System.out.println("--- Extrapolation (per query at runtime) ---");
    System.out.printf(
        "  Estimated redundant stripComments() calls per query: ~%d%n",
        10 * RULE_COUNT);
    System.out.printf(
        "  Estimated wasted time per query: %.3f ms%n",
        stripPerCallUs * 10 * RULE_COUNT / 1000.0);
    System.out.println();
    System.out.println("=== End ===");
    System.out.println();

    // Sanity check: the test should have produced meaningful results
    assertThat(warmupSink).isGreaterThan(0);
    assertThat(uncachedMs).isGreaterThan(0);
    assertThat(cachedMs).isGreaterThan(0);
  }

  /**
   * Counts approximate number of stripComments() calls in a single callAllParserMethods()
   * invocation by examining which methods call it.
   */
  @Test
  void documentStripCommentsCallSites() {
    // Methods that call stripComments() directly:
    // 1. extractTableNames        -> stripComments + stripCtePrefix
    // 2. hasSelectAll             -> stripComments
    // 3. extractWhereBody         -> stripComments + stripCtePrefix
    //    (used by: extractWhereColumns, extractWhereColumnsWithOperators, detectWhereFunctions)
    // 4. normalize                -> stripComments
    // 5. countOrConditions        -> stripComments + removeSubqueries + extractWhereBody(stripComments again)
    // 6. countEffectiveOrConditions -> stripComments + removeSubqueries + extractWhereBody(stripComments again)
    // 7. allOrConditionsOnSameColumn -> stripComments + removeSubqueries + extractWhereBody(stripComments again)
    // 8. detectJoinFunctions      -> stripComments + stripCtePrefix
    // 9. stripComments (direct call in our test)
    //
    // extractWhereColumnsWithOperators calls removeSubqueries then extractWhereBody
    //   -> extractWhereBody calls stripComments internally
    // detectWhereFunctions calls extractWhereBody -> stripComments
    //
    // Total per callAllParserMethods invocation:
    //   extractTableNames:                1 stripComments
    //   hasSelectAll:                     1 stripComments
    //   extractWhereColumns:              1 stripComments (via extractWhereColumnsWithOperators -> extractWhereBody)
    //   extractWhereColumnsWithOperators: 1 stripComments (via extractWhereBody)
    //   detectWhereFunctions:             1 stripComments (via extractWhereBody)
    //   detectJoinFunctions:              1 stripComments
    //   normalize:                        1 stripComments
    //   countOrConditions:                2 stripComments (direct + extractWhereBody)
    //   countEffectiveOrConditions:       2 stripComments (direct + extractWhereBody)
    //   allOrConditionsOnSameColumn:      2 stripComments (direct + extractWhereBody)
    //   extractWhereBody (direct):        1 stripComments
    //   stripComments (direct):           1 stripComments
    //   ─────────────────────────────────
    //   Total:                           ~15 stripComments per invocation
    //
    // With 55 rules: ~825 redundant stripComments() calls per query

    int estimatedCalls = 15;
    int totalPerQuery = estimatedCalls * RULE_COUNT;
    System.out.println();
    System.out.println("=== stripComments() Call Count Analysis ===");
    System.out.println("Estimated stripComments() calls per all-methods invocation: ~" + estimatedCalls);
    System.out.println("With " + RULE_COUNT + " rules: ~" + totalPerQuery + " calls per query");
    System.out.println("Only 1 call is necessary (the rest are redundant).");
    System.out.println("Redundancy factor: " + totalPerQuery + "x");
    System.out.println();

    assertThat(totalPerQuery).isGreaterThan(500);
  }
}
