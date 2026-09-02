package io.queryaudit.core.regression;

import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.parser.SqlParser;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Snapshot contracts for per-test query behavior (issue #166).
 *
 * <p>What the baseline does for <em>findings</em>, contracts do for <em>behavior</em>: a recorded
 * contract freezes each test's query profile (SELECT/INSERT/UPDATE/DELETE counts), and any change —
 * in either direction — fails with a delta until the contract is explicitly re-recorded. A
 * regression detector only catches increases; a contract catches every deviation, which is what
 * makes it reviewable: the re-recorded file's diff is the change.
 *
 * <p>Storage reuses the {@link QueryCountBaseline} pipe-separated format on a separate file
 * ({@value #DEFAULT_FILE_NAME}). Tests without a recorded entry are not enforced — new tests never
 * fail retroactively; re-recording picks them up.
 *
 * @author haroya
 * @since 0.5.0
 */
public final class QueryContracts {

  /** Default contracts file name. */
  public static final String DEFAULT_FILE_NAME = ".query-audit-contracts";

  private QueryContracts() {
    /* utility class */
  }

  /**
   * Compares a test's actual counts against its recorded contract.
   *
   * @return a human-readable failure message with the full delta, or {@code null} when the contract
   *     is met or no contract exists for this test
   */
  public static String verify(
      String testClass,
      String testName,
      QueryCounts actual,
      Map<String, QueryCounts> contracts,
      List<QueryRecord> queries) {
    return verify(null, testClass, testName, actual, contracts, queries);
  }

  /**
   * Compares a test using its stable ID, with an exact 0.5 class/display-name fallback for legacy
   * contract files.
   *
   * @since 0.6.0
   */
  public static String verify(
      String testId,
      String testClass,
      String testName,
      QueryCounts actual,
      Map<String, QueryCounts> contracts,
      List<QueryRecord> queries) {
    QueryCounts contract =
        testId == null
            ? contracts.get(QueryCountBaseline.legacyKey(testClass, testName))
            : QueryCountBaseline.find(contracts, testId, testClass, testName);
    if (contract == null || contract.equals(actual)) {
      return null;
    }

    StringBuilder sb = new StringBuilder();
    sb.append("QueryAudit: ")
        .append(testName)
        .append(" deviates from its recorded query contract (")
        .append(DEFAULT_FILE_NAME)
        .append(").\n");
    appendTypeDelta(
        sb,
        "SELECT",
        contract.selectCount(),
        actual.selectCount(),
        queries,
        SqlParser::isSelectQuery);
    appendTypeDelta(
        sb,
        "INSERT",
        contract.insertCount(),
        actual.insertCount(),
        queries,
        SqlParser::isInsertQuery);
    appendTypeDelta(
        sb,
        "UPDATE",
        contract.updateCount(),
        actual.updateCount(),
        queries,
        SqlParser::isUpdateQuery);
    appendTypeDelta(
        sb,
        "DELETE",
        contract.deleteCount(),
        actual.deleteCount(),
        queries,
        SqlParser::isDeleteQuery);
    sb.append(
        "If the change is intended, re-record the contracts with"
            + " -DqueryAudit.contracts.record=true and review the file diff.");
    return sb.toString();
  }

  /**
   * Appends one delta line when the type deviates; when the count grew, every query of that type is
   * listed with its call site so the offender is identifiable without re-running.
   */
  private static void appendTypeDelta(
      StringBuilder sb,
      String type,
      int expected,
      int actual,
      List<QueryRecord> queries,
      Predicate<String> typeMatcher) {
    if (expected == actual) {
      return;
    }
    sb.append("  ")
        .append(type)
        .append(": contract ")
        .append(expected)
        .append(", executed ")
        .append(actual)
        .append(actual > expected ? " (+" + (actual - expected) + ")" : "")
        .append('\n');
    if (actual > expected) {
      for (QueryRecord query : queries) {
        if (!typeMatcher.test(query.sql())) {
          continue;
        }
        String sql = query.sql();
        sb.append("    ").append(sql.length() > 100 ? sql.substring(0, 100) + "..." : sql);
        String callSite = firstStackFrame(query.stackTrace());
        if (callSite != null) {
          sb.append("\n      at ").append(callSite);
        }
        sb.append('\n');
      }
    }
  }

  private static String firstStackFrame(String stackTrace) {
    if (stackTrace == null || stackTrace.isEmpty()) {
      return null;
    }
    int newline = stackTrace.indexOf('\n');
    return newline < 0 ? stackTrace : stackTrace.substring(0, newline);
  }
}
