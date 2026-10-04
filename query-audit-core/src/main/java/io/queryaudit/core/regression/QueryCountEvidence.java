package io.queryaudit.core.regression;

import io.queryaudit.core.interceptor.CallSites;
import io.queryaudit.core.model.QueryRecord;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Lists the statements behind a failed query budget or contract, one line per statement shape and
 * call site, most repeated first.
 *
 * @author haroya
 * @since 0.8.0
 */
public final class QueryCountEvidence {

  private static final int MAX_SQL_LENGTH = 100;

  private QueryCountEvidence() {}

  /**
   * Appends one {@code <count>x <sql>} line per statement shape and call site, followed by the
   * call site. Groups with equal counts keep the order of their first execution.
   */
  public static void appendGrouped(StringBuilder sb, List<QueryRecord> queries, String indent) {
    Map<Key, Group> groups = new LinkedHashMap<>();
    for (QueryRecord query : queries) {
      String callSite = CallSites.callSite(query.stackTrace());
      String shape = query.normalizedSql() != null ? query.normalizedSql() : query.sql();
      groups.computeIfAbsent(new Key(shape, callSite), key -> new Group(query.sql(), callSite))
          .count++;
    }
    List<Group> ordered = new ArrayList<>(groups.values());
    ordered.sort(Comparator.comparingInt((Group group) -> group.count).reversed());
    for (Group group : ordered) {
      String prefix = group.count + "x ";
      sb.append(indent).append(prefix).append(truncate(group.sql)).append('\n');
      if (group.callSite != null) {
        sb.append(indent)
            .append(" ".repeat(prefix.length()))
            .append("at ")
            .append(group.callSite)
            .append('\n');
      }
    }
  }

  private static String truncate(String sql) {
    return sql.length() > MAX_SQL_LENGTH ? sql.substring(0, MAX_SQL_LENGTH) + "..." : sql;
  }

  private record Key(String shape, String callSite) {}

  private static final class Group {
    final String sql;
    final String callSite;
    int count;

    Group(String sql, String callSite) {
      this.sql = sql;
      this.callSite = callSite;
    }
  }
}
