package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.EnhancedSqlParser;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class CallSiteNPlusOneDetector implements DetectionRule {
  private final int threshold;

  public CallSiteNPlusOneDetector(int threshold) {
    this.threshold = threshold;
  }

  public CallSiteNPlusOneDetector() {
    this(3);
  }

  @Override
  public String getRuleCode() {
    return IssueType.N_PLUS_ONE.getCode();
  }

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    Map<CallSite, List<QueryRecord>> byCallSite = new LinkedHashMap<>();
    for (QueryRecord query : queries) {
      if (query.normalizedSql() == null || query.stackTrace() == null) continue;
      if (query.stackTrace().isEmpty() || !SqlParser.isSelectQuery(query.sql())) continue;
      if (hasBatchedInList(query.sql())) continue;
      byCallSite
          .computeIfAbsent(
              new CallSite(query.normalizedSql(), query.fullStackHash(), query.stackTrace()),
              key -> new ArrayList<>())
          .add(query);
    }

    List<Issue> issues = new ArrayList<>();
    for (Map.Entry<CallSite, List<QueryRecord>> entry : byCallSite.entrySet()) {
      List<QueryRecord> repeated = entry.getValue();
      if (repeated.size() < threshold) continue;
      QueryRecord first = repeated.get(0);
      String table =
          EnhancedSqlParser.extractTableNames(first.normalizedSql()).stream()
              .findFirst()
              .orElse(null);
      issues.add(
          new Issue(
              IssueType.N_PLUS_ONE,
              Severity.ERROR,
              first.sql(),
              table,
              null,
              String.format(
                  "The same SELECT ran %d times from one call site", repeated.size()),
              "Load the rows once before the loop: JOIN FETCH, @EntityGraph, or one query with"
                  + " an IN list.",
              first.stackTrace()));
    }
    return issues;
  }

  static boolean hasBatchedInList(String sql) {
    String lower = sql.toLowerCase(Locale.ROOT);
    int from = 0;
    while (true) {
      int in = lower.indexOf("in", from);
      if (in < 0) return false;
      from = in + 2;
      if (in > 0 && Character.isLetterOrDigit(lower.charAt(in - 1))) continue;
      int i = skipWhitespace(lower, in + 2);
      if (i >= lower.length() || lower.charAt(i) != '(') continue;
      int placeholders = 0;
      boolean expectPlaceholder = true;
      for (i = skipWhitespace(lower, i + 1); i < lower.length(); i = skipWhitespace(lower, i + 1)) {
        char c = lower.charAt(i);
        if (c == '?' && expectPlaceholder) {
          placeholders++;
          expectPlaceholder = false;
        } else if (c == ',' && !expectPlaceholder) {
          expectPlaceholder = true;
        } else {
          if (c == ')' && !expectPlaceholder && placeholders > 1) return true;
          break;
        }
      }
    }
  }

  private static int skipWhitespace(String text, int index) {
    while (index < text.length() && Character.isWhitespace(text.charAt(index))) index++;
    return index;
  }

  private record CallSite(String normalizedSql, int fullStackHash, String stackTrace) {}
}
