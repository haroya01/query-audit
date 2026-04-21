package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.parser.EnhancedSqlParser;
import io.queryaudit.core.parser.SqlParser;
import io.queryaudit.core.parser.WhereColumnReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects redundant duplicate WHERE conditions in a query.
 *
 * <p>Based on real-world observation from Hibernate {@code @Filter} + JPQL double-applying the same
 * condition (e.g., {@code WHERE deleted_at IS NULL AND ... AND deleted_at IS NULL}).
 *
 * <p>Duplicate conditions waste optimizer time and indicate a configuration issue.
 *
 * @author haroya
 * @since 0.2.0
 */
public class RedundantFilterDetector implements DetectionRule {

  // WHERE clause extraction delegated to EnhancedSqlParser.extractWhereBody() to avoid
  // catastrophic backtracking from (.+?) with DOTALL patterns.

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();
    Set<String> seen = new LinkedHashSet<>();

    for (QueryRecord query : queries) {
      String normalized = query.normalizedSql();
      if (normalized == null || seen.contains(normalized)) {
        continue;
      }
      seen.add(normalized);

      String sql = query.sql();
      if (sql == null) {
        continue;
      }

      // Build alias-to-table mapping for correct table resolution
      Map<String, String> aliasToTable = MissingIndexDetector.resolveAliases(sql);

      List<WhereColumnReference> whereColumns = EnhancedSqlParser.extractWhereColumnsWithOperators(sql);
      if (whereColumns.size() < 2) {
        continue;
      }

      // Check if the WHERE clause contains top-level OR conditions
      List<String> orBranches = splitByTopLevelOr(sql);
      boolean hasOrBranches = orBranches.size() > 1;

      // Group by normalized key: (resolved_table + column + operator)
      // This prevents false positives from different tables sharing the same column name
      // (e.g., rooms.id vs story_trading.id in a JOIN query)
      Map<String, Integer> conditionCounts = new HashMap<>();
      Map<String, String> keyToResolvedTable = new HashMap<>();
      for (WhereColumnReference ref : whereColumns) {
        String resolvedTable = resolveTable(ref.tableOrAlias(), aliasToTable);
        String tableKey = resolvedTable != null ? resolvedTable : "_unresolved_";
        String key =
            tableKey
                + ":"
                + ref.columnName().toLowerCase()
                + ":"
                + ref.operator().trim().toLowerCase();
        conditionCounts.merge(key, 1, Integer::sum);
        keyToResolvedTable.putIfAbsent(key, resolvedTable);
      }

      // Report duplicates, but skip columns that appear in different OR branches
      Set<String> reported = new LinkedHashSet<>();
      for (Map.Entry<String, Integer> entry : conditionCounts.entrySet()) {
        if (entry.getValue() > 1 && !reported.contains(entry.getKey())) {
          // Parse the key: table:column:operator
          String[] parts = entry.getKey().split(":", 3);
          String column = parts[1];
          String operator = parts[2];

          // If there are OR branches, check whether the duplicates span different branches
          if (hasOrBranches && appearsInDifferentOrBranches(column, operator, orBranches)) {
            continue; // Not redundant: same column in different OR branches
          }

          reported.add(entry.getKey());

          // Use the resolved table from the alias map, not just the first FROM table
          String table = keyToResolvedTable.get(entry.getKey());
          if (table == null) {
            List<String> tables = EnhancedSqlParser.extractTableNames(sql);
            table = tables.isEmpty() ? null : tables.get(0);
          }

          issues.add(
              new Issue(
                  IssueType.REDUNDANT_FILTER,
                  IssueType.REDUNDANT_FILTER.getDefaultSeverity(),
                  normalized,
                  table,
                  column,
                  "Duplicate WHERE condition on column '"
                      + column
                      + "' with operator '"
                      + operator.toUpperCase()
                      + "' appears "
                      + entry.getValue()
                      + " times",
                  "Remove the duplicate condition. This often happens when Hibernate @Filter "
                      + "and JPQL both apply the same soft-delete filter.",
                  query.stackTrace()));
        }
      }
    }

    return issues;
  }

  /**
   * Resolve an alias to its actual table name using the alias map. Returns null if unresolvable.
   */
  private String resolveTable(String tableOrAlias, Map<String, String> aliasToTable) {
    if (tableOrAlias == null) {
      return null;
    }
    String resolved = aliasToTable.get(tableOrAlias.toLowerCase());
    if (resolved != null) {
      return resolved;
    }
    // Don't use unresolved Hibernate aliases (e.g., m1_0, r1_0) as table names
    if (tableOrAlias.matches("(?i)[a-z]{1,3}\\d+_\\d+")) {
      return null;
    }
    return tableOrAlias.toLowerCase();
  }

  /**
   * Splits the WHERE clause body by top-level OR keywords (respecting parenthesis depth). Also
   * handles the common pattern where the OR group is wrapped in one extra level of parentheses:
   * {@code ((A = ? AND B = ?) OR (A = ? AND B = ?)) AND C = ?}. In that case, the content inside
   * the outermost parenthesized group is extracted and split by its local top-level OR.
   *
   * @return list of OR branch strings; single-element list if no ORs found
   */
  // Package-private for testability
  List<String> splitByTopLevelOr(String sql) {
    String whereBody = EnhancedSqlParser.extractWhereBody(sql);
    if (whereBody == null) {
      return List.of(sql);
    }

    // Try depth 0 first
    List<String> branches = splitByOrAtDepth(whereBody, 0);
    if (branches.size() > 1) {
      return branches;
    }

    // No top-level OR found. Check if there is a leading parenthesized group
    // that contains the OR, e.g., "((A) OR (B)) AND C".
    // Extract the content of the first balanced parenthesized group and split it.
    String trimmed = whereBody.trim();
    if (trimmed.startsWith("(")) {
      String inner = extractBalancedParenContent(trimmed);
      if (inner != null) {
        List<String> innerBranches = splitByOrAtDepth(inner, 0);
        if (innerBranches.size() > 1) {
          return innerBranches;
        }
      }
    }

    return branches;
  }

  /**
   * Extract the content inside the first balanced parenthesized group. E.g., for "((A) OR (B)) AND
   * C" returns "(A) OR (B)". Returns null if no balanced group is found.
   */
  // Package-private for testability
  String extractBalancedParenContent(String s) {
    if (!s.startsWith("(")) {
      return null;
    }
    int depth = 0;
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      if (c == '(') {
        depth++;
      } else if (c == ')') {
        depth--;
        if (depth == 0) {
          // Found the matching close paren
          return s.substring(1, i);
        }
      }
    }
    return null;
  }

  /** Split the string by OR keywords found at exactly depth 0 (top level). */
  // Package-private for testability
  List<String> splitByOrAtDepth(String body, int targetDepth) {
    List<String> branches = new ArrayList<>();
    int depth = 0;
    int start = 0;
    String upper = body.toUpperCase();

    for (int i = 0; i < body.length(); i++) {
      char c = body.charAt(i);
      if (c == '(') {
        depth++;
      } else if (c == ')') {
        depth--;
      } else if (depth == targetDepth && i + 2 < body.length()) {
        // Check for OR keyword (word boundary aware)
        if ((upper.charAt(i) == 'O' && upper.charAt(i + 1) == 'R')
            && (i == 0 || !Character.isLetterOrDigit(body.charAt(i - 1)))
            && (i + 2 >= body.length() || !Character.isLetterOrDigit(body.charAt(i + 2)))) {
          branches.add(body.substring(start, i));
          start = i + 2; // skip "OR"
        }
      }
    }
    branches.add(body.substring(start));
    return branches;
  }

  /**
   * Checks whether a given column+operator combination appears in more than one OR branch with
   * <b>genuinely different</b> right-hand-side values. Duplicates that span branches but compare
   * the column to the same literal (e.g. {@code id = 1 OR id = 1}) are tautologies, not a
   * bidirectional pattern, and must still be reported as redundant (issue #93).
   *
   * <p>When the RHS is a parameter placeholder ({@code ?}) we cannot tell apart runtime values,
   * so we conservatively treat those cases as legitimately different branches to avoid false
   * positives on parameterized queries.
   */
  // Captures everything after the operator up to the next boolean keyword or closing paren so
  // the RHS comparison sees, e.g., "1" rather than "1 AND".
  private static final Pattern RHS_TERMINATOR =
      Pattern.compile("\\s+(?:AND|OR)\\b|[)]|$", Pattern.CASE_INSENSITIVE);

  private boolean appearsInDifferentOrBranches(
      String column, String operator, List<String> orBranches) {
    Pattern colPattern =
        Pattern.compile(
            "(?:(?:\\w+)\\.)?\\b"
                + Pattern.quote(column)
                + "\\b\\s*"
                + Pattern.quote(operator)
                + "\\s*",
            Pattern.CASE_INSENSITIVE);

    Set<String> distinctRhs = new LinkedHashSet<>();
    int branchesWithMatch = 0;
    for (String branch : orBranches) {
      Matcher m = colPattern.matcher(branch);
      if (m.find()) {
        branchesWithMatch++;
        distinctRhs.add(extractRhs(branch, m.end()));
      }
    }
    if (branchesWithMatch < 2) {
      return false;
    }
    // Parameter placeholder: runtime values unknown, treat conservatively as different branches.
    if (distinctRhs.contains("?")) {
      return true;
    }
    // Different literal values → genuinely different conditions; identical literal → tautology.
    return distinctRhs.size() > 1;
  }

  private static String extractRhs(String branch, int fromIndex) {
    String tail = branch.substring(fromIndex);
    Matcher terminator = RHS_TERMINATOR.matcher(tail);
    int end = terminator.find() ? terminator.start() : tail.length();
    return tail.substring(0, end).trim().toLowerCase();
  }
}
