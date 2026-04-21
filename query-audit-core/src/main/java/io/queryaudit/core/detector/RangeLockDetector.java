package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
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
 * Detects range SELECT ... FOR UPDATE/FOR SHARE patterns that may cause gap locks in InnoDB.
 *
 * <p>When a query uses FOR UPDATE with a range condition (BETWEEN, >, <, >=, <=) on a column that
 * lacks an index, InnoDB locks all scanned rows and the gaps between them. This can severely impact
 * concurrency.
 *
 * <p>This detector complements {@link ForUpdateWithoutIndexDetector} by specifically flagging range
 * conditions (not just equality) which are more dangerous because gap locks can block insertions
 * into the locked range.
 *
 * @author haroya
 * @since 0.2.0
 */
public class RangeLockDetector implements DetectionRule {

  private static final Pattern FOR_UPDATE_OR_SHARE =
      Pattern.compile("\\bFOR\\s+(?:UPDATE|SHARE)\\b", Pattern.CASE_INSENSITIVE);

  /** Range operators that cause gap locks in InnoDB. */
  private static final Set<String> RANGE_OPERATORS = Set.of(">", "<", ">=", "<=", "BETWEEN");

  private static final Pattern FROM_ALIAS =
      Pattern.compile("\\bFROM\\s+(\\w+)(?:\\s+(?:AS\\s+)?(\\w+))?", Pattern.CASE_INSENSITIVE);

  private static final Pattern JOIN_ALIAS =
      Pattern.compile("\\bJOIN\\s+(\\w+)(?:\\s+(?:AS\\s+)?(\\w+))?", Pattern.CASE_INSENSITIVE);

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();

    if (indexMetadata == null || indexMetadata.isEmpty()) {
      return issues;
    }

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

      // Must contain FOR UPDATE or FOR SHARE
      if (!FOR_UPDATE_OR_SHARE.matcher(sql).find()) {
        continue;
      }

      // Extract WHERE columns with operators
      List<WhereColumnReference> whereColumns = EnhancedSqlParser.extractWhereColumnsWithOperators(sql);
      if (whereColumns.isEmpty()) {
        continue; // No WHERE clause -- ForUpdateWithoutIndexDetector handles this
      }

      Map<String, String> aliasToTable = resolveAliases(sql);

      for (WhereColumnReference col : whereColumns) {
        String operator = col.operator() != null ? col.operator().trim().toUpperCase() : "";

        // Only flag range operators
        if (!RANGE_OPERATORS.contains(operator)) {
          continue;
        }

        String table = resolveTable(col.tableOrAlias(), aliasToTable);
        if (table == null || !indexMetadata.hasTable(table)) {
          continue;
        }

        // Only flag if the range column lacks an index
        if (indexMetadata.hasIndexOn(table, col.columnName())) {
          continue;
        }

        issues.add(
            new Issue(
                IssueType.RANGE_LOCK_RISK,
                Severity.WARNING,
                normalized,
                table,
                col.columnName(),
                "Range condition with FOR UPDATE on unindexed column '"
                    + col.columnName()
                    + "' may cause gap locks on entire table",
                "Add index on the range column to limit lock scope. Without an index, "
                    + "InnoDB locks all scanned rows and gaps: "
                    + "ALTER TABLE "
                    + table
                    + " ADD INDEX idx_"
                    + col.columnName()
                    + " ("
                    + col.columnName()
                    + ");",
                query.stackTrace()));
      }
    }

    return issues;
  }

  private Map<String, String> resolveAliases(String sql) {
    Map<String, String> aliasToTable = new HashMap<>();

    Matcher fromMatcher = FROM_ALIAS.matcher(sql);
    while (fromMatcher.find()) {
      String table = fromMatcher.group(1);
      String alias = fromMatcher.group(2);
      aliasToTable.put(table.toLowerCase(), table.toLowerCase());
      if (alias != null) {
        aliasToTable.put(alias.toLowerCase(), table.toLowerCase());
      }
    }

    Matcher joinMatcher = JOIN_ALIAS.matcher(sql);
    while (joinMatcher.find()) {
      String table = joinMatcher.group(1);
      String alias = joinMatcher.group(2);
      aliasToTable.put(table.toLowerCase(), table.toLowerCase());
      if (alias != null) {
        aliasToTable.put(alias.toLowerCase(), table.toLowerCase());
      }
    }

    return aliasToTable;
  }

  private String resolveTable(String tableOrAlias, Map<String, String> aliasToTable) {
    if (tableOrAlias != null) {
      String resolved = aliasToTable.get(tableOrAlias.toLowerCase());
      if (resolved != null) return resolved;
      return tableOrAlias.toLowerCase();
    }
    if (aliasToTable.size() <= 2) {
      return aliasToTable.values().stream().findFirst().orElse(null);
    }
    return null;
  }
}
