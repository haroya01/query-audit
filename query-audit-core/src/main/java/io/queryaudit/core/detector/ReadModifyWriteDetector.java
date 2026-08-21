package io.queryaudit.core.detector;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.ColumnReference;
import io.queryaudit.core.parser.EnhancedSqlParser;
import io.queryaudit.core.parser.SqlParser;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Detects check-then-act races: a {@code SELECT} without a locking clause followed — in the same
 * test — by an {@code INSERT}/{@code UPDATE} on the same table, with nothing converting the race
 * into a safe operation (issue #169).
 *
 * <p>"Find-then-save" is the default shape of ORM service code, and single-threaded tests can never
 * fail on it; the race only appears under concurrent load — lost updates and duplicate rows. This
 * is the sequence-level completion of the lock-rule family: those judge locks that are taken, this
 * rule judges the sequence where a lock (or constraint) is missing.
 *
 * <p>Exemptions, in the order they are checked:
 *
 * <ul>
 *   <li>the SELECT used {@code FOR UPDATE}/{@code FOR SHARE} — pessimistic locking;
 *   <li>the write is an upsert ({@code ON DUPLICATE KEY UPDATE} / {@code ON CONFLICT}) — the
 *       constraint resolves the race;
 *   <li>{@code INSERT} case: a unique index covers the SELECT's equality predicate — the constraint
 *       converts the race into a catchable error. Without index metadata for the table the INSERT
 *       case stays silent (conservative: no evidence, no finding);
 *   <li>{@code UPDATE} case: the UPDATE's own {@code WHERE} references a version-style column
 *       ({@code version}, {@code opt_lock}, {@code optlock}, {@code revision}) — optimistic
 *       locking, the JPA {@code @Version} shape;
 *   <li>{@code UPDATE} case: self-referential {@code SET col = col - ?} — the update is itself the
 *       atomic form this rule's suggestion prescribes;
 *   <li>{@code UPDATE} case: no equality-column overlap between the read's and the write's
 *       predicates — a read on one key followed by a write on another is not the pattern.
 * </ul>
 *
 * <p>INFO severity: deliberate last-write-wins semantics exist, and this rule cannot see them.
 *
 * @author haroya
 * @since 0.5.0
 */
public class ReadModifyWriteDetector implements DetectionRule {

  private static final Pattern FOR_UPDATE_OR_SHARE =
      Pattern.compile("\\bFOR\\s+(?:UPDATE|SHARE)\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern UPSERT =
      Pattern.compile(
          "\\bON\\s+(?:DUPLICATE\\s+KEY\\s+UPDATE|CONFLICT)\\b", Pattern.CASE_INSENSITIVE);

  private static final Pattern INSERT_TABLE =
      Pattern.compile(
          "\\bINSERT\\s+(?:IGNORE\\s+)?INTO\\s+[`\"]?([\\w.]+)[`\"]?", Pattern.CASE_INSENSITIVE);

  private static final Pattern UPDATE_TABLE =
      Pattern.compile("\\bUPDATE\\s+[`\"]?([\\w.]+)[`\"]?", Pattern.CASE_INSENSITIVE);

  private static final Set<String> VERSION_COLUMNS =
      Set.of("version", "opt_lock", "optlock", "revision");

  /** SET col = col <op> ... — the update is itself an atomic read-modify-write; no race. */
  private static final Pattern SELF_REFERENTIAL_SET =
      Pattern.compile(
          "\\bSET\\b[^=]*?[`\"]?(\\w+)[`\"]?\\s*=\\s*[`\"]?\\1[`\"]?\\s*[-+*/]",
          Pattern.CASE_INSENSITIVE);

  @Override
  public String getRuleCode() {
    return IssueType.READ_MODIFY_WRITE.getCode();
  }

  @Override
  public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
    List<Issue> issues = new ArrayList<>();
    // table -> the most recent unlocked equality-predicate SELECT on it
    Map<String, UnlockedRead> unlockedReads = new HashMap<>();
    Set<String> reported = new LinkedHashSet<>();

    for (QueryRecord query : queries) {
      String sql = query.sql();
      if (sql == null || sql.isBlank()) {
        continue;
      }

      if (SqlParser.isSelectQuery(sql)) {
        recordUnlockedRead(query, unlockedReads);
        continue;
      }

      String table = null;
      boolean isInsert = false;
      if (SqlParser.isInsertQuery(sql)) {
        table = extractTable(INSERT_TABLE, sql);
        isInsert = true;
      } else if (SqlParser.isUpdateQuery(sql)) {
        table = extractTable(UPDATE_TABLE, sql);
      } else {
        continue;
      }
      if (table == null) {
        continue;
      }

      UnlockedRead read = unlockedReads.get(table);
      if (read == null) {
        continue;
      }
      if (UPSERT.matcher(sql).find()) {
        continue; // the constraint resolves the race
      }
      if (isInsert && !insertIsRaceProne(table, read, indexMetadata)) {
        continue;
      }
      if (!isInsert && updateUsesVersionColumn(sql)) {
        continue; // optimistic locking
      }
      if (!isInsert && SELF_REFERENTIAL_SET.matcher(sql).find()) {
        continue; // SET col = col - ? is already the atomic form
      }
      if (!isInsert && !predicatesOverlap(read, sql)) {
        continue; // the write does not address the record the read checked
      }

      String reportKey =
          read.normalizedSql + "->" + (query.normalizedSql() != null ? query.normalizedSql() : sql);
      if (!reported.add(reportKey)) {
        continue;
      }

      String action = isInsert ? "INSERT" : "UPDATE";
      issues.add(
          new Issue(
              IssueType.READ_MODIFY_WRITE,
              Severity.INFO,
              read.normalizedSql,
              table,
              read.firstEqualityColumn,
              "SELECT on '"
                  + table
                  + "' without a locking clause is followed by "
                  + action
                  + " on the same table — correct single-threaded, lost updates or duplicate rows under concurrency",
              isInsert
                  ? "Back the checked predicate with a unique constraint (and handle the violation), or use an upsert (INSERT ... ON DUPLICATE KEY UPDATE / ON CONFLICT)"
                  : "Lock the read (SELECT ... FOR UPDATE), use optimistic locking (@Version), or fold the change into one atomic UPDATE",
              read.stackTrace));
    }

    return issues;
  }

  private void recordUnlockedRead(QueryRecord query, Map<String, UnlockedRead> unlockedReads) {
    String sql = query.sql();
    List<ColumnReference> whereColumns = EnhancedSqlParser.extractWhereColumns(sql);
    if (whereColumns.isEmpty()) {
      return;
    }
    Map<String, String> aliasToTable = MissingIndexDetector.resolveAliases(sql);
    String table = resolveTable(whereColumns.get(0).tableOrAlias(), aliasToTable);
    if (table == null) {
      return;
    }
    if (FOR_UPDATE_OR_SHARE.matcher(sql).find()) {
      // a locked read supersedes any earlier unlocked one on the same table
      unlockedReads.remove(table);
      return;
    }
    Set<String> equalityColumns = new LinkedHashSet<>();
    for (ColumnReference col : whereColumns) {
      equalityColumns.add(col.columnName().toLowerCase(Locale.ROOT));
    }
    unlockedReads.put(
        table,
        new UnlockedRead(
            query.normalizedSql() != null ? query.normalizedSql() : sql,
            equalityColumns,
            whereColumns.get(0).columnName(),
            query.stackTrace()));
  }

  /**
   * The INSERT case fires only with positive evidence: metadata for the table exists and no unique
   * index covers the SELECT's equality predicate. No metadata, no finding.
   */
  private boolean insertIsRaceProne(String table, UnlockedRead read, IndexMetadata indexMetadata) {
    if (indexMetadata == null || !indexMetadata.hasTable(table)) {
      return false;
    }
    return !indexMetadata.columnsMatchUniqueIndex(table, read.equalityColumns);
  }

  /**
   * The UPDATE case only fires when the write's equality predicate shares at least one column with
   * the read's — the classic RMW touches the same key (select by id, update by id). A read on one
   * key followed by a write on another (login by email, later update by id) is not the pattern.
   */
  private boolean predicatesOverlap(UnlockedRead read, String updateSql) {
    for (ColumnReference col : EnhancedSqlParser.extractWhereColumns(updateSql)) {
      if (read.equalityColumns.contains(col.columnName().toLowerCase(Locale.ROOT))) {
        return true;
      }
    }
    return false;
  }

  private boolean updateUsesVersionColumn(String updateSql) {
    for (ColumnReference col : EnhancedSqlParser.extractWhereColumns(updateSql)) {
      if (VERSION_COLUMNS.contains(col.columnName().toLowerCase(Locale.ROOT))) {
        return true;
      }
    }
    return false;
  }

  private static String extractTable(Pattern pattern, String sql) {
    Matcher matcher = pattern.matcher(sql);
    if (!matcher.find()) {
      return null;
    }
    String table = matcher.group(1).toLowerCase(Locale.ROOT);
    int dot = table.lastIndexOf('.');
    return dot >= 0 ? table.substring(dot + 1) : table;
  }

  private String resolveTable(String tableOrAlias, Map<String, String> aliasToTable) {
    if (tableOrAlias != null) {
      String resolved = aliasToTable.get(tableOrAlias.toLowerCase(Locale.ROOT));
      if (resolved != null) {
        return resolved;
      }
      return tableOrAlias.toLowerCase(Locale.ROOT);
    }
    if (aliasToTable.size() <= 2) {
      return aliasToTable.values().stream().findFirst().orElse(null);
    }
    return null;
  }

  private record UnlockedRead(
      String normalizedSql,
      Set<String> equalityColumns,
      String firstEqualityColumn,
      String stackTrace) {}
}
