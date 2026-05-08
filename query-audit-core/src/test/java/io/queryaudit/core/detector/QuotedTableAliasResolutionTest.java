package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Regression for #82: when Hibernate emits SQL whose FROM/JOIN clause uses backtick- or
 * double-quoted identifiers, {@code MissingIndexDetector.resolveAliases} (regex-based) cannot
 * register the alias. The {@code resolveTable} fallback then drops Hibernate-pattern aliases on
 * the floor (returns {@code null}), silently skipping legitimate missing-index issues.
 *
 * <p>The detectors that share {@code MissingIndexDetector.resolveAliases} are:
 * MissingIndexDetector, CompositeIndexDetector, RedundantFilterDetector, ForUpdateWithoutIndexDetector.
 */
class QuotedTableAliasResolutionTest {

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  private static IndexInfo pk(String table, String column) {
    return new IndexInfo(table, "PRIMARY", column, 1, false, 1000);
  }

  private static IndexMetadata metadata(IndexInfo... infos) {
    Map<String, List<IndexInfo>> map = new HashMap<>();
    for (IndexInfo info : infos) {
      map.computeIfAbsent(info.tableName(), k -> new ArrayList<>()).add(info);
    }
    return new IndexMetadata(map);
  }

  @Test
  @DisplayName("Backtick-quoted FROM table with Hibernate alias: missing-index still reported")
  void backtickQuotedTable_hibernateAlias_reportsMissingIndex() {
    // messages has a PK on id but no index on user_id.
    IndexMetadata meta = metadata(pk("messages", "id"));

    // Hibernate with hibernate.globally_quoted_identifiers=true (or with reserved-word
    // table names) emits backtick-quoted table identifiers.
    String sql = "SELECT m1_0.id FROM `messages` m1_0 WHERE m1_0.user_id = ?";

    List<Issue> issues = new MissingIndexDetector().evaluate(List.of(record(sql)), meta);

    assertThat(issues)
        .as("missing index on messages.user_id should be reported even when FROM is quoted")
        .anyMatch(
            i ->
                i.type() == IssueType.MISSING_WHERE_INDEX
                    && "messages".equalsIgnoreCase(i.table())
                    && "user_id".equalsIgnoreCase(i.column()));
  }

  @Test
  @DisplayName("Double-quoted FROM table with Hibernate alias: missing-index still reported")
  void doubleQuotedTable_hibernateAlias_reportsMissingIndex() {
    IndexMetadata meta = metadata(pk("messages", "id"));

    String sql = "SELECT m1_0.id FROM \"messages\" m1_0 WHERE m1_0.user_id = ?";

    List<Issue> issues = new MissingIndexDetector().evaluate(List.of(record(sql)), meta);

    assertThat(issues)
        .anyMatch(
            i ->
                i.type() == IssueType.MISSING_WHERE_INDEX
                    && "messages".equalsIgnoreCase(i.table())
                    && "user_id".equalsIgnoreCase(i.column()));
  }

  @Test
  @DisplayName("Backtick-quoted JOIN table with Hibernate alias: redundant filter still reported")
  void backtickQuotedJoin_hibernateAlias_reportsRedundantFilter() {
    // True redundancy on the joined table — should be reported under the joined table's name.
    String sql =
        "SELECT m1_0.id FROM messages m1_0 "
            + "JOIN `rooms` r1_0 ON m1_0.room_id = r1_0.id "
            + "WHERE r1_0.status = 'ACTIVE' AND r1_0.status = 'ACTIVE'";

    RedundantFilterDetector detector = new RedundantFilterDetector();
    List<Issue> issues = detector.evaluate(List.of(record(sql)), new IndexMetadata(Map.of()));

    assertThat(issues)
        .anyMatch(
            i ->
                "rooms".equalsIgnoreCase(i.table()) && "status".equalsIgnoreCase(i.column()));
  }
}
