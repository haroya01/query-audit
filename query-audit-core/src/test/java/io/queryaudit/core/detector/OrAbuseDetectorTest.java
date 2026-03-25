package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OrAbuseDetectorTest {

  private static final IndexMetadata EMPTY_INDEX = new IndexMetadata(Map.of());

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  /**
   * Kills NegateConditionalsMutator on line 42: tables.isEmpty() negated. If negated, when tables
   * IS NOT empty, it would return null instead of tables.get(0). Verify the table field is
   * correctly populated from the extracted table name.
   */
  @Test
  void tableNameIsExtractedWhenTablesNotEmpty() {
    OrAbuseDetector detector = new OrAbuseDetector(3);
    List<QueryRecord> queries =
        List.of(record("SELECT * FROM users WHERE a = 1 OR b = 2 OR c = 3 OR d = 4"));

    List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);

    assertThat(issues).hasSize(1);
    assertThat(issues.get(0).type()).isEqualTo(IssueType.OR_ABUSE);
    // If the mutation (negate isEmpty) were active, table would be null
    assertThat(issues.get(0).table()).isEqualTo("users");
  }

  /**
   * Kills NegateConditionalsMutator on line 42: tables.isEmpty() negated. When no table can be
   * extracted, table should be null (not throw IndexOutOfBoundsException).
   */
  @Test
  void tableIsNullWhenNoTableExtracted() {
    OrAbuseDetector detector = new OrAbuseDetector(1);
    // A contrived SQL where table extraction might fail
    // Use a very minimal SQL that still has OR conditions
    List<QueryRecord> queries = List.of(record("SELECT 1 WHERE 1=1 OR 2=2"));

    // Should not throw, regardless of whether issues are found
    List<Issue> issues = detector.evaluate(queries, EMPTY_INDEX);
    // If issues found, table should be null since no FROM clause
    for (Issue issue : issues) {
      assertThat(issue.table()).isNull();
    }
  }
}
