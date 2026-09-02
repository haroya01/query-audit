package io.queryaudit.core.baseline;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.parser.SqlParser;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BaselineTest {

  @TempDir Path tempDir;

  @Test
  void saveAndLoadPreservesNormalizedQueryPattern() throws Exception {
    Path file = tempDir.resolve(".query-audit-baseline");
    String query = "SELECT id FROM orders WHERE status = 'PAID'";
    BaselineEntry entry =
        new BaselineEntry(
            "missing-where-index", "orders", "status", query, "developer", "Reviewed");

    Baseline.save(file, List.of(entry));

    List<BaselineEntry> loaded = Baseline.load(file);

    assertThat(loaded)
        .singleElement()
        .extracting(BaselineEntry::queryPattern)
        .isEqualTo(SqlParser.normalize(query));
    assertThat(
            Baseline.isAcknowledged(
                loaded, issue("SELECT id FROM orders WHERE status = 'SHIPPED'")))
        .isTrue();
  }

  @Test
  void sameRuleAndTableDoNotAcknowledgeDifferentQueryPattern() {
    BaselineEntry entry =
        new BaselineEntry(
            "missing-where-index",
            "orders",
            "status",
            "SELECT id FROM orders WHERE status = ?",
            "developer",
            "Reviewed");

    Issue reviewed = issue("SELECT id FROM orders WHERE status = 'PAID'");
    Issue unrelated =
        issue("SELECT id, customer_id FROM orders WHERE tenant_id = ? AND status = ?");

    assertThat(Baseline.isAcknowledged(List.of(entry), reviewed)).isTrue();
    assertThat(Baseline.isAcknowledged(List.of(entry), unrelated)).isFalse();
  }

  @Test
  void legacyEntryLoadsWithoutBroadlyAcknowledgingSql() throws Exception {
    Path file = tempDir.resolve(".query-audit-baseline");
    Files.writeString(
        file, "missing-where-index | orders | status | developer | Reviewed before 0.6\n");

    List<BaselineEntry> entries = Baseline.load(file);

    assertThat(entries).singleElement().extracting(BaselineEntry::queryPattern).isNull();
    assertThat(Baseline.isAcknowledged(entries, issue("SELECT id FROM orders WHERE status = ?")))
        .isFalse();
  }

  @Test
  void coordinateOnlyMatchesMethodRemainsAvailable() {
    BaselineEntry entry =
        new BaselineEntry("missing-where-index", "orders", "status", null, "developer", "Reviewed");

    assertThat(entry.matches("missing-where-index", "orders", "status")).isTrue();
  }

  private static Issue issue(String query) {
    return new Issue(
        IssueType.MISSING_WHERE_INDEX,
        Severity.ERROR,
        query,
        "orders",
        "status",
        "Missing index",
        "Add an index");
  }
}
