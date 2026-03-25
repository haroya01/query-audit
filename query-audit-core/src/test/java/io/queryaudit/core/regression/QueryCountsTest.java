package io.queryaudit.core.regression;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.QueryRecord;
import java.util.List;
import org.junit.jupiter.api.Test;

class QueryCountsTest {

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 1_000_000L, System.currentTimeMillis(), "");
  }

  @Test
  void countsSelectQueries() {
    List<QueryRecord> queries =
        List.of(
            record("SELECT * FROM users"),
            record("select id FROM rooms"),
            record("  SELECT name FROM messages"));

    QueryCounts counts = QueryCounts.from(queries);

    assertThat(counts.selectCount()).isEqualTo(3);
    assertThat(counts.insertCount()).isEqualTo(0);
    assertThat(counts.updateCount()).isEqualTo(0);
    assertThat(counts.deleteCount()).isEqualTo(0);
    assertThat(counts.totalCount()).isEqualTo(3);
  }

  @Test
  void countsMixedQueryTypes() {
    List<QueryRecord> queries =
        List.of(
            record("SELECT * FROM users"),
            record("INSERT INTO users (name) VALUES ('test')"),
            record("UPDATE users SET name = 'updated'"),
            record("DELETE FROM users WHERE id = 1"),
            record("SELECT * FROM rooms"));

    QueryCounts counts = QueryCounts.from(queries);

    assertThat(counts.selectCount()).isEqualTo(2);
    assertThat(counts.insertCount()).isEqualTo(1);
    assertThat(counts.updateCount()).isEqualTo(1);
    assertThat(counts.deleteCount()).isEqualTo(1);
    assertThat(counts.totalCount()).isEqualTo(5);
  }

  @Test
  void handlesEmptyList() {
    QueryCounts counts = QueryCounts.from(List.of());

    assertThat(counts.selectCount()).isEqualTo(0);
    assertThat(counts.totalCount()).isEqualTo(0);
  }

  @Test
  void handlesNullAndBlankSql() {
    List<QueryRecord> queries =
        List.of(
            new QueryRecord(null, null, 0, 0, null, 0),
            new QueryRecord("", null, 0, 0, null, 0),
            new QueryRecord("   ", null, 0, 0, null, 0),
            record("SELECT * FROM users"));

    QueryCounts counts = QueryCounts.from(queries);

    assertThat(counts.selectCount()).isEqualTo(1);
    assertThat(counts.totalCount()).isEqualTo(1);
  }

  @Test
  void caseInsensitiveClassification() {
    List<QueryRecord> queries =
        List.of(
            record("SELECT * FROM users"),
            record("select * FROM rooms"),
            record("INSERT INTO users VALUES (1)"),
            record("insert into rooms values (1)"));

    QueryCounts counts = QueryCounts.from(queries);

    assertThat(counts.selectCount()).isEqualTo(2);
    assertThat(counts.insertCount()).isEqualTo(2);
    assertThat(counts.totalCount()).isEqualTo(4);
  }
}
