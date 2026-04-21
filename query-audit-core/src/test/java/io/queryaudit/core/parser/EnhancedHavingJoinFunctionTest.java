package io.queryaudit.core.parser;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/** Verifies EnhancedSqlParser.extractHavingClause / extractJoinOnBodies. */
class EnhancedHavingJoinFunctionTest {

  // ─── HAVING ─────────────────────────────────────────────────────
  @Test
  @DisplayName("HAVING body extracted, literal with LIMIT keyword preserved")
  void havingLiteralSafe() {
    String sql =
        "SELECT a, COUNT(*) FROM t GROUP BY a HAVING COUNT(*) > 3 AND label = 'top LIMIT rows'";
    String body = EnhancedSqlParser.extractHavingClause(sql);
    assertThat(body).contains("COUNT(*) > 3");
    assertThat(body).contains("'top LIMIT rows'");
  }

  @Test
  @DisplayName("HAVING terminated by ORDER BY")
  void havingTerminatedByOrderBy() {
    String sql = "SELECT a, COUNT(*) c FROM t GROUP BY a HAVING c > 1 ORDER BY a";
    String body = EnhancedSqlParser.extractHavingClause(sql);
    assertThat(body).contains("c > 1");
    assertThat(body).doesNotContainIgnoringCase("ORDER BY");
  }

  @Test
  @DisplayName("No HAVING returns null")
  void noHaving() {
    assertThat(EnhancedSqlParser.extractHavingClause("SELECT * FROM t")).isNull();
  }

  // ─── JOIN ON ────────────────────────────────────────────────────
  @Test
  @DisplayName("JOIN ON bodies extracted with literal safety")
  void joinOnLiteralSafe() {
    String sql =
        "SELECT * FROM a JOIN b ON a.note = 'WHERE needs review' AND a.id = b.aid "
            + "JOIN c ON b.id = c.bid";
    List<String> ons = EnhancedSqlParser.extractJoinOnBodies(sql);
    assertThat(ons).hasSize(2);
    assertThat(ons.get(0)).contains("a.id = b.aid");
    assertThat(ons.get(0)).containsIgnoringCase("WHERE needs review");
    assertThat(ons.get(1)).contains("b.id = c.bid");
  }

  @Test
  @DisplayName("No JOINs returns empty list")
  void noJoin() {
    assertThat(EnhancedSqlParser.extractJoinOnBodies("SELECT * FROM t")).isEmpty();
  }

}
