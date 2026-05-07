package io.queryaudit.core.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for issue #124 — the prior {@code isQuerySuppressed} used {@code String.contains}
 * with no awareness of string literals or word boundaries, so suppression patterns spuriously
 * matched content inside quoted strings and as substrings of unrelated identifiers.
 */
class QueryAuditConfigSuppressionTest {

  private static QueryAuditConfig configWithPattern(String pattern) {
    return QueryAuditConfig.builder().suppressQueries(Set.of(pattern)).build();
  }

  @Test
  @DisplayName("Pattern inside a string literal does NOT suppress (issue #124)")
  void doesNotMatchInsideStringLiteral() {
    QueryAuditConfig config = configWithPattern("from users");

    String sql = "SELECT * FROM orders WHERE description = 'imported from users history'";

    assertThat(config.isQuerySuppressed(sql)).isFalse();
  }

  @Test
  @DisplayName("Pattern matching the actual table reference still suppresses")
  void matchesActualTableReference() {
    QueryAuditConfig config = configWithPattern("from users");

    String sql = "SELECT * FROM users WHERE id = 1";

    assertThat(config.isQuerySuppressed(sql)).isTrue();
  }

  @Test
  @DisplayName("Pattern is not a substring match against a longer identifier")
  void doesNotMatchAsSubstringOfIdentifier() {
    QueryAuditConfig config = configWithPattern("users");

    String sql = "SELECT * FROM users_archive WHERE id = 1";

    assertThat(config.isQuerySuppressed(sql)).isFalse();
  }

  @Test
  @DisplayName("Pattern still matches the bare identifier when both forms appear")
  void matchesBareIdentifierEvenAlongsideLongerIdentifier() {
    QueryAuditConfig config = configWithPattern("users");

    String sql = "SELECT * FROM users JOIN users_archive ON users.id = users_archive.id";

    assertThat(config.isQuerySuppressed(sql)).isTrue();
  }

  @Test
  @DisplayName("Match is case-insensitive")
  void matchIsCaseInsensitive() {
    QueryAuditConfig config = configWithPattern("from users");

    assertThat(config.isQuerySuppressed("SELECT * FROM USERS")).isTrue();
    assertThat(config.isQuerySuppressed("select * from users")).isTrue();
  }

  @Test
  @DisplayName("Empty / null inputs and patterns are tolerated")
  void emptyInputs() {
    QueryAuditConfig empty = QueryAuditConfig.defaults();
    assertThat(empty.isQuerySuppressed("SELECT * FROM users")).isFalse();

    QueryAuditConfig blankPattern =
        QueryAuditConfig.builder().suppressQueries(Set.of("   ")).build();
    assertThat(blankPattern.isQuerySuppressed("SELECT * FROM users")).isFalse();

    QueryAuditConfig configured = configWithPattern("from users");
    assertThat(configured.isQuerySuppressed(null)).isFalse();
    assertThat(configured.isQuerySuppressed("")).isFalse();
  }
}
