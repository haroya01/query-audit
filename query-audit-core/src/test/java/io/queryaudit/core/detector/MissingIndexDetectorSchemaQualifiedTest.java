package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryRecord;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for issue #128 — schema-qualified table names ({@code myschema.users}) used to
 * silently bypass {@code MissingIndexDetector} because the alias regex captured only the schema
 * portion and the metadata lookup keyed on the bare name.
 */
class MissingIndexDetectorSchemaQualifiedTest {

  private static QueryRecord rec(String sql) {
    return new QueryRecord(sql, 0L, System.currentTimeMillis(), "");
  }

  @Test
  @DisplayName("Schema-qualified table resolves to bare-name metadata")
  void schemaQualifiedTwoPart() {
    String sql = "SELECT * FROM myschema.users WHERE email = 'a@b.com'";
    IndexMetadata meta = new IndexMetadata(Map.of("users", List.of()));

    List<Issue> issues = new MissingIndexDetector().evaluate(List.of(rec(sql)), meta);

    assertThat(issues)
        .as("schema-qualified `myschema.users` should match metadata registered as `users`")
        .isNotEmpty();
    assertThat(issues.get(0).column()).isEqualToIgnoringCase("email");
  }

  @Test
  @DisplayName("Three-part name (db.schema.table) also resolves")
  void threePartName() {
    String sql = "SELECT * FROM mydb.myschema.users WHERE email = 'a@b.com'";
    IndexMetadata meta = new IndexMetadata(Map.of("users", List.of()));

    List<Issue> issues = new MissingIndexDetector().evaluate(List.of(rec(sql)), meta);

    assertThat(issues).isNotEmpty();
  }

  @Test
  @DisplayName("Bare-name behavior is unchanged")
  void bareNameStillWorks() {
    String sql = "SELECT * FROM users WHERE email = 'a@b.com'";
    IndexMetadata meta = new IndexMetadata(Map.of("users", List.of()));

    List<Issue> issues = new MissingIndexDetector().evaluate(List.of(rec(sql)), meta);

    assertThat(issues).isNotEmpty();
  }

  @Test
  @DisplayName("Schema-qualified table with existing index does NOT warn")
  void schemaQualifiedWithIndexNoWarn() {
    String sql = "SELECT * FROM myschema.users WHERE email = 'a@b.com'";
    IndexMetadata meta =
        new IndexMetadata(
            Map.of(
                "users",
                List.of(new IndexInfo("users", "idx_email", "email", 1, true, 100))));

    List<Issue> issues = new MissingIndexDetector().evaluate(List.of(rec(sql)), meta);

    assertThat(issues).isEmpty();
  }

  @Test
  @DisplayName("IndexMetadata.hasTable falls back to unqualified suffix")
  void hasTableFallsBackToSuffix() {
    IndexMetadata meta = new IndexMetadata(Map.of("users", List.of()));

    assertThat(meta.hasTable("users")).isTrue();
    assertThat(meta.hasTable("myschema.users")).isTrue();
    assertThat(meta.hasTable("db.schema.users")).isTrue();
    assertThat(meta.hasTable("orders")).isFalse();
  }
}
