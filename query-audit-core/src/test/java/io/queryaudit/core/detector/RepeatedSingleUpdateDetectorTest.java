package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RepeatedSingleUpdateDetectorTest {

  private static final IndexMetadata POSTS_PRIMARY_KEY = uniqueIndex("posts", "PRIMARY", "id");

  private final RepeatedSingleUpdateDetector detector = new RepeatedSingleUpdateDetector();

  private static QueryRecord record(String sql) {
    return new QueryRecord(sql, 0L, 0L, "");
  }

  private static List<QueryRecord> records(String... statements) {
    return java.util.Arrays.stream(statements)
        .map(RepeatedSingleUpdateDetectorTest::record)
        .toList();
  }

  private static List<QueryRecord> repeat(String sql, int times) {
    List<QueryRecord> records = new ArrayList<>(times);
    for (int i = 0; i < times; i++) {
      records.add(record(sql));
    }
    return records;
  }

  private static IndexMetadata uniqueIndex(String table, String indexName, String... columns) {
    List<IndexInfo> indexes = new ArrayList<>();
    for (int i = 0; i < columns.length; i++) {
      indexes.add(new IndexInfo(table, indexName, columns[i], i + 1, false, 100));
    }
    return new IndexMetadata(Map.of(table, indexes));
  }

  @Nested
  class Detection {

    @Test
    void groupsDifferentValuesWithTheSameShape() {
      List<QueryRecord> queries =
          records(
              "UPDATE posts SET title = 'First' WHERE id = 1",
              "UPDATE posts SET title = 'Second' WHERE id = 2",
              "UPDATE posts SET title = 'Third' WHERE id = 3");

      List<Issue> issues = detector.evaluate(queries, POSTS_PRIMARY_KEY);

      assertThat(issues)
          .singleElement()
          .satisfies(
              issue -> {
                assertThat(issue.type()).isEqualTo(IssueType.REPEATED_SINGLE_UPDATE);
                assertThat(issue.severity()).isEqualTo(Severity.WARNING);
                assertThat(issue.table()).isEqualTo("posts");
                assertThat(issue.detail()).contains("3 times", "posts");
                assertThat(issue.suggestion()).contains("set-based UPDATE", "JDBC batch");
              });
    }

    @Test
    void honorsTheConfiguredThreshold() {
      RepeatedSingleUpdateDetector thresholdFour = new RepeatedSingleUpdateDetector(4);
      List<QueryRecord> queries = repeat("UPDATE posts SET title = ? WHERE id = ?", 4);

      assertThat(detector.evaluate(queries, POSTS_PRIMARY_KEY)).hasSize(1);
      assertThat(thresholdFour.evaluate(queries.subList(0, 3), POSTS_PRIMARY_KEY)).isEmpty();
      assertThat(thresholdFour.evaluate(queries, POSTS_PRIMARY_KEY)).hasSize(1);
    }

    @Test
    void acceptsACompositeUniqueKey() {
      IndexMetadata metadata =
          uniqueIndex("accounts", "uq_tenant_external", "tenant_id", "external_id");
      String sql = "UPDATE accounts SET display_name = ? WHERE tenant_id = ? AND external_id = ?";

      assertThat(detector.evaluate(repeat(sql, 3), metadata)).hasSize(1);
    }

    @Test
    void acceptsAdditionalPredicatesOnceAUniqueKeyIsCovered() {
      String sql = "UPDATE posts SET archived = ? WHERE id = ? AND status = ?";

      assertThat(detector.evaluate(repeat(sql, 3), POSTS_PRIMARY_KEY)).hasSize(1);
    }

    @Test
    void acceptsAnAliasForTheTargetTable() {
      String sql = "UPDATE posts p SET title = ? WHERE p.id = ?";

      assertThat(detector.evaluate(repeat(sql, 3), POSTS_PRIMARY_KEY)).hasSize(1);
    }

    @Test
    void resolvesLowercaseMySqlMetadataForAMixedCaseTarget() {
      IndexMetadata metadata = uniqueIndex("useraccounts", "PRIMARY", "id");
      String sql = "UPDATE `UserAccounts` SET enabled = ? WHERE id = ?";

      assertThat(detector.evaluate(repeat(sql, 3), metadata))
          .singleElement()
          .extracting(Issue::table)
          .isEqualTo("UserAccounts");
    }

    @Test
    void skipsSchemaQualifiedTargetsBecauseMetadataIsSchemaScoped() {
      String sql = "UPDATE app.posts SET title = ? WHERE id = ?";

      assertThat(detector.evaluate(repeat(sql, 3), POSTS_PRIMARY_KEY)).isEmpty();
    }

    @Test
    void preservesFirstSeenOrderAcrossGroups() {
      List<QueryRecord> queries = new ArrayList<>();
      queries.addAll(repeat("UPDATE posts SET title = ? WHERE id = ?", 3));
      queries.addAll(repeat("UPDATE users SET name = ? WHERE id = ?", 3));
      IndexMetadata metadata =
          new IndexMetadata(
              Map.of(
                  "posts", List.of(new IndexInfo("posts", "PRIMARY", "id", 1, false, 10)),
                  "users", List.of(new IndexInfo("users", "PRIMARY", "id", 1, false, 10))));

      assertThat(detector.evaluate(queries, metadata))
          .extracting(Issue::table)
          .containsExactly("posts", "users");
    }
  }

  @Nested
  class SingleRowProof {

    @Test
    void staysSilentWithoutMetadataForTheTargetTable() {
      String sql = "UPDATE posts SET title = ? WHERE id = ?";

      assertThat(detector.evaluate(repeat(sql, 3), null)).isEmpty();
      assertThat(detector.evaluate(repeat(sql, 3), new IndexMetadata(Map.of()))).isEmpty();
      assertThat(detector.evaluate(repeat(sql, 3), uniqueIndex("users", "PRIMARY", "id")))
          .isEmpty();
    }

    @Test
    void requiresAUniqueIndex() {
      IndexMetadata nonUnique =
          new IndexMetadata(
              Map.of("posts", List.of(new IndexInfo("posts", "idx_id", "id", 1, true, 100))));

      assertThat(detector.evaluate(repeat("UPDATE posts SET title = ? WHERE id = ?", 3), nonUnique))
          .isEmpty();
    }

    @Test
    void requiresEveryColumnOfACompositeUniqueIndex() {
      IndexMetadata metadata =
          uniqueIndex("accounts", "uq_tenant_external", "tenant_id", "external_id");

      assertThat(
              detector.evaluate(
                  repeat("UPDATE accounts SET name = ? WHERE external_id = ?", 3), metadata))
          .isEmpty();
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
          "status = ?",
          "id > ?",
          "id IN (?)",
          "id IS NULL",
          "id = parent_id",
          "id = ? + parent_id",
          "id = ? OR id = ?",
          "status BETWEEN ? AND id = ?",
          "NOT (id = ?)",
          "CASE WHEN id = ? THEN true ELSE true END"
        })
    void rejectsPredicatesThatDoNotProveOneRow(String predicate) {
      String sql = "UPDATE posts SET title = ? WHERE " + predicate;

      assertThat(detector.evaluate(repeat(sql, 3), POSTS_PRIMARY_KEY)).isEmpty();
    }

    @Test
    void leavesUnscopedUpdatesToTheSafetyRule() {
      assertThat(detector.evaluate(repeat("UPDATE posts SET title = ?", 3), POSTS_PRIMARY_KEY))
          .isEmpty();
    }

    @Test
    void doesNotTreatASubqueryWhereAsAnOuterWhere() {
      String sql = "UPDATE posts SET title = (SELECT title FROM drafts WHERE drafts.id = ?)";
      List<QueryRecord> queries = repeat(sql, 3);

      assertThat(detector.evaluate(queries, POSTS_PRIMARY_KEY)).isEmpty();
      assertThat(new UpdateWithoutWhereDetector().evaluate(queries, POSTS_PRIMARY_KEY))
          .extracting(Issue::type)
          .containsExactly(IssueType.UPDATE_WITHOUT_WHERE);
    }

    @Test
    void doesNotCombineDifferentUpdateShapes() {
      List<QueryRecord> queries = new ArrayList<>();
      queries.addAll(repeat("UPDATE posts SET title = ? WHERE id = ?", 2));
      queries.addAll(repeat("UPDATE posts SET status = ? WHERE id = ?", 2));

      assertThat(detector.evaluate(queries, POSTS_PRIMARY_KEY)).isEmpty();
    }

    @Test
    void acceptsAUniqueEqualityAlongsideOtherConjuncts() {
      String sql = "UPDATE posts SET title = ? WHERE id = ? AND status > ?";

      assertThat(detector.evaluate(repeat(sql, 3), POSTS_PRIMARY_KEY)).hasSize(1);
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
          "UPDATE posts p JOIN users u ON p.user_id = u.id SET p.flag = ? WHERE u.id = ?",
          "UPDATE posts p STRAIGHT_JOIN users u ON p.user_id = u.id SET p.flag = ? WHERE u.id = ?",
          "UPDATE posts SET flag = ? FROM users WHERE posts.user_id = users.id AND users.id = ?",
          "UPDATE posts p, users u SET p.flag = ? WHERE p.user_id = u.id AND u.id = ?"
        })
    void skipsUpdatesWhoseTargetScopeNeedsTableResolution(String sql) {
      assertThat(detector.evaluate(repeat(sql, 3), POSTS_PRIMARY_KEY)).isEmpty();
    }

    @Test
    void ignoresOtherStatementsAndMissingNormalizedSql() {
      QueryRecord missingNormalized =
          new QueryRecord("UPDATE posts SET title = ? WHERE id = ?", null, 0L, 0L, "", 0);
      List<QueryRecord> queries =
          List.of(
              record("SELECT * FROM posts WHERE id = ?"),
              record("INSERT INTO posts (title) VALUES (?)"),
              record("DELETE FROM posts WHERE id = ?"),
              missingNormalized);

      assertThat(detector.evaluate(queries, POSTS_PRIMARY_KEY)).isEmpty();
    }
  }

  @Nested
  class TableExclusions {

    @Test
    void excludesTemporaryAndStagingTablesByDefault() {
      for (String table :
          List.of(
              "temp_posts",
              "posts_temp",
              "tmp_posts",
              "posts_tmp",
              "staging_posts",
              "posts_staging")) {
        IndexMetadata metadata = uniqueIndex(table, "PRIMARY", "id");
        assertThat(
                detector.evaluate(
                    repeat("UPDATE " + table + " SET value = ? WHERE id = ?", 3), metadata))
            .as(table)
            .isEmpty();
      }
    }

    @Test
    void supportsCaseInsensitiveCustomGlobs() {
      RepeatedSingleUpdateDetector custom = new RepeatedSingleUpdateDetector(3, Set.of("audit_*"));
      IndexMetadata metadata = uniqueIndex("AUDIT_EVENTS", "PRIMARY", "id");

      assertThat(
              custom.evaluate(
                  repeat("UPDATE AUDIT_EVENTS SET payload = ? WHERE id = ?", 3), metadata))
          .isEmpty();
    }

    @Test
    void anEmptyExclusionSetDisablesTheDefaults() {
      RepeatedSingleUpdateDetector noExclusions = new RepeatedSingleUpdateDetector(3, Set.of());
      IndexMetadata metadata = uniqueIndex("staging_posts", "PRIMARY", "id");

      assertThat(
              noExclusions.evaluate(
                  repeat("UPDATE staging_posts SET value = ? WHERE id = ?", 3), metadata))
          .hasSize(1);
    }
  }

  @Test
  void exposesItsRuleCode() {
    assertThat(detector.getRuleCode()).isEqualTo("repeated-single-update");
  }
}
