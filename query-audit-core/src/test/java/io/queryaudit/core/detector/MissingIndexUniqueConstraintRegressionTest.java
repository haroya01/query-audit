package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryRecord;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** A unique index suppresses other index advice only when the predicates prove at most one row. */
class MissingIndexUniqueConstraintRegressionTest {
  private static final List<String> UNBOUNDED_FILTER_AND_SORT =
      List.of(
          "missing-where-index:WARNING:users.customer_name",
          "missing-order-by-index:INFO:users.created_at");
  private static final List<String> MISSING_GROUP_INDEX =
      List.of("missing-group-by-index:WARNING:users.customer_name");

  @ParameterizedTest(name = "{0}")
  @MethodSource("filterAndSortCases")
  void suppressesFilterAndSortAdviceOnlyForACompleteUniqueEqualityConstraint(
      String description, IndexMetadata metadata, String keyPredicate, List<String> expected) {
    String sql =
        "SELECT u.id FROM users u WHERE ("
            + keyPredicate
            + ") AND u.customer_name = 'Alice' ORDER BY u.created_at";

    assertThat(findings(sql, metadata))
        .as(description)
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  static Stream<Arguments> filterAndSortCases() {
    IndexMetadata unique = metadata(index("uq_tenant", false, "tenant_id"));
    IndexMetadata compositeUnique =
        metadata(index("uq_tenant_external", false, "tenant_id", "external_id"));
    IndexMetadata compositePrimary = metadata(index("PRIMARY", false, "tenant_id", "external_id"));
    IndexMetadata compositeNonUnique =
        metadata(index("idx_tenant_external", true, "tenant_id", "external_id"));
    return Stream.of(
        Arguments.of("single UNIQUE equality", unique, "u.tenant_id = 7", List.of()),
        Arguments.of("reversed scalar equality", unique, "7 = u.tenant_id", List.of()),
        Arguments.of("bind equality", unique, "u.tenant_id = ?", List.of()),
        Arguments.of(
            "single PRIMARY equality",
            metadata(index("PRIMARY", false, "tenant_id")),
            "u.tenant_id = 7",
            List.of()),
        Arguments.of(
            "single UNIQUE range does not prove one row",
            unique,
            "u.tenant_id > 7",
            UNBOUNDED_FILTER_AND_SORT),
        Arguments.of(
            "nullable UNIQUE IS NULL can match multiple rows",
            unique,
            "u.tenant_id IS NULL",
            UNBOUNDED_FILTER_AND_SORT),
        Arguments.of(
            "column equality is not scalar equality",
            unique,
            "u.tenant_id = u.external_id",
            UNBOUNDED_FILTER_AND_SORT),
        Arguments.of(
            "negated equality does not prove one row",
            unique,
            "NOT (u.tenant_id = 7)",
            UNBOUNDED_FILTER_AND_SORT),
        Arguments.of(
            "composite UNIQUE leading key alone",
            compositeUnique,
            "u.tenant_id = 7",
            UNBOUNDED_FILTER_AND_SORT),
        Arguments.of(
            "composite UNIQUE trailing key alone",
            compositeUnique,
            "u.external_id = 9",
            UNBOUNDED_FILTER_AND_SORT),
        Arguments.of(
            "composite UNIQUE full equality",
            compositeUnique,
            "u.tenant_id = 7 AND u.external_id = 9",
            List.of()),
        Arguments.of(
            "nested positive conjunction preserves complete key proof",
            compositeUnique,
            "((u.tenant_id = 7) AND ((u.external_id = 9)))",
            List.of()),
        Arguments.of(
            "full equality does not depend on predicate order",
            compositeUnique,
            "u.external_id = 9 AND u.tenant_id = 7",
            List.of()),
        Arguments.of(
            "full equality does not depend on index key order",
            metadata(index("uq_external_tenant", false, "external_id", "tenant_id")),
            "u.tenant_id = 7 AND u.external_id = 9",
            List.of()),
        Arguments.of(
            "composite PRIMARY partial equality",
            compositePrimary,
            "u.tenant_id = 7",
            UNBOUNDED_FILTER_AND_SORT),
        Arguments.of(
            "composite PRIMARY full equality",
            compositePrimary,
            "u.tenant_id = 7 AND u.external_id = 9",
            List.of()),
        Arguments.of(
            "one equality plus one range is not full unique equality",
            compositeUnique,
            "u.tenant_id = 7 AND u.external_id > 9",
            UNBOUNDED_FILTER_AND_SORT),
        Arguments.of(
            "multi-value IN does not supply the missing equality",
            compositeUnique,
            "u.tenant_id IN (7, 8) AND u.external_id = 9",
            UNBOUNDED_FILTER_AND_SORT),
        Arguments.of(
            "OR across unique key columns does not constrain the whole key",
            compositeUnique,
            "u.tenant_id = 7 OR u.external_id = 9",
            UNBOUNDED_FILTER_AND_SORT),
        Arguments.of(
            "non-unique partial key remains advisory",
            compositeNonUnique,
            "u.tenant_id = 7",
            UNBOUNDED_FILTER_AND_SORT),
        Arguments.of(
            "non-unique full key still does not prove one row",
            compositeNonUnique,
            "u.tenant_id = 7 AND u.external_id = 9",
            UNBOUNDED_FILTER_AND_SORT),
        Arguments.of(
            "columns from different incomplete unique constraints cannot be combined",
            metadata(
                index("uq_tenant_external", false, "tenant_id", "external_id"),
                index("uq_region_reference", false, "region_id", "reference_id")),
            "u.tenant_id = 7 AND u.region_id = 9",
            UNBOUNDED_FILTER_AND_SORT));
  }

  @Test
  void doesNotCombinePartialUniqueKeysAcrossSelfJoinAliases() {
    IndexMetadata metadata =
        metadata(index("uq_tenant_external", false, "tenant_id", "external_id"));
    String sql =
        "SELECT u2.id FROM users u1 JOIN users u2 ON u1.tenant_id = u2.tenant_id "
            + "WHERE u1.tenant_id = 7 AND u2.external_id = 9 "
            + "AND u2.customer_name = 'Alice' ORDER BY u2.created_at";

    assertThat(findings(sql, metadata))
        .containsExactlyInAnyOrderElementsOf(UNBOUNDED_FILTER_AND_SORT);
  }

  @Test
  void oneUniqueAliasDoesNotMakeTheOtherSelfJoinAliasUnique() {
    IndexMetadata metadata =
        metadata(
            index("uq_tenant", false, "tenant_id"), index("idx_external", true, "external_id"));
    String sql =
        "SELECT u2.id FROM users u1 JOIN users u2 ON u1.external_id = u2.external_id "
            + "WHERE u1.tenant_id = 7 AND u2.customer_name = 'Alice' ORDER BY u2.created_at";

    assertThat(findings(sql, metadata))
        .containsExactlyInAnyOrderElementsOf(UNBOUNDED_FILTER_AND_SORT);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("groupByCases")
  void suppressesFunctionallyDependentGroupColumnsOnlyWhenTheEntirePrimaryKeyIsGrouped(
      String description, IndexMetadata metadata, String groupedKeys, List<String> expected) {
    // No WHERE clause: this isolates GROUP BY's primary-key proof from filter selectivity policy.
    String sql = "SELECT COUNT(*) FROM users u GROUP BY " + groupedKeys + ", u.customer_name";

    assertThat(findings(sql, metadata))
        .as(description)
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  static Stream<Arguments> groupByCases() {
    IndexMetadata compositePrimary = metadata(index("PRIMARY", false, "tenant_id", "external_id"));
    return Stream.of(
        Arguments.of(
            "GROUP BY complete single PRIMARY",
            metadata(index("PRIMARY", false, "tenant_id")),
            "u.tenant_id",
            List.of()),
        Arguments.of(
            "GROUP BY leading composite PRIMARY key alone",
            compositePrimary,
            "u.tenant_id",
            MISSING_GROUP_INDEX),
        Arguments.of(
            "GROUP BY trailing composite PRIMARY key alone",
            compositePrimary,
            "u.external_id",
            MISSING_GROUP_INDEX),
        Arguments.of(
            "GROUP BY complete composite PRIMARY",
            compositePrimary,
            "u.tenant_id, u.external_id",
            List.of()),
        Arguments.of(
            "GROUP BY complete PRIMARY in reverse order",
            compositePrimary,
            "u.external_id, u.tenant_id",
            List.of()),
        Arguments.of(
            "GROUP BY non-unique full key does not prove functional dependency",
            metadata(index("idx_tenant_external", true, "tenant_id", "external_id")),
            "u.tenant_id, u.external_id",
            MISSING_GROUP_INDEX));
  }

  private static List<String> findings(String sql, IndexMetadata metadata) {
    QueryRecord query = new QueryRecord(sql, 0L, 0L, "MissingIndexUniqueConstraintRegressionTest");
    return new MissingIndexDetector()
        .evaluate(List.of(query), metadata).stream()
            .map(MissingIndexUniqueConstraintRegressionTest::signature)
            .toList();
  }

  private static String signature(Issue issue) {
    return issue.type().getCode()
        + ":"
        + issue.severity()
        + ":"
        + issue.table()
        + "."
        + issue.column();
  }

  private static List<IndexInfo> index(String name, boolean nonUnique, String... columns) {
    List<IndexInfo> entries = new ArrayList<>();
    for (int position = 0; position < columns.length; position++) {
      entries.add(new IndexInfo("users", name, columns[position], position + 1, nonUnique, 1000));
    }
    return entries;
  }

  @SafeVarargs
  private static IndexMetadata metadata(List<IndexInfo>... indexes) {
    List<IndexInfo> entries = new ArrayList<>();
    for (List<IndexInfo> index : indexes) {
      entries.addAll(index);
    }
    return new IndexMetadata(Map.of("users", entries));
  }
}
