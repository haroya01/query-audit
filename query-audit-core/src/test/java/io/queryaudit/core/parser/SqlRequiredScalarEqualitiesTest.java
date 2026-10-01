package io.queryaudit.core.parser;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class SqlRequiredScalarEqualitiesTest {
  @ParameterizedTest(name = "{0}")
  @MethodSource("predicates")
  void extractsOnlyNecessaryScalarEqualities(String predicate, List<String> expectedColumns) {
    List<ColumnReference> expected =
        expectedColumns.stream().map(column -> new ColumnReference("users", column)).toList();

    assertThat(
            EnhancedSqlParser.extractRequiredScalarEqualities(
                "SELECT u.id FROM users u WHERE " + predicate))
        .containsExactlyElementsOf(expected);
  }

  static Stream<Arguments> predicates() {
    return Stream.of(
        Arguments.of(
            "u.tenant_id = 7 AND u.external_id = 'abc'", List.of("tenant_id", "external_id")),
        Arguments.of(
            "? = u.tenant_id AND ((u.external_id = :ref))", List.of("tenant_id", "external_id")),
        Arguments.of("u.id = -7 AND u.id = -7", List.of("id")),
        Arguments.of("u.id = 7 AND (u.name = 'A' OR u.name = 'B')", List.of("id")),
        Arguments.of("u.tenant_id = 7 OR u.external_id = 9", List.of()),
        Arguments.of("NOT (u.id = 7)", List.of()),
        Arguments.of("u.id = u.parent_id", List.of()),
        Arguments.of("u.id = ABS(u.parent_id)", List.of()),
        Arguments.of("u.id IN (7, 9)", List.of()),
        Arguments.of("u.id IS NULL", List.of()),
        Arguments.of("CASE WHEN u.id = 7 THEN true ELSE true END", List.of()),
        Arguments.of("u.id = (SELECT MAX(o.id) FROM other o)", List.of()),
        Arguments.of("EXISTS (SELECT 1 FROM other o WHERE o.id = u.id)", List.of()));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT a.id FROM users a JOIN users b ON a.id = b.id WHERE a.tenant_id = 7 AND"
            + " b.external_id = 9",
        "SELECT a.id FROM users a, users b WHERE a.tenant_id = 7 AND b.external_id = 9",
        "WITH users AS (SELECT id FROM source) SELECT id FROM users WHERE id = 7",
        "SELECT id FROM (SELECT id FROM users) u WHERE id = 7",
        "SELECT id FROM users WHERE id = 7 UNION ALL SELECT id FROM users WHERE id = 9",
        "UPDATE users SET name = 'changed' WHERE id = 7",
        "SELECT id FROM users WHERE id = 7 @unsupported"
      })
  void unsupportedOrMultiRelationScopesDoNotProduceProofEvidence(String sql) {
    assertThat(EnhancedSqlParser.extractRequiredScalarEqualities(sql)).isEmpty();
    assertThat(EnhancedSqlParser.extractRequiredScalarEqualities(sql)).isEmpty();
  }

  @Test
  void aliasesAndQuotedIdentifiersBecomeBaseTableColumnEvidence() {
    String sql = "SELECT u.\"id\" FROM \"users\" u WHERE u.\"id\" = 7";

    assertThat(EnhancedSqlParser.extractRequiredScalarEqualities(sql))
        .containsExactly(new ColumnReference("users", "id"));
  }

  @Test
  void usesTheExistingRoutingLengthBoundaryWithoutRegexProof() {
    String sql = "SELECT id FROM users WHERE id = 7";
    String exactLimit = sql + " ".repeat(SqlParserRouting.AST_LENGTH_LIMIT - sql.length());

    assertThat(EnhancedSqlParser.extractRequiredScalarEqualities(exactLimit))
        .containsExactly(new ColumnReference("users", "id"));
    assertThat(EnhancedSqlParser.extractRequiredScalarEqualities(exactLimit + " ")).isEmpty();
    assertThat(EnhancedSqlParser.extractRequiredScalarEqualities(null)).isEmpty();
  }

  @Test
  void evidenceIsImmutableAndDoesNotChangeOtherExtractionResults() {
    String sql = "SELECT id FROM users WHERE id = 7 AND parent_id = 9";
    var originalColumns = EnhancedSqlParser.extractWhereColumns(sql);
    List<ColumnReference> evidence = EnhancedSqlParser.extractRequiredScalarEqualities(sql);

    assertThatThrownBy(() -> evidence.add(new ColumnReference("users", "injected")))
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(evidence::clear).isInstanceOf(UnsupportedOperationException.class);
    assertThat(EnhancedSqlParser.extractWhereColumns(sql)).isEqualTo(originalColumns);
    assertThat(EnhancedSqlParser.extractRequiredScalarEqualities(sql)).isEqualTo(evidence);
  }
}
