package io.queryaudit.core.reporter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.Severity;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class FindingIdTest {
  private static final String TEST_ID = "[engine:junit-jupiter]/[class:app.Test]/[method:loads()]";

  @Test
  void normalizesLiteralValuesCommentsAndWhitespaceWithoutLosingSqlStructure() {
    assertThat(id("SELECT u.email FROM users u WHERE u.id=42 AND u.name='Alice'"))
        .isEqualTo(
            id(" select u.email\nFROM users u /* note */ WHERE u.id = 7 AND u.name = 'Bob' -- end"))
        .isEqualTo(id("select u.email from users u where u.id=? and u.name=?"));
    assertThat(id("SELECT a FROM users")).isNotEqualTo(id("SELECT ab FROM users"));
    assertThat(id("SELECT a FROM users WHERE id >= 1"))
        .isNotEqualTo(id("SELECT a FROM users WHERE id > 1"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "id = ?",
        "id=?",
        "id=42",
        "id=/* comment */42",
        "id=-- comment\n42",
        "id/* comment */=42"
      })
  void bindMarkersAndAdjacentCommentsKeepTokenBoundaries(String predicate) {
    assertThat(id("SELECT email FROM users WHERE " + predicate))
        .isEqualTo(id("SELECT email FROM users WHERE id = 7"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "'value'",
        "'it''s quoted'",
        "E'value\\\\escaped'",
        "$$value$$",
        "$태그$value$태그$",
        "123",
        "1.2e-3",
        "0xFF",
        "0b0101",
        "0o77",
        "1_000",
        "TRUE",
        "FALSE",
        "NULL"
      })
  void canonicalizesSupportedLiterals(String literal) {
    assertThat(id("SELECT email FROM users WHERE token = " + literal))
        .isEqualTo(id("SELECT email FROM users WHERE token = ?"));
  }

  @Test
  void quotedIdentifiersAndTheirContentsStayDistinct() {
    assertThat(id("SELECT \"Email\" FROM \"Users\""))
        .isNotEqualTo(id("SELECT \"email\" FROM \"Users\""))
        .isNotEqualTo(id("SELECT \"Email\" FROM \"Accounts\""))
        .isNotEqualTo(id("SELECT 'Email' FROM \"Users\""));
    assertThat(id("SELECT `field1` FROM users")).isNotEqualTo(id("SELECT `field2` FROM users"));
    assertThat(id("SELECT [field1] FROM users")).isNotEqualTo(id("SELECT [field2] FROM users"));
    assertThat(id("SELECT \"a  b\" FROM users")).isNotEqualTo(id("SELECT \"a b\" FROM users"));
  }

  @Test
  void unquotedIdentifiersKeepTheirCaseWhileCommonKeywordsNormalize() {
    assertThat(id("SELECT x FROM Users WHERE id=42"))
        .isEqualTo(id("select x from Users where id=7"))
        .isNotEqualTo(id("SELECT x FROM users WHERE id=42"))
        .isNotEqualTo(id("SELECT X FROM Users WHERE id=42"));
    assertThat(id("SELECT u.id FROM users u")).isNotEqualTo(id("SELECT U.id FROM users U"));
    assertThat(FindingId.of(TEST_ID, issue("SELECT x FROM Users", null, null, null)))
        .isNotEqualTo(FindingId.of(TEST_ID, issue("SELECT x FROM users", null, null, null)));
  }

  @Test
  void distinguishesColumnsTablesAndSchemas() {
    assertThat(FindingId.of(TEST_ID, issue("SELECT * FROM users", "users", "email", null)))
        .isNotEqualTo(FindingId.of(TEST_ID, issue("SELECT * FROM users", "users", "name", null)))
        .isNotEqualTo(
            FindingId.of(TEST_ID, issue("SELECT * FROM users", "other.users", "email", null)));
    assertThat(FindingId.of(TEST_ID, issue("SELECT * FROM users", "\"Users\"", "email", null)))
        .isNotEqualTo(
            FindingId.of(TEST_ID, issue("SELECT * FROM users", "\"users\"", "email", null)));
  }

  @Test
  void keepsNullEmptyAndDelimiterBearingFieldsSeparate() {
    assertThat(FindingId.of(TEST_ID, issue(null, null, "id", null)))
        .isNotEqualTo(FindingId.of(TEST_ID, issue("", null, "id", null)))
        .isNotEqualTo(FindingId.of(TEST_ID, issue(null, "", "id", null)))
        .isNotEqualTo(FindingId.of(TEST_ID, issue(null, "null", "id", null)));
    assertThat(FindingId.of(TEST_ID, issue(null, "a|b", "c", null)))
        .isNotEqualTo(FindingId.of(TEST_ID, issue(null, "a", "b|c", null)));
    assertThat(FindingId.of(TEST_ID, issue(null, "\uD800", "c", null)))
        .isNotEqualTo(FindingId.of(TEST_ID, issue(null, "\uD801", "c", null)));
    assertThat(FindingId.of(TEST_ID, issue(null, null, null, null)))
        .isNotEqualTo(FindingId.of(TEST_ID, issue(null, null, null, "")));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "app.Repository.load:42",
        "app.Repository.load:-1",
        "app.Repository.load(Repository.java:42)",
        "at app.Repository.load(/private/work/Repository.java:900)",
        "at app.Repository.load(C:\\work\\Repository.java:900)",
        "app.module@1.2/app.Repository.load(Repository.java:42)",
        "app.Repository.load"
      })
  void normalizesBothCapturedStackFormats(String location) {
    assertThat(FindingId.sourceMethod(location)).isEqualTo("app.Repository.load");
    assertThat(FindingId.of(TEST_ID, issue("select id from users", "users", "id", location)))
        .isEqualTo(
            FindingId.of(
                TEST_ID, issue("select id from users", "users", "id", "app.Repository.load:2")));
  }

  @Test
  void usesFirstApplicationFrameAndIgnoresFrameworksAndDeeperFrames() {
    assertThat(
            FindingId.sourceMethod(
                "at org.hibernate.Loader.load(Loader.java:1)\napp.Repository.load:2\napp.Service.run:3"))
        .isEqualTo("app.Repository.load");
    assertThat(FindingId.sourceMethod("unknown location\napp.Service.run:4"))
        .isEqualTo("app.Service.run");
    assertThat(FindingId.sourceMethod("at org.junit.Test.run(Test.java:1)")).isNull();
  }

  @Test
  void preservesUnrecognizedCustomSourceLocations() {
    assertThat(FindingId.sourceMethod("repository:loader-one")).isNull();
    assertThat(FindingId.of(TEST_ID, issue(null, null, null, "repository:loader-one")))
        .isNotEqualTo(FindingId.of(TEST_ID, issue(null, null, null, "repository:loader-two")))
        .isNotEqualTo(FindingId.of(TEST_ID, issue(null, null, null, null)));
  }

  @Test
  void frameworkOnlyStacksDoNotIntroduceSourceLineInstability() {
    assertThat(FindingId.of(TEST_ID, issue(null, null, null, "org.hibernate.Loader.load:10")))
        .isEqualTo(FindingId.of(TEST_ID, issue(null, null, null, "org.hibernate.Loader.load:99")))
        .isEqualTo(FindingId.of(TEST_ID, issue(null, null, null, null)));
    assertThat(
            FindingId.of(
                TEST_ID,
                issue(null, null, null, "repository:loader\norg.hibernate.Loader.load:10")))
        .isEqualTo(
            FindingId.of(
                TEST_ID,
                issue(null, null, null, "repository:loader\norg.hibernate.Loader.load:99")));
  }

  @Test
  void ignoresPresentationFieldsButPreservesTestAndMethodIdentity() {
    Issue first = issue("SELECT id FROM users", "users", "id", "app.Repository.load:1");
    Issue changed =
        new Issue(
            first.type(),
            Severity.WARNING,
            first.query(),
            first.table(),
            first.column(),
            "different detail",
            "different suggestion",
            "app.Repository.load:999");
    assertThat(FindingId.of(TEST_ID, first)).isEqualTo(FindingId.of(TEST_ID, changed));
    assertThat(FindingId.of(TEST_ID, first)).isNotEqualTo(FindingId.of(TEST_ID + "other", first));
    assertThat(FindingId.of(TEST_ID, first))
        .isNotEqualTo(
            FindingId.of(TEST_ID, issue(first.query(), "users", "id", "app.Repository.other:1")));
  }

  @Test
  void findByIdUsesEntityAndCallSiteInsteadOfEntityId() {
    Issue first =
        synthetic(IssueType.FIND_BY_ID_FOR_ASSOCIATION, "findById: app.User#alice", "User");
    Issue second =
        synthetic(IssueType.FIND_BY_ID_FOR_ASSOCIATION, "findById: app.User#bob", "User");
    assertThat(FindingId.of(TEST_ID, first)).isEqualTo(FindingId.of(TEST_ID, second));
    assertThat(FindingId.of(TEST_ID, first))
        .isNotEqualTo(
            FindingId.of(
                TEST_ID,
                synthetic(
                    IssueType.FIND_BY_ID_FOR_ASSOCIATION, "findById: another.User#alice", "User")));
  }

  @Test
  void lazyLoadRolesAreStructuralNamesNotSqlLiterals() {
    assertThat(
            FindingId.of(
                TEST_ID, synthetic(IssueType.N_PLUS_ONE, "Lazy load: app.Order1.items", "items")))
        .isNotEqualTo(
            FindingId.of(
                TEST_ID, synthetic(IssueType.N_PLUS_ONE, "Lazy load: app.Order2.items", "items")));
  }

  @Test
  void unknownSyntaxHintsAndAmbiguousQuotesAreNotDiscarded() {
    assertThat(id("SELECT /*! STRAIGHT_JOIN */ id FROM users"))
        .isNotEqualTo(id("SELECT id FROM users"));
    assertThat(id("SELECT data #>> '{a}' FROM users"))
        .isNotEqualTo(id("SELECT data #> '{a}' FROM users"));
    assertThat(id("SELECT 'unterminated")).isNotEqualTo(id("SELECT 'other"));
    assertThat(id("SELECT 'a\\', 'b'")).isNotEqualTo(id("SELECT 'c\\', 'b'"));
    assertThat(id("SELECT id FROM users /* open")).isNotEqualTo(id("SELECT id FROM users"));
    assertThat(id("SELECT id FROM users WHERE id < = 1"))
        .isNotEqualTo(id("SELECT id FROM users WHERE id <= 1"));
    assertThat(id("SELECT data ?| ARRAY['a'] FROM users"))
        .isNotEqualTo(id("SELECT data ?& ARRAY['a'] FROM users"));
    assertThat(id("SELECT id FROM users WHERE id = -1"))
        .isNotEqualTo(id("SELECT id FROM users WHERE id = 1"));
    assertThat(id("SELECT price FROM users WHERE id=1--discount"))
        .isNotEqualTo(id("SELECT price FROM users WHERE id=1"));
    assertThat(id("SELECT 1 /* outer /* inner */ + LENGTH('*/')"))
        .isNotEqualTo(id("SELECT 1 /* outer /* inner */ + ASCII('*/')"));
  }

  @Test
  void outputHasVersionedOpaqueFormatAndIsIndependentOfDefaultLocale() {
    String expected = id("SELECT ID FROM ITEMS");
    Locale previous = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag("tr-TR"));
      assertThat(id("SELECT ID FROM ITEMS")).isEqualTo(expected);
    } finally {
      Locale.setDefault(previous);
    }
    assertThat(expected).matches("qa-finding-v1:[0-9a-f]{64}");
  }

  @Test
  void versionOneEncodingHasAFixedGoldenVector() {
    assertThat(id("SELECT id FROM users WHERE id=42"))
        .isEqualTo(
            "qa-finding-v1:2c4df7c89701f9a8a166a6b1ec0fc106773a17467092355dd62ccf1742d0d7e9");
    assertThat(
            FindingId.legacyKey(
                TEST_ID,
                "missing-where-index",
                "SELECT id FROM users WHERE id=42",
                "app.Repository.load:42",
                "users",
                "id"))
        .isEqualTo(
            "qa-finding-legacy:4f87ed457cd3da84b3e1b1f2cb4fecc3d1a0d16253e284cf49ecf7d207f1060c");
  }

  @Test
  void legacyKeysHaveTheirOwnDomainAndIncludeTableAndColumn() {
    String first =
        FindingId.legacyKey(
            TEST_ID,
            "missing-where-index",
            "SELECT id FROM users",
            "app.Repository.load:1",
            "users",
            "id");
    assertThat(first)
        .startsWith("qa-finding-legacy:")
        .isNotEqualTo(id("SELECT id FROM users"))
        .isNotEqualTo(
            FindingId.legacyKey(
                TEST_ID,
                "missing-where-index",
                "SELECT id FROM users",
                "app.Repository.load:1",
                "users",
                "email"));
    assertThat(first)
        .isEqualTo(
            FindingId.legacyKey(
                TEST_ID,
                "missing-where-index",
                "SELECT id FROM users",
                "app.Repository.load:900",
                "users",
                "id"));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" "})
  void nativeIdsRequireAStableTestIdentity(String testId) {
    assertThatThrownBy(() -> FindingId.of(testId, issue(null, null, null, null)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private static String id(String query) {
    return FindingId.of(TEST_ID, issue(query, "users", "id", "app.Repository.load:42"));
  }

  private static Issue issue(String query, String table, String column, String source) {
    return new Issue(
        IssueType.MISSING_WHERE_INDEX,
        Severity.ERROR,
        query,
        table,
        column,
        "detail",
        "suggestion",
        source);
  }

  private static Issue synthetic(IssueType type, String query, String table) {
    return new Issue(
        type, Severity.INFO, query, table, null, "detail", "suggestion", "app.Repository.load:42");
  }
}
