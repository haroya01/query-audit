package io.queryaudit.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.MissingIndexDetector;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Tag("integration")
@Testcontainers
@DisplayName("PostgreSQL IndexMetadataProvider integration test")
class PostgreSqlIntegrationTest {

  @Container
  static PostgreSQLContainer<?> postgres =
      new PostgreSQLContainer<>("postgres:16-alpine")
          .withDatabaseName("testdb")
          .withInitScript("init.sql");

  private static IndexMetadata metadata;

  @BeforeAll
  static void loadMetadata() throws SQLException {
    PostgreSqlIndexMetadataProvider provider = new PostgreSqlIndexMetadataProvider();
    try (Connection conn =
        DriverManager.getConnection(
            postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword())) {
      metadata = provider.getIndexMetadata(conn);
    }
  }

  @Test
  @DisplayName("detects both user tables")
  void detectsBothTables() {
    assertThat(metadata.hasTable("users")).isTrue();
    assertThat(metadata.hasTable("orders")).isTrue();
  }

  @Test
  @DisplayName("users table has correct number of index entries")
  void usersTableIndexCount() {
    // users_pkey(id), idx_email(email), idx_username(username), idx_status_created(status,
    // created_at)
    // = 5 IndexInfo rows (composite has 2 entries)
    List<IndexInfo> indexes = metadata.getIndexesForTable("users");
    assertThat(indexes).hasSize(5);
  }

  @Test
  @DisplayName("orders table has correct number of index entries")
  void ordersTableIndexCount() {
    // orders_pkey(id), idx_user_id(user_id), idx_status(status)
    List<IndexInfo> indexes = metadata.getIndexesForTable("orders");
    assertThat(indexes).hasSize(3);
  }

  @Test
  @DisplayName("PRIMARY key is detected on users table")
  void primaryKeyDetected() {
    List<IndexInfo> indexes = metadata.getIndexesForTable("users");
    // PostgreSQL names the primary key constraint as <table>_pkey
    List<IndexInfo> pk =
        indexes.stream()
            .filter(i -> i.indexName() != null && i.indexName().contains("pkey"))
            .collect(Collectors.toList());

    assertThat(pk).hasSize(1);
    assertThat(pk.get(0).columnName()).isEqualTo("id");
    assertThat(pk.get(0).nonUnique()).isFalse();
  }

  @Test
  @DisplayName("unique index idx_email is detected as non-unique=false")
  void uniqueIndexDetected() {
    List<IndexInfo> indexes = metadata.getIndexesForTable("users");
    List<IndexInfo> emailIdx =
        indexes.stream()
            .filter(i -> "idx_email".equals(i.indexName()))
            .collect(Collectors.toList());

    assertThat(emailIdx).hasSize(1);
    assertThat(emailIdx.get(0).columnName()).isEqualTo("email");
    assertThat(emailIdx.get(0).nonUnique()).isFalse();
  }

  @Test
  @DisplayName("regular index idx_username is detected as non-unique=true")
  void regularIndexDetected() {
    List<IndexInfo> indexes = metadata.getIndexesForTable("users");
    List<IndexInfo> usernameIdx =
        indexes.stream()
            .filter(i -> "idx_username".equals(i.indexName()))
            .collect(Collectors.toList());

    assertThat(usernameIdx).hasSize(1);
    assertThat(usernameIdx.get(0).columnName()).isEqualTo("username");
    assertThat(usernameIdx.get(0).nonUnique()).isTrue();
  }

  @Test
  @DisplayName("composite index idx_status_created has columns in correct order")
  void compositeIndexColumnsInOrder() {
    List<IndexInfo> indexes = metadata.getIndexesForTable("users");
    List<IndexInfo> compositeIdx =
        indexes.stream()
            .filter(i -> "idx_status_created".equals(i.indexName()))
            .collect(Collectors.toList());

    assertThat(compositeIdx).hasSize(2);

    // Sort by seqInIndex to verify column ordering
    List<IndexInfo> sorted =
        compositeIdx.stream()
            .sorted((a, b) -> Integer.compare(a.seqInIndex(), b.seqInIndex()))
            .collect(Collectors.toList());

    assertThat(sorted.get(0).columnName()).isEqualTo("status");
    assertThat(sorted.get(1).columnName()).isEqualTo("created_at");
    // Verify ordering is preserved (first < second)
    assertThat(sorted.get(0).seqInIndex()).isLessThan(sorted.get(1).seqInIndex());
  }

  @Test
  @DisplayName("hasIndexOn returns true for indexed columns and false for unindexed")
  void hasIndexOnWorks() {
    assertThat(metadata.hasIndexOn("users", "email")).isTrue();
    assertThat(metadata.hasIndexOn("users", "username")).isTrue();
    assertThat(metadata.hasIndexOn("users", "id")).isTrue();
    assertThat(metadata.hasIndexOn("orders", "user_id")).isTrue();

    // 'total' column on orders has no index
    assertThat(metadata.hasIndexOn("orders", "total")).isFalse();
  }

  @Test
  @DisplayName("getCompositeIndexes returns composite indexes")
  void compositeIndexesDetected() {
    Map<String, List<IndexInfo>> composites = metadata.getCompositeIndexes("users");
    assertThat(composites).containsKey("idx_status_created");
    assertThat(composites.get("idx_status_created")).hasSize(2);
  }

  @Test
  @DisplayName("QueryAuditAnalyzer detects missing index on unindexed column via real metadata")
  void analyzerDetectsMissingIndex() {
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
    List<QueryRecord> queries =
        List.of(
            new QueryRecord(
                "SELECT * FROM orders WHERE total > 100", 1_000_000L, System.nanoTime(), null));

    QueryAuditReport report = analyzer.analyze("test", queries, metadata);

    List<Issue> missingIndexIssues =
        report.getConfirmedIssues().stream()
            .filter(i -> i.type() == IssueType.MISSING_WHERE_INDEX)
            .collect(Collectors.toList());

    assertThat(missingIndexIssues)
        .anyMatch(i -> "total".equals(i.column()) && "orders".equals(i.table()));
  }

  @Test
  @DisplayName("MissingIndexDetector does NOT flag indexed columns")
  void missingIndexDetectorSkipsIndexedColumns() {
    MissingIndexDetector detector = new MissingIndexDetector();
    List<QueryRecord> queries =
        List.of(
            new QueryRecord(
                "SELECT * FROM users WHERE email = 'test@example.com'",
                1_000_000L,
                System.nanoTime(),
                null));

    List<Issue> issues = detector.evaluate(queries, metadata);

    List<Issue> emailIssues =
        issues.stream()
            .filter(
                i -> i.type() == IssueType.MISSING_WHERE_INDEX && "email".equals(i.column()))
            .collect(Collectors.toList());

    assertThat(emailIssues).isEmpty();
  }

  @Test
  @DisplayName("MissingIndexDetector flags unindexed JOIN column")
  void missingIndexDetectorFlagsUnindexedJoin() {
    MissingIndexDetector detector = new MissingIndexDetector();
    List<QueryRecord> queries =
        List.of(
            new QueryRecord(
                "SELECT u.username, o.total FROM users u JOIN orders o ON u.id = o.total",
                1_000_000L,
                System.nanoTime(),
                null));

    List<Issue> issues = detector.evaluate(queries, metadata);

    List<Issue> joinIssues =
        issues.stream()
            .filter(i -> i.type() == IssueType.MISSING_JOIN_INDEX && "total".equals(i.column()))
            .collect(Collectors.toList());

    assertThat(joinIssues).isNotEmpty();
  }
}
