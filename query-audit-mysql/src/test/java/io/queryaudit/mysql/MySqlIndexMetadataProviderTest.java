package io.queryaudit.mysql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.*;

import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class MySqlIndexMetadataProviderTest {

  private MySqlIndexMetadataProvider provider;

  @Mock private Connection connection;

  @Mock private Statement statement;

  @Mock private ResultSet tableResultSet;

  @Mock private ResultSet indexResultSet;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    provider = new MySqlIndexMetadataProvider();
  }

  @Test
  @DisplayName("supportedDatabase() returns 'mysql'")
  void supportedDatabaseReturnsMysql() {
    assertThat(provider.supportedDatabase()).isEqualTo("mysql");
  }

  @Nested
  @DisplayName("getIndexMetadata with mocked Connection")
  class GetIndexMetadataTests {

    @Test
    @DisplayName("returns correct IndexMetadata for a table with indexes")
    void returnsIndexMetadataForTableWithIndexes() throws SQLException {
      // Mock the INFORMATION_SCHEMA.TABLES query
      Statement tableStatement = mock(Statement.class);
      when(connection.createStatement())
          .thenReturn(tableStatement) // first call: listUserTables
          .thenReturn(statement); // second call: collectIndexes

      when(tableStatement.executeQuery(contains("INFORMATION_SCHEMA.TABLES")))
          .thenReturn(tableResultSet);
      when(tableResultSet.next()).thenReturn(true, false);
      when(tableResultSet.getString("TABLE_NAME")).thenReturn("users");

      // Mock the SHOW INDEX query
      when(statement.executeQuery(contains("SHOW INDEX"))).thenReturn(indexResultSet);
      when(indexResultSet.next()).thenReturn(true, true, false);
      when(indexResultSet.getString("Table")).thenReturn("users", "users");
      when(indexResultSet.getString("Key_name")).thenReturn("PRIMARY", "idx_email");
      when(indexResultSet.getInt("Seq_in_index")).thenReturn(1, 1);
      when(indexResultSet.getString("Column_name")).thenReturn("id", "email");
      when(indexResultSet.getInt("Non_unique")).thenReturn(0, 1);
      when(indexResultSet.getLong("Cardinality")).thenReturn(1000L, 950L);

      IndexMetadata metadata = provider.getIndexMetadata(connection);

      assertThat(metadata.isEmpty()).isFalse();
      assertThat(metadata.hasTable("users")).isTrue();

      List<IndexInfo> indexes = metadata.getIndexesForTable("users");
      assertThat(indexes).hasSize(2);

      IndexInfo primary = indexes.get(0);
      assertThat(primary.tableName()).isEqualTo("users");
      assertThat(primary.indexName()).isEqualTo("PRIMARY");
      assertThat(primary.columnName()).isEqualTo("id");
      assertThat(primary.seqInIndex()).isEqualTo(1);
      assertThat(primary.nonUnique()).isFalse();
      assertThat(primary.cardinality()).isEqualTo(1000L);

      IndexInfo emailIdx = indexes.get(1);
      assertThat(emailIdx.tableName()).isEqualTo("users");
      assertThat(emailIdx.indexName()).isEqualTo("idx_email");
      assertThat(emailIdx.columnName()).isEqualTo("email");
      assertThat(emailIdx.nonUnique()).isTrue();
      assertThat(emailIdx.cardinality()).isEqualTo(950L);
    }

    @Test
    @DisplayName("returns empty IndexMetadata when no tables exist")
    void returnsEmptyMetadataWhenNoTables() throws SQLException {
      when(connection.createStatement()).thenReturn(statement);
      when(statement.executeQuery(contains("INFORMATION_SCHEMA.TABLES")))
          .thenReturn(tableResultSet);
      when(tableResultSet.next()).thenReturn(false);

      IndexMetadata metadata = provider.getIndexMetadata(connection);

      assertThat(metadata.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("returns metadata for multiple tables")
    void returnsMetadataForMultipleTables() throws SQLException {
      Statement stmt1 = mock(Statement.class);
      Statement stmt2 = mock(Statement.class);
      Statement stmt3 = mock(Statement.class);
      ResultSet indexRs1 = mock(ResultSet.class);
      ResultSet indexRs2 = mock(ResultSet.class);

      when(connection.createStatement())
          .thenReturn(stmt1) // listUserTables
          .thenReturn(stmt2) // collectIndexes for "orders"
          .thenReturn(stmt3); // collectIndexes for "products"

      // Tables query
      when(stmt1.executeQuery(contains("INFORMATION_SCHEMA.TABLES"))).thenReturn(tableResultSet);
      when(tableResultSet.next()).thenReturn(true, true, false);
      when(tableResultSet.getString("TABLE_NAME")).thenReturn("orders", "products");

      // SHOW INDEX for orders
      when(stmt2.executeQuery(contains("SHOW INDEX"))).thenReturn(indexRs1);
      when(indexRs1.next()).thenReturn(true, false);
      when(indexRs1.getString("Table")).thenReturn("orders");
      when(indexRs1.getString("Key_name")).thenReturn("PRIMARY");
      when(indexRs1.getInt("Seq_in_index")).thenReturn(1);
      when(indexRs1.getString("Column_name")).thenReturn("order_id");
      when(indexRs1.getInt("Non_unique")).thenReturn(0);
      when(indexRs1.getLong("Cardinality")).thenReturn(5000L);

      // SHOW INDEX for products
      when(stmt3.executeQuery(contains("SHOW INDEX"))).thenReturn(indexRs2);
      when(indexRs2.next()).thenReturn(true, false);
      when(indexRs2.getString("Table")).thenReturn("products");
      when(indexRs2.getString("Key_name")).thenReturn("PRIMARY");
      when(indexRs2.getInt("Seq_in_index")).thenReturn(1);
      when(indexRs2.getString("Column_name")).thenReturn("product_id");
      when(indexRs2.getInt("Non_unique")).thenReturn(0);
      when(indexRs2.getLong("Cardinality")).thenReturn(200L);

      IndexMetadata metadata = provider.getIndexMetadata(connection);

      assertThat(metadata.hasTable("orders")).isTrue();
      assertThat(metadata.hasTable("products")).isTrue();
      assertThat(metadata.getIndexesForTable("orders")).hasSize(1);
      assertThat(metadata.getIndexesForTable("products")).hasSize(1);
    }

    @Test
    @DisplayName("skips tables that have no indexes")
    void skipsTablesWithNoIndexes() throws SQLException {
      Statement stmt1 = mock(Statement.class);
      Statement stmt2 = mock(Statement.class);
      ResultSet emptyIndexRs = mock(ResultSet.class);

      when(connection.createStatement())
          .thenReturn(stmt1) // listUserTables
          .thenReturn(stmt2); // collectIndexes

      when(stmt1.executeQuery(contains("INFORMATION_SCHEMA.TABLES"))).thenReturn(tableResultSet);
      when(tableResultSet.next()).thenReturn(true, false);
      when(tableResultSet.getString("TABLE_NAME")).thenReturn("empty_table");

      when(stmt2.executeQuery(contains("SHOW INDEX"))).thenReturn(emptyIndexRs);
      when(emptyIndexRs.next()).thenReturn(false);

      IndexMetadata metadata = provider.getIndexMetadata(connection);

      assertThat(metadata.isEmpty()).isTrue();
      assertThat(metadata.hasTable("empty_table")).isFalse();
    }
  }

  @Nested
  @DisplayName("escapeIdentifier via reflection")
  class EscapeIdentifierTests {

    private Method escapeIdentifier;

    @BeforeEach
    void setUp() throws Exception {
      escapeIdentifier =
          MySqlIndexMetadataProvider.class.getDeclaredMethod("escapeIdentifier", String.class);
      escapeIdentifier.setAccessible(true);
    }

    private String escape(String input) throws Exception {
      return (String) escapeIdentifier.invoke(null, input);
    }

    @Test
    @DisplayName("passes through normal identifiers unchanged")
    void normalIdentifierUnchanged() throws Exception {
      assertThat(escape("users")).isEqualTo("users");
      assertThat(escape("order_items")).isEqualTo("order_items");
    }

    @Test
    @DisplayName("escapes backtick characters by doubling them")
    void escapesBackticks() throws Exception {
      assertThat(escape("table`name")).isEqualTo("table``name");
      assertThat(escape("`")).isEqualTo("``");
      assertThat(escape("a`b`c")).isEqualTo("a``b``c");
    }

    @Test
    @DisplayName("handles empty string")
    void handlesEmptyString() throws Exception {
      assertThat(escape("")).isEqualTo("");
    }

    @Test
    @DisplayName("handles string with only backticks")
    void handlesOnlyBackticks() throws Exception {
      assertThat(escape("```")).isEqualTo("``````");
    }
  }
}
