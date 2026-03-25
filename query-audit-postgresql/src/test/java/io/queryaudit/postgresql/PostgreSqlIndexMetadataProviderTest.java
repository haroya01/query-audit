package io.queryaudit.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.*;

import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import java.sql.*;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class PostgreSqlIndexMetadataProviderTest {

  private PostgreSqlIndexMetadataProvider provider;

  @Mock private Connection connection;

  @Mock private Statement statement;

  @Mock private ResultSet tableResultSet;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    provider = new PostgreSqlIndexMetadataProvider();
  }

  @Test
  @DisplayName("supportedDatabase() returns 'postgresql'")
  void supportedDatabaseReturnsPostgresql() {
    assertThat(provider.supportedDatabase()).isEqualTo("postgresql");
  }

  @Nested
  @DisplayName("getIndexMetadata with mocked Connection")
  class GetIndexMetadataTests {

    @Test
    @DisplayName("returns correct IndexMetadata for a table with indexes")
    void returnsIndexMetadataForTableWithIndexes() throws SQLException {
      // Mock table listing
      when(connection.createStatement()).thenReturn(statement);
      when(statement.executeQuery(contains("pg_tables"))).thenReturn(tableResultSet);
      when(tableResultSet.next()).thenReturn(true, false);
      when(tableResultSet.getString("tablename")).thenReturn("users");

      // Mock cardinality query
      PreparedStatement cardinalityPstmt = mock(PreparedStatement.class);
      ResultSet cardinalityRs = mock(ResultSet.class);
      // Mock index query
      PreparedStatement indexPstmt = mock(PreparedStatement.class);
      ResultSet indexRs = mock(ResultSet.class);

      when(connection.prepareStatement(contains("reltuples"))).thenReturn(cardinalityPstmt);
      when(cardinalityPstmt.executeQuery()).thenReturn(cardinalityRs);
      when(cardinalityRs.next()).thenReturn(true);
      when(cardinalityRs.getFloat("reltuples")).thenReturn(1000.0f);

      when(connection.prepareStatement(contains("pg_index"))).thenReturn(indexPstmt);
      when(indexPstmt.executeQuery()).thenReturn(indexRs);
      when(indexRs.next()).thenReturn(true, true, false);
      when(indexRs.getString("index_name")).thenReturn("users_pkey", "idx_users_email");
      when(indexRs.getString("column_name")).thenReturn("id", "email");
      when(indexRs.getBoolean("is_unique")).thenReturn(true, false);
      when(indexRs.getInt("seq_in_index")).thenReturn(1, 1);

      IndexMetadata metadata = provider.getIndexMetadata(connection);

      assertThat(metadata.isEmpty()).isFalse();
      assertThat(metadata.hasTable("users")).isTrue();

      List<IndexInfo> indexes = metadata.getIndexesForTable("users");
      assertThat(indexes).hasSize(2);

      IndexInfo primary = indexes.get(0);
      assertThat(primary.tableName()).isEqualTo("users");
      assertThat(primary.indexName()).isEqualTo("users_pkey");
      assertThat(primary.columnName()).isEqualTo("id");
      assertThat(primary.seqInIndex()).isEqualTo(1);
      assertThat(primary.nonUnique()).isFalse();
      assertThat(primary.cardinality()).isEqualTo(1000L);

      IndexInfo emailIdx = indexes.get(1);
      assertThat(emailIdx.tableName()).isEqualTo("users");
      assertThat(emailIdx.indexName()).isEqualTo("idx_users_email");
      assertThat(emailIdx.columnName()).isEqualTo("email");
      assertThat(emailIdx.nonUnique()).isTrue();
      assertThat(emailIdx.cardinality()).isEqualTo(1000L);
    }

    @Test
    @DisplayName("returns empty IndexMetadata when no tables exist")
    void returnsEmptyMetadataWhenNoTables() throws SQLException {
      when(connection.createStatement()).thenReturn(statement);
      when(statement.executeQuery(contains("pg_tables"))).thenReturn(tableResultSet);
      when(tableResultSet.next()).thenReturn(false);

      IndexMetadata metadata = provider.getIndexMetadata(connection);

      assertThat(metadata.isEmpty()).isTrue();
    }

    @Test
    @DisplayName("returns metadata for multiple tables")
    void returnsMetadataForMultipleTables() throws SQLException {
      // Mock table listing returning two tables
      when(connection.createStatement()).thenReturn(statement);
      when(statement.executeQuery(contains("pg_tables"))).thenReturn(tableResultSet);
      when(tableResultSet.next()).thenReturn(true, true, false);
      when(tableResultSet.getString("tablename")).thenReturn("orders", "products");

      // Mock cardinality for orders
      PreparedStatement cardPstmt1 = mock(PreparedStatement.class);
      ResultSet cardRs1 = mock(ResultSet.class);
      // Mock cardinality for products
      PreparedStatement cardPstmt2 = mock(PreparedStatement.class);
      ResultSet cardRs2 = mock(ResultSet.class);

      when(connection.prepareStatement(contains("reltuples")))
          .thenReturn(cardPstmt1)
          .thenReturn(cardPstmt2);
      when(cardPstmt1.executeQuery()).thenReturn(cardRs1);
      when(cardRs1.next()).thenReturn(true);
      when(cardRs1.getFloat("reltuples")).thenReturn(5000.0f);
      when(cardPstmt2.executeQuery()).thenReturn(cardRs2);
      when(cardRs2.next()).thenReturn(true);
      when(cardRs2.getFloat("reltuples")).thenReturn(200.0f);

      // Mock index query for orders
      PreparedStatement indexPstmt1 = mock(PreparedStatement.class);
      ResultSet indexRs1 = mock(ResultSet.class);
      // Mock index query for products
      PreparedStatement indexPstmt2 = mock(PreparedStatement.class);
      ResultSet indexRs2 = mock(ResultSet.class);

      when(connection.prepareStatement(contains("pg_index")))
          .thenReturn(indexPstmt1)
          .thenReturn(indexPstmt2);

      when(indexPstmt1.executeQuery()).thenReturn(indexRs1);
      when(indexRs1.next()).thenReturn(true, false);
      when(indexRs1.getString("index_name")).thenReturn("orders_pkey");
      when(indexRs1.getString("column_name")).thenReturn("order_id");
      when(indexRs1.getBoolean("is_unique")).thenReturn(true);
      when(indexRs1.getInt("seq_in_index")).thenReturn(1);

      when(indexPstmt2.executeQuery()).thenReturn(indexRs2);
      when(indexRs2.next()).thenReturn(true, false);
      when(indexRs2.getString("index_name")).thenReturn("products_pkey");
      when(indexRs2.getString("column_name")).thenReturn("product_id");
      when(indexRs2.getBoolean("is_unique")).thenReturn(true);
      when(indexRs2.getInt("seq_in_index")).thenReturn(1);

      IndexMetadata metadata = provider.getIndexMetadata(connection);

      assertThat(metadata.hasTable("orders")).isTrue();
      assertThat(metadata.hasTable("products")).isTrue();
      assertThat(metadata.getIndexesForTable("orders")).hasSize(1);
      assertThat(metadata.getIndexesForTable("products")).hasSize(1);
    }

    @Test
    @DisplayName("skips tables that have no indexes")
    void skipsTablesWithNoIndexes() throws SQLException {
      when(connection.createStatement()).thenReturn(statement);
      when(statement.executeQuery(contains("pg_tables"))).thenReturn(tableResultSet);
      when(tableResultSet.next()).thenReturn(true, false);
      when(tableResultSet.getString("tablename")).thenReturn("empty_table");

      // Mock cardinality
      PreparedStatement cardPstmt = mock(PreparedStatement.class);
      ResultSet cardRs = mock(ResultSet.class);
      when(connection.prepareStatement(contains("reltuples"))).thenReturn(cardPstmt);
      when(cardPstmt.executeQuery()).thenReturn(cardRs);
      when(cardRs.next()).thenReturn(true);
      when(cardRs.getFloat("reltuples")).thenReturn(0.0f);

      // Mock empty index result
      PreparedStatement indexPstmt = mock(PreparedStatement.class);
      ResultSet emptyIndexRs = mock(ResultSet.class);
      when(connection.prepareStatement(contains("pg_index"))).thenReturn(indexPstmt);
      when(indexPstmt.executeQuery()).thenReturn(emptyIndexRs);
      when(emptyIndexRs.next()).thenReturn(false);

      IndexMetadata metadata = provider.getIndexMetadata(connection);

      assertThat(metadata.isEmpty()).isTrue();
      assertThat(metadata.hasTable("empty_table")).isFalse();
    }

    @Test
    @DisplayName("handles composite (multi-column) indexes")
    void handlesCompositeIndexes() throws SQLException {
      when(connection.createStatement()).thenReturn(statement);
      when(statement.executeQuery(contains("pg_tables"))).thenReturn(tableResultSet);
      when(tableResultSet.next()).thenReturn(true, false);
      when(tableResultSet.getString("tablename")).thenReturn("events");

      // Mock cardinality
      PreparedStatement cardPstmt = mock(PreparedStatement.class);
      ResultSet cardRs = mock(ResultSet.class);
      when(connection.prepareStatement(contains("reltuples"))).thenReturn(cardPstmt);
      when(cardPstmt.executeQuery()).thenReturn(cardRs);
      when(cardRs.next()).thenReturn(true);
      when(cardRs.getFloat("reltuples")).thenReturn(10000.0f);

      // Mock composite index (two columns in one index)
      PreparedStatement indexPstmt = mock(PreparedStatement.class);
      ResultSet indexRs = mock(ResultSet.class);
      when(connection.prepareStatement(contains("pg_index"))).thenReturn(indexPstmt);
      when(indexPstmt.executeQuery()).thenReturn(indexRs);
      when(indexRs.next()).thenReturn(true, true, false);
      when(indexRs.getString("index_name"))
          .thenReturn("idx_events_user_date", "idx_events_user_date");
      when(indexRs.getString("column_name")).thenReturn("user_id", "event_date");
      when(indexRs.getBoolean("is_unique")).thenReturn(false, false);
      when(indexRs.getInt("seq_in_index")).thenReturn(1, 2);

      IndexMetadata metadata = provider.getIndexMetadata(connection);

      assertThat(metadata.hasTable("events")).isTrue();
      List<IndexInfo> indexes = metadata.getIndexesForTable("events");
      assertThat(indexes).hasSize(2);

      assertThat(indexes.get(0).columnName()).isEqualTo("user_id");
      assertThat(indexes.get(0).seqInIndex()).isEqualTo(1);
      assertThat(indexes.get(1).columnName()).isEqualTo("event_date");
      assertThat(indexes.get(1).seqInIndex()).isEqualTo(2);

      // Both belong to same index
      assertThat(indexes.get(0).indexName()).isEqualTo(indexes.get(1).indexName());
    }
  }
}
