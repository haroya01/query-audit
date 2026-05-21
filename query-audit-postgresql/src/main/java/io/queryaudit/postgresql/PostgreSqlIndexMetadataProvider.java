package io.queryaudit.postgresql;

import io.queryaudit.core.analyzer.IndexMetadataProvider;
import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * PostgreSQL implementation of {@link IndexMetadataProvider}.
 *
 * <p>Collects index metadata for all user tables in the current schema by querying PostgreSQL
 * system catalogs ({@code pg_class}, {@code pg_index}, {@code pg_attribute}, and {@code pg_stats}).
 *
 * @author haroya
 * @since 0.2.0
 */
public class PostgreSqlIndexMetadataProvider implements IndexMetadataProvider {

  private static final String LIST_TABLES_SQL =
      "SELECT tablename FROM pg_tables WHERE schemaname = current_schema()";

  // Both queries scope by current_schema() to avoid cross-schema name collisions: pg_class is
  // database-wide and pg_class.relname is unique only per-namespace. Without the join, a same-
  // named table in another schema (e.g. public.orders + staging.orders) would silently leak its
  // indexes / cardinality into the result for the table actually iterated from
  // pg_tables WHERE schemaname = current_schema(). See issue #147.
  private static final String INDEX_QUERY =
      """
            SELECT
                i.relname   AS index_name,
                a.attname   AS column_name,
                ix.indisunique AS is_unique,
                array_position(ix.indkey, a.attnum) AS seq_in_index
            FROM pg_class t
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_index ix ON t.oid = ix.indrelid
            JOIN pg_class i  ON i.oid = ix.indexrelid
            JOIN pg_attribute a ON a.attrelid = t.oid
                AND a.attnum = ANY(ix.indkey)
            WHERE t.relname = ? AND t.relkind = 'r' AND n.nspname = current_schema()
            ORDER BY i.relname, array_position(ix.indkey, a.attnum)
            """;

  private static final String CARDINALITY_QUERY =
      """
            SELECT c.reltuples
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = ? AND n.nspname = current_schema()
            """;

  @Override
  public String supportedDatabase() {
    return "postgresql";
  }

  @Override
  public IndexMetadata getIndexMetadata(Connection connection) throws SQLException {
    Map<String, List<IndexInfo>> indexesByTable = new HashMap<>();

    List<String> tableNames = listUserTables(connection);
    for (String tableName : tableNames) {
      long cardinality = getCardinality(connection, tableName);
      List<IndexInfo> indexes = collectIndexes(connection, tableName, cardinality);
      if (!indexes.isEmpty()) {
        indexesByTable.put(tableName, indexes);
      }
    }

    return new IndexMetadata(indexesByTable);
  }

  private List<String> listUserTables(Connection connection) throws SQLException {
    List<String> tables = new ArrayList<>();
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(LIST_TABLES_SQL)) {
      while (rs.next()) {
        tables.add(rs.getString("tablename"));
      }
    }
    return tables;
  }

  private List<IndexInfo> collectIndexes(Connection connection, String tableName, long cardinality)
      throws SQLException {
    List<IndexInfo> indexes = new ArrayList<>();
    try (PreparedStatement pstmt = connection.prepareStatement(INDEX_QUERY)) {
      pstmt.setString(1, tableName);
      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          String indexName = rs.getString("index_name");
          String columnName = rs.getString("column_name");
          boolean isUnique = rs.getBoolean("is_unique");
          int seqInIndex = rs.getInt("seq_in_index");

          indexes.add(
              new IndexInfo(tableName, indexName, columnName, seqInIndex, !isUnique, cardinality));
        }
      }
    }
    return indexes;
  }

  private long getCardinality(Connection connection, String tableName) throws SQLException {
    try (PreparedStatement pstmt = connection.prepareStatement(CARDINALITY_QUERY)) {
      pstmt.setString(1, tableName);
      try (ResultSet rs = pstmt.executeQuery()) {
        if (rs.next()) {
          return (long) rs.getFloat("reltuples");
        }
      }
    }
    return 0L;
  }
}
