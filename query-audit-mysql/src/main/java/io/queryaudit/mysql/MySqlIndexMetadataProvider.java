package io.queryaudit.mysql;

import io.queryaudit.core.analyzer.IndexMetadataProvider;
import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * MySQL implementation of {@link IndexMetadataProvider}.
 *
 * <p>Collects index metadata for all user tables in the current database by querying {@code
 * INFORMATION_SCHEMA.TABLES} and {@code SHOW INDEX}.
 *
 * @author haroya
 * @since 0.2.0
 */
public class MySqlIndexMetadataProvider implements IndexMetadataProvider {

  private static final String LIST_TABLES_SQL =
      "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
          + "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE'";

  @Override
  public String supportedDatabase() {
    return "mysql";
  }

  @Override
  public IndexMetadata getIndexMetadata(Connection connection) throws SQLException {
    Map<String, List<IndexInfo>> indexesByTable = new HashMap<>();

    List<String> tableNames = listUserTables(connection);
    for (String tableName : tableNames) {
      List<IndexInfo> indexes = collectIndexes(connection, tableName);
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
        tables.add(rs.getString("TABLE_NAME"));
      }
    }
    return tables;
  }

  private List<IndexInfo> collectIndexes(Connection connection, String tableName)
      throws SQLException {
    List<IndexInfo> indexes = new ArrayList<>();
    // Use prepared-style quoting to avoid SQL injection via table names.
    // SHOW INDEX does not support parameterized queries, so we quote the identifier.
    String sql = "SHOW INDEX FROM `" + escapeIdentifier(tableName) + "`";
    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {
      while (rs.next()) {
        String table = rs.getString("Table");
        String keyName = rs.getString("Key_name");
        int seqInIndex = rs.getInt("Seq_in_index");
        String columnName = rs.getString("Column_name");
        boolean nonUnique = rs.getInt("Non_unique") != 0;
        long cardinality = rs.getLong("Cardinality");

        indexes.add(
            new IndexInfo(
                table, keyName, columnName, seqInIndex, nonUnique, cardinality));
      }
    }
    return indexes;
  }

  /** Escapes backtick characters in a MySQL identifier to prevent injection. */
  private static String escapeIdentifier(String identifier) {
    return identifier.replace("`", "``");
  }
}
