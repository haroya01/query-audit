package io.queryaudit.core.analyzer;

import io.queryaudit.core.model.IndexMetadata;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Service provider interface for retrieving index metadata from a specific database.
 * Implementations query the database catalog to discover existing indexes and are discovered
 * via {@link java.util.ServiceLoader}.
 *
 * @author haroya
 * @since 0.2.0
 */
public interface IndexMetadataProvider {
  String supportedDatabase();

  IndexMetadata getIndexMetadata(Connection connection) throws SQLException;
}
