package io.queryaudit.core.detector;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Warns once per detector per JVM when an index-metadata-dependent detector disables itself because
 * the metadata is missing (issue #96).
 *
 * @author haroya
 * @since 0.3.0
 */
final class MetadataSkipLog {

  private static final Set<String> WARNED = ConcurrentHashMap.newKeySet();

  private MetadataSkipLog() {}

  static void warnEmptyMetadataOnce(String detectorName) {
    if (WARNED.add(detectorName)) {
      System.err.println(
          "[QueryAudit] IndexMetadata is empty — "
              + detectorName
              + " is disabled. This detector requires index metadata, typically collected by the "
              + "Spring Boot starter or a database-specific IndexMetadataProvider (MySQL/PostgreSQL).");
    }
  }

  static void resetForTesting() {
    WARNED.clear();
  }
}
