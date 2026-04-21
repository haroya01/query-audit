package io.queryaudit.core.detector;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Warns, at most once per detector per JVM, when an index-metadata-dependent detector disables
 * itself because the metadata is missing or empty. Without this trail users had no way to tell
 * why a rule wasn't firing — the detectors just silently returned (issue #96).
 *
 * <p>The skip itself is still safe behavior (we can't analyze what we can't see); this class
 * only adds a one-line heads-up to {@code System.err} so the gap becomes observable.
 *
 * @author haroya
 * @since 0.3.0
 */
final class MetadataSkipLog {

  private static final Set<String> WARNED = ConcurrentHashMap.newKeySet();

  private MetadataSkipLog() {}

  /**
   * Emit a single warning line for {@code detectorName} if this is the first time the JVM has
   * seen the metadata-missing skip for that detector. Subsequent calls for the same detector are
   * no-ops.
   */
  static void warnEmptyMetadataOnce(String detectorName) {
    if (WARNED.add(detectorName)) {
      System.err.println(
          "[QueryAudit] IndexMetadata is empty — "
              + detectorName
              + " is disabled. This detector requires index metadata, typically collected by the "
              + "Spring Boot starter or a database-specific IndexMetadataProvider (MySQL/PostgreSQL).");
    }
  }

  // @VisibleForTesting — reset the per-JVM warned set so tests can assert on output.
  static void resetForTesting() {
    WARNED.clear();
  }
}
