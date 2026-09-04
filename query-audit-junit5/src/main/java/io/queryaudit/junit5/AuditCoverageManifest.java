package io.queryaudit.junit5;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import org.junit.platform.engine.UniqueId;

/** One exact JUnit unique ID per line; blank lines and comments are ignored. */
final class AuditCoverageManifest {

  static final String DEFAULT_FILE_NAME = ".query-audit-tests";
  static final String PATH_PROPERTY = "queryAudit.coverageManifest";

  private AuditCoverageManifest() {}

  static boolean isConfigured() {
    return System.getProperty(PATH_PROPERTY) != null
        || !Files.notExists(Path.of(DEFAULT_FILE_NAME));
  }

  static Set<String> load(Path file) throws IOException {
    Set<String> identities = new LinkedHashSet<>();
    int lineNumber = 0;
    for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
      lineNumber++;
      String identity = line.strip();
      if (identity.isEmpty() || identity.startsWith("#")) {
        continue;
      }
      try {
        UniqueId parsed = UniqueId.parse(identity);
        if (parsed.getSegments().size() < 3
            || !parsed.getEngineId().orElse("").equals("junit-jupiter")) {
          throw new IllegalArgumentException("Expected a JUnit Jupiter test ID");
        }
        if (!identities.add(identity)) {
          throw new IllegalArgumentException("Duplicate test ID");
        }
      } catch (RuntimeException e) {
        throw new IOException("Invalid audit coverage manifest at line " + lineNumber, e);
      }
    }
    if (identities.isEmpty()) {
      throw new IOException("The audit coverage manifest must contain at least one test ID");
    }
    return Collections.unmodifiableSet(identities);
  }
}
