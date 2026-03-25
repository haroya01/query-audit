package io.queryaudit.core.regression;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Reads and writes the {@code .query-audit-counts} baseline file.
 *
 * <h3>File format</h3>
 *
 * <pre>
 * # Query Guard Count Baseline
 * # Format: testClass | testMethod | selectCount | insertCount | updateCount | deleteCount | totalCount
 * RoomApiTest | testCreateRoom | 12 | 3 | 0 | 0 | 15
 * RoomApiTest | testDeleteRoom | 8 | 0 | 1 | 1 | 10
 * </pre>
 *
 * <p>Blank lines and lines starting with {@code #} are ignored. Fields are separated by {@code |}
 * and trimmed.
 *
 * @author haroya
 * @since 0.2.0
 */
public final class QueryCountBaseline {

  /** Default baseline file name. */
  public static final String DEFAULT_FILE_NAME = ".query-audit-counts";

  private QueryCountBaseline() {
    /* utility class */
  }

  /** Builds the lookup key for a test. */
  public static String key(String testClass, String testMethod) {
    return testClass + "|" + testMethod;
  }

  /**
   * Loads the count baseline from the given file.
   *
   * @return an unmodifiable map of test-key to {@link QueryCounts}, or an empty map if the file
   *     does not exist or cannot be read.
   */
  public static Map<String, QueryCounts> load(Path file) {
    if (file == null || !Files.isRegularFile(file)) {
      return Map.of();
    }

    Map<String, QueryCounts> result = new LinkedHashMap<>();
    try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
      String line;
      while ((line = reader.readLine()) != null) {
        String trimmed = line.trim();
        if (trimmed.isEmpty() || trimmed.startsWith("#")) {
          continue;
        }

        String[] parts = trimmed.split("\\|", -1);
        if (parts.length < 7) {
          // Malformed line -- skip silently
          continue;
        }

        try {
          String testClass = parts[0].trim();
          String testMethod = parts[1].trim();
          int selectCount = Integer.parseInt(parts[2].trim());
          int insertCount = Integer.parseInt(parts[3].trim());
          int updateCount = Integer.parseInt(parts[4].trim());
          int deleteCount = Integer.parseInt(parts[5].trim());
          int totalCount = Integer.parseInt(parts[6].trim());

          result.put(
              key(testClass, testMethod),
              new QueryCounts(selectCount, insertCount, updateCount, deleteCount, totalCount));
        } catch (NumberFormatException ignored) {
          // Malformed numbers -- skip
        }
      }
    } catch (IOException e) {
      return Map.of();
    }

    return Collections.unmodifiableMap(result);
  }

  /**
   * Writes the count baseline to the given file. Entries are sorted by key for deterministic
   * output.
   *
   * @throws IOException if the file cannot be written
   */
  public static void save(Path file, Map<String, QueryCounts> counts) throws IOException {
    if (file.getParent() != null) {
      Files.createDirectories(file.getParent());
    }

    // Sort by key for deterministic output
    Map<String, QueryCounts> sorted = new TreeMap<>(counts);

    try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
      writer.write("# Query Guard Count Baseline");
      writer.newLine();
      writer.write(
          "# Format: testClass | testMethod | selectCount | insertCount | updateCount | deleteCount | totalCount");
      writer.newLine();

      for (Map.Entry<String, QueryCounts> entry : sorted.entrySet()) {
        String[] keyParts = entry.getKey().split("\\|", 2);
        if (keyParts.length < 2) continue;

        QueryCounts c = entry.getValue();
        writer.write(
            String.format(
                "%s | %s | %d | %d | %d | %d | %d",
                keyParts[0].trim(),
                keyParts[1].trim(),
                c.selectCount(),
                c.insertCount(),
                c.updateCount(),
                c.deleteCount(),
                c.totalCount()));
        writer.newLine();
      }
    }
  }
}
