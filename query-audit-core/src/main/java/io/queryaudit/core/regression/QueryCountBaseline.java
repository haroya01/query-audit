package io.queryaudit.core.regression;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
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
   * @return an unmodifiable map of test-key to {@link QueryCounts}, or an empty map if the path is
   *     {@code null} or the file does not exist
   * @throws IllegalStateException if an existing file cannot be read or contains a malformed entry
   */
  public static Map<String, QueryCounts> load(Path file) {
    if (file == null) {
      return Map.of();
    }

    Map<String, QueryCounts> result = new LinkedHashMap<>();
    try {
      if (Files.notExists(file, LinkOption.NOFOLLOW_LINKS)) {
        return Map.of();
      }
      if (!Files.isRegularFile(file)) {
        throw unreadableFile(file, "path is not a regular file", null);
      }

      try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
        String line;
        int lineNumber = 0;
        while ((line = reader.readLine()) != null) {
          lineNumber++;
          String trimmed = line.trim();
          if (trimmed.isEmpty() || trimmed.startsWith("#")) {
            continue;
          }

          Map.Entry<String, QueryCounts> entry = parseEntry(file, lineNumber, trimmed);
          if (result.putIfAbsent(entry.getKey(), entry.getValue()) != null) {
            throw invalidFile(file, lineNumber, "duplicate entry for " + entry.getKey(), null);
          }
        }
      }
    } catch (IOException e) {
      throw unreadableFile(file, messageOf(e), e);
    } catch (SecurityException e) {
      throw unreadableFile(file, messageOf(e), e);
    }

    return Collections.unmodifiableMap(result);
  }

  private static Map.Entry<String, QueryCounts> parseEntry(Path file, int lineNumber, String line) {
    String[] parts = line.split("\\|", -1);
    if (parts.length != 7) {
      throw invalidFile(
          file, lineNumber, "expected 7 pipe-separated fields, found " + parts.length, null);
    }

    String testClass = parts[0].trim();
    String testMethod = parts[1].trim();
    if (testClass.isEmpty() || testMethod.isEmpty()) {
      throw invalidFile(file, lineNumber, "test class and test method must not be blank", null);
    }

    int selectCount = parseCount(file, lineNumber, "selectCount", parts[2]);
    int insertCount = parseCount(file, lineNumber, "insertCount", parts[3]);
    int updateCount = parseCount(file, lineNumber, "updateCount", parts[4]);
    int deleteCount = parseCount(file, lineNumber, "deleteCount", parts[5]);
    int totalCount = parseCount(file, lineNumber, "totalCount", parts[6]);
    long calculatedTotal = (long) selectCount + insertCount + updateCount + deleteCount;
    if (totalCount != calculatedTotal) {
      throw invalidFile(
          file,
          lineNumber,
          "totalCount must equal the four query type counts: expected "
              + calculatedTotal
              + ", found "
              + totalCount,
          null);
    }

    return Map.entry(
        key(testClass, testMethod),
        new QueryCounts(selectCount, insertCount, updateCount, deleteCount, totalCount));
  }

  private static int parseCount(Path file, int lineNumber, String field, String value) {
    String trimmed = value.trim();
    try {
      int count = Integer.parseInt(trimmed);
      if (count < 0) {
        throw invalidFile(file, lineNumber, field + " must not be negative: " + count, null);
      }
      return count;
    } catch (NumberFormatException e) {
      throw invalidFile(file, lineNumber, field + " must be an integer: " + trimmed, e);
    }
  }

  private static IllegalStateException invalidFile(
      Path file, int lineNumber, String detail, Exception cause) {
    String message =
        "Invalid QueryAudit policy file "
            + file.toAbsolutePath()
            + " at line "
            + lineNumber
            + ": "
            + detail;
    return cause == null
        ? new IllegalStateException(message)
        : new IllegalStateException(message, cause);
  }

  private static IllegalStateException unreadableFile(Path file, String detail, Exception cause) {
    String message = "Cannot read QueryAudit policy file " + file.toAbsolutePath() + ": " + detail;
    return cause == null
        ? new IllegalStateException(message)
        : new IllegalStateException(message, cause);
  }

  private static String messageOf(Exception exception) {
    return exception.getMessage() == null
        ? exception.getClass().getSimpleName()
        : exception.getMessage();
  }

  /**
   * Writes the count baseline to the given file. Entries are sorted by key for deterministic
   * output.
   *
   * @throws IOException if the file cannot be written
   */
  public static void save(Path file, Map<String, QueryCounts> counts) throws IOException {
    save(file, counts, "Query Guard Count Baseline");
  }

  /**
   * Writes the counts with a caller-supplied header title, so other stores reusing this format
   * (e.g. the query contracts file) stay self-describing.
   *
   * @since 0.5.0
   */
  public static void save(Path file, Map<String, QueryCounts> counts, String headerTitle)
      throws IOException {
    if (file.getParent() != null) {
      Files.createDirectories(file.getParent());
    }

    // Sort by key for deterministic output
    Map<String, QueryCounts> sorted = new TreeMap<>(counts);

    try (BufferedWriter writer = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
      writer.write("# " + headerTitle);
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
