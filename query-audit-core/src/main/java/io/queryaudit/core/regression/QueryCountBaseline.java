package io.queryaudit.core.regression;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Reads and writes the {@code .query-audit-counts} baseline file.
 *
 * <h3>File format</h3>
 *
 * <pre>
 * # Query Guard Count Baseline
 * # Format: identityType | identityValue | selectCount | insertCount | updateCount | deleteCount | totalCount
 * &#64;junit | [engine:junit-jupiter]/[class:com.example.RoomApiTest]/[method:testCreateRoom()] | 12 | 3 | 0 | 0 | 15
 * </pre>
 *
 * <p>The seven-field layout remains compatible with 0.5 files, whose first two fields are the test
 * class and display name. Stable-ID entries always win when both formats identify the current test.
 *
 * @author haroya
 * @since 0.2.0
 */
public final class QueryCountBaseline {

  /** Default baseline file name. */
  public static final String DEFAULT_FILE_NAME = ".query-audit-counts";

  private static final String JUNIT_IDENTITY_TYPE = "@junit";

  private QueryCountBaseline() {
    /* utility class */
  }

  /** Builds the stable lookup key for a test. */
  public static String key(String testId) {
    if (testId == null || testId.isBlank()) {
      throw new IllegalArgumentException("testId must not be blank");
    }
    return legacyKey(JUNIT_IDENTITY_TYPE, testId);
  }

  /**
   * Builds the legacy 0.5 lookup key. New integrations should use {@link #key(String)}.
   *
   * @deprecated display names do not uniquely identify parameterized, nested, or overloaded tests
   */
  @Deprecated(since = "0.6.0")
  public static String key(String testClass, String testMethod) {
    return legacyKey(testClass, testMethod);
  }

  /** Looks up a stable entry first, then an exact legacy class/display-name entry. */
  public static QueryCounts find(
      Map<String, QueryCounts> counts, String testId, String testClass, String testName) {
    if (counts == null || counts.isEmpty()) {
      return null;
    }
    QueryCounts stable = counts.get(key(testId));
    return stable != null ? stable : counts.get(legacyKey(testClass, testName));
  }

  /** Returns whether lookup would need the ambiguous 0.5 class/display-name identity. */
  public static boolean usesLegacyIdentity(
      Map<String, QueryCounts> counts, String testId, String testClass, String testName) {
    return counts != null
        && !counts.containsKey(key(testId))
        && hasLegacyIdentity(counts, testClass, testName);
  }

  /** Returns whether a 0.5 class/display-name entry exists for the supplied test. */
  public static boolean hasLegacyIdentity(
      Map<String, QueryCounts> counts, String testClass, String testName) {
    return counts != null && counts.containsKey(legacyKey(testClass, testName));
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
    String[] parts = splitFields(line);
    if (parts.length != 7) {
      throw invalidFile(
          file, lineNumber, "expected 7 pipe-separated fields, found " + parts.length, null);
    }

    String identityType = parts[0].trim();
    String identityValue = parts[1].trim();
    if (identityType.isEmpty() || identityValue.isEmpty()) {
      throw invalidFile(file, lineNumber, "test identity fields must not be blank", null);
    }
    if (JUNIT_IDENTITY_TYPE.equals(identityType)) {
      identityValue = unescapeJunitIdentity(file, lineNumber, identityValue);
      if (identityValue.isBlank()) {
        throw invalidFile(file, lineNumber, "test identity fields must not be blank", null);
      }
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
        legacyKey(identityType, identityValue),
        new QueryCounts(selectCount, insertCount, updateCount, deleteCount, totalCount));
  }

  /**
   * Splits stable-ID rows without treating an escaped pipe in the identity value as a delimiter.
   * Legacy rows deliberately retain the original raw {@link String#split(String, int)} behavior so
   * existing display names and backslashes keep their 0.5 meaning.
   */
  private static String[] splitFields(String line) {
    int firstPipe = line.indexOf('|');
    boolean stableEntry =
        firstPipe >= 0 && JUNIT_IDENTITY_TYPE.equals(line.substring(0, firstPipe).trim());
    if (!stableEntry) {
      return line.split("\\|", -1);
    }

    List<String> fields = new ArrayList<>(7);
    StringBuilder field = new StringBuilder();
    for (int i = 0; i < line.length(); i++) {
      char ch = line.charAt(i);
      if (ch == '\\') {
        field.append(ch);
        if (i + 1 < line.length()) {
          field.append(line.charAt(++i));
        }
      } else if (ch == '|') {
        fields.add(field.toString());
        field.setLength(0);
      } else {
        field.append(ch);
      }
    }
    fields.add(field.toString());
    return fields.toArray(String[]::new);
  }

  private static String unescapeJunitIdentity(Path file, int lineNumber, String value) {
    StringBuilder unescaped = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      char ch = value.charAt(i);
      if (ch != '\\') {
        unescaped.append(ch);
        continue;
      }
      if (i + 1 >= value.length()) {
        throw invalidFile(
            file, lineNumber, "@junit identityValue ends with an incomplete escape", null);
      }
      char escaped = value.charAt(++i);
      switch (escaped) {
        case '|' -> unescaped.append('|');
        case '\\' -> unescaped.append('\\');
        case 'r' -> unescaped.append('\r');
        case 'n' -> unescaped.append('\n');
        default ->
            throw invalidFile(
                file,
                lineNumber,
                "unsupported @junit identityValue escape \\"
                    + escaped
                    + "; expected one of \\|, \\\\, \\r, \\n",
                null);
      }
    }
    return unescaped.toString();
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
          "# Format: identityType | identityValue | selectCount | insertCount | updateCount | deleteCount | totalCount");
      writer.newLine();
      writer.write(
          "# @junit identityValue escapes: \\| (pipe), \\\\ (backslash), \\r (CR), \\n (LF)");
      writer.newLine();

      for (Map.Entry<String, QueryCounts> entry : sorted.entrySet()) {
        QueryCounts c = entry.getValue();
        String[] keyParts = entry.getKey().split("\\|", 2);
        if (keyParts.length < 2) {
          continue;
        }
        String identityType = keyParts[0].trim();
        String identityValue = keyParts[1].trim();
        if (JUNIT_IDENTITY_TYPE.equals(identityType)) {
          identityValue = escapeJunitIdentity(identityValue);
        }
        writer.write(
            String.format(
                "%s | %s | %d | %d | %d | %d | %d",
                identityType,
                identityValue,
                c.selectCount(),
                c.insertCount(),
                c.updateCount(),
                c.deleteCount(),
                c.totalCount()));
        writer.newLine();
      }
    }
  }

  private static String escapeJunitIdentity(String value) {
    StringBuilder escaped = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); i++) {
      switch (value.charAt(i)) {
        case '|' -> escaped.append("\\|");
        case '\\' -> escaped.append("\\\\");
        case '\r' -> escaped.append("\\r");
        case '\n' -> escaped.append("\\n");
        default -> escaped.append(value.charAt(i));
      }
    }
    return escaped.toString();
  }

  static String legacyKey(String identityType, String identityValue) {
    return identityType + "|" + identityValue;
  }
}
