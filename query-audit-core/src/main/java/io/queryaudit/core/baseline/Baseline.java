package io.queryaudit.core.baseline;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.parser.SqlParser;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Reads and writes the {@code .query-audit-baseline} file.
 *
 * <h3>File format</h3>
 *
 * <pre>
 * # Query Guard Baseline — acknowledged issues
 * # Format: issue-code | table | column | acknowledged-by | reason | query-pattern
 * missing-where-index | users | deleted_at | dev@example.com | Low cardinality | select id from users where deleted_at is null
 * </pre>
 *
 * <p>Blank lines and lines starting with {@code #} are ignored. Fields are separated by {@code |}
 * and trimmed. An empty field is treated as {@code null}. Legacy five-field rows still load, but
 * cannot acknowledge SQL-backed findings until a query pattern is added.
 *
 * @author haroya
 * @since 0.2.0
 */
public final class Baseline {

  /** Default baseline file name. */
  public static final String DEFAULT_FILE_NAME = ".query-audit-baseline";

  private Baseline() {
    /* utility class */
  }

  /**
   * Loads baseline entries from the given file.
   *
   * @return an unmodifiable list of entries, or an empty list if the file does not exist or cannot
   *     be read.
   */
  public static List<BaselineEntry> load(Path baselineFile) {
    if (baselineFile == null || !Files.isRegularFile(baselineFile)) {
      return List.of();
    }

    List<BaselineEntry> entries = new ArrayList<>();
    try (BufferedReader reader = Files.newBufferedReader(baselineFile, StandardCharsets.UTF_8)) {
      String line;
      while ((line = reader.readLine()) != null) {
        String trimmed = line.trim();
        if (trimmed.isEmpty() || trimmed.startsWith("#")) {
          continue;
        }

        String[] parts = trimmed.split("\\|", 6);
        if (parts.length < 5) {
          // Malformed line — skip silently
          continue;
        }

        String issueCode = blankToNull(parts[0]);
        if (issueCode == null) continue; // issue code is required

        String table = blankToNull(parts[1]);
        String column = blankToNull(parts[2]);
        String acknowledgedBy = blankToNull(parts[3]);
        String reason = blankToNull(parts[4]);
        String queryPattern = parts.length == 6 ? SqlParser.normalize(blankToNull(parts[5])) : null;

        entries.add(
            new BaselineEntry(issueCode, table, column, queryPattern, acknowledgedBy, reason));
      }
    } catch (IOException e) {
      // Cannot read baseline — treat as empty
      return List.of();
    }

    return Collections.unmodifiableList(entries);
  }

  /**
   * Writes baseline entries to the given file.
   *
   * @throws IOException if the file cannot be written
   */
  public static void save(Path baselineFile, List<BaselineEntry> entries) throws IOException {
    Files.createDirectories(baselineFile.getParent());
    try (BufferedWriter writer = Files.newBufferedWriter(baselineFile, StandardCharsets.UTF_8)) {
      writer.write("# Query Guard Baseline — acknowledged issues");
      writer.newLine();
      writer.write(
          "# Format: issue-code | table | column | acknowledged-by | reason | query-pattern");
      writer.newLine();

      for (BaselineEntry entry : entries) {
        writer.write(nullToEmpty(entry.issueCode()));
        writer.write(" | ");
        writer.write(nullToEmpty(entry.table()));
        writer.write(" | ");
        writer.write(nullToEmpty(entry.column()));
        writer.write(" | ");
        writer.write(nullToEmpty(entry.acknowledgedBy()));
        writer.write(" | ");
        writer.write(nullToEmpty(entry.reason()));
        writer.write(" | ");
        writer.write(nullToEmpty(SqlParser.normalize(entry.queryPattern())));
        writer.newLine();
      }
    }
  }

  /** Returns {@code true} if the given issue is acknowledged by any entry in the baseline. */
  public static boolean isAcknowledged(List<BaselineEntry> baseline, Issue issue) {
    if (baseline == null || baseline.isEmpty() || issue == null) {
      return false;
    }
    String code = issue.type().getCode();
    String table = issue.table();
    String column = issue.column();
    for (BaselineEntry entry : baseline) {
      if (entry.matches(code, table, column, issue.query())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Finds the first matching baseline entry for the given issue, or {@code null} if none matches.
   */
  public static BaselineEntry findMatch(List<BaselineEntry> baseline, Issue issue) {
    if (baseline == null || baseline.isEmpty() || issue == null) {
      return null;
    }
    String code = issue.type().getCode();
    String table = issue.table();
    String column = issue.column();
    for (BaselineEntry entry : baseline) {
      if (entry.matches(code, table, column, issue.query())) {
        return entry;
      }
    }
    return null;
  }

  // ------------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------------

  private static String blankToNull(String s) {
    if (s == null) return null;
    String trimmed = s.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }

  private static String nullToEmpty(String s) {
    return s == null ? "" : s;
  }
}
