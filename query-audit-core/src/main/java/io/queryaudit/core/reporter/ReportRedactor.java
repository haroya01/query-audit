package io.queryaudit.core.reporter;

import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Applies the artifact policy at serialization time, leaving captured evidence unchanged. */
final class ReportRedactor {

  private static final byte LITERAL = 1;
  private static final byte COMMENT = 2;
  private static final Pattern WHITESPACE = Pattern.compile("\\s+");
  private static final Pattern NUMBER =
      Pattern.compile(
          "(?:0[xX][0-9a-fA-F_]+|0[bB][01_]+|0[oO][0-7_]+|(?:\\d[\\d_]*(?:\\.[\\d_]*)?|\\.[\\d_]+)(?:[eE][+-]?[\\d_]+)?)");
  private static final Pattern LITERAL_KEYWORD = Pattern.compile("(?i)(?:true|false|null)\\b");
  private static final Pattern DOLLAR_QUOTE =
      Pattern.compile("\\$(?:[A-Za-z_\\x{80}-\\x{10ffff}][A-Za-z_0-9\\x{80}-\\x{10ffff}]*)?\\$");
  private static final Pattern JAVA_FRAME =
      Pattern.compile(
          "(?:at\\s+)?([\\p{L}_$][\\p{L}\\p{N}_$.]*\\.[\\p{L}_$<>][\\p{L}\\p{N}_$<>]*)\\(([^()]*):(\\d+)\\)");
  private static final Pattern COMPACT_FRAME =
      Pattern.compile("([\\p{L}_$][\\p{L}\\p{N}_$.]*\\.[\\p{L}_$<>][\\p{L}\\p{N}_$<>]*):(\\d+)");
  private static final Set<String> FRAMEWORK_PREFIXES =
      Set.of(
          "java.",
          "javax.",
          "jdk.",
          "sun.",
          "org.hibernate.",
          "org.springframework.",
          "org.junit.",
          "org.mockito.",
          "org.postgresql.",
          "com.mysql.",
          "com.zaxxer.",
          "net.ttddyy.",
          "io.queryaudit.");
  private static final int MAX_APPLICATION_FRAMES = 5;
  private final ReportRedaction mode;

  ReportRedactor(ReportRedaction mode) {
    this.mode = Objects.requireNonNull(mode, "mode");
  }

  String sql(String value) {
    if (mode == ReportRedaction.FULL || value == null) {
      return value;
    }
    byte[] hidden = new byte[value.length()];
    // SQL modes disagree about backslash escapes. Hide every span that can be a value in either
    // interpretation; guessing one mode can expose the next literal after an escaped quote.
    markSensitiveSpans(value, hidden, true);
    markSensitiveSpans(value, hidden, false);
    StringBuilder out = new StringBuilder(value.length());
    for (int i = 0; i < value.length(); ) {
      byte kind = hidden[i];
      if (kind == 0) {
        out.append(value.charAt(i++));
      } else {
        out.append(kind == LITERAL ? '?' : ' ');
        do {
          i++;
        } while (i < value.length() && hidden[i] == kind);
      }
    }
    return WHITESPACE.matcher(out).replaceAll(" ").trim();
  }

  private static void markSensitiveSpans(String value, byte[] hidden, boolean backslashEscapes) {
    for (int i = 0; i < value.length(); ) {
      char c = value.charAt(i);
      int end = i + 1;
      byte kind = LITERAL;
      String dollarQuote = c == '$' ? dollarQuoteAt(value, i) : null;
      if (value.startsWith("--", i) || c == '#') {
        end = lineEnd(value, i);
        kind = COMMENT;
      } else if (value.startsWith("/*", i)) {
        end = blockCommentEnd(value, i);
        kind = COMMENT;
      } else if (c == '\'' || c == '"') {
        end = quotedEnd(value, i, c, backslashEscapes);
        if (end < 0) {
          end = value.length();
        }
      } else if (c == '`') {
        // Backticks delimit MySQL identifiers, which remain structural report data.
        int identifierEnd = quotedEnd(value, i, c, backslashEscapes);
        if (identifierEnd >= 0) {
          i = identifierEnd;
          continue;
        }
        end = value.length();
      } else if ((c == 'q' || c == 'Q') && i + 2 < value.length() && value.charAt(i + 1) == '\'') {
        char open = value.charAt(i + 2);
        char close =
            switch (open) {
              case '[' -> ']';
              case '(' -> ')';
              case '{' -> '}';
              case '<' -> '>';
              default -> open;
            };
        int closeAt = value.indexOf("" + close + '\'', i + 3);
        end = closeAt < 0 ? value.length() : closeAt + 2;
      } else if (dollarQuote != null) {
        int closeAt = value.indexOf(dollarQuote, i + dollarQuote.length());
        end = closeAt < 0 ? value.length() : closeAt + dollarQuote.length();
      } else if ((i == 0 || !identifierPart(value.charAt(i - 1)))
          && (Character.isDigit(c) || c == '.' || Character.isLetter(c))) {
        Pattern pattern = Character.isLetter(c) ? LITERAL_KEYWORD : NUMBER;
        Matcher matcher = pattern.matcher(value).region(i, value.length());
        if (!matcher.lookingAt()) {
          i++;
          continue;
        }
        end = matcher.end();
      } else {
        i++;
        continue;
      }
      for (int pos = i; pos < end; pos++) {
        // A potential literal takes precedence over a comment in the other SQL mode.
        if (hidden[pos] != LITERAL) {
          hidden[pos] = kind;
        }
      }
      i = end;
    }
  }

  Issue issue(Issue issue) {
    if (mode == ReportRedaction.FULL) {
      return issue;
    }
    // Free-form diagnostics can contain unquoted values extracted from SQL. Do not copy them.
    return new Issue(
        issue.type(),
        issue.severity(),
        issue.type() == IssueType.FIND_BY_ID_FOR_ASSOCIATION ? "findById(?)" : sql(issue.query()),
        sql(issue.table()),
        sql(issue.column()),
        issue.type().getDescription(),
        safeSuggestion(issue),
        sourceLocation(issue.sourceLocation()));
  }

  String diagnostic(String value) {
    return mode == ReportRedaction.FULL || value == null
        ? value
        : "Details omitted by report redaction";
  }

  String sourceLocation(String value) {
    String frames = stackTrace(value);
    return frames == null ? null : frames.lines().findFirst().orElse(null);
  }

  String stackTrace(String value) {
    if (mode == ReportRedaction.FULL || value == null) {
      return value;
    }
    Set<String> frames = new LinkedHashSet<>();
    for (String line : value.split("\\R")) {
      String candidate = line.trim();
      Matcher compact = COMPACT_FRAME.matcher(candidate);
      Matcher java = JAVA_FRAME.matcher(candidate);
      String method;
      String frame;
      if (compact.matches()) {
        method = compact.group(1);
        frame = method + ":" + compact.group(2);
      } else if (java.matches()) {
        method = java.group(1);
        String file = java.group(2).replace('\\', '/');
        file = file.substring(file.lastIndexOf('/') + 1);
        if (!file.matches("[\\p{L}\\p{N}_$.-]+")) {
          continue;
        }
        frame = "at " + method + "(" + file + ":" + java.group(3) + ")";
      } else {
        continue;
      }
      if (FRAMEWORK_PREFIXES.stream().anyMatch(method::startsWith)) {
        continue;
      }
      frames.add(frame);
      if (frames.size() == MAX_APPLICATION_FRAMES) {
        break;
      }
    }
    return frames.isEmpty() ? null : String.join("\n", frames);
  }

  private static String safeSuggestion(Issue issue) {
    RemediationHints.Remediation hint = RemediationHints.forIssue(issue);
    if (hint == null) {
      return null;
    }
    return switch (hint.kind()) {
      case "add-index" -> "Check the query plan before adding an index on the reported columns.";
      case "batch-fetch" -> "Consider a fetch join, entity graph, or batch loading.";
      case "batch-insert", "batch-update" -> "Consider a set-based operation or JDBC batching.";
      case "add-where-clause" -> "Restrict the rows affected by this statement.";
      case "add-limit" -> "Bound the result size when the caller does not need every row.";
      case "select-explicit-columns" -> "Select only the columns required by the caller.";
      default -> null;
    };
  }

  private static boolean identifierPart(char c) {
    return Character.isLetterOrDigit(c) || c == '_' || c == '$';
  }

  private static String dollarQuoteAt(String value, int start) {
    Matcher matcher = DOLLAR_QUOTE.matcher(value).region(start, value.length());
    return matcher.lookingAt() ? matcher.group() : null;
  }

  private static int lineEnd(String value, int start) {
    int end = start;
    while (end < value.length() && value.charAt(end) != '\n' && value.charAt(end) != '\r') {
      end++;
    }
    return end;
  }

  private static int blockCommentEnd(String value, int start) {
    int depth = 1;
    int i = start + 2;
    while (i < value.length() && depth > 0) {
      if (value.startsWith("/*", i)) {
        depth++;
        i += 2;
      } else if (value.startsWith("*/", i)) {
        depth--;
        i += 2;
      } else {
        i++;
      }
    }
    return i;
  }

  private static int quotedEnd(String value, int start, char quote, boolean backslashEscapes) {
    for (int i = start + 1; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c == '\\' && backslashEscapes) {
        i++;
      } else if (c == quote) {
        if (i + 1 < value.length() && value.charAt(i + 1) == quote) {
          i++;
        } else {
          return i + 1;
        }
      }
    }
    return -1;
  }
}
