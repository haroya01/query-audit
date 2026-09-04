package io.queryaudit.core.reporter;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Versioned identities for findings, independent of their presentation and report redaction. */
final class FindingId {
  static final String PREFIX = "qa-finding-v1:";

  private static final Pattern FRAME =
      Pattern.compile(
          "(?:at\\s+)?(?:[\\w.$@-]+/)?"
              + "([\\p{L}_$][\\p{L}\\p{N}_$.]*\\.[\\p{L}_$<>][\\p{L}\\p{N}_$<>]*)"
              + "(?::-[0-9]+|:[0-9]+|\\([^\\r\\n]*\\))?");
  private static final List<String> FRAMEWORK_PREFIXES =
      List.of(
          "java.",
          "javax.",
          "jdk.",
          "sun.",
          "com.sun.",
          "org.hibernate.",
          "org.springframework.",
          "org.junit.",
          "org.mockito.",
          "org.gradle.",
          "org.postgresql.",
          "com.mysql.",
          "com.zaxxer.",
          "net.ttddyy.",
          "io.queryaudit.");

  private FindingId() {}

  /** The issue must be the original finding, before any serialization redaction. */
  static String of(String testId, Issue issue) {
    if (testId == null || testId.isBlank()) {
      throw new IllegalArgumentException("testId must not be blank");
    }
    Objects.requireNonNull(issue, "issue");
    return PREFIX
        + digest(
            "query-audit/finding/v1",
            testId,
            issue.type().getCode(),
            query(issue.type().getCode(), issue.query()),
            sourceIdentity(issue.sourceLocation()),
            identifier(issue.table()),
            identifier(issue.column()));
  }

  /** A compatibility key for rendered legacy fields; it is not a native finding ID. */
  static String legacyKey(
      String testIdentity, String type, String query, String source, String table, String column) {
    return "qa-finding-legacy:"
        + digest(
            "query-audit/finding/legacy/v1",
            testIdentity,
            type,
            query(type, query),
            sourceIdentity(source),
            identifier(table),
            identifier(column));
  }

  static String sourceMethod(String value) {
    if (value == null) {
      return null;
    }
    for (String line : value.split("\\R")) {
      Matcher frame = FRAME.matcher(line.strip());
      if (frame.matches()) {
        String method = frame.group(1);
        if (FRAMEWORK_PREFIXES.stream().noneMatch(method::startsWith)) {
          return method;
        }
      }
    }
    return null;
  }

  private static String sourceIdentity(String value) {
    if (value == null) {
      return null;
    }
    String method = sourceMethod(value);
    if (method != null) {
      return method;
    }
    if (value.isBlank()) {
      return "";
    }
    StringBuilder unknown = new StringBuilder();
    for (String line : value.split("\\R")) {
      String text = line.strip();
      if (!text.isEmpty() && !FRAME.matcher(text).matches()) {
        if (!unknown.isEmpty()) {
          unknown.append('\n');
        }
        unknown.append(text);
      }
    }
    // Custom locations have no known line/path format. Preserve them rather than merge call sites.
    // Recognized framework-only stacks have no application location and contribute no identity.
    return unknown.isEmpty() ? null : unknown.toString();
  }

  private static String query(String type, String value) {
    if (value == null) {
      return null;
    }
    String text = value.strip();
    if (IssueType.FIND_BY_ID_FOR_ASSOCIATION.getCode().equals(type)
        && text.startsWith("findById:")) {
      int entityEnd = text.indexOf('#');
      if (entityEnd >= 0) {
        return text.substring(0, entityEnd).stripTrailing() + "(?)";
      }
    }
    if (IssueType.N_PLUS_ONE.getCode().equals(type) && text.startsWith("Lazy load:")) {
      return text;
    }
    return FindingQuery.canonicalize(value);
  }

  private static String identifier(String value) {
    // Quoted names can be case-sensitive and a schema prefix distinguishes different objects.
    return value == null ? null : value.strip();
  }

  private static String digest(String... values) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      for (String value : values) {
        digest.update((byte) (value == null ? 'N' : 'S'));
        if (value == null) {
          continue;
        }
        int length = value.length();
        for (int shift = 24; shift >= 0; shift -= 8) {
          digest.update((byte) (length >>> shift));
        }
        // Encode UTF-16 code units explicitly, including unpaired surrogates, without a charset's
        // replacement-character collisions. Lengths are code-unit counts, not byte counts.
        for (int index = 0; index < length; index++) {
          char unit = value.charAt(index);
          digest.update((byte) (unit >>> 8));
          digest.update((byte) unit);
        }
      }
      return HexFormat.of().formatHex(digest.digest());
    } catch (NoSuchAlgorithmException failure) {
      throw new IllegalStateException("SHA-256 is unavailable", failure);
    }
  }
}
