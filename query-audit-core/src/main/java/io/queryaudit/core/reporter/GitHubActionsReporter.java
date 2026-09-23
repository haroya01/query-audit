package io.queryaudit.core.reporter;

import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.Severity;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Emits a {@link QueryAuditReport} in GitHub Actions' workflow-command format: one {@code ::error}
 * / {@code ::warning} / {@code ::notice} line per issue on {@code System.out}, plus a Markdown
 * summary appended to {@code $GITHUB_STEP_SUMMARY} when set.
 *
 * @author haroya
 * @since 0.3.0
 */
public class GitHubActionsReporter implements Reporter {

  private static final Pattern SOURCE_LOCATION =
      Pattern.compile("^(?<fqcn>[A-Za-z_][\\w.$]*?)\\.(?<method>[A-Za-z_$][\\w$]*):(?<line>\\d+)$");

  private final PrintStream out;
  private final Path summaryPath;
  private final ReportRedactor redactor;

  public GitHubActionsReporter() {
    this(System.out, resolveSummaryPathFromEnv());
  }

  public GitHubActionsReporter(ReportRedaction redaction) {
    this(System.out, resolveSummaryPathFromEnv(), redaction);
  }

  public GitHubActionsReporter(PrintStream out, Path summaryPath) {
    this(out, summaryPath, ReportRedaction.REDACTED);
  }

  public GitHubActionsReporter(PrintStream out, Path summaryPath, ReportRedaction redaction) {
    this.redactor = new ReportRedactor(redaction);
    this.out = out;
    this.summaryPath = summaryPath;
  }

  @Override
  public void report(QueryAuditReport report) {
    if (report == null) {
      return;
    }
    emitAnnotations(report);
    if (summaryPath != null) {
      appendSummary(report);
    }
  }

  private void emitAnnotations(QueryAuditReport report) {
    var findings = report.getFindings();
    for (Finding finding :
        findings.confirmed().stream().sorted(Comparator.comparing(Finding::severity)).toList()) {
      String level =
          switch (finding.severity()) {
            case ERROR -> "error";
            case WARNING -> "warning";
            case INFO -> "notice";
          };
      emit(level, finding, report);
    }
    for (Finding finding : findings.informational()) {
      emit("notice", finding, report);
    }
  }

  private void emit(String level, Finding original, QueryAuditReport report) {
    // Occurrence identity belongs to the original evidence, never to its redacted projection.
    String id = FindingId.of(report.getTestId(), original);
    Finding finding = redactor.finding(original);
    String title =
        (report.getTestClass() == null ? "" : report.getTestClass() + " — ")
            + finding.kindId().value();
    emitCommand(level, title, finding.sourceLocation(), messageFor(finding, id));
  }

  private void emitCommand(String level, String title, String sourceLocation, String message) {
    StringBuilder props = new StringBuilder();
    Location loc = parseLocation(sourceLocation);
    if (loc != null) {
      append(props, "file=" + escapeProp(loc.file));
      append(props, "line=" + loc.line);
    }
    append(props, "title=" + escapeProp(title));

    StringBuilder sb = new StringBuilder("::");
    sb.append(level);
    if (props.length() > 0) {
      sb.append(' ').append(props);
    }
    sb.append("::");
    sb.append(escapeBody(message));

    out.println(sb);
  }

  private static void append(StringBuilder props, String kv) {
    if (props.length() > 0) {
      props.append(',');
    }
    props.append(kv);
  }

  private static String messageFor(Finding issue, String id) {
    StringBuilder body = new StringBuilder();
    if (issue.detail() != null) {
      body.append(issue.detail());
    } else {
      body.append(LegacyFindingPresentation.description(issue));
    }
    body.append("\nFinding ID: ").append(id);
    if (issue.suggestion() != null && !issue.suggestion().isEmpty()) {
      body.append("\n\nSuggestion: ").append(issue.suggestion());
    }
    if (issue.query() != null) {
      body.append("\n\nQuery: ").append(issue.query());
    }
    return body.toString();
  }

  private void appendSummary(QueryAuditReport report) {
    var findings = report.getFindings();
    long errorCount = findings.errors().size();
    long warningCount = findings.warnings().size();
    int infoCount = findings.informational().size();

    StringBuilder md = new StringBuilder();
    md.append("### query-audit — ")
        .append(markdownText(report.getTestClass() != null ? report.getTestClass() : "report"))
        .append("\n\n");
    md.append("| Severity | Count |\n");
    md.append("| --- | ---: |\n");
    md.append("| ERROR | ").append(errorCount).append(" |\n");
    md.append("| WARNING | ").append(warningCount).append(" |\n");
    md.append("| INFO | ").append(infoCount).append(" |\n\n");

    if (findings.hasConfirmed() || infoCount > 0) {
      md.append("<details><summary>Top issues</summary>\n\n");
      appendFindingSummary(md, "ERROR", findings.errors(), report);
      appendFindingSummary(md, "WARNING", findings.warnings(), report);
      appendFindingSummary(md, "INFO", findings.informational(), report);
      appendFindingSummary(
          md,
          "CONFIRMED INFO",
          findings.confirmed().stream()
              .filter(finding -> finding.severity() == Severity.INFO)
              .toList(),
          report);
      md.append("\n</details>\n");
    }
    if (!findings.acknowledged().isEmpty()) {
      md.append("\nAcknowledged findings: ").append(findings.acknowledged().size()).append("\n");
      appendFindingSummary(md, "ACKNOWLEDGED", findings.acknowledged(), report);
    }

    try {
      Files.writeString(
          summaryPath, md.toString(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    } catch (IOException e) {
      System.err.println("[QueryAudit] Could not write GitHub step summary: " + e.getMessage());
    }
  }

  private void appendFindingSummary(
      StringBuilder md, String level, List<Finding> issues, QueryAuditReport report) {
    if (issues.isEmpty()) {
      return;
    }
    md.append("\n**").append(level).append("**\n\n");
    int limit = Math.min(issues.size(), 5);
    for (int i = 0; i < limit; i++) {
      Finding original = issues.get(i);
      String id = FindingId.of(report.getTestId(), original);
      Finding issue = redactor.finding(original);
      md.append("- ").append(markdownCode(issue.kindId().value()));
      if (issue.table() != null) {
        md.append(" on ").append(markdownCode(issue.table()));
      }
      if (issue.detail() != null) {
        md.append(" — ").append(markdownText(firstLine(issue.detail())));
      }
      md.append(" — Finding ID: ").append(markdownCode(id));
      md.append("\n");
    }
    if (issues.size() > limit) {
      md.append("- _…and ").append(issues.size() - limit).append(" more_\n");
    }
  }

  private static String firstLine(String s) {
    for (int index = 0; index < s.length(); index++) {
      if (isLineBreak(s.charAt(index))) return s.substring(0, index);
    }
    return s;
  }

  private static String markdownText(String value) {
    StringBuilder escaped = new StringBuilder();
    for (char character : singleLine(value).toCharArray()) {
      switch (character) {
        case '&' -> escaped.append("&amp;");
        case '<' -> escaped.append("&lt;");
        case '>' -> escaped.append("&gt;");
        case '\\', '`', '*', '_', '[', ']', '|', '~' -> escaped.append('\\').append(character);
        default -> escaped.append(character);
      }
    }
    return escaped.toString();
  }

  private static String markdownCode(String value) {
    String text = singleLine(value);
    if (text.isEmpty()) return "<code></code>";
    int longest = 0;
    int current = 0;
    for (char character : text.toCharArray()) {
      current = character == '`' ? current + 1 : 0;
      longest = Math.max(longest, current);
    }
    // Markdown does not honor backslash/entity escaping inside code spans. Use a delimiter that
    // cannot occur in the value, and padding when an edge could join or trim the delimiter.
    String delimiter = "`".repeat(longest + 1);
    boolean padded =
        text.startsWith("`")
            || text.endsWith("`")
            || (text.startsWith(" ") && text.endsWith(" ") && !text.isBlank());
    return delimiter + (padded ? " " : "") + text + (padded ? " " : "") + delimiter;
  }

  private static String singleLine(String value) {
    StringBuilder result = new StringBuilder(value.length());
    for (int index = 0; index < value.length(); index++) {
      char character = value.charAt(index);
      result.append(isLineBreak(character) ? ' ' : character);
      if (character == '\r' && index + 1 < value.length() && value.charAt(index + 1) == '\n')
        index++;
    }
    return result.toString();
  }

  private static boolean isLineBreak(char value) {
    return value == '\r'
        || value == '\n'
        || value == '\u000b'
        || value == '\f'
        || value == '\u0085'
        || value == '\u2028'
        || value == '\u2029';
  }

  // ── Location parsing ──────────────────────────────────────────────────

  static Location parseLocation(String sourceLocation) {
    if (sourceLocation == null || sourceLocation.isEmpty()) {
      return null;
    }
    Matcher m = SOURCE_LOCATION.matcher(sourceLocation.trim());
    if (!m.matches()) {
      return null;
    }
    String fqcn = m.group("fqcn");
    int lastDot = fqcn.lastIndexOf('.');
    if (lastDot < 0) {
      return null;
    }
    String pkg = fqcn.substring(0, lastDot).replace('.', '/');
    String simpleClass = fqcn.substring(lastDot + 1);
    int innerMarker = simpleClass.indexOf('$');
    if (innerMarker >= 0) {
      simpleClass = simpleClass.substring(0, innerMarker);
    }
    String file = "src/main/java/" + pkg + "/" + simpleClass + ".java";
    try {
      return new Location(file, Integer.parseInt(m.group("line")));
    } catch (NumberFormatException e) {
      return null;
    }
  }

  record Location(String file, int line) {}

  // ── Escaping (GitHub workflow commands) ───────────────────────────────

  private static String escapeProp(String s) {
    if (s == null) return "";
    return s.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A").replace(",", "%2C");
  }

  // Property escape without comma encoding — commands separate property list from message with
  // '::'.
  private static String escapeBody(String s) {
    if (s == null) return "";
    return s.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A");
  }

  // ── Env bootstrap ─────────────────────────────────────────────────────

  private static Path resolveSummaryPathFromEnv() {
    String v = System.getenv("GITHUB_STEP_SUMMARY");
    return (v == null || v.isEmpty()) ? null : Path.of(v);
  }
}
