package io.queryaudit.core.reporter;

import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryAuditReport;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
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

  public GitHubActionsReporter() {
    this(System.out, resolveSummaryPathFromEnv());
  }

  public GitHubActionsReporter(PrintStream out, Path summaryPath) {
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
    List<Issue> errors = report.getErrors();
    if (errors != null) {
      for (Issue i : errors) {
        emit("error", i, report);
      }
    }
    List<Issue> warnings = report.getWarnings();
    if (warnings != null) {
      for (Issue i : warnings) {
        emit("warning", i, report);
      }
    }
    List<Issue> infos = report.getInfoIssues();
    if (infos != null) {
      for (Issue i : infos) {
        emit("notice", i, report);
      }
    }
  }

  private void emit(String level, Issue issue, QueryAuditReport report) {
    StringBuilder props = new StringBuilder();
    Location loc = parseLocation(issue.sourceLocation());
    if (loc != null) {
      append(props, "file=" + escapeProp(loc.file));
      append(props, "line=" + loc.line);
    }
    append(props, "title=" + escapeProp(titleFor(issue, report)));

    StringBuilder sb = new StringBuilder("::");
    sb.append(level);
    if (props.length() > 0) {
      sb.append(' ').append(props);
    }
    sb.append("::");
    sb.append(escapeBody(messageFor(issue)));

    out.println(sb);
  }

  private static void append(StringBuilder props, String kv) {
    if (props.length() > 0) {
      props.append(',');
    }
    props.append(kv);
  }

  private static String titleFor(Issue issue, QueryAuditReport report) {
    String type = issue.type() != null ? issue.type().getCode() : "query-audit";
    String testClass = report.getTestClass();
    return testClass != null ? testClass + " — " + type : type;
  }

  private static String messageFor(Issue issue) {
    StringBuilder body = new StringBuilder();
    if (issue.detail() != null) {
      body.append(issue.detail());
    } else {
      body.append(issue.type() != null ? issue.type().getDescription() : "issue");
    }
    if (issue.suggestion() != null && !issue.suggestion().isEmpty()) {
      body.append("\n\nSuggestion: ").append(issue.suggestion());
    }
    if (issue.query() != null) {
      body.append("\n\nQuery: ").append(issue.query());
    }
    return body.toString();
  }

  private void appendSummary(QueryAuditReport report) {
    int errorCount = safeSize(report.getErrors());
    int warningCount = safeSize(report.getWarnings());
    int infoCount = safeSize(report.getInfoIssues());

    StringBuilder md = new StringBuilder();
    md.append("### query-audit — ")
        .append(report.getTestClass() != null ? report.getTestClass() : "report")
        .append("\n\n");
    md.append("| Severity | Count |\n");
    md.append("| --- | ---: |\n");
    md.append("| ERROR | ").append(errorCount).append(" |\n");
    md.append("| WARNING | ").append(warningCount).append(" |\n");
    md.append("| INFO | ").append(infoCount).append(" |\n\n");

    if (errorCount + warningCount + infoCount > 0) {
      md.append("<details><summary>Top issues</summary>\n\n");
      appendTopIssues(md, "ERROR", report.getErrors());
      appendTopIssues(md, "WARNING", report.getWarnings());
      appendTopIssues(md, "INFO", report.getInfoIssues());
      md.append("\n</details>\n");
    }

    try {
      Files.writeString(
          summaryPath, md.toString(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    } catch (IOException e) {
      System.err.println("[QueryAudit] Could not write GitHub step summary: " + e.getMessage());
    }
  }

  private static void appendTopIssues(StringBuilder md, String level, List<Issue> issues) {
    if (issues == null || issues.isEmpty()) {
      return;
    }
    md.append("\n**").append(level).append("**\n\n");
    int limit = Math.min(issues.size(), 5);
    for (int i = 0; i < limit; i++) {
      Issue issue = issues.get(i);
      md.append("- `")
          .append(issue.type() != null ? issue.type().getCode() : "query-audit")
          .append("`");
      if (issue.table() != null) {
        md.append(" on `").append(issue.table()).append("`");
      }
      if (issue.detail() != null) {
        md.append(" — ").append(firstLine(issue.detail()));
      }
      md.append("\n");
    }
    if (issues.size() > limit) {
      md.append("- _…and ").append(issues.size() - limit).append(" more_\n");
    }
  }

  private static String firstLine(String s) {
    int nl = s.indexOf('\n');
    return nl < 0 ? s : s.substring(0, nl);
  }

  private static int safeSize(List<?> list) {
    return list == null ? 0 : list.size();
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
