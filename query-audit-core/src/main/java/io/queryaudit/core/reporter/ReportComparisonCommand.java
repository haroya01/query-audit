package io.queryaudit.core.reporter;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** CLI argument and file handling, separate from report comparison semantics. */
final class ReportComparisonCommand {
  private static final String USAGE =
      "usage: java io.queryaudit.core.reporter.ReportComparator"
          + " <before.json> <after.json> [verdict.json] [--require-resolved <findingId>]...";

  private ReportComparisonCommand() {}

  private record Arguments(Path baseline, Path candidate, Path output, List<String> requiredIds) {
    static Arguments parse(String[] args) {
      List<String> paths = new ArrayList<>();
      List<String> targets = new ArrayList<>();
      boolean positionalOnly = false;
      for (int i = 0; i < args.length; i++) {
        String argument = args[i];
        if (!positionalOnly && argument.equals("--")) {
          positionalOnly = true;
        } else if (!positionalOnly && argument.equals("--require-resolved")) {
          if (++i == args.length) {
            throw new IllegalArgumentException("--require-resolved needs a finding ID");
          }
          targets.add(args[i]);
        } else if (!positionalOnly && argument.startsWith("--require-resolved=")) {
          targets.add(argument.substring("--require-resolved=".length()));
        } else if (!positionalOnly && argument.startsWith("--")) {
          throw new IllegalArgumentException("Unknown option: " + argument);
        } else {
          paths.add(argument);
        }
      }
      if (paths.size() < 2 || paths.size() > 3) {
        throw new IllegalArgumentException(
            "Expected two report paths and an optional verdict path");
      }
      return new Arguments(
          Path.of(paths.get(0)),
          Path.of(paths.get(1)),
          paths.size() == 3 ? Path.of(paths.get(2)) : null,
          ComparisonTargets.requested(targets));
    }
  }

  static int run(String[] args) {
    Arguments arguments;
    try {
      arguments = Arguments.parse(args);
    } catch (IllegalArgumentException e) {
      System.err.println("[QueryAudit] " + e.getMessage());
      System.err.println(USAGE);
      return 2;
    }
    ReportComparator.Verdict verdict;
    try {
      verdict =
          ReportComparator.compare(
              Files.readString(arguments.baseline(), StandardCharsets.UTF_8),
              Files.readString(arguments.candidate(), StandardCharsets.UTF_8),
              arguments.requiredIds());
    } catch (Exception e) {
      System.err.println("[QueryAudit] compare failed: " + e.getMessage());
      return 2;
    }
    System.out.println(ReportComparator.toSummary(verdict));
    if (arguments.output() != null) {
      try {
        Files.writeString(
            arguments.output(), ReportComparator.toJson(verdict), StandardCharsets.UTF_8);
        System.out.println("[QueryAudit] verdict: " + arguments.output().toAbsolutePath());
      } catch (Exception e) {
        System.err.println(
            "[QueryAudit] could not write verdict to '"
                + arguments.output()
                + "': "
                + e.getMessage());
        return 2;
      }
    }
    return ReportComparator.exitCode(verdict);
  }
}
