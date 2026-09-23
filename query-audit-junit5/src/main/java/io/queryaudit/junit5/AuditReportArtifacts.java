package io.queryaudit.junit5;

import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.reporter.JsonReporter;
import java.awt.Desktop;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/** Atomic artifact replacement and local browser opening; no analysis or assertion policy. */
final class AuditReportArtifacts {
  @FunctionalInterface
  interface JsonWriter {
    void write(AuditRunResult result, Path directory, ReportRedaction redaction) throws IOException;
  }

  @FunctionalInterface
  interface MoveOperation {
    void move(Path source, Path target) throws IOException;
  }

  private final MoveOperation moveOperation;

  AuditReportArtifacts(MoveOperation moveOperation) {
    this.moveOperation = moveOperation;
  }

  void write(AuditRunResult runResult, Path outputDir, ReportRedaction redaction)
      throws IOException {
    Path jsonPath = outputDir.resolve("report.json");
    Path temporaryPath = null;
    try {
      Files.createDirectories(outputDir);
      temporaryPath = Files.createTempFile(outputDir, ".report-", ".json.tmp");
      Files.writeString(
          temporaryPath,
          JsonReporter.toRunEnvelopeJson(runResult, redaction),
          StandardCharsets.UTF_8);
      moveOperation.move(temporaryPath, jsonPath);
      temporaryPath = null;
    } catch (IOException | RuntimeException failure) {
      deleteFailedReport(temporaryPath, failure);
      deleteFailedReport(jsonPath, failure);
      throw failure;
    }
    System.out.println("[QueryAudit] JSON report: " + jsonPath.toAbsolutePath());
  }

  static void moveFile(Path source, Path target) throws IOException {
    try {
      Files.move(
          source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (AtomicMoveNotSupportedException ignored) {
      Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  private static void deleteFailedReport(Path path, Throwable failure) {
    if (path == null) {
      return;
    }
    try {
      Files.deleteIfExists(path);
    } catch (IOException | RuntimeException cleanupFailure) {
      failure.addSuppressed(cleanupFailure);
    }
  }

  static void openInBrowser(Path reportPath) {
    try {
      File reportFile = reportPath.toFile();
      if (!reportFile.exists()) {
        return;
      }

      if (Desktop.isDesktopSupported()) {
        Desktop desktop = Desktop.getDesktop();
        if (desktop.isSupported(Desktop.Action.BROWSE)) {
          desktop.browse(reportFile.toURI());
          System.out.println("[QueryAudit] Report opened in browser.");
          return;
        }
      }

      String os = System.getProperty("os.name", "").toLowerCase();
      ProcessBuilder pb;
      if (os.contains("mac")) {
        pb = new ProcessBuilder("open", reportFile.getAbsolutePath());
      } else if (os.contains("win")) {
        pb = new ProcessBuilder("cmd", "/c", "start", "", reportFile.getAbsolutePath());
      } else {
        pb = new ProcessBuilder("xdg-open", reportFile.getAbsolutePath());
      }
      pb.redirectErrorStream(true);
      pb.start();
      System.out.println("[QueryAudit] Report opened in browser.");
    } catch (Exception e) {
      System.err.println("[QueryAudit] Could not open browser: " + e.getMessage());
      System.err.println("[QueryAudit] Open manually: " + reportPath.toAbsolutePath());
    }
  }
}
