package io.queryaudit.junit5;

import io.queryaudit.core.config.ReportFormat;
import java.nio.file.Path;
import java.util.Locale;

/** Raised when QueryAudit cannot write the suite artifact selected for the current test run. */
final class ReportWriteException extends RuntimeException {

  private final ReportFormat format;
  private final Path reportPath;

  ReportWriteException(ReportFormat format, Path reportPath, Exception cause) {
    super(message(format, reportPath), cause);
    this.format = format;
    this.reportPath = reportPath.toAbsolutePath().normalize();
  }

  ReportFormat format() {
    return format;
  }

  Path reportPath() {
    return reportPath;
  }

  private static String message(ReportFormat format, Path reportPath) {
    return "QueryAudit could not write the "
        + format.name().toLowerCase(Locale.ROOT)
        + " report to '"
        + reportPath.toAbsolutePath().normalize()
        + "'. The audit run is incomplete.";
  }
}
