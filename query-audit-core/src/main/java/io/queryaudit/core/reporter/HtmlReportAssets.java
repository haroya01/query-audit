package io.queryaudit.core.reporter;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

/**
 * Bundled assets stay embedded so each generated page works without a server or companion files.
 */
final class HtmlReportAssets {
  private static final String STYLES = load("report.css");
  private static final String SCRIPT = load("report.js");

  private HtmlReportAssets() {}

  static void appendStyles(StringBuilder html) {
    html.append("<style>\n").append(STYLES).append("</style>\n");
  }

  static void appendScript(StringBuilder html) {
    html.append("<script>\n").append(SCRIPT).append("</script>\n");
  }

  private static String load(String name) {
    String path = "/io/queryaudit/core/reporter/html/" + name;
    try (InputStream input = HtmlReportAssets.class.getResourceAsStream(path)) {
      if (input == null) {
        throw new IllegalStateException("Missing bundled HTML report asset: " + path);
      }
      return new String(input.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException failure) {
      throw new UncheckedIOException("Cannot read bundled HTML report asset: " + path, failure);
    }
  }
}
