package io.queryaudit.core.provenance;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Properties;

/**
 * Reads artifact identities without substituting a guessed or unknown version.
 *
 * @since 0.6.0
 */
public final class AuditRuntimeIdentity {
  private AuditRuntimeIdentity() {}

  public static String queryAuditVersion() {
    try (InputStream input =
        AuditRuntimeIdentity.class.getResourceAsStream(
            "/META-INF/query-audit/version.properties")) {
      if (input == null) {
        throw new IllegalStateException("QueryAudit version metadata is missing");
      }
      Properties properties = new Properties();
      properties.load(input);
      String version = properties.getProperty("version");
      if (version == null || version.isBlank()) {
        throw new IllegalStateException("QueryAudit version metadata does not contain a version");
      }
      return version;
    } catch (IOException exception) {
      throw new IllegalStateException("QueryAudit version metadata cannot be read", exception);
    }
  }

  /** Bundled database adapters have no user-supplied settings outside the captured inputs. */
  public static boolean hasKnownCapabilityInputs(Class<?> type) {
    return switch (type.getName()) {
      case "io.queryaudit.mysql.MySqlIndexMetadataProvider",
              "io.queryaudit.mysql.MySqlExplainAnalyzer",
              "io.queryaudit.postgresql.PostgreSqlIndexMetadataProvider",
              "io.queryaudit.postgresql.PostgreSqlExplainAnalyzer" ->
          true;
      default -> false;
    };
  }

  /** Identifies an unverified extension without requiring a resource for hidden lambda classes. */
  public static String unverifiedImplementation(Class<?> type) {
    try {
      return implementation(type);
    } catch (IllegalStateException unavailableBytes) {
      return type.getName().split("/", 2)[0] + "@unverified";
    }
  }

  /** Identifies the loaded implementation, including custom detectors without manifest metadata. */
  public static String implementation(Class<?> type) {
    String resource = "/" + type.getName().replace('.', '/') + ".class";
    try (InputStream input = type.getResourceAsStream(resource)) {
      if (input == null) {
        throw new IllegalStateException("Cannot identify implementation " + type.getName());
      }
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] buffer = new byte[8192];
      int count;
      while ((count = input.read(buffer)) != -1) {
        digest.update(buffer, 0, count);
      }
      return type.getName() + "@" + HexFormat.of().formatHex(digest.digest());
    } catch (IOException | NoSuchAlgorithmException exception) {
      throw new IllegalStateException(
          "Cannot identify implementation " + type.getName(), exception);
    }
  }
}
