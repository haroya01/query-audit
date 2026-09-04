package io.queryaudit.core.reporter;

import io.queryaudit.core.provenance.AuditCapabilities;
import io.queryaudit.core.provenance.AuditCapability;
import io.queryaudit.core.provenance.AuditInputFingerprints;
import io.queryaudit.core.provenance.ComparisonInputs;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Encodes the effective inputs of each audit without exporting configuration or policy contents.
 */
final class ComparisonInputsJson {

  private ComparisonInputsJson() {}

  static void append(StringBuilder json, Map<String, ComparisonInputs> inputsByTest) {
    json.append('{');
    boolean first = true;
    for (var entry : new TreeMap<>(inputsByTest).entrySet()) {
      if (!first) {
        json.append(',');
      }
      first = false;
      json.append("\n    ");
      string(json, entry.getKey());
      json.append(": ");
      appendInputs(json, entry.getValue());
    }
    if (!inputsByTest.isEmpty()) {
      json.append("\n  ");
    }
    json.append('}');
  }

  private static void appendInputs(StringBuilder json, ComparisonInputs inputs) {
    json.append('{');
    field(json, "queryAuditVersion", inputs.queryAuditVersion());
    json.append(',');
    field(json, "profile", inputs.profile());
    json.append(',');
    field(json, "databaseDialect", inputs.databaseDialect());
    json.append(",\"parser\":{");
    field(json, "name", inputs.parserName());
    json.append(',');
    field(json, "version", inputs.parserVersion());
    json.append("},\"detectorCapabilities\":[");
    for (int index = 0; index < inputs.detectorCapabilities().size(); index++) {
      if (index > 0) {
        json.append(',');
      }
      string(json, safeIdentity(inputs.detectorCapabilities().get(index)));
    }
    json.append("],\"detectorInputsComplete\":").append(inputs.detectorInputsComplete());
    json.append(",\"capabilities\":{");
    capability(json, "indexMetadata", inputs.capabilities().indexMetadata());
    json.append(',');
    capability(json, "hibernateEvents", inputs.capabilities().hibernateEvents());
    json.append(',');
    capability(json, "explain", inputs.capabilities().explain());
    json.append(',');
    capability(json, "repositoryReturnTypes", inputs.capabilities().repositoryReturnTypes());
    json.append("},\"fingerprints\":{");
    field(json, "ruleSettings", inputs.fingerprints().ruleSettings());
    json.append(',');
    field(json, "thresholds", inputs.fingerprints().thresholds());
    json.append(',');
    field(json, "suppressions", inputs.fingerprints().suppressions());
    json.append(',');
    field(json, "queryContracts", inputs.fingerprints().queryContracts());
    json.append(',');
    field(json, "findingBaseline", inputs.fingerprints().findingBaseline());
    json.append("}}");
  }

  private static void capability(StringBuilder json, String name, AuditCapability capability) {
    string(json, name);
    json.append(":{");
    field(json, "state", capability.state().name());
    json.append(',');
    field(json, "source", capability.source());
    json.append(",\"inputsComplete\":").append(capability.inputsComplete());
    json.append('}');
  }

  private static void field(StringBuilder json, String name, String value) {
    string(json, name);
    json.append(':');
    string(json, safeIdentity(value));
  }

  private static String safeIdentity(String value) {
    if (value.matches("[A-Za-z0-9_.:$@;+\\-]+") || value.equals("microsoft sql server")) {
      return value;
    }
    return fingerprint(value);
  }

  static String fingerprint(String value) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      for (int index = 0; index < value.length(); index++) {
        char unit = value.charAt(index);
        digest.update((byte) (unit >>> 8));
        digest.update((byte) unit);
      }
      return "sha256:" + HexFormat.of().formatHex(digest.digest());
    } catch (NoSuchAlgorithmException exception) {
      throw new IllegalStateException("SHA-256 is unavailable", exception);
    }
  }

  static void string(StringBuilder json, String value) {
    if (value == null) {
      json.append("null");
    } else {
      json.append('"').append(JsonReporter.escapeJson(value)).append('"');
    }
  }

  static Map<String, ComparisonInputs> read(Map<?, ?> envelope, boolean required) {
    if (!required) {
      return Map.of();
    }
    Map<?, ?> values = object(envelope.get("comparisonInputs"), "comparisonInputs");
    Map<String, ComparisonInputs> inputs = new LinkedHashMap<>();
    for (var entry : values.entrySet()) {
      if (!(entry.getKey() instanceof String testId) || testId.isBlank()) {
        throw invalid("comparisonInputs keys must be nonblank test IDs");
      }
      inputs.put(testId, readInputs(object(entry.getValue(), "comparisonInputs entry")));
    }
    return Map.copyOf(inputs);
  }

  private static ComparisonInputs readInputs(Map<?, ?> value) {
    requireFields(
        value,
        Set.of(
            "queryAuditVersion",
            "profile",
            "databaseDialect",
            "parser",
            "detectorCapabilities",
            "detectorInputsComplete",
            "capabilities",
            "fingerprints"));
    Map<?, ?> parser = object(value.get("parser"), "parser");
    requireFields(parser, Set.of("name", "version"));
    Map<?, ?> capabilities = object(value.get("capabilities"), "capabilities");
    requireFields(
        capabilities,
        Set.of("indexMetadata", "hibernateEvents", "explain", "repositoryReturnTypes"));
    Map<?, ?> hashes = object(value.get("fingerprints"), "fingerprints");
    requireFields(
        hashes,
        Set.of("ruleSettings", "thresholds", "suppressions", "queryContracts", "findingBaseline"));
    try {
      return new ComparisonInputs(
          text(value, "queryAuditVersion"),
          text(value, "profile"),
          text(value, "databaseDialect"),
          text(parser, "name"),
          text(parser, "version"),
          strings(value, "detectorCapabilities"),
          bool(value, "detectorInputsComplete"),
          new AuditCapabilities(
              readCapability(capabilities.get("indexMetadata")),
              readCapability(capabilities.get("hibernateEvents")),
              readCapability(capabilities.get("explain")),
              readCapability(capabilities.get("repositoryReturnTypes"))),
          new AuditInputFingerprints(
              text(hashes, "ruleSettings"),
              text(hashes, "thresholds"),
              text(hashes, "suppressions"),
              text(hashes, "queryContracts"),
              text(hashes, "findingBaseline")));
    } catch (IllegalArgumentException exception) {
      throw invalid("comparisonInputs contains invalid or unverifiable metadata");
    }
  }

  private static AuditCapability readCapability(Object value) {
    Map<?, ?> capability = object(value, "capability");
    requireFields(capability, Set.of("state", "source", "inputsComplete"));
    return new AuditCapability(
        AuditCapability.State.valueOf(text(capability, "state")),
        text(capability, "source"),
        bool(capability, "inputsComplete"));
  }

  private static Map<?, ?> object(Object value, String field) {
    if (!(value instanceof Map<?, ?> object)) {
      throw invalid(field + " must be an object");
    }
    return object;
  }

  private static void requireFields(Map<?, ?> value, Set<String> fields) {
    if (!value.keySet().equals(fields)) {
      throw invalid("comparisonInputs fields are missing or unsupported");
    }
  }

  private static String text(Map<?, ?> value, String field) {
    if (!(value.get(field) instanceof String text)) {
      throw invalid(field + " must be a string");
    }
    return text;
  }

  private static boolean bool(Map<?, ?> value, String field) {
    if (!(value.get(field) instanceof Boolean bool)) {
      throw invalid(field + " must be a boolean");
    }
    return bool;
  }

  private static List<String> strings(Map<?, ?> value, String field) {
    if (!(value.get(field) instanceof List<?> list)) {
      throw invalid(field + " must be an array");
    }
    List<String> strings = new ArrayList<>();
    for (Object entry : list) {
      if (!(entry instanceof String text)) {
        throw invalid(field + " entries must be strings");
      }
      strings.add(text);
    }
    return strings;
  }

  private static IllegalArgumentException invalid(String message) {
    return new IllegalArgumentException("Invalid report comparison inputs: " + message);
  }
}
