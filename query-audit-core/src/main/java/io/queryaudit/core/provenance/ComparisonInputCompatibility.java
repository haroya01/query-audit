package io.queryaudit.core.provenance;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Conservative compatibility checks for inputs that can change audit findings.
 *
 * @since 0.6.0
 */
public final class ComparisonInputCompatibility {
  private ComparisonInputCompatibility() {}

  public static List<ComparisonInputDifference> compare(
      String testId, ComparisonInputs baseline, ComparisonInputs candidate) {
    List<ComparisonInputDifference> differences = new ArrayList<>();
    if (baseline == null || candidate == null) {
      differences.add(
          new ComparisonInputDifference(
              testId,
              "comparisonInputs",
              baseline == null ? null : "available",
              candidate == null ? null : "available"));
      return List.copyOf(differences);
    }
    Map<String, String> before = fields(baseline);
    Map<String, String> after = fields(candidate);
    for (String field : new TreeSet<>(before.keySet())) {
      String previous = before.get(field);
      String current = after.get(field);
      boolean changed = !Objects.equals(previous, current);
      boolean failedCapability =
          field.endsWith(".state") && ("FAILED".equals(previous) || "FAILED".equals(current));
      boolean incompleteInputs =
          (field.equals("detectorInputsComplete") || field.endsWith(".inputsComplete"))
              && ("false".equals(previous) || "false".equals(current));
      if (changed || failedCapability || incompleteInputs) {
        differences.add(new ComparisonInputDifference(testId, field, previous, current));
      }
    }
    return List.copyOf(differences);
  }

  private static Map<String, String> fields(ComparisonInputs inputs) {
    Map<String, String> values = new TreeMap<>();
    values.put("queryAuditVersion", inputs.queryAuditVersion());
    values.put("profile", inputs.profile());
    values.put("databaseDialect", inputs.databaseDialect());
    values.put("parser.name", inputs.parserName());
    values.put("parser.version", inputs.parserVersion());
    values.put("detectorInputsComplete", Boolean.toString(inputs.detectorInputsComplete()));
    values.put("detectorCapabilities", CanonicalFingerprint.of(inputs.detectorCapabilities()));
    capability(values, "indexMetadata", inputs.capabilities().indexMetadata());
    capability(values, "hibernateEvents", inputs.capabilities().hibernateEvents());
    capability(values, "explain", inputs.capabilities().explain());
    capability(values, "repositoryReturnTypes", inputs.capabilities().repositoryReturnTypes());
    AuditInputFingerprints hashes = inputs.fingerprints();
    values.put("fingerprints.ruleSettings", hashes.ruleSettings());
    values.put("fingerprints.thresholds", hashes.thresholds());
    values.put("fingerprints.suppressions", hashes.suppressions());
    values.put("fingerprints.queryContracts", hashes.queryContracts());
    values.put("fingerprints.findingBaseline", hashes.findingBaseline());
    return values;
  }

  private static void capability(
      Map<String, String> values, String name, AuditCapability capability) {
    values.put("capabilities." + name + ".state", capability.state().name());
    values.put("capabilities." + name + ".source", CanonicalFingerprint.of(capability.source()));
    values.put(
        "capabilities." + name + ".inputsComplete", Boolean.toString(capability.inputsComplete()));
  }
}
