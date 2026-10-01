package io.queryaudit.core.extension.internal;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.DetectionRule;
import io.queryaudit.core.extension.AuditRule;
import io.queryaudit.core.extension.AuditRuleException;
import io.queryaudit.core.extension.FindingKindId;
import io.queryaudit.core.extension.LegacyDetectionRuleAdapter;
import io.queryaudit.core.extension.RuleContext;
import io.queryaudit.core.extension.RuleDescriptor;
import io.queryaudit.core.extension.RuleId;
import io.queryaudit.core.model.Finding;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.Set;

/** Host implementation detail; not a supported extension point. */
public final class AuditRuleRuntime {
  private final List<RegisteredRule> registrations;

  private record RegisteredRule(AuditRule rule, RuleDescriptor descriptor, String registrationId) {
    AuditRuleException failure(AuditRuleException.Reason reason) {
      return new AuditRuleException(registrationId, descriptor.id(), reason);
    }
  }

  public AuditRuleRuntime(
      QueryAuditConfig config, List<AuditRule> explicit, List<DetectionRule> legacyRules) {
    this(config, explicit, null, legacyRules);
  }

  public AuditRuleRuntime(
      QueryAuditConfig config, Map<String, AuditRule> explicit, List<DetectionRule> legacyRules) {
    this(config, List.of(), explicit, legacyRules);
  }

  private AuditRuleRuntime(
      QueryAuditConfig config,
      List<AuditRule> explicit,
      Map<String, AuditRule> identified,
      List<DetectionRule> legacyRules) {
    List<AuditRule> discovered =
        ServiceLoader.load(AuditRule.class).stream()
            .sorted(Comparator.comparing(provider -> provider.type().getName()))
            .map(ServiceLoader.Provider::get)
            .toList();
    List<RegisteredRule> discoveredRegistrations = selected(config, discovered);
    List<RegisteredRule> explicitRegistrations =
        identified == null ? selected(config, explicit) : selectedRegistered(config, identified);
    Set<Class<?>> discoveredClasses = new HashSet<>();
    discoveredRegistrations.forEach(
        entry -> discoveredClasses.add(implementationClass(entry.rule())));
    for (RegisteredRule entry : explicitRegistrations) {
      if (discoveredClasses.contains(implementationClass(entry.rule()))) {
        throw new IllegalArgumentException(
            "Audit rule "
                + implementationClass(entry.rule()).getName()
                + " is registered through both ServiceLoader and explicit extensions; choose one"
                + " registration path");
      }
    }
    List<RegisteredRule> all = new ArrayList<>(discoveredRegistrations);
    all.addAll(explicitRegistrations);
    rejectConflictingIdentities(all, legacyRules);
    registrations = List.copyOf(all);
  }

  public List<AuditRule> rules() {
    return registrations.stream().map(RegisteredRule::rule).toList();
  }

  public List<RuleDescriptor> descriptors() {
    return registrations.stream().map(RegisteredRule::descriptor).toList();
  }

  public List<Finding> evaluate(RuleContext context) {
    List<Finding> result = new ArrayList<>();
    for (RegisteredRule registration : registrations) {
      result.addAll(
          registration.registrationId() == null
              ? evaluate(registration.rule(), registration.descriptor(), context)
              : evaluateRegistered(registration, context));
    }
    return List.copyOf(result);
  }

  public static RuleDescriptor validateDescriptor(AuditRule rule) {
    Objects.requireNonNull(rule, "rule");
    RuleDescriptor descriptor = Objects.requireNonNull(rule.descriptor(), "Audit rule descriptor");
    if (invalidDeclaration(rule, descriptor)) {
      throw new IllegalArgumentException(
          "Custom audit rule "
              + descriptor.id()
              + " must not use built-in finding kinds or reserved core/query-audit namespaces");
    }
    return descriptor;
  }

  public static List<Finding> evaluate(
      AuditRule rule, RuleDescriptor expected, RuleContext context) {
    if (!expected.equals(validateDescriptor(rule))) {
      throw new IllegalArgumentException("Audit rule descriptor changed: " + expected.id());
    }
    List<Finding> returned = rule.evaluate(Objects.requireNonNull(context, "context"));
    if (returned == null) {
      throw new IllegalArgumentException("Audit rule returned null findings: " + expected.id());
    }
    List<Finding> snapshot = checkedFindings(expected, returned);
    if (!expected.equals(validateDescriptor(rule))) {
      throw new IllegalArgumentException(
          "Audit rule descriptor changed during evaluation: " + expected.id());
    }
    return snapshot;
  }

  private static List<Finding> checkedFindings(RuleDescriptor expected, List<Finding> returned) {
    List<Finding> snapshot = new ArrayList<>(returned);
    for (Finding finding : snapshot) {
      if (finding == null || !expected.findingKinds().contains(finding.kindId())) {
        throw new IllegalArgumentException(
            "Audit rule "
                + expected.id()
                + " returned a null finding or an undeclared finding kind");
      }
    }
    return List.copyOf(snapshot);
  }

  private static List<Finding> evaluateRegistered(
      RegisteredRule registration, RuleContext context) {
    Objects.requireNonNull(context, "context");
    requireStableDescriptor(registration);
    List<Finding> returned;
    try {
      returned = registration.rule().evaluate(context);
    } catch (RuntimeException | LinkageError failure) {
      throw registration.failure(AuditRuleException.Reason.EXECUTION_FAILED);
    }
    List<Finding> snapshot;
    try {
      snapshot = checkedFindings(registration.descriptor(), returned);
    } catch (RuntimeException | LinkageError failure) {
      throw registration.failure(AuditRuleException.Reason.INVALID_RESULT);
    }
    requireStableDescriptor(registration);
    return snapshot;
  }

  private static void requireStableDescriptor(RegisteredRule registration) {
    RuleDescriptor current =
        registeredDescriptor(registration.rule(), registration.registrationId());
    if (!registration.descriptor().equals(current))
      throw registration.failure(AuditRuleException.Reason.DESCRIPTOR_CHANGED);
  }

  private static RuleDescriptor registeredDescriptor(AuditRule rule, String registrationId) {
    RuleDescriptor descriptor;
    try {
      descriptor = Objects.requireNonNull(rule.descriptor(), "Audit rule descriptor");
    } catch (RuntimeException | LinkageError failure) {
      throw new AuditRuleException(
          registrationId, null, AuditRuleException.Reason.DECLARATION_FAILED);
    }
    if (invalidDeclaration(rule, descriptor))
      throw new AuditRuleException(
          registrationId, descriptor.id(), AuditRuleException.Reason.INVALID_DECLARATION);
    return descriptor;
  }

  private static List<RegisteredRule> selectedRegistered(
      QueryAuditConfig config, Map<String, AuditRule> rules) {
    List<RegisteredRule> selected = new ArrayList<>();
    int ordinal = 0;
    for (Map.Entry<String, AuditRule> entry : rules.entrySet()) {
      String id = RuleRegistrationIds.safe(entry.getKey(), ++ordinal);
      RuleDescriptor descriptor = registeredDescriptor(entry.getValue(), id);
      if (descriptor.findingKinds().stream().anyMatch(kind -> !config.isRuleExcluded(kind.value())))
        selected.add(new RegisteredRule(entry.getValue(), descriptor, id));
    }
    return selected;
  }

  private static boolean invalidDeclaration(AuditRule rule, RuleDescriptor descriptor) {
    return !(rule instanceof LegacyDetectionRuleAdapter)
        && (reservedNamespace(descriptor.id().value())
            || descriptor.findingKinds().stream()
                .anyMatch(kind -> kind.isBuiltin() || reservedNamespace(kind.value())));
  }

  private static List<RegisteredRule> selected(QueryAuditConfig config, List<AuditRule> rules) {
    List<RegisteredRule> selected = new ArrayList<>();
    for (AuditRule rule : rules) {
      RuleDescriptor descriptor = validateDescriptor(rule);
      if (descriptor.findingKinds().stream()
          .anyMatch(kind -> !config.isRuleExcluded(kind.value()))) {
        selected.add(new RegisteredRule(rule, descriptor, null));
      }
    }
    return selected;
  }

  private static void rejectConflictingIdentities(
      List<RegisteredRule> registrations, List<DetectionRule> legacyRules) {
    Set<RuleId> ids = new HashSet<>();
    Map<FindingKindId, RuleId> kindOwners = new HashMap<>();
    Set<Class<?>> legacyClasses = new HashSet<>();
    legacyRules.forEach(rule -> legacyClasses.add(rule.getClass()));
    for (RegisteredRule registration : registrations) {
      RuleDescriptor descriptor = registration.descriptor();
      if (!ids.add(descriptor.id())) {
        throw new IllegalArgumentException("Duplicate audit rule ID: " + descriptor.id());
      }
      if (registration.rule() instanceof LegacyDetectionRuleAdapter adapter
          && legacyClasses.contains(adapter.delegate().getClass())) {
        throw new IllegalArgumentException(
            "Legacy detector "
                + adapter.delegate().getClass().getName()
                + " is registered both directly and through an AuditRule adapter; choose one"
                + " registration path");
      }
      for (FindingKindId kind : descriptor.findingKinds()) {
        RuleId previous = kindOwners.putIfAbsent(kind, descriptor.id());
        if (previous != null) {
          throw new IllegalArgumentException(
              "Finding kind "
                  + kind
                  + " is declared by both "
                  + previous
                  + " and "
                  + descriptor.id());
        }
      }
    }
  }

  private static Class<?> implementationClass(AuditRule rule) {
    return rule instanceof LegacyDetectionRuleAdapter adapter
        ? adapter.delegate().getClass()
        : rule.getClass();
  }

  private static boolean reservedNamespace(String value) {
    return value.startsWith("core:") || value.startsWith("query-audit:");
  }
}
