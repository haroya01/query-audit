package io.queryaudit.core.detector;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.extension.AuditRuleException;
import io.queryaudit.core.extension.internal.RuleRegistrationIds;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

/** Builds the ordered set of detection rules used by an analyzer. */
final class DetectionRuleRegistry {

  record RuleSet(List<DetectionRuleRegistration> registrations, boolean inputsComplete) {
    RuleSet {
      registrations = List.copyOf(registrations);
    }

    List<DetectionRule> rules() {
      return registrations.stream().map(DetectionRuleRegistration::rule).toList();
    }
  }

  private final QueryAuditConfig config;

  DetectionRuleRegistry(QueryAuditConfig config) {
    this.config = config;
  }

  List<DetectionRule> createRules() {
    return createRules(null);
  }

  List<DetectionRule> createRules(List<DetectionRule> additionalRules) {
    return createRuleSet(additionalRules).rules();
  }

  RuleSet createRuleSet(List<DetectionRule> additionalRules) {
    List<DetectionRuleRegistration> explicit =
        additionalRules == null
            ? List.of()
            : additionalRules.stream()
                .map(rule -> new DetectionRuleRegistration(rule, null))
                .toList();
    return assemble(explicit);
  }

  RuleSet createRegisteredRuleSet(Map<String, DetectionRule> additionalRules) {
    List<DetectionRuleRegistration> explicit = new ArrayList<>();
    additionalRules.forEach(
        (id, rule) ->
            explicit.add(
                new DetectionRuleRegistration(
                    rule, RuleRegistrationIds.safe(id, explicit.size() + 1))));
    return assemble(explicit);
  }

  private RuleSet assemble(List<DetectionRuleRegistration> additionalRules) {
    List<DetectionRule> rules = createBuiltInRules();
    rules.removeIf(this::isRuleDisabled);
    List<DetectionRule> discovered = new ArrayList<>();
    ServiceLoader.load(DetectionRule.class).forEach(discovered::add);
    // Apply the same selection policy regardless of how the host registered an external rule.
    discovered.removeIf(this::isRuleDisabled);
    List<DetectionRuleRegistration> explicit =
        additionalRules.stream()
            .filter(registration -> !isRegistrationDisabled(registration))
            .toList();
    rejectCrossPathDuplicates(
        discovered, explicit.stream().map(DetectionRuleRegistration::rule).toList());
    rules.addAll(discovered);
    List<DetectionRuleRegistration> registrations = new ArrayList<>();
    rules.forEach(rule -> registrations.add(new DetectionRuleRegistration(rule, null)));
    registrations.addAll(explicit);

    // A registration ID identifies a rule, not all of its configuration or dependencies.
    return new RuleSet(registrations, discovered.isEmpty() && explicit.isEmpty());
  }

  private boolean isRegistrationDisabled(DetectionRuleRegistration registration) {
    if (registration.registrationId() == null) return isRuleDisabled(registration.rule());
    try {
      return isRuleDisabled(registration.rule());
    } catch (RuntimeException | LinkageError failure) {
      throw registration.failure(AuditRuleException.Reason.DECLARATION_FAILED);
    }
  }

  private static void rejectCrossPathDuplicates(
      List<DetectionRule> discovered, List<DetectionRule> explicit) {
    Set<Class<?>> discoveredTypes = new HashSet<>();
    discovered.forEach(rule -> discoveredTypes.add(rule.getClass()));
    for (DetectionRule rule : explicit) {
      if (discoveredTypes.contains(rule.getClass())) {
        throw new IllegalArgumentException(
            "Detection rule "
                + rule.getClass().getName()
                + " is registered through both ServiceLoader and explicit extensions; "
                + "choose one registration path");
      }
    }
  }

  private List<DetectionRule> createBuiltInRules() {
    List<DetectionRule> rules = new ArrayList<>();
    rules.add(new CallSiteNPlusOneDetector(config.getNPlusOneThreshold()));
    rules.add(new NPlusOneDetector(config.getNPlusOneThreshold()));
    rules.add(new SelectAllDetector());
    rules.add(new WhereFunctionDetector());
    rules.add(new OrAbuseDetector(config.getOrClauseThreshold()));
    rules.add(new OffsetPaginationDetector(config.getOffsetPaginationThreshold()));
    rules.add(new MissingIndexDetector());
    rules.add(new CompositeIndexDetector());
    rules.add(new LikeWildcardDetector());
    // DuplicateQueryDetector is disabled because datasource-proxy provides SQL with '?'
    // placeholders, so different parameter values cannot be distinguished. NPlusOneDetector
    // already covers repeated patterns. Re-enable it when parameter tracking is available.
    rules.add(new CartesianJoinDetector());
    rules.add(new CorrelatedSubqueryDetector());
    rules.add(new ForUpdateWithoutIndexDetector());
    rules.add(new RedundantFilterDetector());
    rules.add(new SargabilityDetector());
    rules.add(new IndexRedundancyDetector());
    rules.add(new SlowQueryDetector(config.getSlowQueryWarningMs(), config.getSlowQueryErrorMs()));
    rules.add(new CountInsteadOfExistsDetector(config.isCountInsteadOfExistsEnabled()));
    rules.add(new UnboundedResultSetDetector(config.getRepositoryReturnTypeResolver()));
    rules.add(new WriteAmplificationDetector(config.getWriteAmplificationThreshold()));
    rules.add(new ImplicitTypeConversionDetector());
    rules.add(new UnionWithoutAllDetector());
    rules.add(new CoveringIndexDetector());
    rules.add(new OrderByLimitWithoutIndexDetector());
    rules.add(new LargeInListDetector(config.getLargeInListThreshold()));
    rules.add(new DistinctMisuseDetector());
    rules.add(new NullComparisonDetector());
    rules.add(new HavingMisuseDetector());
    rules.add(new RangeLockDetector());
    rules.add(new ReadModifyWriteDetector());
    rules.add(new UpdateWithoutWhereDetector());
    rules.add(new DmlWithoutIndexDetector());
    rules.add(
        new RepeatedSingleInsertDetector(
            config.getRepeatedInsertThreshold(), config.getRepeatedInsertExcludeTables()));
    rules.add(
        new RepeatedSingleUpdateDetector(
            config.getRepeatedUpdateThreshold(), config.getRepeatedUpdateExcludeTables()));
    rules.add(new InsertSelectAllDetector());
    rules.add(new OrderByRandDetector());
    rules.add(new NotInSubqueryDetector());
    rules.add(new TooManyJoinsDetector(config.getTooManyJoinsThreshold()));
    rules.add(new ImplicitJoinDetector());
    rules.add(new StringConcatInWhereDetector());
    rules.add(new SelectCountStarWithoutWhereDetector());
    rules.add(new InsertOnDuplicateKeyDetector());
    rules.add(new GroupByFunctionDetector());
    rules.add(new ForUpdateNonUniqueIndexDetector());
    rules.add(new SubqueryInDmlDetector());
    rules.add(new InsertSelectLocksSourceDetector());
    rules.add(new CollectionManagementDetector());
    rules.add(new DerivedDeleteDetector());
    rules.add(new ExcessiveColumnFetchDetector(config.getExcessiveColumnThreshold()));
    rules.add(new ImplicitColumnsInsertDetector());
    rules.add(new RegexpInsteadOfLikeDetector());
    rules.add(new FindInSetDetector());
    rules.add(new UnusedJoinDetector());
    rules.add(new MergeableQueriesDetector());
    rules.add(new NonDeterministicPaginationDetector());
    rules.add(new LimitWithoutOrderByDetector());
    rules.add(new WindowFunctionWithoutPartitionDetector());
    rules.add(new ForUpdateWithoutTimeoutDetector());
    rules.add(new CaseInWhereDetector());
    rules.add(new ForceIndexHintDetector());
    return rules;
  }

  private boolean isRuleDisabled(DetectionRule rule) {
    String ruleCode = rule.getRuleCode();
    if (ruleCode != null) {
      return config.isRuleExcluded(ruleCode);
    }

    String className = rule.getClass().getSimpleName();
    for (String disabledCode : config.getDisabledRules()) {
      if (matchesRuleCode(className, disabledCode)) {
        return true;
      }
    }
    return false;
  }

  private boolean matchesRuleCode(String className, String code) {
    StringBuilder expected = new StringBuilder();
    for (String part : code.split("-")) {
      if (part.equals("n")) {
        expected.append("N");
      } else if (!part.isEmpty()) {
        expected.append(Character.toUpperCase(part.charAt(0)));
        if (part.length() > 1) {
          expected.append(part.substring(1));
        }
      }
    }
    return className.contains(expected);
  }
}
