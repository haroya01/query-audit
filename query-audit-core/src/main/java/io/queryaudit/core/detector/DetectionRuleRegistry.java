package io.queryaudit.core.detector;

import io.queryaudit.core.config.QueryAuditConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

/** Builds the ordered set of detection rules used by an analyzer. */
final class DetectionRuleRegistry {

  record RuleSet(List<DetectionRule> rules, boolean inputsComplete) {
    RuleSet {
      rules = List.copyOf(rules);
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
    List<DetectionRule> rules = createBuiltInRules();
    rules.removeIf(this::isRuleDisabled);
    List<DetectionRule> discovered = new ArrayList<>();
    ServiceLoader.load(DetectionRule.class).forEach(discovered::add);
    discovered.removeIf(this::isRuleDisabled);
    rules.addAll(discovered);

    boolean inputsComplete = discovered.isEmpty();
    if (additionalRules != null && !additionalRules.isEmpty()) {
      rules.addAll(additionalRules);
      inputsComplete = false;
    }
    return new RuleSet(rules, inputsComplete);
  }

  private List<DetectionRule> createBuiltInRules() {
    List<DetectionRule> rules = new ArrayList<>();
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
