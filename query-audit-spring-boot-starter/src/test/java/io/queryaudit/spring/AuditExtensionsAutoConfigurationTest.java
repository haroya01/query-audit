package io.queryaudit.spring;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.analyzer.ExplainAnalyzer;
import io.queryaudit.core.analyzer.IndexMetadataProvider;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.detector.DetectionRule;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.detector.SelectAllDetector;
import io.queryaudit.core.extension.AuditExtensions;
import io.queryaudit.core.extension.AuditRule;
import io.queryaudit.core.extension.FindingKindId;
import io.queryaudit.core.extension.RuleContext;
import io.queryaudit.core.extension.RuleDescriptor;
import io.queryaudit.core.extension.RuleId;
import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.reporter.delivery.AuditReportSink;
import io.queryaudit.core.reporter.delivery.ReportSinkRegistration;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

class AuditExtensionsAutoConfigurationTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(QueryAuditAutoConfiguration.class));

  @Test
  void providesAnEmptyCatalogWithoutUserExtensions() {
    runner.run(
        context -> {
          assertThat(context).hasSingleBean(AuditExtensions.class);
          AuditExtensions extensions = context.getBean(AuditExtensions.class);
          assertThat(extensions.rules()).isEmpty();
          assertThat(extensions.indexMetadataProviders()).isEmpty();
          assertThat(extensions.explainAnalyzers()).isEmpty();
        });
  }

  @Test
  void collectsAllSpiBeansInBeanNameOrderAndRetainsBuiltInRules() {
    DetectionRule firstRule = (queries, metadata) -> List.of();
    DetectionRule secondRule = (queries, metadata) -> List.of();
    CombinedProvider firstProvider = new CombinedProvider();
    CombinedProvider secondProvider = new CombinedProvider();

    runner
        .withBean("zRule", DetectionRule.class, () -> secondRule)
        .withBean("aRule", DetectionRule.class, () -> firstRule)
        .withBean("zProvider", CombinedProvider.class, () -> secondProvider)
        .withBean("aProvider", CombinedProvider.class, () -> firstProvider)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              AuditExtensions extensions = context.getBean(AuditExtensions.class);
              assertThat(extensions.rules()).containsExactly(firstRule, secondRule);
              assertThat(extensions.indexMetadataProviders())
                  .containsExactly(firstProvider, secondProvider);
              assertThat(extensions.explainAnalyzers())
                  .containsExactly(firstProvider, secondProvider);

              QueryAuditAnalyzer analyzer =
                  new QueryAuditAnalyzer(
                      context.getBean(QueryAuditConfig.class), List.of(), extensions.rules());
              assertThat(analyzer.getRules()).contains(firstRule, secondRule);
              assertThat(analyzer.getRules()).anyMatch(SelectAllDetector.class::isInstance);
            });
  }

  @Test
  void aliasesDoNotRegisterTheSameBeanTwice() {
    runner
        .withUserConfiguration(AliasedRuleConfiguration.class)
        .run(
            context ->
                assertThat(context.getBean(AuditExtensions.class).rules())
                    .containsExactly(context.getBean("customRule", DetectionRule.class)));
  }

  @Test
  void collectsOpenRulesAndOptionalSinkBeans() {
    AuditRule rule =
        new AuditRule() {
          public RuleDescriptor descriptor() {
            return new RuleDescriptor(
                new RuleId("shop:latency"), "1", Set.of(FindingKindId.of("shop:slow")), Map.of());
          }

          public List<Finding> evaluate(RuleContext context) {
            return List.of();
          }
        };
    AuditReportSink sink = run -> {};
    runner
        .withBean("latency", AuditRule.class, () -> rule)
        .withBean("notification", AuditReportSink.class, () -> sink)
        .run(
            context -> {
              AuditExtensions extensions = context.getBean(AuditExtensions.class);
              assertThat(extensions.auditRules()).containsExactly(rule);
              assertThat(extensions.reportSinks())
                  .containsExactly(
                      new ReportSinkRegistration("report-sink:notification", false, sink));
            });
  }

  @Test
  void explicitSinkRegistrationOwnsRequiredPolicyWithoutDuplicateBeanDelivery() {
    AuditReportSink sink = run -> {};
    ReportSinkRegistration required = new ReportSinkRegistration("ci:archive", true, sink);
    runner
        .withBean("archive", AuditReportSink.class, () -> sink)
        .withBean("archivePolicy", ReportSinkRegistration.class, () -> required)
        .run(
            context ->
                assertThat(context.getBean(AuditExtensions.class).reportSinks())
                    .containsExactly(required));
  }

  @Test
  void distinctBeanNamesRemainExplicitRegistrationsEvenForTheSameInstance() {
    DetectionRule shared = (queries, metadata) -> List.of();
    runner
        .withBean("secondRegistration", DetectionRule.class, () -> shared)
        .withBean("firstRegistration", DetectionRule.class, () -> shared)
        .run(
            context ->
                assertThat(context.getBean(AuditExtensions.class).rules())
                    .containsExactly(shared, shared));
  }

  @Test
  void aBeanCanParticipateInEverySpiWithoutRegistrationIdConflicts() {
    CombinedExtension extension = new CombinedExtension();
    runner
        .withBean("combined", CombinedExtension.class, () -> extension)
        .run(
            context -> {
              assertThat(context).hasNotFailed();
              AuditExtensions extensions = context.getBean(AuditExtensions.class);
              assertThat(extensions.rules()).containsExactly(extension);
              assertThat(extensions.indexMetadataProviders()).containsExactly(extension);
              assertThat(extensions.explainAnalyzers()).containsExactly(extension);
            });
  }

  @Test
  void explicitCatalogBacksOffAutomaticBeanCollection() {
    DetectionRule selected = (queries, metadata) -> List.of();
    DetectionRule ignored = (queries, metadata) -> List.of();
    AuditExtensions explicit = AuditExtensions.builder().rule("explicit", selected).build();

    runner
        .withBean("userExtensions", AuditExtensions.class, () -> explicit)
        .withBean("notAutomaticallyAdded", DetectionRule.class, () -> ignored)
        .run(
            context -> {
              assertThat(context).hasSingleBean(AuditExtensions.class);
              assertThat(context).doesNotHaveBean("queryAuditExtensions");
              assertThat(context.getBean(AuditExtensions.class)).isSameAs(explicit);
              assertThat(context.getBean(AuditExtensions.class).rules()).containsExactly(selected);
            });
  }

  @Test
  void defaultConfigurationBacksOffAndInterceptorUsesTheUserConfiguration() {
    QueryAuditConfig custom = QueryAuditConfig.builder().maxQueries(71).build();
    runner
        .withBean("userConfig", QueryAuditConfig.class, () -> custom)
        .run(
            context -> {
              assertThat(context).hasSingleBean(QueryAuditConfig.class);
              assertThat(context).doesNotHaveBean("queryAuditConfig");
              assertThat(context.getBean(QueryAuditConfig.class)).isSameAs(custom);
              assertThat(context.getBean(QueryInterceptor.class).getMaxQueries()).isEqualTo(71);
            });
  }

  @Test
  void defaultInterceptorBacksOffWithoutMutatingTheUserInstance() {
    QueryInterceptor custom = new QueryInterceptor();
    custom.setMaxQueries(83);
    runner
        .withBean("userInterceptor", QueryInterceptor.class, () -> custom)
        .run(
            context -> {
              assertThat(context).hasSingleBean(QueryInterceptor.class);
              assertThat(context).doesNotHaveBean("queryAuditInterceptor");
              assertThat(context.getBean(QueryInterceptor.class)).isSameAs(custom);
              assertThat(custom.getMaxQueries()).isEqualTo(83);
            });
  }

  @Configuration(proxyBeanMethods = false)
  static class AliasedRuleConfiguration {
    @Bean(name = {"customRule", "ruleAlias"})
    DetectionRule customRule() {
      return (queries, metadata) -> List.of();
    }
  }

  private static class CombinedProvider implements IndexMetadataProvider, ExplainAnalyzer {
    @Override
    public String supportedDatabase() {
      return "h2";
    }

    @Override
    public IndexMetadata getIndexMetadata(Connection connection) {
      return new IndexMetadata(Map.of());
    }

    @Override
    public List<Issue> analyze(Connection connection, List<QueryRecord> queries) {
      return List.of();
    }
  }

  private static final class CombinedExtension extends CombinedProvider implements DetectionRule {
    @Override
    public List<Issue> evaluate(List<QueryRecord> queries, IndexMetadata indexMetadata) {
      return List.of();
    }
  }
}
