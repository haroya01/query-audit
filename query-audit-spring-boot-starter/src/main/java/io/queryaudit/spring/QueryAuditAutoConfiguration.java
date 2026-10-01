package io.queryaudit.spring;

import io.queryaudit.core.analyzer.ExplainAnalyzer;
import io.queryaudit.core.analyzer.IndexMetadataProvider;
import io.queryaudit.core.config.AuditMode;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.ReportFormat;
import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.config.RuleProfile;
import io.queryaudit.core.contract.QueryContractScope;
import io.queryaudit.core.detector.DetectionRule;
import io.queryaudit.core.extension.AuditExtensions;
import io.queryaudit.core.extension.AuditRule;
import io.queryaudit.core.interceptor.DataSourceProxyFactory;
import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.reporter.delivery.AuditReportSink;
import io.queryaudit.core.reporter.delivery.ReportSinkRegistration;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import javax.sql.DataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Auto-configuration for QueryAudit.
 *
 * <p>When enabled, wraps every {@link DataSource} bean with a datasource-proxy that records
 * executed queries through a shared {@link QueryInterceptor}.
 *
 * @author haroya
 * @since 0.2.0
 */
@Configuration
@ConditionalOnClass(DataSource.class)
@EnableConfigurationProperties(QueryAuditProperties.class)
public class QueryAuditAutoConfiguration {

  private static final Log logger = LogFactory.getLog(QueryAuditAutoConfiguration.class);

  @Bean(name = {"queryAuditConfig", "queryGuardConfig"})
  @ConditionalOnMissingBean(QueryAuditConfig.class)
  public QueryAuditConfig queryAuditConfig(QueryAuditProperties properties) {
    Map<String, Severity> severityOverrides = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.getSeverityOverrides().entrySet()) {
      severityOverrides.put(entry.getKey(), Severity.valueOf(entry.getValue()));
    }

    QueryAuditConfig config =
        QueryAuditConfig.builder()
            .enabled(properties.isEnabled())
            .failOnDetection(properties.isFailOnDetection())
            .auditMode(AuditMode.parse(properties.getMode()))
            .ruleProfile(RuleProfile.parse(properties.getProfile()))
            .enabledRules(new HashSet<>(properties.getEnabledRules()))
            .nPlusOneThreshold(properties.getNPlusOne().getThreshold())
            .offsetPaginationThreshold(properties.getOffsetPagination().getThreshold())
            .orClauseThreshold(properties.getOrClause().getThreshold())
            .suppressPatterns(new HashSet<>(properties.getSuppressPatterns()))
            .suppressQueries(new HashSet<>(properties.getSuppressQueries()))
            .showInfo(properties.getReport().isShowInfo())
            .reportFormat(ReportFormat.parse(properties.getReport().getFormat()))
            .reportRedaction(ReportRedaction.parse(properties.getReport().getRedaction()))
            .reportOutputDir(properties.getReport().getOutputDir())
            .baselinePath(properties.getBaselinePath())
            .contractsPath(properties.getContracts().getPath())
            .autoOpenReport(properties.isAutoOpenReport())
            .maxQueries(properties.getMaxQueries())
            .disabledRules(new HashSet<>(properties.getDisabledRules()))
            .severityOverrides(severityOverrides)
            .largeInListThreshold(properties.getLargeInList().getThreshold())
            .tooManyJoinsThreshold(properties.getTooManyJoins().getThreshold())
            .excessiveColumnThreshold(properties.getExcessiveColumn().getThreshold())
            .repeatedInsertThreshold(properties.getRepeatedInsert().getThreshold())
            .repeatedInsertExcludeTables(
                new HashSet<>(properties.getRepeatedInsert().getExcludeTables()))
            .repeatedUpdateThreshold(properties.getRepeatedUpdate().getThreshold())
            .repeatedUpdateExcludeTables(
                new HashSet<>(properties.getRepeatedUpdate().getExcludeTables()))
            .writeAmplificationThreshold(properties.getWriteAmplification().getThreshold())
            .slowQueryWarningMs(properties.getSlowQuery().getWarningMs())
            .slowQueryErrorMs(properties.getSlowQuery().getErrorMs())
            .countInsteadOfExistsEnabled(properties.getCountInsteadOfExists().isEnabled())
            .connectionHeldIdleThresholdMs(properties.getConnectionHeldIdle().getThresholdMs())
            .build();
    if (config.isEnabled()) {
      logger.info(
          "QueryAudit rule profile: " + config.getRuleProfile().name().toLowerCase(Locale.ROOT));
    }
    return config;
  }

  @Bean(name = {"queryAuditInterceptor", "queryGuardInterceptor"})
  @ConditionalOnMissingBean(QueryInterceptor.class)
  public QueryInterceptor queryAuditInterceptor(QueryAuditConfig config) {
    QueryInterceptor interceptor = new QueryInterceptor();
    interceptor.setMaxQueries(config.getMaxQueries());
    return interceptor;
  }

  @Bean
  @ConditionalOnMissingBean(QueryContractScope.class)
  public QueryContractScope queryContractScope(
      QueryInterceptor interceptor,
      QueryAuditProperties properties,
      ListableBeanFactory beanFactory) {
    QueryContractScope scope =
        QueryContractScope.of(interceptor, Path.of(properties.getContracts().getPath()));
    List<String> executors = properties.getContracts().getAwaitExecutors();
    return executors.isEmpty()
        ? scope
        : scope.awaitingCompletion(new ExecutorIdleAwaiter(beanFactory, executors));
  }

  /**
   * Collects user extension beans without replacing built-in rules or ServiceLoader providers. Bean
   * names define stable registration IDs and alphabetical execution order within each SPI. Spring
   * aliases are not separate registrations; deliberately distinct bean names are. A bean
   * implementing multiple SPIs is registered once in each supported role.
   *
   * <p>A user-supplied catalog replaces this bean collection step, giving programmatic registration
   * the same meaning in Spring and plain JUnit. Registered beans remain owned by Spring.
   */
  @Bean
  @ConditionalOnMissingBean(AuditExtensions.class)
  public AuditExtensions queryAuditExtensions(ListableBeanFactory beanFactory) {
    AuditExtensions.Builder builder = AuditExtensions.builder();
    new TreeMap<>(beanFactory.getBeansOfType(DetectionRule.class))
        .forEach((name, rule) -> builder.rule("rule:" + name, rule));
    new TreeMap<>(beanFactory.getBeansOfType(AuditRule.class))
        .forEach((name, rule) -> builder.auditRule("audit-rule:" + name, rule));
    new TreeMap<>(beanFactory.getBeansOfType(IndexMetadataProvider.class))
        .forEach(
            (name, provider) -> builder.indexMetadataProvider("index-metadata:" + name, provider));
    new TreeMap<>(beanFactory.getBeansOfType(ExplainAnalyzer.class))
        .forEach((name, analyzer) -> builder.explainAnalyzer("explain:" + name, analyzer));
    Map<String, ReportSinkRegistration> registrations =
        new TreeMap<>(beanFactory.getBeansOfType(ReportSinkRegistration.class));
    registrations.forEach(
        (name, registration) ->
            builder.reportSink(registration.id(), registration.required(), registration.sink()));
    new TreeMap<>(beanFactory.getBeansOfType(AuditReportSink.class))
        .forEach(
            (name, sink) -> {
              boolean explicitlyRegistered =
                  registrations.values().stream()
                      .anyMatch(registration -> registration.sink() == sink);
              if (!explicitlyRegistered) {
                builder.reportSink("report-sink:" + name, false, sink);
              }
            });
    return builder.build();
  }

  /**
   * Wraps every {@link DataSource} bean with a query-recording proxy. Both {@code
   * query-audit.enabled} and {@code query-audit.wrap-data-source.enabled} default to {@code true};
   * either can be flipped to {@code false} to skip the wrap. {@code wrap-data-source.enabled =
   * false} is the documented escape hatch for issue #134 — it disables only the auto-wrap while
   * keeping {@link QueryAuditConfig} and {@link QueryInterceptor} available, so {@code @QueryAudit}
   * per-test wrapping still works.
   */
  @Bean(name = {"queryAuditDataSourcePostProcessor", "queryGuardDataSourcePostProcessor"})
  @ConditionalOnProperty(
      name = {"query-audit.enabled", "query-audit.wrap-data-source.enabled"},
      havingValue = "true",
      matchIfMissing = true)
  public BeanPostProcessor queryAuditDataSourcePostProcessor(QueryInterceptor interceptor) {
    return new BeanPostProcessor() {
      @Override
      public Object postProcessAfterInitialization(Object bean, String beanName)
          throws BeansException {
        if (bean instanceof DataSource ds) {
          return DataSourceProxyFactory.wrap(ds, interceptor);
        }
        return bean;
      }
    };
  }
}
