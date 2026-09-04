package io.queryaudit.spring;

import io.queryaudit.core.config.AuditMode;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.ReportFormat;
import io.queryaudit.core.config.RuleProfile;
import io.queryaudit.core.interceptor.DataSourceProxyFactory;
import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.Severity;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
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
            .reportOutputDir(properties.getReport().getOutputDir())
            .baselinePath(properties.getBaselinePath())
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
  public QueryInterceptor queryAuditInterceptor(QueryAuditConfig config) {
    QueryInterceptor interceptor = new QueryInterceptor();
    interceptor.setMaxQueries(config.getMaxQueries());
    return interceptor;
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
