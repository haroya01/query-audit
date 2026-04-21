package io.queryaudit.spring;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.interceptor.DataSourceProxyFactory;
import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.Severity;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import javax.sql.DataSource;
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

  @Bean(name = {"queryAuditConfig", "queryGuardConfig"})
  public QueryAuditConfig queryAuditConfig(QueryAuditProperties properties) {
    Map<String, Severity> severityOverrides = new HashMap<>();
    for (Map.Entry<String, String> entry : properties.getSeverityOverrides().entrySet()) {
      severityOverrides.put(entry.getKey(), Severity.valueOf(entry.getValue()));
    }

    return QueryAuditConfig.builder()
        .enabled(properties.isEnabled())
        .failOnDetection(properties.isFailOnDetection())
        .nPlusOneThreshold(properties.getNPlusOne().getThreshold())
        .offsetPaginationThreshold(properties.getOffsetPagination().getThreshold())
        .orClauseThreshold(properties.getOrClause().getThreshold())
        .suppressPatterns(new HashSet<>(properties.getSuppressPatterns()))
        .suppressQueries(new HashSet<>(properties.getSuppressQueries()))
        .showInfo(properties.getReport().isShowInfo())
        .baselinePath(properties.getBaselinePath())
        .autoOpenReport(properties.isAutoOpenReport())
        .maxQueries(properties.getMaxQueries())
        .disabledRules(new HashSet<>(properties.getDisabledRules()))
        .severityOverrides(severityOverrides)
        .largeInListThreshold(properties.getLargeInList().getThreshold())
        .tooManyJoinsThreshold(properties.getTooManyJoins().getThreshold())
        .excessiveColumnThreshold(properties.getExcessiveColumn().getThreshold())
        .repeatedInsertThreshold(properties.getRepeatedInsert().getThreshold())
        .writeAmplificationThreshold(properties.getWriteAmplification().getThreshold())
        .slowQueryWarningMs(properties.getSlowQuery().getWarningMs())
        .slowQueryErrorMs(properties.getSlowQuery().getErrorMs())
        .build();
  }

  @Bean(name = {"queryAuditInterceptor", "queryGuardInterceptor"})
  public QueryInterceptor queryAuditInterceptor(QueryAuditConfig config) {
    QueryInterceptor interceptor = new QueryInterceptor();
    interceptor.setMaxQueries(config.getMaxQueries());
    return interceptor;
  }

  @Bean(name = {"queryAuditDataSourcePostProcessor", "queryGuardDataSourcePostProcessor"})
  @ConditionalOnProperty(name = "query-audit.enabled", havingValue = "true", matchIfMissing = true)
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
