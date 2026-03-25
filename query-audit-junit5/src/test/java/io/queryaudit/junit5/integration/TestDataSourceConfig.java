package io.queryaudit.junit5.integration;

import io.queryaudit.core.interceptor.DataSourceProxyFactory;
import io.queryaudit.core.interceptor.QueryInterceptor;
import javax.sql.DataSource;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TestDataSourceConfig {

  @Bean
  public QueryInterceptor queryGuardInterceptor() {
    return new QueryInterceptor();
  }

  @Bean
  public static BeanPostProcessor dsProxy(QueryInterceptor interceptor) {
    return new BeanPostProcessor() {
      @Override
      public Object postProcessAfterInitialization(Object bean, String beanName)
          throws BeansException {
        if (bean instanceof DataSource ds && "dataSource".equals(beanName)) {
          return DataSourceProxyFactory.wrap(ds, interceptor);
        }
        return bean;
      }
    };
  }
}
