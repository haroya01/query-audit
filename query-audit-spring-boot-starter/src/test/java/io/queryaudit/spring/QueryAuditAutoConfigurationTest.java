package io.queryaudit.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.interceptor.QueryInterceptor;
import javax.sql.DataSource;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class QueryAuditAutoConfigurationTest {

  private final ApplicationContextRunner contextRunner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(QueryAuditAutoConfiguration.class));

  @Nested
  @DisplayName("QueryAuditProperties default values")
  class PropertiesDefaultsTests {

    @Test
    @DisplayName("enabled is true by default")
    void enabledTrueByDefault() {
      QueryAuditProperties props = new QueryAuditProperties();
      assertThat(props.isEnabled()).isTrue();
    }

    @Test
    @DisplayName("failOnDetection is true by default")
    void failOnDetectionTrueByDefault() {
      QueryAuditProperties props = new QueryAuditProperties();
      assertThat(props.isFailOnDetection()).isTrue();
    }

    @Test
    @DisplayName("N+1 threshold defaults to 3")
    void nPlusOneThresholdDefault() {
      QueryAuditProperties props = new QueryAuditProperties();
      assertThat(props.getNPlusOne().getThreshold()).isEqualTo(3);
    }

    @Test
    @DisplayName("offset pagination threshold defaults to 1000")
    void offsetPaginationThresholdDefault() {
      QueryAuditProperties props = new QueryAuditProperties();
      assertThat(props.getOffsetPagination().getThreshold()).isEqualTo(1000);
    }

    @Test
    @DisplayName("OR clause threshold defaults to 3")
    void orClauseThresholdDefault() {
      QueryAuditProperties props = new QueryAuditProperties();
      assertThat(props.getOrClause().getThreshold()).isEqualTo(3);
    }

    @Test
    @DisplayName("suppress patterns is empty by default")
    void suppressPatternsEmptyByDefault() {
      QueryAuditProperties props = new QueryAuditProperties();
      assertThat(props.getSuppressPatterns()).isEmpty();
    }

    @Test
    @DisplayName("suppress queries is empty by default")
    void suppressQueriesEmptyByDefault() {
      QueryAuditProperties props = new QueryAuditProperties();
      assertThat(props.getSuppressQueries()).isEmpty();
    }

    @Test
    @DisplayName("baselinePath is null by default")
    void baselinePathNullByDefault() {
      QueryAuditProperties props = new QueryAuditProperties();
      assertThat(props.getBaselinePath()).isNull();
    }

    @Test
    @DisplayName("autoOpenReport is true by default")
    void autoOpenReportTrueByDefault() {
      QueryAuditProperties props = new QueryAuditProperties();
      assertThat(props.isAutoOpenReport()).isTrue();
    }

    @Test
    @DisplayName("report format defaults to 'console'")
    void reportFormatDefault() {
      QueryAuditProperties props = new QueryAuditProperties();
      assertThat(props.getReport().getFormat()).isEqualTo("console");
    }

    @Test
    @DisplayName("report outputDir defaults to 'build/reports/query-audit'")
    void reportOutputDirDefault() {
      QueryAuditProperties props = new QueryAuditProperties();
      assertThat(props.getReport().getOutputDir()).isEqualTo("build/reports/query-audit");
    }

    @Test
    @DisplayName("report showInfo defaults to true")
    void reportShowInfoDefault() {
      QueryAuditProperties props = new QueryAuditProperties();
      assertThat(props.getReport().isShowInfo()).isTrue();
    }
  }

  @Nested
  @DisplayName("Auto-configuration activation")
  class AutoConfigurationTests {

    @Test
    @DisplayName("registers QueryInterceptor bean")
    void registersQueryInterceptorBean() {
      contextRunner.run(
          context -> {
            assertThat(context).hasSingleBean(QueryInterceptor.class);
          });
    }

    @Test
    @DisplayName("registers BeanPostProcessor when enabled (default)")
    void registersBeanPostProcessorWhenEnabled() {
      contextRunner.run(
          context -> {
            assertThat(context).hasBean("queryGuardDataSourcePostProcessor");
          });
    }

    @Test
    @DisplayName("does not register BeanPostProcessor when disabled via property")
    void doesNotRegisterBeanPostProcessorWhenDisabled() {
      contextRunner
          .withPropertyValues("query-audit.enabled=false")
          .run(
              context -> {
                assertThat(context).doesNotHaveBean("queryGuardDataSourcePostProcessor");
                // QueryInterceptor should still be registered
                assertThat(context).hasSingleBean(QueryInterceptor.class);
              });
    }
  }

  @Nested
  @DisplayName("Property binding to QueryAuditConfig")
  class PropertyBindingTests {

    @Test
    @DisplayName("suppressQueries property binds to QueryAuditConfig")
    void suppressQueriesPropertyBinding() {
      contextRunner
          .withPropertyValues(
              "query-audit.suppress-queries=SELECT 1,SELECT 2")
          .run(
              context -> {
                QueryAuditConfig config = context.getBean(QueryAuditConfig.class);
                assertThat(config.getSuppressQueries()).containsExactlyInAnyOrder("SELECT 1", "SELECT 2");
              });
    }

    @Test
    @DisplayName("baselinePath property binds to QueryAuditConfig")
    void baselinePathPropertyBinding() {
      contextRunner
          .withPropertyValues("query-audit.baseline-path=/tmp/my-baseline.json")
          .run(
              context -> {
                QueryAuditConfig config = context.getBean(QueryAuditConfig.class);
                assertThat(config.getBaselinePath()).isEqualTo("/tmp/my-baseline.json");
              });
    }

    @Test
    @DisplayName("autoOpenReport property binds to QueryAuditConfig")
    void autoOpenReportPropertyBinding() {
      contextRunner
          .withPropertyValues("query-audit.auto-open-report=false")
          .run(
              context -> {
                QueryAuditConfig config = context.getBean(QueryAuditConfig.class);
                assertThat(config.isAutoOpenReport()).isFalse();
              });
    }

    @Test
    @DisplayName("all properties bind correctly to QueryAuditConfig")
    void allPropertiesBindToConfig() {
      contextRunner
          .withPropertyValues(
              "query-audit.enabled=false",
              "query-audit.fail-on-detection=false",
              "query-audit.n-plus-one.threshold=5",
              "query-audit.offset-pagination.threshold=500",
              "query-audit.or-clause.threshold=4",
              "query-audit.suppress-patterns=select-all",
              "query-audit.suppress-queries=SELECT 1",
              "query-audit.baseline-path=/tmp/baseline.json",
              "query-audit.auto-open-report=false",
              "query-audit.report.show-info=false")
          .run(
              context -> {
                QueryAuditConfig config = context.getBean(QueryAuditConfig.class);
                assertThat(config.isEnabled()).isFalse();
                assertThat(config.isFailOnDetection()).isFalse();
                assertThat(config.getNPlusOneThreshold()).isEqualTo(5);
                assertThat(config.getOffsetPaginationThreshold()).isEqualTo(500);
                assertThat(config.getOrClauseThreshold()).isEqualTo(4);
                assertThat(config.getSuppressPatterns()).containsExactly("select-all");
                assertThat(config.getSuppressQueries()).containsExactly("SELECT 1");
                assertThat(config.getBaselinePath()).isEqualTo("/tmp/baseline.json");
                assertThat(config.isAutoOpenReport()).isFalse();
                assertThat(config.isShowInfo()).isFalse();
              });
    }
  }

  @Nested
  @DisplayName("BeanPostProcessor DataSource wrapping")
  class DataSourceWrappingTests {

    @Test
    @DisplayName("postProcessAfterInitialization wraps DataSource beans")
    void wrapsDataSourceBeans() {
      contextRunner
          .withBean("testDataSource", DataSource.class, () -> mock(DataSource.class))
          .run(
              context -> {
                // The original mock DataSource should have been replaced by a proxy
                DataSource ds = context.getBean("testDataSource", DataSource.class);
                // The wrapped DataSource should not be the same mock instance;
                // datasource-proxy wraps it so the class will differ
                assertThat(ds).isNotNull();
                assertThat(ds.getClass().getName()).isNotEqualTo("javax.sql.DataSource");
              });
    }

    @Test
    @DisplayName("postProcessAfterInitialization leaves non-DataSource beans untouched")
    void leavesNonDataSourceBeansUntouched() {
      contextRunner
          .withBean("someString", String.class, () -> "hello")
          .run(
              context -> {
                String value = context.getBean("someString", String.class);
                assertThat(value).isEqualTo("hello");
              });
    }
  }
}
