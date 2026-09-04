package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.queryaudit.core.provenance.AuditCapability;
import org.junit.jupiter.api.Test;

class HibernateCapabilityFailureTest {
  @Test
  void aPresentHibernateIntegrationThatCannotInitializeIsFailedNotAbsent() {
    HibernateIntegration.Registration result =
        new HibernateIntegration().registerWithCapabilitiesForEmf(new BrokenEntityManagerFactory());
    assertThat(result.tracker()).isNull();
    assertThat(result.capability().state()).isEqualTo(AuditCapability.State.FAILED);
    assertThat(result.failure()).isNotBlank();
  }

  @Test
  void aMissingBeanIsAbsentButAFailedBeanIsNot() {
    assertThat(HibernateIntegration.entityManagerFactoryBean(new MissingBeanContext())).isNull();
    assertThatThrownBy(() -> HibernateIntegration.entityManagerFactoryBean(new FailedBeanContext()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Could not initialize the EntityManagerFactory");
  }

  public static class MissingBeanContext {
    public Object getBean(Class<?> type) {
      throw new org.springframework.beans.factory.NoSuchBeanDefinitionException(type);
    }
  }

  public static class FailedBeanContext {
    public Object getBean(Class<?> type) {
      throw new IllegalStateException("private bean configuration");
    }
  }

  public static class BrokenEntityManagerFactory {
    public <T> T unwrap(Class<T> type) {
      throw new IllegalStateException("private integration details");
    }
  }
}
