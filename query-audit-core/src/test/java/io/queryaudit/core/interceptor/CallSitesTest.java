package io.queryaudit.core.interceptor;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CallSitesTest {

  @ParameterizedTest
  @ValueSource(
      strings = {
        "com.example.Customer$HibernateProxy",
        "com.example.Customer$HibernateProxy$aB3dQ9",
        "com.example.Customer$ByteBuddy$x1",
        "com.example.OrderService$$SpringCGLIB$$0",
        "com.example.OrderService$$EnhancerBySpringCGLIB$$1a2b",
        "com.example.User$$EnhancerByCGLIB$$abc"
      })
  void recognizesGeneratedSubclasses(String className) {
    assertThat(CallSites.isGeneratedSubclass(className)).isTrue();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"com.example.Customer", "com.example.HibernateProxyFactory", "com.example.Proxy"})
  void keepsOrdinaryClasses(String className) {
    assertThat(CallSites.isGeneratedSubclass(className)).isFalse();
    assertThat(CallSites.isJdkProxy(className)).isFalse();
  }

  @ParameterizedTest
  @ValueSource(strings = {"demo.$Proxy140", "jdk.proxy3.$Proxy296", "com.sun.proxy.$Proxy12"})
  void recognizesJdkProxiesInAnyPackage(String className) {
    assertThat(CallSites.isJdkProxy(className)).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = {"demo.$ProxyFactory", "demo.$Proxy", "demo.Proxy140"})
  void doesNotTreatSimilarNamesAsJdkProxies(String className) {
    assertThat(CallSites.isJdkProxy(className)).isFalse();
  }

  @Test
  void callSiteSkipsRepositoryAndEntityProxies() {
    String stack =
        String.join(
            "\n",
            "demo.$Proxy136.findAll:-1",
            "demo.Domain$Customer$HibernateProxy.getName:-1",
            "demo.OrderService.lambda$summaries$0:40",
            "demo.OrderService.summaries:39");

    assertThat(CallSites.callSite(stack)).isEqualTo("demo.OrderService.lambda$summaries$0:40");
    assertThat(CallSites.applicationFrames(stack, 5))
        .containsExactly("demo.OrderService.lambda$summaries$0:40", "demo.OrderService.summaries:39");
  }

  @Test
  void acceptsFramesWithFileNames() {
    String stack = "demo.$Proxy12.findById(Unknown Source)\ndemo.Svc.load(Svc.java:12)";

    assertThat(CallSites.callSite(stack)).isEqualTo("demo.Svc.load(Svc.java:12)");
  }

  @Test
  void fallsBackToTheFirstFrameWhenOnlyProxiesRemain() {
    assertThat(CallSites.callSite("demo.$Proxy136.findAll:-1\njava.lang.Thread.run:1"))
        .isEqualTo("demo.$Proxy136.findAll:-1");
  }

  @Test
  void emptyStacksHaveNoCallSite() {
    assertThat(CallSites.callSite(null)).isNull();
    assertThat(CallSites.callSite("")).isNull();
    assertThat(CallSites.applicationFrames("\n", 5)).isEmpty();
  }
}
