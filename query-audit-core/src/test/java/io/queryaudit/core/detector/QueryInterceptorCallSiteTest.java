package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.QueryRecord;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.function.Consumer;
import net.ttddyy.dsproxy.ExecutionInfo;
import net.ttddyy.dsproxy.QueryInfo;
import org.junit.jupiter.api.Test;

class QueryInterceptorCallSiteTest {

  interface Repository {
    void find();
  }

  @Test
  void dropsProxyAndReflectionFramesFromTheCapturedCallSite() {
    QueryInterceptor interceptor = new QueryInterceptor();
    interceptor.start();
    Repository repository =
        (Repository)
            Proxy.newProxyInstance(
                Repository.class.getClassLoader(),
                new Class<?>[] {Repository.class},
                (proxy, method, args) -> {
                  fire(interceptor);
                  return null;
                });
    repository.find();
    interceptor.stop();

    String stack = interceptor.getRecordedQueries().get(0).stackTrace();
    assertThat(stack)
        .doesNotContain("jdk.proxy")
        .doesNotContain("java.lang.reflect.")
        .contains("QueryInterceptorCallSiteTest");
  }

  static final class Team$HibernateProxy$aB3dQ9 {
    void getName(QueryInterceptor interceptor) {
      fire(interceptor);
    }
  }

  static final class Team$HibernateProxy$Zx7Lm2 {
    void getName(QueryInterceptor interceptor) {
      fire(interceptor);
    }
  }

  @Test
  void generatedProxyNamesDoNotEnterTheCallSite() {
    List<Consumer<QueryInterceptor>> proxies =
        List.of(
            new Team$HibernateProxy$aB3dQ9()::getName, new Team$HibernateProxy$Zx7Lm2()::getName);
    QueryInterceptor interceptor = new QueryInterceptor();
    interceptor.start();
    for (Consumer<QueryInterceptor> proxy : proxies) proxy.accept(interceptor);
    interceptor.stop();

    List<QueryRecord> records = interceptor.getRecordedQueries();
    assertThat(records.get(0).stackTrace())
        .doesNotContain("HibernateProxy")
        .isEqualTo(records.get(1).stackTrace());
    assertThat(records.get(0).fullStackHash()).isEqualTo(records.get(1).fullStackHash());
  }

  @Test
  void repeatedQueriesFromOneLoopShareTheirFullCallSite() {
    QueryInterceptor interceptor = new QueryInterceptor();
    interceptor.start();
    for (int i = 0; i < 3; i++) fire(interceptor);
    interceptor.stop();

    List<QueryRecord> records = interceptor.getRecordedQueries();
    assertThat(records)
        .extracting(QueryRecord::fullStackHash)
        .containsOnly(records.get(0).fullStackHash());
  }

  @Test
  void callSitesBeyondTheVisibleFramesKeepDistinctHashes() {
    QueryInterceptor interceptor = new QueryInterceptor();
    interceptor.start();
    fromFirstCaller(interceptor);
    fromSecondCaller(interceptor);
    interceptor.stop();

    List<QueryRecord> records = interceptor.getRecordedQueries();
    assertThat(records.get(0).stackTrace()).isEqualTo(records.get(1).stackTrace());
    assertThat(records.get(0).fullStackHash()).isNotEqualTo(records.get(1).fullStackHash());
  }

  private static void fromFirstCaller(QueryInterceptor interceptor) {
    deep(12, interceptor);
  }

  private static void fromSecondCaller(QueryInterceptor interceptor) {
    deep(12, interceptor);
  }

  private static void deep(int remaining, QueryInterceptor interceptor) {
    if (remaining == 0) {
      fire(interceptor);
      return;
    }
    deep(remaining - 1, interceptor);
  }

  private static void fire(QueryInterceptor interceptor) {
    ExecutionInfo execution = new ExecutionInfo();
    execution.setElapsedTime(1L);
    interceptor.afterQuery(execution, List.of(new QueryInfo("SELECT 1")));
  }
}
