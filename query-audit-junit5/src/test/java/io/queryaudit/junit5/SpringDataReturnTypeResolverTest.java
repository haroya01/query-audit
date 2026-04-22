package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.detector.RepositoryReturnType;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class SpringDataReturnTypeResolverTest {

  // ── extractProxyKey ─────────────────────────────────────────────

  @Nested
  class ExtractProxyKey {

    @Test
    void extractsProxyClassAndMethod() {
      String stack =
          "jdk.proxy3.$Proxy296.findByRoomId:-1\n"
              + "com.example.service.RoomService.getMembers:42";

      assertThat(SpringDataReturnTypeResolver.extractProxyKey(stack))
          .isEqualTo("jdk.proxy3.$Proxy296.findByRoomId");
    }

    @Test
    void extractsFirstProxyFrame() {
      String stack =
          "jdk.proxy2.$Proxy50.findByPhone:-1\n"
              + "jdk.proxy3.$Proxy101.findAll:-1\n"
              + "com.example.Service.run:10";

      assertThat(SpringDataReturnTypeResolver.extractProxyKey(stack))
          .isEqualTo("jdk.proxy2.$Proxy50.findByPhone");
    }

    @Test
    void returnsNullForNonProxyStack() {
      String stack =
          "com.example.dao.CustomDao.fetch:30\n" + "com.example.service.SomeService.get:12";

      assertThat(SpringDataReturnTypeResolver.extractProxyKey(stack)).isNull();
    }

    @Test
    void returnsNullForEmptyStack() {
      assertThat(SpringDataReturnTypeResolver.extractProxyKey("")).isNull();
    }

    @Test
    void returnsNullForNull() {
      assertThat(SpringDataReturnTypeResolver.extractProxyKey(null)).isNull();
    }
  }

  // ── extractProxyMethodName ──────────────────────────────────────

  @Nested
  class ExtractProxyMethodName {

    @Test
    void extractsMethodNameOnly() {
      String stack =
          "jdk.proxy3.$Proxy296.findByRoomId:-1\n"
              + "com.example.service.RoomService.getMembers:42";

      assertThat(SpringDataReturnTypeResolver.extractProxyMethodName(stack))
          .isEqualTo("findByRoomId");
    }

    @Test
    void returnsNullForNonProxy() {
      String stack = "com.example.Service.run:10";
      assertThat(SpringDataReturnTypeResolver.extractProxyMethodName(stack)).isNull();
    }
  }

  // ── classifyReturnType ──────────────────────────────────────────

  @Nested
  class ClassifyReturnType {

    @Test
    void classifiesOptionalAsOptional() throws Exception {
      Method m = SampleMethods.class.getMethod("findOptional");
      assertThat(SpringDataReturnTypeResolver.classifyReturnType(m))
          .isEqualTo(RepositoryReturnType.OPTIONAL);
    }

    @Test
    void classifiesListAsCollection() throws Exception {
      Method m = SampleMethods.class.getMethod("findList");
      assertThat(SpringDataReturnTypeResolver.classifyReturnType(m))
          .isEqualTo(RepositoryReturnType.COLLECTION);
    }

    @Test
    void classifiesSetAsCollection() throws Exception {
      Method m = SampleMethods.class.getMethod("findSet");
      assertThat(SpringDataReturnTypeResolver.classifyReturnType(m))
          .isEqualTo(RepositoryReturnType.COLLECTION);
    }

    @Test
    void classifiesCollectionAsCollection() throws Exception {
      Method m = SampleMethods.class.getMethod("findCollection");
      assertThat(SpringDataReturnTypeResolver.classifyReturnType(m))
          .isEqualTo(RepositoryReturnType.COLLECTION);
    }

    @Test
    void classifiesStreamAsCollection() throws Exception {
      Method m = SampleMethods.class.getMethod("findStream");
      assertThat(SpringDataReturnTypeResolver.classifyReturnType(m))
          .isEqualTo(RepositoryReturnType.COLLECTION);
    }

    @Test
    void classifiesSingleEntityAsSingleEntity() throws Exception {
      Method m = SampleMethods.class.getMethod("findSingle");
      assertThat(SpringDataReturnTypeResolver.classifyReturnType(m))
          .isEqualTo(RepositoryReturnType.SINGLE_ENTITY);
    }

    @Test
    void classifiesLongAsSingleEntity() throws Exception {
      Method m = SampleMethods.class.getMethod("countByStatus");
      assertThat(SpringDataReturnTypeResolver.classifyReturnType(m))
          .isEqualTo(RepositoryReturnType.SINGLE_ENTITY);
    }

    @Test
    void classifiesVoidAsSingleEntity() throws Exception {
      Method m = SampleMethods.class.getMethod("deleteByName");
      assertThat(SpringDataReturnTypeResolver.classifyReturnType(m))
          .isEqualTo(RepositoryReturnType.SINGLE_ENTITY);
    }

    @SuppressWarnings("unused")
    interface SampleMethods {
      Optional<Object> findOptional();

      List<Object> findList();

      Set<Object> findSet();

      Collection<Object> findCollection();

      Stream<Object> findStream();

      Object findSingle();

      long countByStatus();

      void deleteByName();
    }
  }

  // ── method name collision: different proxy keys ─────────────────

  @Nested
  class ProxyKeyDisambiguation {

    @Test
    void differentProxiesProduceDifferentKeys() {
      String userRepoStack =
          "jdk.proxy3.$Proxy100.findByStatus:-1\n" + "com.example.service.UserService.find:10";
      String orderRepoStack =
          "jdk.proxy3.$Proxy200.findByStatus:-1\n" + "com.example.service.OrderService.find:10";

      String userKey = SpringDataReturnTypeResolver.extractProxyKey(userRepoStack);
      String orderKey = SpringDataReturnTypeResolver.extractProxyKey(orderRepoStack);

      // 같은 메서드명이지만 다른 프록시 → 다른 키
      assertThat(userKey).isEqualTo("jdk.proxy3.$Proxy100.findByStatus");
      assertThat(orderKey).isEqualTo("jdk.proxy3.$Proxy200.findByStatus");
      assertThat(userKey).isNotEqualTo(orderKey);
    }
  }

  // ── unregistered proxy returns UNKNOWN (no method-name fallback) ──

  @Nested
  class UnregisteredProxyReturnsUnknown {

    @Test
    void resolveReturnsUnknownForUnregisteredProxy() {
      // Resolver with no registered repositories (empty ApplicationContext mock)
      // — any proxy key will miss the exact match and should return UNKNOWN,
      // NOT attempt a method-name-only fallback.
      SpringDataReturnTypeResolver resolver =
          new SpringDataReturnTypeResolver(new FakeEmptyApplicationContext());

      String stack =
          "jdk.proxy3.$Proxy999.findByStatus:-1\n" + "com.example.service.SomeService.call:10";

      RepositoryReturnType result = resolver.resolve(stack);

      assertThat(result).isEqualTo(RepositoryReturnType.UNKNOWN);
    }
  }

  /**
   * Minimal fake ApplicationContext that returns an empty bean map. getBeansOfType(Class) is the
   * only method called by SpringDataReturnTypeResolver.
   */
  @SuppressWarnings("unused")
  static class FakeEmptyApplicationContext {
    public Map<String, Object> getBeansOfType(Class<?> type) {
      return Map.of();
    }
  }
}
