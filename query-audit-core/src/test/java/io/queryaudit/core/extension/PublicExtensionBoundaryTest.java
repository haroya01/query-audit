package io.queryaudit.core.extension;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.core.model.AuditFindings;
import io.queryaudit.core.model.Finding;
import io.queryaudit.core.reporter.delivery.AuditReportSink;
import io.queryaudit.core.reporter.delivery.PublicationResult;
import io.queryaudit.core.reporter.delivery.PublishedAuditRun;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/** Guards the public type boundary, not internal implementation dependencies or human usability. */
class PublicExtensionBoundaryTest {
  private static final List<String> FORBIDDEN =
      List.of(
          "org.junit.",
          "org.springframework.",
          "org.hibernate.",
          "jakarta.persistence.",
          "net.sf.jsqlparser.",
          "io.queryaudit.junit5.",
          "io.queryaudit.spring.",
          "io.queryaudit.core.interceptor.",
          "io.queryaudit.core.extension.internal.",
          "java.sql.Connection");

  static Stream<Class<?>> publicContracts() {
    return Stream.of(
        AuditRule.class,
        RuleContext.class,
        RuleDescriptor.class,
        RuleId.class,
        FindingKindId.class,
        AuditRuleTestKit.class,
        AuditRuleException.class,
        Finding.class,
        AuditFindings.class,
        AuditReportSink.class,
        PublishedAuditRun.class,
        PublicationResult.class);
  }

  @ParameterizedTest(name = "{0} keeps framework and mutable capture types out of its public graph")
  @MethodSource("publicContracts")
  void rulesAndSinksDoNotRequireFrameworkContextOrParserAst(Class<?> contract) {
    List<String> violations = new ArrayList<>();
    inspect(contract, contract.getSimpleName(), new HashSet<>(), violations);
    assertThat(violations).as("Public extension signatures must remain host-independent").isEmpty();
  }

  static Stream<Class<?>> leakingSignatures() {
    return Stream.of(
        GenericLeak.class,
        FieldLeak.class,
        ClassBoundLeak.class,
        MethodBoundLeak.class,
        ConstructorBoundLeak.class,
        ConstructorExceptionLeak.class,
        OwnerTypeLeak.class);
  }

  @ParameterizedTest(name = "{0} is detected by the boundary checker itself")
  @MethodSource("leakingSignatures")
  void theBoundaryCheckDetectsEachKindOfNestedSignatureLeak(Class<?> fixture) {
    List<String> violations = new ArrayList<>();
    inspect(fixture, fixture.getSimpleName(), new HashSet<>(), violations);
    assertThat(violations)
        .anyMatch(
            path ->
                path.contains(fixture.getSimpleName())
                    && (path.endsWith("java.sql.Connection")
                        || path.endsWith("org.hibernate.HibernateException")));
  }

  private interface GenericLeak {
    <T extends Connection> T connection();
  }

  private static final class FieldLeak {
    public List<? extends Connection> connections;
  }

  private interface ClassBoundLeak<T extends Connection> {}

  private interface MethodBoundLeak {
    <T extends Connection> void publish();
  }

  private static final class ConstructorBoundLeak {
    public <T extends Connection> ConstructorBoundLeak() {}
  }

  private static final class ConstructorExceptionLeak {
    public ConstructorExceptionLeak() throws org.hibernate.HibernateException {}
  }

  private static final class GenericOwner<T> {
    public final class Nested {}
  }

  private interface OwnerTypeLeak {
    GenericOwner<Connection>.Nested owner();
  }

  private static void inspect(Type type, String path, Set<Type> visited, List<String> violations) {
    if (!visited.add(type)) return;
    if (type instanceof ParameterizedType parameterized) {
      if (parameterized.getOwnerType() != null)
        inspect(parameterized.getOwnerType(), path, visited, violations);
      inspect(parameterized.getRawType(), path, visited, violations);
      for (Type argument : parameterized.getActualTypeArguments())
        inspect(argument, path, visited, violations);
    } else if (type instanceof GenericArrayType array) {
      inspect(array.getGenericComponentType(), path, visited, violations);
    } else if (type instanceof WildcardType wildcard) {
      for (Type bound : wildcard.getUpperBounds()) inspect(bound, path, visited, violations);
      for (Type bound : wildcard.getLowerBounds()) inspect(bound, path, visited, violations);
    } else if (type instanceof TypeVariable<?> variable) {
      for (Type bound : variable.getBounds()) inspect(bound, path, visited, violations);
    } else if (type instanceof Class<?> value) {
      if (value.isArray()) {
        inspect(value.getComponentType(), path, visited, violations);
        return;
      }
      if (FORBIDDEN.stream().anyMatch(value.getName()::startsWith)) {
        violations.add(path + " -> " + value.getName());
        return;
      }
      if (!value.getName().startsWith("io.queryaudit.")) return;
      for (Type variable : value.getTypeParameters()) inspect(variable, path, visited, violations);
      for (Type parent : value.getGenericInterfaces()) inspect(parent, path, visited, violations);
      if (value.getGenericSuperclass() != null)
        inspect(value.getGenericSuperclass(), path, visited, violations);
      for (var field : value.getDeclaredFields()) {
        if (Modifier.isPublic(field.getModifiers()))
          inspect(
              field.getGenericType(),
              path + " -> " + value.getSimpleName() + "." + field.getName(),
              visited,
              violations);
      }
      for (var constructor : value.getConstructors()) {
        String member = path + " -> " + value.getSimpleName() + " constructor";
        for (Type variable : constructor.getTypeParameters())
          inspect(variable, member, visited, violations);
        for (Type parameter : constructor.getGenericParameterTypes())
          inspect(parameter, member, visited, violations);
        for (Type exception : constructor.getGenericExceptionTypes())
          inspect(exception, member, visited, violations);
      }
      for (var method : value.getDeclaredMethods()) {
        if (!Modifier.isPublic(method.getModifiers()) || method.isSynthetic()) continue;
        String member = path + " -> " + value.getSimpleName() + "." + method.getName();
        for (Type variable : method.getTypeParameters())
          inspect(variable, member, visited, violations);
        inspect(method.getGenericReturnType(), member, visited, violations);
        for (Type parameter : method.getGenericParameterTypes())
          inspect(parameter, member, visited, violations);
        for (Type exception : method.getGenericExceptionTypes())
          inspect(exception, member, visited, violations);
      }
    }
  }
}
