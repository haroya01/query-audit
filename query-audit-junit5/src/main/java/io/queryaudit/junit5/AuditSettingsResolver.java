package io.queryaudit.junit5;

import io.queryaudit.core.config.AuditMode;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.ReportFormat;
import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.config.RuleProfile;
import io.queryaudit.core.detector.RepositoryReturnTypeResolver;
import io.queryaudit.core.regression.QueryContracts;
import io.queryaudit.core.regression.QueryCountBaseline;
import java.lang.reflect.Method;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Resolves host settings without retaining mutable per-test configuration. Each call starts from
 * the current Spring/default config and applies the existing annotation and property precedence.
 * Activation state and capture resources remain owned by the JUnit scope.
 */
final class AuditSettingsResolver {
  private final Function<ExtensionContext, QueryAuditConfig> springConfigLookup;

  AuditSettingsResolver() {
    this(AuditSettingsResolver::lookupSpringConfig);
  }

  AuditSettingsResolver(Function<ExtensionContext, QueryAuditConfig> springConfigLookup) {
    this.springConfigLookup = Objects.requireNonNull(springConfigLookup, "springConfigLookup");
  }

  static Path resolveReportOutputDirectory(QueryAuditConfig config) {
    String configuredPath =
        System.getProperty("queryAudit.reportOutputDir", config.getReportOutputDir());
    if (configuredPath == null || configuredPath.isBlank()) {
      throw new ExtensionConfigurationException(
          "QueryAudit: report output directory must not be blank. Configure"
              + " query-audit.report.output-dir with a valid directory.");
    }
    try {
      return Path.of(configuredPath).toAbsolutePath().normalize();
    } catch (InvalidPathException e) {
      throw new ExtensionConfigurationException(
          "QueryAudit: invalid report output directory '" + configuredPath + "'.", e);
    }
  }

  static boolean isContractRecordMode() {
    return Boolean.parseBoolean(
        resolveSystemProperty(
            "queryAudit.contracts.record", "queryGuard.contracts.record", "false"));
  }

  static Path resolveContractsPath() {
    String sysProp = resolveSystemProperty("queryAudit.contractsPath", "queryGuard.contractsPath");
    if (sysProp != null && !sysProp.isEmpty()) {
      return Path.of(sysProp);
    }
    return Path.of(QueryContracts.DEFAULT_FILE_NAME);
  }

  static boolean isClassExcluded(Class<?> testClass) {
    Class<?> clazz = testClass;
    while (clazz != null) {
      if (clazz.isAnnotationPresent(QueryAuditExclude.class)) {
        return true;
      }
      clazz = clazz.getEnclosingClass();
    }
    return false;
  }

  AuditMode resolveAuditMode(ExtensionContext context) {
    String sysProp = resolveSystemProperty("queryAudit.mode", "queryGuard.mode");
    if (sysProp != null && !sysProp.isBlank()) {
      return AuditMode.parse(sysProp);
    }
    QueryAuditConfig springConfig = resolveSpringConfig(context);
    if (springConfig != null) {
      return springConfig.getAuditMode();
    }
    return AuditMode.ANNOTATED;
  }

  static boolean hasDirectExtendWith(ExtensionContext context) {
    Class<?> clazz = context.getRequiredTestClass();
    while (clazz != null) {
      for (ExtendWith extendWith : clazz.getAnnotationsByType(ExtendWith.class)) {
        for (Class<?> registered : extendWith.value()) {
          if (registered == QueryAuditExtension.class) {
            return true;
          }
        }
      }
      clazz = clazz.getEnclosingClass();
    }
    return false;
  }

  static boolean hasFocusedAuditAnnotation(ExtensionContext context) {
    Optional<Method> method = context.getTestMethod();
    if (method.isPresent() && hasFocusedAuditAnnotation(method.get())) {
      return true;
    }

    Class<?> clazz = context.getRequiredTestClass();
    while (clazz != null) {
      if (clazz.isAnnotationPresent(DetectNPlusOne.class)) {
        return true;
      }
      clazz = clazz.getEnclosingClass();
    }
    return false;
  }

  static boolean isMethodLevelOptIn(Method method) {
    return method.isAnnotationPresent(QueryAudit.class) || hasFocusedAuditAnnotation(method);
  }

  static boolean hasFocusedAuditAnnotation(Method method) {
    return method.isAnnotationPresent(DetectNPlusOne.class)
        || method.isAnnotationPresent(ExpectQueries.class)
        || method.isAnnotationPresent(ExpectMaxQueryCount.class);
  }

  QueryAuditConfig buildConfig(ExtensionContext context, RepositoryReturnTypeResolver resolver) {
    // Layer 1: Start from Spring config (application.yml) if available, else hardcoded defaults
    QueryAuditConfig springConfig = resolveSpringConfig(context);
    QueryAuditConfig.Builder builder =
        springConfig != null
            ? QueryAuditConfig.Builder.from(springConfig)
            : QueryAuditConfig.builder();

    // Layer 2: @EnableQueryInspector override
    if (hasEnableQueryInspector(context)) {
      builder.failOnDetection(false);
    }

    // Layer 3: @QueryAudit annotation overrides (only explicitly specified values)
    QueryAudit annotation = findAnnotation(context);
    FindingFailurePolicy.from(annotation);
    if (annotation != null) {
      // failOnDetection: only override when explicitly specified in the annotation
      if (annotation.failOnDetection().isSpecified()) {
        builder.failOnDetection(annotation.failOnDetection().toBoolean());
      }

      if (annotation.nPlusOneThreshold() >= 0) {
        builder.nPlusOneThreshold(annotation.nPlusOneThreshold());
      }

      for (String suppress : annotation.suppress()) {
        builder.addSuppressPattern(suppress);
      }

      if (!annotation.baselinePath().isEmpty()) {
        builder.baselinePath(annotation.baselinePath());
      }

      builder.includeSetupQueries(annotation.includeSetupQueries());
    }

    // Layer 4: @DetectNPlusOne override (highest priority for threshold)
    DetectNPlusOne detectNPlusOne = findDetectNPlusOne(context);
    if (detectNPlusOne != null) {
      builder.nPlusOneThreshold(detectNPlusOne.threshold());
    }

    String profile = System.getProperty("queryAudit.profile");
    if (profile != null) {
      builder.ruleProfile(RuleProfile.parse(profile));
    }

    String reportFormat =
        resolveSystemProperty("queryAudit.reportFormat", "queryGuard.reportFormat");
    if (reportFormat != null) {
      builder.reportFormat(ReportFormat.parse(reportFormat));
    }

    String reportRedaction = System.getProperty("queryAudit.reportRedaction");
    if (reportRedaction != null) {
      builder.reportRedaction(ReportRedaction.parse(reportRedaction));
    }

    // Wire return type resolver if available
    if (resolver != null) {
      builder.repositoryReturnTypeResolver(resolver);
    }

    return builder.build();
  }

  QueryAuditConfig resolveSpringConfig(ExtensionContext context) {
    return springConfigLookup.apply(context);
  }

  private static QueryAuditConfig lookupSpringConfig(ExtensionContext context) {
    try {
      Object appContext = resolveApplicationContext(context);
      if (appContext != null) {
        Method getBean = appContext.getClass().getMethod("getBean", Class.class);
        Object bean = getBean.invoke(appContext, QueryAuditConfig.class);
        if (bean instanceof QueryAuditConfig config) {
          return config;
        }
      }
    } catch (Exception ignored) {
      // Spring not available, no context, or no QueryAuditConfig bean — fall back to defaults
    }
    return null;
  }

  boolean hasEnableQueryInspector(ExtensionContext context) {
    Class<?> clazz = context.getRequiredTestClass();
    while (clazz != null) {
      if (clazz.isAnnotationPresent(EnableQueryInspector.class)) return true;
      clazz = clazz.getEnclosingClass();
    }
    return false;
  }

  DetectNPlusOne findDetectNPlusOne(ExtensionContext context) {
    Optional<Method> method = context.getTestMethod();
    if (method.isPresent()) {
      DetectNPlusOne annotation = method.get().getAnnotation(DetectNPlusOne.class);
      if (annotation != null) {
        return annotation;
      }
    }
    Class<?> clazz = context.getRequiredTestClass();
    while (clazz != null) {
      DetectNPlusOne annotation = clazz.getAnnotation(DetectNPlusOne.class);
      if (annotation != null) {
        return annotation;
      }
      clazz = clazz.getEnclosingClass();
    }
    return null;
  }

  QueryAudit findAnnotation(ExtensionContext context) {
    // getTestMethod() returns Optional.empty() in afterAll (class-level context)
    Optional<Method> testMethod = context.getTestMethod();
    if (testMethod.isPresent()) {
      QueryAudit annotation = testMethod.get().getAnnotation(QueryAudit.class);
      if (annotation != null) return annotation;
    }

    Class<?> clazz = context.getRequiredTestClass();
    while (clazz != null) {
      QueryAudit annotation = clazz.getAnnotation(QueryAudit.class);
      if (annotation != null) return annotation;
      clazz = clazz.getEnclosingClass();
    }

    return null;
  }

  Path resolveCountBaselinePath(ExtensionContext context) {
    String sysProp =
        resolveSystemProperty("queryAudit.countBaselinePath", "queryGuard.countBaselinePath");
    if (sysProp != null && !sysProp.isEmpty()) {
      return Path.of(sysProp);
    }
    return Path.of(QueryCountBaseline.DEFAULT_FILE_NAME);
  }

  boolean shouldAutoOpenReport(ExtensionContext context) {
    String sysProp = System.getProperty("queryaudit.autoOpenReport");
    if (sysProp != null) {
      return Boolean.parseBoolean(sysProp);
    }

    String envVar = System.getenv("QUERYGUARD_AUTO_OPEN_REPORT");
    if (envVar != null) {
      return Boolean.parseBoolean(envVar);
    }

    // Explicit annotation overrides CI detection
    QueryAudit annotation = findAnnotation(context);
    if (annotation != null && annotation.autoOpenReport().isSpecified()) {
      return annotation.autoOpenReport().toBoolean();
    }

    if (System.getenv("CI") != null
        || System.getenv("JENKINS_HOME") != null
        || System.getenv("GITHUB_ACTIONS") != null
        || System.getenv("GITLAB_CI") != null) {
      return false;
    }

    QueryAuditConfig springConfig = resolveSpringConfig(context);
    if (springConfig != null) {
      return springConfig.isAutoOpenReport();
    }

    // Default: auto-open when running locally (not in CI)
    return true;
  }

  static Object resolveApplicationContext(ExtensionContext context) {
    try {
      Class<?> springExtensionClass =
          Class.forName("org.springframework.test.context.junit.jupiter.SpringExtension");
      Method getAppContext =
          springExtensionClass.getMethod("getApplicationContext", ExtensionContext.class);
      return getAppContext.invoke(null, context);
    } catch (Exception | NoClassDefFoundError ignored) {
      return null;
    }
  }

  static String resolveSystemProperty(String... keys) {
    for (String key : keys) {
      String v = System.getProperty(key);
      if (v != null) return v;
    }
    return null;
  }

  static String resolveSystemProperty(String primary, String legacy, String defaultValue) {
    String v = resolveSystemProperty(primary, legacy);
    return v != null ? v : defaultValue;
  }
}
