package io.queryaudit.junit5;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.ReportFormat;
import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.extension.AuditExtensions;
import io.queryaudit.core.interceptor.QueryCaptureSession;
import io.queryaudit.core.interceptor.QueryInterceptor;
import io.queryaudit.core.model.AuditRunResult;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.model.LifecyclePhase;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.jupiter.api.extension.ReflectiveInvocationContext;

/**
 * JUnit lifecycle adapter: activate, acquire capture resources, analyze, retain evidence, assert,
 * and finalize the suite. Internal collaborators own storage, policy files, detector orchestration,
 * and artifact delivery; this adapter owns callback ordering and public registration.
 */
public class QueryAuditExtension
    implements BeforeAllCallback,
        BeforeEachCallback,
        BeforeTestExecutionCallback,
        AfterTestExecutionCallback,
        AfterEachCallback,
        AfterAllCallback,
        InvocationInterceptor {
  private static final ExtensionContext.Namespace NAMESPACE = AuditScope.NAMESPACE;
  private final AuditSettingsResolver settingsResolver = new AuditSettingsResolver();
  private final AuditReportArtifacts artifacts = new AuditReportArtifacts(this::moveJsonReportFile);
  private final AuditExtensionResolver extensionResolver;
  private final AuditActivation activation;
  private final AuditScopeInitializer initializer;
  private final QueryCountPolicies counts = new QueryCountPolicies();
  private final ExplainAnalysis explain = new ExplainAnalysis();
  private final AuditTestAnalysis analysis;
  private final AuditTestAssertions assertions;
  private final AuditTestReporting reporting = new AuditTestReporting();

  public QueryAuditExtension() {
    this(new DataSourceResolver(), new IndexMetadataCollector(), new HibernateIntegration());
  }

  /**
   * Registers a shared immutable catalog via a static {@code @RegisterExtension} field and opts the
   * class in without {@code @QueryAudit}. Use one registration path, not both. Extension
   * implementations must be thread-safe; capture state belongs to the JUnit scope.
   */
  public QueryAuditExtension(AuditExtensions extensions) {
    this(
        new DataSourceResolver(),
        new IndexMetadataCollector(),
        new HibernateIntegration(),
        Objects.requireNonNull(extensions, "extensions"));
  }

  QueryAuditExtension(
      DataSourceResolver dataSources,
      IndexMetadataCollector metadata,
      HibernateIntegration hibernate) {
    this(dataSources, metadata, hibernate, null);
  }

  private QueryAuditExtension(
      DataSourceResolver dataSources,
      IndexMetadataCollector metadata,
      HibernateIntegration hibernate,
      AuditExtensions extensions) {
    Objects.requireNonNull(dataSources, "dataSourceResolver");
    Objects.requireNonNull(metadata, "metadataCollector");
    Objects.requireNonNull(hibernate, "hibernateIntegration");
    extensionResolver = new AuditExtensionResolver(NAMESPACE, extensions);
    activation = new AuditActivation(settingsResolver, extensionResolver.explicitlyRegistered());
    initializer =
        new AuditScopeInitializer(
            dataSources, metadata, hibernate, new AuditPolicyFiles(settingsResolver));
    analysis =
        new AuditTestAnalysis(
            hibernate,
            counts,
            explain,
            new ComparisonInputRecorder(settingsResolver),
            settingsResolver);
    assertions = new AuditTestAssertions(settingsResolver, counts);
  }

  @Override
  public void beforeAll(ExtensionContext context) throws Exception {
    AuditScope scope = AuditScope.of(context);
    try (var ignored = QueryCaptureSession.suppress()) {
      extensionResolver.register(context, scope.interceptor() != null);
      boolean active = activation.computeActive(scope);
      QueryAuditConfig config = active ? buildConfig(context) : null;
      if (config != null && !config.isEnabled()) active = false;
      scope.flag(AuditScope.Flag.ACTIVE, active);
      if (!active) return;

      registerReportFinalizer(context, config);
      initializer.initialize(
          scope, config, extensionsFor(context), AuditScopeInitializer.Boundary.CLASS);
    } catch (Exception | Error failure) {
      AuditDiagnostics.unexpectedInitializationFailure(scope, failure);
      throw failure;
    }
  }

  @Override
  public void interceptBeforeAllMethod(
      Invocation<Void> invocation,
      ReflectiveInvocationContext<Method> invocationContext,
      ExtensionContext extensionContext)
      throws Throwable {
    try (var ignored = QueryCaptureSession.suppress()) {
      invocation.proceed();
    }
  }

  @Override
  public void interceptAfterAllMethod(
      Invocation<Void> invocation,
      ReflectiveInvocationContext<Method> invocationContext,
      ExtensionContext extensionContext)
      throws Throwable {
    try (var ignored = QueryCaptureSession.suppress()) {
      invocation.proceed();
    }
  }

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    AuditScope scope = AuditScope.of(context);
    try {
      QueryAuditConfig config = prepareCapture(scope);
      if (config == null) {
        scope.suppressCaptureForInvocation();
        return;
      }
      scope.startCapture(config.getMaxQueries());
    } catch (Exception | Error failure) {
      scope.closeCapture();
      AuditDiagnostics.unexpectedInitializationFailure(scope, failure);
      throw failure;
    }
  }

  private QueryAuditConfig prepareCapture(AuditScope scope) throws Exception {
    ExtensionContext context = scope.context();
    try (var ignored = QueryCaptureSession.suppress()) {
      extensionResolver.requireStaticRegistration(context);
      if (!activation.isActive(scope)) return null;
      QueryAuditConfig config = buildConfig(context);
      if (!config.isEnabled()) {
        scope.flag(AuditScope.Flag.ACTIVE, false);
        scope.flag(AuditScope.Flag.METHOD_ACTIVE, false);
        return null;
      }
      registerReportFinalizer(context, config);
      AuditActivation.rejectDynamicTestFactoryAudit(scope);
      initializer.initialize(
          scope, config, extensionsFor(context), AuditScopeInitializer.Boundary.METHOD);
      return config;
    }
  }

  @Override
  public void beforeTestExecution(ExtensionContext context) {
    transition(context, LifecyclePhase.TEST);
  }

  @Override
  public void afterTestExecution(ExtensionContext context) {
    transition(context, LifecyclePhase.TEARDOWN);
  }

  private void transition(ExtensionContext context, LifecyclePhase phase) {
    AuditScope scope = AuditScope.of(context);
    if (activation.callbacksAllowed(scope) && scope.interceptor() != null)
      scope.transitionCapture(phase);
  }

  @Override
  public void afterEach(ExtensionContext context) {
    AuditScope scope = AuditScope.of(context);
    if (!activation.callbacksAllowed(scope)
        || Boolean.TRUE.equals(scope.flag(AuditScope.Flag.ANALYSIS_DONE))) return;
    scope.flag(AuditScope.Flag.ANALYSIS_DONE, true);
    if (scope.interceptor() == null) return;
    try (var ignored = QueryCaptureSession.suppress()) {
      AnalyzedAudit result = analysis.analyze(scope, extensionsFor(context));
      // Retain diagnostics before an assertion throws. Suite publication happens after all tests.
      reporting.present(scope, result);
      assertions.verify(scope, result);
    } catch (AuditPolicyViolation failure) {
      throw failure;
    } catch (RuntimeException | Error failure) {
      AuditDiagnostics.incomplete(
          scope,
          IncompleteReasonCode.AUDIT_ANALYSIS_FAILED,
          "Could not complete query analysis for "
              + scope.target()
              + ": "
              + AuditDiagnostics.failureMessage(failure));
      throw failure;
    }
  }

  @Override
  public void afterAll(ExtensionContext context) {
    AuditScope scope = AuditScope.of(context);
    if (context.getRequiredTestClass().getEnclosingClass() != null && !scope.ownsResources())
      return;
    initializer.close(scope);
    SuiteReportFinalizer finalizer = scope.finalizer();
    if (finalizer != null && shouldAutoOpenReport(context)) finalizer.enableAutoOpen();
  }

  void registerReportFinalizer(ExtensionContext context, QueryAuditConfig config) {
    AuditScope scope = AuditScope.of(context);
    try {
      Path directory = AuditSettingsResolver.resolveReportOutputDirectory(config);
      ReportFinalizer finalizer =
          scope.finalizer(
              () ->
                  new ReportFinalizer(
                      this,
                      directory,
                      config.getReportFormat(),
                      scope.runState(),
                      config.getReportRedaction()));
      finalizer.requireConfiguration(
          directory, config.getReportFormat(), config.getReportRedaction());
      finalizer.requireReportSinks(extensionsFor(context).reportSinks());
      AuditCoverageSession coverage = AuditCoverageListener.currentSession(context);
      finalizer.requireCoverageSession(coverage);
      if (AuditCoverageManifest.isConfigured() && coverage == null) {
        throw new ExtensionConfigurationException(
            "QueryAudit: an audit coverage manifest is configured, but the platform listener is"
                + " unavailable. Enable JUnit Platform test execution listener auto-registration.");
      }
    } catch (RuntimeException | Error failure) {
      AuditDiagnostics.initializationFailure(
          scope,
          IncompleteReasonCode.AUDIT_INITIALIZATION_FAILED,
          "Could not configure suite reporting: " + AuditDiagnostics.failureMessage(failure));
      throw failure;
    }
  }

  private AuditExtensions extensionsFor(ExtensionContext context) {
    return extensionResolver.resolve(
        context, () -> AuditSettingsResolver.resolveApplicationContext(context));
  }

  // Small compatibility seams for existing callback-focused tests and overridable artifact IO.
  private QueryAuditConfig buildConfig(ExtensionContext context) {
    return settingsResolver.buildConfig(context, AuditScope.of(context).returnTypeResolver());
  }

  private boolean shouldAutoOpenReport(ExtensionContext context) {
    return settingsResolver.shouldAutoOpenReport(context);
  }

  private boolean computeActive(ExtensionContext context) {
    return activation.computeActive(AuditScope.of(context));
  }

  private boolean isAuditActive(ExtensionContext context) {
    return activation.isActive(AuditScope.of(context));
  }

  QueryAuditReport runExplainAnalysis(
      ExtensionContext context,
      QueryAuditReport report,
      List<QueryRecord> queries,
      QueryAuditAnalyzer analyzer) {
    return explain.analyze(
        AuditScope.of(context), extensionsFor(context), report, queries, analyzer);
  }

  QueryAuditReport detectQueryCountRegression(
      ExtensionContext context,
      QueryAuditReport report,
      List<QueryRecord> queries,
      String testId,
      String testClass,
      String testName,
      QueryAuditAnalyzer analyzer) {
    return counts.detectRegression(
        AuditScope.of(context), report, queries, testId, testClass, testName, analyzer);
  }

  QueryAuditReport mergeConnectionHeldIdleIssues(
      QueryAuditReport report, QueryInterceptor interceptor, QueryAuditAnalyzer analyzer) {
    return analysis.mergeConnectionHeldIdleIssues(report, interceptor, analyzer);
  }

  static QueryAuditReport applyInfoVisibility(QueryAuditReport report, boolean showInfo) {
    return AuditTestReporting.visible(report, showInfo);
  }

  static String buildExpectQueriesFailureMessage(
      ExpectQueries annotation, List<QueryRecord> queries, String testName) {
    return annotation == null
        ? null
        : AuditAssertions.buildExpectQueriesFailureMessage(annotation, queries, testName);
  }

  static Path coverageReportOutputDirectory() {
    return AuditSettingsResolver.resolveReportOutputDirectory(QueryAuditConfig.builder().build());
  }

  static boolean isAuditPolicyFailure(Throwable failure) {
    return AuditPolicyViolation.isPurePolicyFailure(failure);
  }

  static LegacyIdentityRegistry claimLegacyIdentity(
      ExtensionContext context,
      String claimKey,
      String testId,
      boolean usesLegacyIdentity,
      String testClass,
      String testName) {
    return LegacyPolicyIdentities.claim(
        AuditScope.of(context), claimKey, testId, usesLegacyIdentity, testClass, testName);
  }

  void writeJsonReport(AuditRunResult runResult, Path outputDir) throws IOException {
    writeJsonReport(runResult, outputDir, ReportRedaction.REDACTED);
  }

  void writeJsonReport(AuditRunResult runResult, Path outputDir, ReportRedaction redaction)
      throws IOException {
    artifacts.write(runResult, outputDir, redaction);
  }

  void moveJsonReportFile(Path source, Path target) throws IOException {
    AuditReportArtifacts.moveFile(source, target);
  }

  // Retain historical package-level type names and Store keys; behavior belongs to the concrete
  // collaborators.
  static final class AuditRunState extends SuiteAuditState {}

  static final class LegacyIdentityRegistry extends LegacyPolicyIdentities.Registry {}

  static final class ReportFinalizer extends SuiteReportFinalizer {
    ReportFinalizer(QueryAuditExtension extension, Path directory, ReportFormat format) {
      this(extension, directory, format, new AuditRunState());
    }

    ReportFinalizer(
        QueryAuditExtension extension, Path directory, ReportFormat format, AuditRunState state) {
      this(extension, directory, format, state, ReportRedaction.REDACTED);
    }

    ReportFinalizer(
        QueryAuditExtension extension,
        Path directory,
        ReportFormat format,
        AuditRunState state,
        ReportRedaction redaction) {
      super(
          extension::writeJsonReport,
          AuditReportArtifacts::openInBrowser,
          directory,
          format,
          state,
          redaction);
    }
  }
}
