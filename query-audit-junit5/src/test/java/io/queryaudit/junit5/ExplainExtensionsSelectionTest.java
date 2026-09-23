package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.queryaudit.core.analyzer.ExplainAnalysisException;
import io.queryaudit.core.analyzer.ExplainAnalyzer;
import io.queryaudit.core.baseline.BaselineEntry;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.RuleProfile;
import io.queryaudit.core.detector.QueryAuditAnalyzer;
import io.queryaudit.core.extension.AuditExtensions;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.provenance.AuditCapability;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Execution(ExecutionMode.SAME_THREAD)
public class ExplainExtensionsSelectionTest {
  private static final String SERVICE = "META-INF/services/" + ExplainAnalyzer.class.getName();
  private static final String SQL = "SELECT id FROM orders ORDER BY created_at";
  private static final List<QueryRecord> QUERIES = List.of(new QueryRecord(SQL, 10L, 0L, ""));
  private static final AtomicInteger THROWING_PROVIDER_CONSTRUCTIONS = new AtomicInteger();

  @TempDir Path temporaryDirectory;

  @BeforeEach
  void resetProviderCounter() {
    THROWING_PROVIDER_CONSTRUCTIONS.set(0);
  }

  @ParameterizedTest
  @EnumSource(DiscoveryFailure.class)
  void matchingExplicitProviderDoesNotLoadBrokenLegacyProviders(DiscoveryFailure failure)
      throws Exception {
    RecordingAnalyzer explicit = new RecordingAnalyzer("h2");
    Fixture fixture = fixture(explicit);
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config().build(), List.of());

    QueryAuditReport report = withBrokenDiscovery(failure, fixture, analyzer);

    assertThat(explicit.executions).isEqualTo(1);
    assertThat(THROWING_PROVIDER_CONSTRUCTIONS).hasValue(0);
    assertThat(report.getInfoIssues()).extracting(Issue::type).containsExactly(IssueType.FILESORT);
    assertThat(fixture.capability().state()).isEqualTo(AuditCapability.State.AVAILABLE);
    assertThat(fixture.capability().inputsComplete()).isFalse();
    assertThat(fixture.runState().result(List.of()).outcome()).isEqualTo(AuditOutcome.PASS);
  }

  @ParameterizedTest
  @EnumSource(DiscoveryFailure.class)
  void discoveryFailureIsRecordedWhenNoExplicitProviderMatches(DiscoveryFailure failure)
      throws Exception {
    RecordingAnalyzer unrelated = new RecordingAnalyzer("postgresql");
    Fixture fixture = fixture(unrelated);
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config().build(), List.of());

    QueryAuditReport report = withBrokenDiscovery(failure, fixture, analyzer);

    assertThat(unrelated.executions).isZero();
    assertThat(THROWING_PROVIDER_CONSTRUCTIONS)
        .hasValue(failure == DiscoveryFailure.THROWING_CONSTRUCTOR ? 1 : 0);
    assertThat(report.getConfirmedIssues()).isEmpty();
    assertThat(report.getInfoIssues()).isEmpty();
    assertThat(fixture.capability().state()).isEqualTo(AuditCapability.State.FAILED);
    var result = fixture.runState().result(List.of());
    assertThat(result.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
    assertThat(result.incompleteReasons())
        .extracting(reason -> reason.code())
        .containsExactly(IncompleteReasonCode.CAPABILITY_EXECUTION_FAILED);
    assertThat(result.incompleteReasons())
        .allSatisfy(
            reason -> assertThat(reason.detail()).doesNotContain("private_provider_marker"));
  }

  @Test
  void disabledFindingPolicyAlsoAppliesToExplicitProviderResults() throws Exception {
    RecordingAnalyzer explicit = new RecordingAnalyzer("h2");
    Fixture fixture = fixture(explicit);
    QueryAuditAnalyzer analyzer =
        new QueryAuditAnalyzer(config().addDisabledRule("filesort").build(), List.of());

    QueryAuditReport report = run(fixture, analyzer);

    assertThat(explicit.executions).isEqualTo(1);
    assertThat(report.getConfirmedIssues()).isEmpty();
    assertThat(report.getInfoIssues()).isEmpty();
    assertThat(report.getAcknowledgedIssues()).isEmpty();
  }

  @Test
  void suppressionPolicyAlsoAppliesToExplicitProviderResults() throws Exception {
    RecordingAnalyzer explicit = new RecordingAnalyzer("h2");
    Fixture fixture = fixture(explicit);
    QueryAuditAnalyzer analyzer =
        new QueryAuditAnalyzer(config().addSuppressPattern("filesort").build(), List.of());

    QueryAuditReport report = run(fixture, analyzer);

    assertThat(explicit.executions).isEqualTo(1);
    assertThat(report.getConfirmedIssues()).isEmpty();
    assertThat(report.getInfoIssues()).isEmpty();
    assertThat(report.getAcknowledgedIssues()).isEmpty();
  }

  @Test
  void severityOverridesAlsoReclassifyExplicitProviderResults() throws Exception {
    Fixture fixture = fixture(new RecordingAnalyzer("h2"));
    QueryAuditAnalyzer analyzer =
        new QueryAuditAnalyzer(
            config().addSeverityOverride("filesort", Severity.ERROR).build(), List.of());

    QueryAuditReport report = run(fixture, analyzer);

    assertThat(report.getConfirmedIssues())
        .singleElement()
        .satisfies(issue -> assertThat(issue.severity()).isEqualTo(Severity.ERROR));
    assertThat(report.getInfoIssues()).isEmpty();
  }

  @Test
  void baselineAcknowledgementAlsoAppliesToExplicitProviderResults() throws Exception {
    Fixture fixture = fixture(new RecordingAnalyzer("h2"));
    BaselineEntry accepted =
        new BaselineEntry("filesort", "orders", null, SQL, "team", "expected sort");
    QueryAuditAnalyzer analyzer = new QueryAuditAnalyzer(config().build(), List.of(accepted));

    QueryAuditReport report = run(fixture, analyzer);

    assertThat(report.getAcknowledgedIssues())
        .extracting(Issue::type)
        .containsExactly(IssueType.FILESORT);
    assertThat(report.getConfirmedIssues()).isEmpty();
    assertThat(report.getInfoIssues()).isEmpty();
  }

  @Test
  void ambiguityIdentifiesBothExplicitExplainRegistrations() throws Exception {
    Fixture fixture =
        fixture(
            AuditExtensions.builder()
                .explainAnalyzer("explain:first", new RecordingAnalyzer("h2"))
                .explainAnalyzer("explain:second", new RecordingAnalyzer("h2"))
                .build());
    run(fixture, new QueryAuditAnalyzer(config().build(), List.of()));

    assertThat(fixture.runState().result(List.of()).outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
    assertThat(fixture.runState().result(List.of()).incompleteReasons())
        .singleElement()
        .satisfies(
            reason ->
                assertThat(reason.detail())
                    .contains("AMBIGUOUS_EXPLICIT_PROVIDERS", "explain:first", "explain:second"));
  }

  @Test
  void failedExplainExecutionPreservesIdentityButNotExceptionPayload() throws Exception {
    Fixture fixture = fixture(failingProvider(false));
    run(fixture, new QueryAuditAnalyzer(config().build(), List.of()));

    assertThat(fixture.capability().state()).isEqualTo(AuditCapability.State.FAILED);
    assertThat(fixture.runState().result(List.of()).incompleteReasons())
        .singleElement()
        .satisfies(
            reason ->
                assertThat(reason.detail())
                    .contains("company:explain", "PROVIDER_EXECUTION_FAILED")
                    .doesNotContain("private_provider_marker"));
  }

  @Test
  void partialExplainFailurePreservesCompletedFindingsAndSafeIdentity() throws Exception {
    Fixture fixture = fixture(failingProvider(true));
    QueryAuditReport report = run(fixture, new QueryAuditAnalyzer(config().build(), List.of()));

    assertThat(report.getInfoIssues()).extracting(Issue::type).containsExactly(IssueType.FILESORT);
    assertThat(fixture.runState().result(List.of()).incompleteReasons())
        .singleElement()
        .satisfies(
            reason ->
                assertThat(reason.detail())
                    .contains("company:explain", "EXECUTION_FAILED")
                    .doesNotContain("private_provider_marker"));
  }

  @Test
  void nullExplainResultIsIncompleteAndIdentifiesTheProvider() throws Exception {
    Fixture fixture =
        fixture(
            new ExplainAnalyzer() {
              public String supportedDatabase() {
                return "h2";
              }

              public List<Issue> analyze(Connection connection, List<QueryRecord> queries) {
                return null;
              }
            });
    run(fixture, new QueryAuditAnalyzer(config().build(), List.of()));

    assertThat(fixture.capability().state()).isEqualTo(AuditCapability.State.FAILED);
    assertThat(fixture.runState().result(List.of()).outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
    assertThat(fixture.runState().result(List.of()).incompleteReasons())
        .singleElement()
        .satisfies(
            reason ->
                assertThat(reason.detail())
                    .contains("company:explain", "PROVIDER_EXECUTION_FAILED"));
  }

  @Test
  void unsafeExplainRegistrationIsNotEchoedOnExecutionFailure() throws Exception {
    Fixture fixture =
        fixture(
            AuditExtensions.builder()
                .explainAnalyzer("private-registration\nSELECT secret", failingProvider(false))
                .build());
    run(fixture, new QueryAuditAnalyzer(config().build(), List.of()));

    assertThat(fixture.runState().result(List.of()).incompleteReasons())
        .singleElement()
        .satisfies(
            reason ->
                assertThat(reason.detail())
                    .contains("unidentified-provider:1")
                    .doesNotContain(
                        "private-registration", "SELECT secret", "private_provider_marker"));
  }

  private static ExplainAnalyzer failingProvider(boolean preservePartial) {
    return new ExplainAnalyzer() {
      public String supportedDatabase() {
        return "h2";
      }

      public List<Issue> analyze(Connection connection, List<QueryRecord> queries) {
        var failure = new IllegalStateException("private_provider_marker");
        if (preservePartial) {
          throw new ExplainAnalysisException(
              List.of(
                  new Issue(
                      IssueType.FILESORT,
                      Severity.INFO,
                      SQL,
                      "orders",
                      null,
                      "completed plan",
                      "index")),
              failure);
        }
        throw failure;
      }
    };
  }

  private QueryAuditReport withBrokenDiscovery(
      DiscoveryFailure failure, Fixture fixture, QueryAuditAnalyzer analyzer) throws Exception {
    Path descriptor = temporaryDirectory.resolve(SERVICE);
    Files.createDirectories(descriptor.getParent());
    Files.writeString(
        descriptor,
        failure == DiscoveryFailure.THROWING_CONSTRUCTOR
            ? ThrowingExplainAnalyzer.class.getName() + "\n"
            : "not a legal provider class name!\n");
    Thread thread = Thread.currentThread();
    ClassLoader original = thread.getContextClassLoader();
    try (URLClassLoader isolatedServices =
        new URLClassLoader(new URL[] {temporaryDirectory.toUri().toURL()}, original) {
          @Override
          public Enumeration<URL> getResources(String name) throws IOException {
            return name.equals(SERVICE) ? findResources(name) : super.getResources(name);
          }
        }) {
      thread.setContextClassLoader(isolatedServices);
      try {
        return run(fixture, analyzer);
      } finally {
        thread.setContextClassLoader(original);
      }
    }
  }

  private static QueryAuditConfig.Builder config() {
    return QueryAuditConfig.builder().ruleProfile(RuleProfile.STRICT);
  }

  private static QueryAuditReport run(Fixture fixture, QueryAuditAnalyzer analyzer) {
    QueryAuditReport original =
        new QueryAuditReport(
            "ExplainExtensionsSelectionTest",
            "audited",
            List.of(),
            List.of(),
            List.of(),
            QUERIES,
            1,
            1,
            10L);
    return new QueryAuditExtension()
        .runExplainAnalysis(fixture.methodContext(), original, QUERIES, analyzer);
  }

  private static Fixture fixture(ExplainAnalyzer explicit) throws Exception {
    return fixture(AuditExtensions.builder().explainAnalyzer("company:explain", explicit).build());
  }

  private static Fixture fixture(AuditExtensions extensions) throws Exception {
    DataSource dataSource = mock(DataSource.class);
    Connection connection = mock(Connection.class);
    DatabaseMetaData metadata = mock(DatabaseMetaData.class);
    when(dataSource.getConnection()).thenReturn(connection);
    when(connection.getMetaData()).thenReturn(metadata);
    when(metadata.getDatabaseProductName()).thenReturn("H2");

    MapStore rootStore = new MapStore();
    QueryAuditExtension.AuditRunState runState = new QueryAuditExtension.AuditRunState();
    rootStore.put(QueryAuditExtension.AuditRunState.class.getName(), runState);
    MapStore classStore = new MapStore();
    classStore.put("dataSource", dataSource);
    classStore.put(AuditExtensions.class.getName(), extensions);
    MapStore methodStore = new MapStore();

    ExtensionContext root = context(rootStore, null, null);
    ExtensionContext classContext = context(classStore, root, root);
    ExtensionContext method = context(methodStore, classContext, root);
    return new Fixture(method, methodStore, runState);
  }

  private static ExtensionContext context(
      MapStore store, ExtensionContext parent, ExtensionContext root) {
    ExtensionContext context = mock(ExtensionContext.class);
    when(context.getStore(any(ExtensionContext.Namespace.class))).thenReturn(store);
    when(context.getParent()).thenReturn(Optional.ofNullable(parent));
    when(context.getRoot()).thenReturn(root == null ? context : root);
    return context;
  }

  private record Fixture(
      ExtensionContext methodContext,
      MapStore methodStore,
      QueryAuditExtension.AuditRunState runState) {
    AuditCapability capability() {
      return methodStore.get("explainCapability", AuditCapability.class);
    }
  }

  private enum DiscoveryFailure {
    THROWING_CONSTRUCTOR,
    MALFORMED_DESCRIPTOR
  }

  public static final class ThrowingExplainAnalyzer implements ExplainAnalyzer {
    public ThrowingExplainAnalyzer() {
      THROWING_PROVIDER_CONSTRUCTIONS.incrementAndGet();
      throw new IllegalStateException("private_provider_marker");
    }

    @Override
    public String supportedDatabase() {
      return "h2";
    }

    @Override
    public List<Issue> analyze(Connection connection, List<QueryRecord> queries) {
      throw new AssertionError("The broken provider cannot be constructed");
    }
  }

  private static final class RecordingAnalyzer implements ExplainAnalyzer {
    private final String database;
    private int executions;

    private RecordingAnalyzer(String database) {
      this.database = database;
    }

    @Override
    public String supportedDatabase() {
      return database;
    }

    @Override
    public List<Issue> analyze(Connection connection, List<QueryRecord> queries) {
      executions++;
      return List.of(
          new Issue(
              IssueType.FILESORT,
              Severity.INFO,
              queries.get(0).sql(),
              "orders",
              null,
              "Explicit provider finding",
              "Add a supporting index"));
    }
  }

  private static final class MapStore implements ExtensionContext.Store {
    private final Map<Object, Object> values = new HashMap<>();

    @Override
    public Object get(Object key) {
      return values.get(key);
    }

    @Override
    public <V> V get(Object key, Class<V> requiredType) {
      return requiredType.cast(values.get(key));
    }

    @Override
    public <K, V> Object getOrComputeIfAbsent(K key, Function<K, V> defaultCreator) {
      return values.computeIfAbsent(key, ignored -> defaultCreator.apply(key));
    }

    @Override
    public <K, V> V getOrComputeIfAbsent(
        K key, Function<K, V> defaultCreator, Class<V> requiredType) {
      return requiredType.cast(getOrComputeIfAbsent(key, defaultCreator));
    }

    @Override
    public void put(Object key, Object value) {
      values.put(key, value);
    }

    @Override
    public Object remove(Object key) {
      return values.remove(key);
    }

    @Override
    public <V> V remove(Object key, Class<V> requiredType) {
      return requiredType.cast(values.remove(key));
    }
  }
}
