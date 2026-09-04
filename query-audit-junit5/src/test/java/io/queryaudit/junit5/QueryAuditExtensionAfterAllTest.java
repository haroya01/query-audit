package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.ReportFormat;
import io.queryaudit.core.model.AuditIncompleteReason;
import io.queryaudit.core.model.AuditOutcome;
import io.queryaudit.core.model.IncompleteReasonCode;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.Severity;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionConfigurationException;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.io.TempDir;

@DisplayName("QueryAuditExtension — afterAll report finalization (issue #41)")
class QueryAuditExtensionAfterAllTest {

  private static final ExtensionContext.Namespace NAMESPACE =
      ExtensionContext.Namespace.create(QueryAuditExtension.class);

  @BeforeEach
  void setUp() {
    HtmlReportAggregator.getInstance().reset();
  }

  // ── Helpers ──────────────────────────────────────────────────────

  private static QueryAuditReport dummyReport(String testClass, String testName) {
    Issue issue =
        new Issue(
            IssueType.N_PLUS_ONE,
            Severity.ERROR,
            "SELECT * FROM orders WHERE user_id = ?",
            "orders",
            null,
            "Repeated query detected",
            "Use JOIN FETCH");
    return new QueryAuditReport(
        testClass, testName, List.of(issue), List.of(), List.of(), List.of(), 1, 5, 100_000L);
  }

  /**
   * Creates a mock ExtensionContext for a top-level test class. Uses a real Class object to avoid
   * Mockito's inability to mock Class.
   */
  @SuppressWarnings("unchecked")
  private static ExtensionContext mockContext(
      Class<?> testClass, ExtensionContext root, ExtensionContext.Store rootStore) {
    ExtensionContext ctx = mock(ExtensionContext.class);

    when(ctx.getRequiredTestClass()).thenReturn((Class) testClass);
    when(ctx.getRoot()).thenReturn(root);
    when(ctx.getTestMethod()).thenReturn(Optional.empty());
    when(ctx.getParent()).thenReturn(Optional.of(root));

    ExtensionContext.Store classStore = mock(ExtensionContext.Store.class);
    when(ctx.getStore(NAMESPACE)).thenReturn(classStore);

    return ctx;
  }

  /**
   * Creates a store backed by a real ConcurrentHashMap so that getOrComputeIfAbsent behaves
   * correctly across multiple calls.
   */
  private static ExtensionContext.Store createRootStore() {
    Map<Object, Object> backingMap = new ConcurrentHashMap<>();

    ExtensionContext.Store store = mock(ExtensionContext.Store.class);

    when(store.getOrComputeIfAbsent(anyString(), any()))
        .thenAnswer(
            invocation -> {
              String key = invocation.getArgument(0);
              Function<Object, Object> factory = invocation.getArgument(1);
              return backingMap.computeIfAbsent(key, factory);
            });

    when(store.get(anyString())).thenAnswer(inv -> backingMap.get(inv.getArgument(0)));

    return store;
  }

  // ── Tests ────────────────────────────────────────────────────────

  @Nested
  @DisplayName("ReportFinalizer output directory")
  class FinalizerRegistration {

    @Test
    @DisplayName("plain JUnit uses the documented default directory")
    void plainJUnitUsesDefaultDirectory() {
      ExtensionContext.Store rootStore = createRootStore();
      ExtensionContext root = mock(ExtensionContext.class);
      when(root.getStore(NAMESPACE)).thenReturn(rootStore);

      QueryAuditExtension extension = new QueryAuditExtension();
      ExtensionContext context = mockContext(String.class, root, rootStore);

      extension.registerReportFinalizer(context, QueryAuditConfig.defaults());

      QueryAuditExtension.ReportFinalizer finalizer =
          (QueryAuditExtension.ReportFinalizer)
              rootStore.get(QueryAuditExtension.ReportFinalizer.class.getName());
      assertThat(finalizer.outputDirectory())
          .isEqualTo(
              Path.of(QueryAuditConfig.DEFAULT_REPORT_OUTPUT_DIR).toAbsolutePath().normalize());
      assertThat(finalizer.reportFormat()).isEqualTo(ReportFormat.CONSOLE);
    }

    @Test
    @DisplayName("equivalent normalized directories share one finalizer")
    void equivalentDirectoriesShareFinalizer(@TempDir Path tempDir) {
      ExtensionContext.Store rootStore = createRootStore();
      ExtensionContext root = mock(ExtensionContext.class);
      when(root.getStore(NAMESPACE)).thenReturn(rootStore);

      QueryAuditExtension extension = new QueryAuditExtension();
      QueryAuditConfig directPath =
          QueryAuditConfig.builder().reportOutputDir(tempDir.resolve("reports").toString()).build();
      QueryAuditConfig equivalentPath =
          QueryAuditConfig.builder()
              .reportOutputDir(tempDir.resolve("nested/../reports").toString())
              .build();

      extension.registerReportFinalizer(mockContext(String.class, root, rootStore), directPath);
      new QueryAuditExtension()
          .registerReportFinalizer(mockContext(Integer.class, root, rootStore), equivalentPath);

      verify(rootStore, times(2))
          .getOrComputeIfAbsent(eq(QueryAuditExtension.ReportFinalizer.class.getName()), any());
      QueryAuditExtension.ReportFinalizer finalizer =
          (QueryAuditExtension.ReportFinalizer)
              rootStore.get(QueryAuditExtension.ReportFinalizer.class.getName());
      assertThat(finalizer.outputDirectory()).isEqualTo(tempDir.resolve("reports"));
    }

    @Test
    @DisplayName("different directories in one root fail with a clear configuration error")
    void conflictingDirectoriesFail(@TempDir Path tempDir) {
      ExtensionContext.Store rootStore = createRootStore();
      ExtensionContext root = mock(ExtensionContext.class);
      when(root.getStore(NAMESPACE)).thenReturn(rootStore);

      QueryAuditExtension extension = new QueryAuditExtension();
      Path firstDirectory = tempDir.resolve("first");
      Path secondDirectory = tempDir.resolve("second");
      QueryAuditConfig first =
          QueryAuditConfig.builder().reportOutputDir(firstDirectory.toString()).build();
      QueryAuditConfig second =
          QueryAuditConfig.builder().reportOutputDir(secondDirectory.toString()).build();

      extension.registerReportFinalizer(mockContext(String.class, root, rootStore), first);

      assertThatThrownBy(
              () ->
                  extension.registerReportFinalizer(
                      mockContext(Integer.class, root, rootStore), second))
          .isInstanceOf(ExtensionConfigurationException.class)
          .hasMessageContaining(firstDirectory.toString())
          .hasMessageContaining(secondDirectory.toString())
          .hasMessageContaining("query-audit.report.output-dir");
      assertInitializationFailure(rootStore);
    }

    @Test
    @DisplayName("different formats in one root fail with a clear configuration error")
    void conflictingFormatsFail(@TempDir Path tempDir) {
      ExtensionContext.Store rootStore = createRootStore();
      ExtensionContext root = mock(ExtensionContext.class);
      when(root.getStore(NAMESPACE)).thenReturn(rootStore);

      QueryAuditConfig json =
          QueryAuditConfig.builder()
              .reportFormat(ReportFormat.JSON)
              .reportOutputDir(tempDir.toString())
              .build();
      QueryAuditConfig html =
          QueryAuditConfig.builder()
              .reportFormat(ReportFormat.HTML)
              .reportOutputDir(tempDir.toString())
              .build();

      QueryAuditExtension extension = new QueryAuditExtension();
      extension.registerReportFinalizer(mockContext(String.class, root, rootStore), json);

      assertThatThrownBy(
              () ->
                  extension.registerReportFinalizer(
                      mockContext(Integer.class, root, rootStore), html))
          .isInstanceOf(ExtensionConfigurationException.class)
          .hasMessageContaining("'json'")
          .hasMessageContaining("'html'")
          .hasMessageContaining("query-audit.report.format");
      assertInitializationFailure(rootStore);
    }

    @Test
    @DisplayName("a blank configured directory fails before the suite starts")
    void blankDirectoryFails() {
      ExtensionContext.Store rootStore = createRootStore();
      ExtensionContext root = mock(ExtensionContext.class);
      when(root.getStore(NAMESPACE)).thenReturn(rootStore);
      QueryAuditConfig config = QueryAuditConfig.builder().reportOutputDir(" ").build();

      assertThatThrownBy(
              () ->
                  new QueryAuditExtension()
                      .registerReportFinalizer(mockContext(String.class, root, rootStore), config))
          .isInstanceOf(ExtensionConfigurationException.class)
          .hasMessageContaining("must not be blank")
          .hasMessageContaining("query-audit.report.output-dir");
      assertInitializationFailure(rootStore);
    }

    private void assertInitializationFailure(ExtensionContext.Store rootStore) {
      QueryAuditExtension.AuditRunState runState =
          (QueryAuditExtension.AuditRunState)
              rootStore.get(QueryAuditExtension.AuditRunState.class.getName());
      assertThat(runState.result(List.of()))
          .satisfies(
              result -> {
                assertThat(result.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
                assertThat(result.incompleteReasons())
                    .extracting(reason -> reason.code())
                    .containsExactly(IncompleteReasonCode.AUDIT_INITIALIZATION_FAILED);
              });
    }
  }

  @Nested
  @DisplayName("ReportFinalizer.close() honors the selected format")
  class FinalizerClose {

    @Test
    @DisplayName("console format does not create file reports")
    void consoleDoesNotCreateFileReports(@TempDir Path tempDir) {
      addReports();

      Path outputDirectory = tempDir.resolve("console-reports");
      QueryAuditExtension.ReportFinalizer finalizer =
          new QueryAuditExtension.ReportFinalizer(
              new QueryAuditExtension(), outputDirectory, ReportFormat.CONSOLE);

      finalizer.close();

      assertThat(outputDirectory).doesNotExist();
    }

    @Test
    @DisplayName("JSON format writes only the machine report")
    void jsonWritesOnlyJson(@TempDir Path tempDir) throws IOException {
      addReports();

      Path outputDirectory = tempDir.resolve("json-reports");
      QueryAuditExtension.ReportFinalizer finalizer =
          new QueryAuditExtension.ReportFinalizer(
              new QueryAuditExtension(), outputDirectory, ReportFormat.JSON);

      finalizer.close();

      assertThat(outputDirectory.resolve("report.json")).exists();
      assertThat(outputDirectory.resolve("index.html")).doesNotExist();
      assertThat(Files.readString(outputDirectory.resolve("report.json")))
          .contains("\"outcome\": \"PASS\"")
          .contains("\"incompleteReasons\": []");
    }

    @Test
    @DisplayName("HTML format writes only the browser report")
    void htmlWritesOnlyHtml(@TempDir Path tempDir) {
      addReports();

      Path outputDirectory = tempDir.resolve("html-reports");
      QueryAuditExtension.ReportFinalizer finalizer =
          new QueryAuditExtension.ReportFinalizer(
              new QueryAuditExtension(), outputDirectory, ReportFormat.HTML);

      finalizer.close();

      assertThat(HtmlReportAggregator.getInstance().getReports()).hasSize(3);
      assertThat(outputDirectory.resolve("index.html")).exists();
      assertThat(outputDirectory.resolve("report.json")).doesNotExist();
    }

    @Test
    @DisplayName("a JSON write failure invalidates the audit run")
    void jsonWriteFailureInvalidatesRun(@TempDir Path tempDir) throws IOException {
      addReports();
      Path outputDirectory = blockDirectory(tempDir);
      QueryAuditExtension.AuditRunState runState = new QueryAuditExtension.AuditRunState();
      QueryAuditExtension.ReportFinalizer finalizer =
          new QueryAuditExtension.ReportFinalizer(
              new QueryAuditExtension(), outputDirectory, ReportFormat.JSON, runState);

      assertThatThrownBy(finalizer::close)
          .isInstanceOf(ReportWriteException.class)
          .hasMessageContaining("json report")
          .hasMessageContaining(outputDirectory.resolve("report.json").toString())
          .hasMessageContaining("audit run is incomplete")
          .hasCauseInstanceOf(IOException.class)
          .satisfies(
              failure -> {
                ReportWriteException exception = (ReportWriteException) failure;
                assertThat(exception.format()).isEqualTo(ReportFormat.JSON);
                assertThat(exception.reportPath())
                    .isEqualTo(outputDirectory.resolve("report.json"));
              });
      assertThat(finalizer.result(HtmlReportAggregator.getInstance().getReports()))
          .satisfies(
              result -> {
                assertThat(result.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
                assertThat(result.incompleteReasons())
                    .extracting(reason -> reason.code())
                    .containsExactly(IncompleteReasonCode.REPORT_WRITE_FAILED);
              });
    }

    @Test
    @DisplayName("a failed JSON replacement removes a stale passing report")
    void jsonWriteFailureRemovesStalePass(@TempDir Path tempDir) throws IOException {
      addReports();
      Path outputDirectory = Files.createDirectory(tempDir.resolve("reports"));
      Path reportPath = outputDirectory.resolve("report.json");
      Files.writeString(reportPath, "{\"schemaVersion\":\"1.1.0\",\"outcome\":\"PASS\"}");
      QueryAuditExtension.AuditRunState runState = new QueryAuditExtension.AuditRunState();
      QueryAuditExtension extension =
          new QueryAuditExtension() {
            @Override
            void moveJsonReportFile(Path source, Path target) throws IOException {
              throw new IOException("simulated replacement failure");
            }
          };
      QueryAuditExtension.ReportFinalizer finalizer =
          new QueryAuditExtension.ReportFinalizer(
              extension, outputDirectory, ReportFormat.JSON, runState);

      assertThatThrownBy(finalizer::close)
          .isInstanceOf(ReportWriteException.class)
          .hasRootCauseMessage("simulated replacement failure");

      assertThat(reportPath).doesNotExist();
      assertThat(outputDirectory).isEmptyDirectory();
      assertThat(finalizer.result(HtmlReportAggregator.getInstance().getReports()))
          .satisfies(
              result -> {
                assertThat(result.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
                assertThat(result.incompleteReasons())
                    .extracting(reason -> reason.code())
                    .containsExactly(IncompleteReasonCode.REPORT_WRITE_FAILED);
              });
    }

    @Test
    @DisplayName("an HTML write failure invalidates the audit run")
    void htmlWriteFailureInvalidatesRun(@TempDir Path tempDir) throws IOException {
      addReports();
      Path outputDirectory = blockDirectory(tempDir);
      QueryAuditExtension.AuditRunState runState = new QueryAuditExtension.AuditRunState();
      QueryAuditExtension.ReportFinalizer finalizer =
          new QueryAuditExtension.ReportFinalizer(
              new QueryAuditExtension(), outputDirectory, ReportFormat.HTML, runState);

      assertThatThrownBy(finalizer::close)
          .isInstanceOf(ReportWriteException.class)
          .hasMessageContaining("html report")
          .hasMessageContaining(outputDirectory.resolve("index.html").toString())
          .hasMessageContaining("audit run is incomplete")
          .hasCauseInstanceOf(IOException.class)
          .satisfies(
              failure -> {
                ReportWriteException exception = (ReportWriteException) failure;
                assertThat(exception.format()).isEqualTo(ReportFormat.HTML);
                assertThat(exception.reportPath()).isEqualTo(outputDirectory.resolve("index.html"));
              });
      assertThat(finalizer.result(HtmlReportAggregator.getInstance().getReports()))
          .satisfies(
              result -> {
                assertThat(result.outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
                assertThat(result.incompleteReasons())
                    .extracting(reason -> reason.code())
                    .containsExactly(IncompleteReasonCode.REPORT_WRITE_FAILED);
              });
    }

    @Test
    @DisplayName("an incomplete JSON run writes its reason without test reports")
    void jsonWritesIncompleteRunWithoutReports(@TempDir Path tempDir) throws IOException {
      QueryAuditExtension.AuditRunState runState = new QueryAuditExtension.AuditRunState();
      runState.markIncomplete(
          new AuditIncompleteReason(
              IncompleteReasonCode.DATASOURCE_UNAVAILABLE, "OrderServiceTest#loadsOrders"));
      Path outputDirectory = tempDir.resolve("incomplete");
      QueryAuditExtension.ReportFinalizer finalizer =
          new QueryAuditExtension.ReportFinalizer(
              new QueryAuditExtension(), outputDirectory, ReportFormat.JSON, runState);

      finalizer.close();

      assertThat(outputDirectory.resolve("index.html")).doesNotExist();
      assertThat(Files.readString(outputDirectory.resolve("report.json")))
          .contains("\"outcome\": \"INCONCLUSIVE\"")
          .contains("\"code\": \"DATASOURCE_UNAVAILABLE\"")
          .contains("\"detail\": \"Details omitted by report redaction\"");
    }

    @Test
    @DisplayName("incomplete state takes precedence over a policy failure")
    void incompleteStateTakesPrecedenceOverFailure() {
      QueryAuditExtension.AuditRunState runState = new QueryAuditExtension.AuditRunState();
      runState.markPolicyFailed();
      runState.markIncomplete(AuditIncompleteReason.of(IncompleteReasonCode.QUERY_LIMIT_REACHED));

      assertThat(runState.result(List.of()).outcome()).isEqualTo(AuditOutcome.INCONCLUSIVE);
    }

    private void addReports() {
      HtmlReportAggregator aggregator = HtmlReportAggregator.getInstance();
      aggregator.addReport(dummyReport("ClassA", "test1"));
      aggregator.addReport(dummyReport("ClassB", "test2"));
      aggregator.addReport(dummyReport("ClassC", "test3"));
    }

    private Path blockDirectory(Path tempDir) throws IOException {
      Path outputDirectory = tempDir.resolve("not-a-directory");
      Files.writeString(outputDirectory, "occupied");
      return outputDirectory;
    }

    @Test
    @DisplayName("close() does nothing when no reports accumulated")
    void closeWithNoReports_doesNothing(@TempDir Path tempDir) {
      QueryAuditExtension extension = new QueryAuditExtension();
      Path outputDirectory = tempDir.resolve("empty");
      QueryAuditExtension.ReportFinalizer finalizer =
          new QueryAuditExtension.ReportFinalizer(extension, outputDirectory, ReportFormat.HTML);

      finalizer.close();

      assertThat(HtmlReportAggregator.getInstance().getReports()).isEmpty();
      assertThat(outputDirectory).doesNotExist();
    }
  }

  @Nested
  @DisplayName("Nested test classes skip afterAll report logic")
  class NestedClassHandling {

    // A real inner class (has enclosing class)
    class InnerTestClass {}

    @Test
    @SuppressWarnings("unchecked")
    @DisplayName("afterAll returns early for @Nested inner classes")
    void nestedClassSkipsReportFinalization() {
      ExtensionContext.Store rootStore = createRootStore();
      ExtensionContext root = mock(ExtensionContext.class);
      when(root.getStore(NAMESPACE)).thenReturn(rootStore);

      QueryAuditExtension extension = new QueryAuditExtension();

      ExtensionContext ctx = mock(ExtensionContext.class);
      when(ctx.getRequiredTestClass()).thenReturn((Class) InnerTestClass.class);

      HtmlReportAggregator.getInstance().addReport(dummyReport("Outer", "test1"));

      extension.afterAll(ctx);

      // Root store should NOT have been accessed — no finalizer registered
      verify(ctx, never()).getRoot();
    }
  }
}
