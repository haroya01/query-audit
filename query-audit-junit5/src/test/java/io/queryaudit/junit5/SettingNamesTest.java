package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.ReportFormat;
import io.queryaudit.core.config.ReportRedaction;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class SettingNamesTest {
  private static final List<String> KEYS =
      List.of(
          "queryAudit.report.format",
          "queryAudit.reportFormat",
          "queryGuard.reportFormat",
          "queryAudit.report.redaction",
          "queryAudit.reportRedaction",
          "queryAudit.report.outputDir",
          "queryAudit.reportOutputDir",
          "queryAudit.contracts.path",
          "queryAudit.contractsPath",
          "queryGuard.contractsPath",
          "queryAudit.counts.path",
          "queryAudit.countBaselinePath",
          "queryGuard.countBaselinePath",
          "queryAudit.counts.record",
          "queryAudit.updateBaseline",
          "queryGuard.updateBaseline",
          "queryAudit.coverage.manifest",
          "queryAudit.coverageManifest",
          "queryAudit.failOnDetection",
          "queryAudit.baselinePath");
  private final Map<String, String> saved = new HashMap<>();

  @BeforeEach
  void clear() {
    KEYS.forEach(
        key -> {
          saved.put(key, System.getProperty(key));
          System.clearProperty(key);
        });
  }

  @AfterEach
  void restore() {
    saved.forEach(
        (key, value) -> {
          if (value == null) System.clearProperty(key);
          else System.setProperty(key, value);
        });
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"queryAudit.report.format", "queryAudit.reportFormat", "queryGuard.reportFormat"})
  void everyReportFormatNameSelectsTheFormat(String key) throws Exception {
    System.setProperty(key, "json");

    assertThat(config().getReportFormat()).isEqualTo(ReportFormat.JSON);
  }

  @ParameterizedTest
  @ValueSource(strings = {"queryAudit.report.redaction", "queryAudit.reportRedaction"})
  void everyRedactionNameSelectsTheRedaction(String key) throws Exception {
    System.setProperty(key, "full");

    assertThat(config().getReportRedaction()).isEqualTo(ReportRedaction.FULL);
  }

  @ParameterizedTest
  @ValueSource(strings = {"queryAudit.report.outputDir", "queryAudit.reportOutputDir"})
  void everyOutputDirectoryNameSelectsTheDirectory(String key) {
    System.setProperty(key, "build/custom-reports");

    assertThat(AuditSettingsResolver.resolveReportOutputDirectory(QueryAuditConfig.defaults()))
        .isEqualTo(Path.of("build/custom-reports").toAbsolutePath().normalize());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "queryAudit.contracts.path",
        "queryAudit.contractsPath",
        "queryGuard.contractsPath"
      })
  void everyContractsPathNameSelectsTheContracts(String key) {
    System.setProperty(key, "config/contracts");

    assertThat(AuditSettingsResolver.resolveContractsPath(null))
        .isEqualTo(Path.of("config/contracts"));
  }

  @Test
  void contractsDefaultToTheSharedFile() {
    assertThat(AuditSettingsResolver.resolveContractsPath(null))
        .isEqualTo(Path.of(".query-audit-contracts"));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "queryAudit.counts.path",
        "queryAudit.countBaselinePath",
        "queryGuard.countBaselinePath"
      })
  void everyCountsPathNameSelectsTheCountBaseline(String key) {
    System.setProperty(key, "config/counts");

    assertThat(new AuditSettingsResolver(context -> null).resolveCountBaselinePath(null))
        .isEqualTo(Path.of("config/counts"));
  }

  @Test
  void countsDefaultToTheCountFileWhenNothingIsSet() {
    assertThat(new AuditSettingsResolver(context -> null).resolveCountBaselinePath(null))
        .isEqualTo(Path.of(".query-audit-counts"));
    assertThat(AuditSettingsResolver.isCountRecordMode()).isFalse();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "queryAudit.counts.record",
        "queryAudit.updateBaseline",
        "queryGuard.updateBaseline"
      })
  void everyCountRecordNameTurnsRecordingOn(String key) {
    System.setProperty(key, "true");

    assertThat(AuditSettingsResolver.isCountRecordMode()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(strings = {"queryAudit.coverage.manifest", "queryAudit.coverageManifest"})
  void everyCoverageManifestNameSelectsTheManifest(String key) {
    System.setProperty(key, "config/expected-tests");

    assertThat(AuditCoverageManifest.configuredPath()).isEqualTo("config/expected-tests");
  }

  @Test
  void failOnDetectionAndBaselinePathFollowTheSameNamingRule() throws Exception {
    System.setProperty("queryAudit.failOnDetection", "false");
    System.setProperty("queryAudit.baselinePath", "config/acknowledged");

    QueryAuditConfig config = config();

    assertThat(config.isFailOnDetection()).isFalse();
    assertThat(config.getBaselinePath()).isEqualTo("config/acknowledged");
  }

  private static QueryAuditConfig config() throws Exception {
    ExtensionContext context = mock(ExtensionContext.class);
    doReturn(Fixture.class).when(context).getRequiredTestClass();
    when(context.getTestMethod())
        .thenReturn(Optional.of(Fixture.class.getDeclaredMethod("method")));
    return new AuditSettingsResolver(ignored -> null).buildConfig(context, null);
  }

  static class Fixture {
    void method() {}
  }
}
