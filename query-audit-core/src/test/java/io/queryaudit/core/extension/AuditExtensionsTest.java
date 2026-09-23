package io.queryaudit.core.extension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import io.queryaudit.core.analyzer.ExplainAnalyzer;
import io.queryaudit.core.analyzer.IndexMetadataProvider;
import io.queryaudit.core.detector.DetectionRule;
import io.queryaudit.core.reporter.delivery.AuditReportSink;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class AuditExtensionsTest {
  private static final DetectionRule FIRST_RULE = (queries, indexes) -> List.of();
  private static final DetectionRule SECOND_RULE = (queries, indexes) -> List.of();

  @Test
  void emptyCatalogHasNoExplicitRegistrations() {
    AuditExtensions extensions = AuditExtensions.empty();

    assertThat(extensions.registrationIds()).isEmpty();
    assertThat(extensions.rules()).isEmpty();
    assertThat(extensions.rulesById()).isEmpty();
    assertThat(extensions.auditRulesById()).isEmpty();
    assertThat(extensions.indexMetadataProviders()).isEmpty();
    assertThat(extensions.indexMetadataProvidersById()).isEmpty();
    assertThat(extensions.explainAnalyzers()).isEmpty();
    assertThat(extensions.explainAnalyzersById()).isEmpty();
    assertThat(extensions.auditRules()).isEmpty();
    assertThat(extensions.reportSinks()).isEmpty();
  }

  @Test
  void retainsRegistrationOrderWithinEachExtensionType() {
    IndexMetadataProvider firstIndex = mock(IndexMetadataProvider.class);
    IndexMetadataProvider secondIndex = mock(IndexMetadataProvider.class);
    ExplainAnalyzer firstExplain = mock(ExplainAnalyzer.class);
    ExplainAnalyzer secondExplain = mock(ExplainAnalyzer.class);

    AuditExtensions extensions =
        AuditExtensions.builder()
            .rule("first-rule", FIRST_RULE)
            .indexMetadataProvider("first-index", firstIndex)
            .explainAnalyzer("first-explain", firstExplain)
            .rule("second-rule", SECOND_RULE)
            .indexMetadataProvider("second-index", secondIndex)
            .explainAnalyzer("second-explain", secondExplain)
            .build();

    assertThat(extensions.rules()).containsExactly(FIRST_RULE, SECOND_RULE);
    assertThat(extensions.registrationIds())
        .containsExactly(
            "first-rule",
            "first-index",
            "first-explain",
            "second-rule",
            "second-index",
            "second-explain");
    assertThat(extensions.indexMetadataProviders()).containsExactly(firstIndex, secondIndex);
    assertThat(extensions.explainAnalyzers()).containsExactly(firstExplain, secondExplain);
    assertThatThrownBy(() -> extensions.rules().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> extensions.indexMetadataProviders().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> extensions.explainAnalyzers().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> extensions.registrationIds().add("unexpected"))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void builtCatalogDoesNotChangeWhenTheBuilderIsReused() {
    AuditExtensions.Builder builder = AuditExtensions.builder().rule("first", FIRST_RULE);
    AuditExtensions first = builder.build();
    IndexMetadataProvider index = mock(IndexMetadataProvider.class);
    ExplainAnalyzer explain = mock(ExplainAnalyzer.class);
    AuditExtensions second =
        builder
            .rule("second", SECOND_RULE)
            .indexMetadataProvider("index", index)
            .explainAnalyzer("explain", explain)
            .build();

    assertThat(first.rules()).containsExactly(FIRST_RULE);
    assertThat(first.registrationIds()).containsExactly("first");
    assertThat(first.indexMetadataProviders()).isEmpty();
    assertThat(first.explainAnalyzers()).isEmpty();
    assertThat(second.rules()).containsExactly(FIRST_RULE, SECOND_RULE);
    assertThat(second.registrationIds()).containsExactly("first", "second", "index", "explain");
    assertThat(second.indexMetadataProviders()).containsExactly(index);
    assertThat(second.explainAnalyzers()).containsExactly(explain);
  }

  @Test
  void explainIdsPreserveImmutableRegistrationOrderAndRepeatedInstances() {
    ExplainAnalyzer provider = mock(ExplainAnalyzer.class);
    var builder = AuditExtensions.builder().explainAnalyzer("first", provider);
    var first = builder.build();
    var second = builder.explainAnalyzer("second", provider).build();
    assertThat(first.explainAnalyzersById()).containsOnlyKeys("first");
    assertThat(second.explainAnalyzersById().keySet()).containsExactly("first", "second");
    assertThat(second.explainAnalyzers()).containsExactly(provider, provider);
    assertThatThrownBy(() -> first.explainAnalyzersById().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void duplicateIdsFailEvenForTheSameObjectOrAcrossExtensionTypes() {
    AuditExtensions.Builder builder = AuditExtensions.builder().rule("company-rule", FIRST_RULE);

    assertThatThrownBy(() -> builder.rule("company-rule", FIRST_RULE))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Duplicate extension registration ID: company-rule");
    assertThatThrownBy(
            () -> builder.indexMetadataProvider("company-rule", mock(IndexMetadataProvider.class)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> builder.explainAnalyzer("company-rule", mock(ExplainAnalyzer.class)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(builder.build().rules()).containsExactly(FIRST_RULE);
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "\t\n"})
  void registrationIdsMustBeNonblank(String id) {
    AuditExtensions.Builder builder = AuditExtensions.builder();

    assertThatThrownBy(() -> builder.rule(id, FIRST_RULE))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> builder.indexMetadataProvider(id, mock(IndexMetadataProvider.class)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> builder.explainAnalyzer(id, mock(ExplainAnalyzer.class)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void nullImplementationsFailWithoutReservingTheRegistrationId() {
    AuditExtensions.Builder builder = AuditExtensions.builder();

    assertThatThrownBy(() -> builder.rule("rule", null)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> builder.indexMetadataProvider("index", null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> builder.explainAnalyzer("explain", null))
        .isInstanceOf(NullPointerException.class);

    assertThat(builder.rule("rule", FIRST_RULE).build().rules()).containsExactly(FIRST_RULE);
  }

  @Test
  void differentIdsRemainExplicitRegistrationsEvenForTheSameObject() {
    AuditExtensions extensions =
        AuditExtensions.builder().rule("first", FIRST_RULE).rule("second", FIRST_RULE).build();

    assertThat(extensions.rules()).containsExactly(FIRST_RULE, FIRST_RULE);
    assertThat(extensions.rulesById().keySet()).containsExactly("first", "second");
    assertThat(extensions.rulesById().values()).containsExactly(FIRST_RULE, FIRST_RULE);
    assertThatThrownBy(() -> extensions.rulesById().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void openRegistrationIdsAreImmutableOrderedSnapshotsIndependentOfBuilderReuse() {
    AuditRule rule = mock(AuditRule.class);
    var builder = AuditExtensions.builder().auditRule("first", rule);
    var first = builder.build();
    var second = builder.auditRule("second", rule).build();
    assertThat(first.auditRulesById()).containsOnlyKeys("first");
    assertThat(second.auditRulesById().keySet()).containsExactly("first", "second");
    assertThat(second.auditRulesById().values()).containsExactly(rule, rule);
    assertThat(second.auditRules()).containsExactly(rule, rule);
    assertThatThrownBy(() -> first.auditRulesById().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void metadataRegistrationIdsRetainOrderIdentityAndSnapshotIsolation() {
    IndexMetadataProvider provider = mock(IndexMetadataProvider.class);
    AuditExtensions.Builder builder =
        AuditExtensions.builder()
            .indexMetadataProvider("index:primary", provider)
            .rule("unrelated", FIRST_RULE);
    AuditExtensions first = builder.build();
    AuditExtensions second = builder.indexMetadataProvider("index:secondary", provider).build();

    assertThat(first.indexMetadataProvidersById()).containsOnlyKeys("index:primary");
    assertThat(second.indexMetadataProvidersById().keySet())
        .containsExactly("index:primary", "index:secondary");
    assertThat(second.indexMetadataProvidersById().values()).containsExactly(provider, provider);
    assertThat(second.indexMetadataProviders()).containsExactly(provider, provider);
    assertThatThrownBy(() -> first.indexMetadataProvidersById().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void openRulesAndSinksShareCatalogIdsAndSnapshotTheirOrder() {
    AuditRule firstRule = mock(AuditRule.class);
    AuditRule secondRule = mock(AuditRule.class);
    AuditReportSink firstSink = mock(AuditReportSink.class);
    AuditReportSink secondSink = mock(AuditReportSink.class);
    AuditExtensions.Builder builder =
        AuditExtensions.builder()
            .auditRule("first-rule", firstRule)
            .reportSink("first-sink", true, firstSink);
    AuditExtensions first = builder.build();
    AuditExtensions second =
        builder
            .auditRule("second-rule", secondRule)
            .reportSink("second-sink", false, secondSink)
            .build();
    assertThat(first.auditRules()).containsExactly(firstRule);
    assertThat(first.reportSinks()).hasSize(1);
    assertThat(second.auditRules()).containsExactly(firstRule, secondRule);
    assertThat(second.reportSinks())
        .extracting(registration -> registration.id())
        .containsExactly("first-sink", "second-sink");
    assertThat(second.reportSinks().get(0).required()).isTrue();
    assertThat(second.reportSinks().get(0).sink()).isSameAs(firstSink);
    assertThat(second.registrationIds())
        .containsExactly("first-rule", "first-sink", "second-rule", "second-sink");
    assertThatThrownBy(() -> first.auditRules().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> first.reportSinks().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> builder.rule("first-rule", FIRST_RULE))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> builder.reportSink("first-rule", false, firstSink))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> builder.auditRule("first-sink", firstRule))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> builder.auditRule("unused-rule", null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> builder.reportSink("unused-sink", false, null))
        .isInstanceOf(NullPointerException.class);
    assertThat(
            builder
                .auditRule("unused-rule", firstRule)
                .reportSink("unused-sink", false, firstSink)
                .build()
                .auditRules())
        .hasSize(3);
  }
}
