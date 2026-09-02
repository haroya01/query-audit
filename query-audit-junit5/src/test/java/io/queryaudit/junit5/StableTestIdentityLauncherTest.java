package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectUniqueId;
import static org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder.request;

import io.queryaudit.junit5.IdentityCaptureExtension.CapturedIdentity;
import io.queryaudit.junit5.identity.RenamableFixture;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherFactory;

class StableTestIdentityLauncherTest {

  @AfterEach
  void clearCapture() {
    IdentityCaptureExtension.reset();
  }

  @Test
  void uniqueIdsDistinguishPackagesOverloadsNestedTestsAndInvocations() {
    execute(
        request()
            .selectors(
                selectClass(io.queryaudit.junit5.identity.alpha.SameNameFixture.class),
                selectClass(io.queryaudit.junit5.identity.beta.SameNameFixture.class))
            .build());

    List<CapturedIdentity> captured = IdentityCaptureExtension.captured();

    assertThat(captured).hasSize(5);
    assertThat(captured).extracting(CapturedIdentity::testId).doesNotHaveDuplicates();
    assertThat(captured).extracting(CapturedIdentity::displayName).containsOnly("duplicate");
    assertThat(captured)
        .extracting(CapturedIdentity::testId)
        .anyMatch(id -> id.contains("[class:io.queryaudit.junit5.identity.alpha.SameNameFixture]"))
        .anyMatch(id -> id.contains("[class:io.queryaudit.junit5.identity.beta.SameNameFixture]"))
        .anyMatch(id -> id.contains("[method:work()]"))
        .anyMatch(id -> id.contains("[test-template:work(java.lang.String)]"))
        .anyMatch(id -> id.contains("[nested-class:NestedGroup]/[method:work()]"))
        .anyMatch(id -> id.endsWith("[test-template-invocation:#1]"))
        .anyMatch(id -> id.endsWith("[test-template-invocation:#2]"));
    assertThat(captured).extracting(CapturedIdentity::selectorType).containsOnly("junit-unique-id");
    assertThat(captured)
        .allSatisfy(identity -> assertThat(identity.selectorValue()).isEqualTo(identity.testId()));
  }

  @Test
  void selectorRerunsOneParameterizedInvocation() {
    execute(
        request()
            .selectors(selectClass(io.queryaudit.junit5.identity.alpha.SameNameFixture.class))
            .build());
    String secondInvocationId =
        IdentityCaptureExtension.captured().stream()
            .map(CapturedIdentity::testId)
            .filter(id -> id.endsWith("[test-template-invocation:#2]"))
            .findFirst()
            .orElseThrow();

    IdentityCaptureExtension.reset();
    execute(request().selectors(selectUniqueId(secondInvocationId)).build());

    assertThat(IdentityCaptureExtension.captured())
        .singleElement()
        .extracting(CapturedIdentity::testId)
        .isEqualTo(secondInvocationId);
  }

  @Test
  void displayNameChangesDoNotChangeTheUniqueId() {
    IdentityCaptureExtension.MutableDisplayNames.usePrefix("first: ");
    execute(request().selectors(selectClass(RenamableFixture.class)).build());
    CapturedIdentity first = IdentityCaptureExtension.captured().get(0);

    IdentityCaptureExtension.reset();
    IdentityCaptureExtension.MutableDisplayNames.usePrefix("renamed: ");
    execute(request().selectors(selectClass(RenamableFixture.class)).build());
    CapturedIdentity renamed = IdentityCaptureExtension.captured().get(0);

    assertThat(first.displayName()).isEqualTo("first: stableMethod");
    assertThat(renamed.displayName()).isEqualTo("renamed: stableMethod");
    assertThat(renamed.testId()).isEqualTo(first.testId());
  }

  private static void execute(LauncherDiscoveryRequest request) {
    Launcher launcher = LauncherFactory.create();
    launcher.execute(request);
  }
}
