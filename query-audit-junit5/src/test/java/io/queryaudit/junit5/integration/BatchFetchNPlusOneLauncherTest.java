package io.queryaudit.junit5.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.junit5.BooleanOverride;
import io.queryaudit.junit5.EnableQueryInspector;
import io.queryaudit.junit5.QueryAudit;
import io.queryaudit.junit5.integration.entity.Member;
import io.queryaudit.junit5.integration.entity.Team;
import io.queryaudit.junit5.integration.repository.MemberRepository;
import io.queryaudit.junit5.integration.repository.TeamRepository;
import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.junit.platform.launcher.listeners.TestExecutionSummary;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class BatchFetchNPlusOneLauncherTest {
  private static final String ENABLED = "queryaudit.test.batchFetchNPlusOne";

  @Test
  void aBatchedFetchPassesWhileTheSameUnbatchedLoopFails(@TempDir Path output) throws IOException {
    TestExecutionSummary summary = run(output, BatchedFixture.class, UnbatchedFixture.class);

    assertThat(summary.getTestsFoundCount()).isEqualTo(2);
    assertThat(summary.getTestsSucceededCount()).isEqualTo(1);
    assertThat(summary.getFailures())
        .singleElement()
        .satisfies(
            failure -> {
              assertThat(failure.getTestIdentifier().getUniqueId()).contains("UnbatchedFixture");
              assertThat(failure.getException()).isInstanceOf(AssertionError.class);
            });
    assertThat(lazyLoadInfo(output, "$BatchedFixture")).isEmpty();
    assertThat(lazyLoadInfo(output, "$UnbatchedFixture")).isNotEmpty();
  }

  @Test
  void batchFetchedProxiesAddNoLazyLoadInfo(@TempDir Path output) throws IOException {
    TestExecutionSummary summary =
        run(output, ProxyBatchedFixture.class, ProxyUnbatchedFixture.class);

    assertThat(summary.getTestsSucceededCount()).isEqualTo(2);
    assertThat(lazyLoadInfo(output, "$ProxyBatchedFixture")).isEmpty();
    assertThat(lazyLoadInfo(output, "$ProxyUnbatchedFixture")).isNotEmpty();
  }

  @Test
  void collectionsLoadedInSeveralBatchesAddNoLazyLoadInfo(@TempDir Path output)
      throws IOException {
    TestExecutionSummary summary = run(output, CollectionBatchesFixture.class);

    assertThat(summary.getTestsSucceededCount()).isEqualTo(1);
    assertThat(lazyLoadInfo(output, "$CollectionBatchesFixture")).isEmpty();
  }

  private static TestExecutionSummary run(Path output, Class<?>... fixtures) {
    Map<String, String> saved = new HashMap<>();
    for (String key : List.of(ENABLED, "queryAudit.reportOutputDir", "queryAudit.reportFormat"))
      saved.put(key, System.getProperty(key));
    HtmlReportAggregator.getInstance().reset();
    try {
      System.setProperty(ENABLED, "true");
      System.setProperty("queryAudit.reportOutputDir", output.toString());
      System.setProperty("queryAudit.reportFormat", "json");
      SummaryGeneratingListener listener = new SummaryGeneratingListener();
      LauncherFactory.create()
          .execute(
              LauncherDiscoveryRequestBuilder.request()
                  .selectors(Arrays.stream(fixtures).map(fixture -> selectClass(fixture)).toList())
                  .configurationParameter("junit.jupiter.execution.parallel.enabled", "false")
                  .build(),
              listener);
      return listener.getSummary();
    } finally {
      saved.forEach(
          (key, value) -> {
            if (value == null) System.clearProperty(key);
            else System.setProperty(key, value);
          });
      HtmlReportAggregator.getInstance().reset();
    }
  }

  private static List<String> lazyLoadInfo(Path output, String fixture) throws IOException {
    List<Map<String, Object>> reports =
        JsonPath.read(Files.readString(output.resolve("report.json")), "$.reports");
    List<String> types = new ArrayList<>();
    for (Map<String, Object> report : reports) {
      if (!String.valueOf(report.get("testId")).contains(fixture + "]")) continue;
      for (Object issue : (List<?>) report.get("infoIssues")) {
        String type = String.valueOf(((Map<?, ?>) issue).get("type"));
        if (type.equals(IssueType.N_PLUS_ONE.getCode())) types.add(type);
      }
    }
    return types;
  }

  abstract static class SeededTeams {
    @Autowired TeamRepository teamRepository;
    @Autowired MemberRepository memberRepository;
    @Autowired PlatformTransactionManager transactionManager;

    int teams() {
      return 5;
    }

    @BeforeEach
    void seed() {
      for (int i = 0; i < teams(); i++) {
        Team team = teamRepository.save(new Team("Batch " + i));
        Member member = new Member("Member " + i, "batch" + i + "@test.com", "ACTIVE");
        member.setTeam(team);
        memberRepository.save(member);
      }
    }

    @AfterEach
    void cleanUp() {
      memberRepository.deleteAllInBatch();
      teamRepository.deleteAllInBatch();
    }
  }

  abstract static class TeamsWithMembers extends SeededTeams {
    @Test
    void readsMembersOfEveryTeam() {
      new TransactionTemplate(transactionManager)
          .executeWithoutResult(
              status -> {
                for (Team team : teamRepository.findAll()) {
                  assertThat(team.getMembers()).isNotEmpty();
                }
              });
    }
  }

  @SpringBootTest(
      classes = TestApplication.class,
      properties = "spring.jpa.properties.hibernate.default_batch_fetch_size=10")
  @QueryAudit(failOn = IssueType.N_PLUS_ONE, autoOpenReport = BooleanOverride.FALSE)
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class BatchedFixture extends TeamsWithMembers {}

  @SpringBootTest(classes = TestApplication.class)
  @QueryAudit(failOn = IssueType.N_PLUS_ONE, autoOpenReport = BooleanOverride.FALSE)
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class UnbatchedFixture extends TeamsWithMembers {}

  abstract static class MembersWithTeams extends SeededTeams {
    @Test
    void readsTeamOfEveryMember() {
      new TransactionTemplate(transactionManager)
          .executeWithoutResult(
              status -> {
                for (Member member : memberRepository.findAll()) {
                  assertThat(member.getTeam().getName()).startsWith("Batch");
                }
              });
    }
  }

  @SpringBootTest(
      classes = TestApplication.class,
      properties = "spring.jpa.properties.hibernate.default_batch_fetch_size=10")
  @EnableQueryInspector
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class ProxyBatchedFixture extends MembersWithTeams {}

  @SpringBootTest(classes = TestApplication.class)
  @EnableQueryInspector
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class ProxyUnbatchedFixture extends MembersWithTeams {}

  @SpringBootTest(
      classes = TestApplication.class,
      properties = "spring.jpa.properties.hibernate.default_batch_fetch_size=10")
  @EnableQueryInspector
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class CollectionBatchesFixture extends TeamsWithMembers {
    @Override
    int teams() {
      return 30;
    }
  }
}
