package io.queryaudit.junit5.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.junit5.BooleanOverride;
import io.queryaudit.junit5.QueryAudit;
import io.queryaudit.junit5.integration.entity.Member;
import io.queryaudit.junit5.integration.entity.Team;
import io.queryaudit.junit5.integration.repository.MemberRepository;
import io.queryaudit.junit5.integration.repository.TeamRepository;
import java.nio.file.Path;
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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
class BatchFetchNPlusOneLauncherTest {
  private static final String ENABLED = "queryaudit.test.batchFetchNPlusOne";

  @Test
  void aBatchedFetchPassesWhileTheSameUnbatchedLoopFails(@TempDir Path output) {
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
                  .selectors(selectClass(BatchedFixture.class), selectClass(UnbatchedFixture.class))
                  .configurationParameter("junit.jupiter.execution.parallel.enabled", "false")
                  .build(),
              listener);

      assertThat(listener.getSummary().getTestsFoundCount()).isEqualTo(2);
      assertThat(listener.getSummary().getTestsSucceededCount()).isEqualTo(1);
      assertThat(listener.getSummary().getFailures())
          .singleElement()
          .satisfies(
              failure -> {
                assertThat(failure.getTestIdentifier().getUniqueId()).contains("UnbatchedFixture");
                assertThat(failure.getException()).isInstanceOf(AssertionError.class);
              });
    } finally {
      saved.forEach(
          (key, value) -> {
            if (value == null) System.clearProperty(key);
            else System.setProperty(key, value);
          });
      HtmlReportAggregator.getInstance().reset();
    }
  }

  abstract static class TeamsWithMembers {
    @Autowired TeamRepository teamRepository;
    @Autowired MemberRepository memberRepository;
    @Autowired PlatformTransactionManager transactionManager;

    @BeforeEach
    void seed() {
      for (int i = 0; i < 5; i++) {
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
}
