package io.queryaudit.junit5.integration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import com.jayway.jsonpath.JsonPath;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.junit5.BooleanOverride;
import io.queryaudit.junit5.QueryAudit;
import io.queryaudit.junit5.integration.entity.Member;
import io.queryaudit.junit5.integration.entity.Team;
import io.queryaudit.junit5.integration.repository.MemberRepository;
import io.queryaudit.junit5.integration.repository.TeamRepository;
import jakarta.persistence.EntityManager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.domain.PageRequest;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@ResourceLock(Resources.SYSTEM_PROPERTIES)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CallSiteNPlusOneScenarioTest {
  private static final String ENABLED = "queryaudit.test.callSiteScenarios";

  private final Map<String, Throwable> failures = new HashMap<>();
  private final Map<String, List<String>> confirmed = new HashMap<>();
  private final Map<String, List<String>> stacks = new HashMap<>();

  @BeforeAll
  void runScenarios(@TempDir Path output) throws Exception {
    Map<String, String> saved = new HashMap<>();
    for (String key : List.of(ENABLED, "queryAudit.reportOutputDir", "queryAudit.reportFormat"))
      saved.put(key, System.getProperty(key));
    HtmlReportAggregator.getInstance().reset();
    try {
      System.setProperty(ENABLED, "true");
      System.setProperty("queryAudit.reportOutputDir", output.toString());
      System.setProperty("queryAudit.reportFormat", "json");
      LauncherFactory.create()
          .execute(
              LauncherDiscoveryRequestBuilder.request()
                  .selectors(selectClass(Scenarios.class))
                  .configurationParameter("junit.jupiter.execution.parallel.enabled", "false")
                  .build(),
              new TestExecutionListener() {
                @Override
                public void executionFinished(TestIdentifier test, TestExecutionResult result) {
                  if (test.isTest() && result.getThrowable().isPresent()) {
                    failures.put(test.getDisplayName(), result.getThrowable().get());
                  }
                }
              });
    } finally {
      saved.forEach(
          (key, value) -> {
            if (value == null) System.clearProperty(key);
            else System.setProperty(key, value);
          });
      HtmlReportAggregator.getInstance().reset();
    }
    List<Map<String, Object>> reports =
        JsonPath.read(Files.readString(output.resolve("report.json")), "$.reports");
    for (Map<String, Object> report : reports) {
      String method = report.get("testName") + "";
      List<String> types = new ArrayList<>();
      for (Object issue : (List<?>) report.get("confirmedIssues")) {
        Map<?, ?> finding = (Map<?, ?>) issue;
        types.add(finding.get("type") + "");
      }
      confirmed.put(method, types);
      List<String> traces = new ArrayList<>();
      for (Object query : (List<?>) report.get("queries")) {
        traces.add(((Map<?, ?>) query).get("stackTrace") + "");
      }
      stacks.put(method, traces);
    }
  }

  @Test
  void aLazyManyToOneReadInALoopIsAnNPlusOne() {
    assertNPlusOne("lazyManyToOneInLoop()");
  }

  @Test
  void aLazyProxyIsNotReportedAsTheCallSite() {
    assertThat(failures.get("lazyManyToOneInLoop()"))
        .hasMessageContaining(
            "Call stack:\n      at " + Scenarios.class.getName() + ".lambda$lazyManyToOneInLoop$")
        .hasMessageNotContaining("HibernateProxy");
    assertThat(stacks.get("lazyManyToOneInLoop()"))
        .isNotEmpty()
        .noneMatch(stack -> stack.contains("HibernateProxy"));
  }

  @Test
  void aLazyCollectionReadInALoopIsAnNPlusOne() {
    assertNPlusOne("lazyCollectionInLoop()");
  }

  @Test
  void aRepositoryCallInALoopIsReportedThroughSpringDataProxies() {
    assertNPlusOne("findByIdInLoop()");
    assertThat(failures.get("findByIdInLoop()"))
        .hasMessageContaining(
            "Call stack:\n      at " + Scenarios.class.getName() + ".findByIdInLoop:")
        .hasMessageNotContaining("jdk.proxy")
        .hasMessageNotContaining("at org.springframework");
  }

  @Test
  void aStreamThatLoadsEachElementIsAnNPlusOne() {
    assertNPlusOne("findByIdInStream()");
  }

  @Test
  void aTransactionalServiceLoopNamesTheServiceNotItsProxy() {
    assertNPlusOne("transactionalServiceLoop()");
    assertThat(failures.get("transactionalServiceLoop()"))
        .hasMessageContaining(
            "Call stack:\n      at " + TeamQueryService.class.getName() + ".memberCounts:")
        .hasMessageNotContaining("CGLIB");
  }

  @Test
  void fetchingTheAssociationOnceIsNotAnNPlusOne() {
    assertClean("joinFetch()");
    assertClean("findAllById()");
  }

  @Test
  void twoRepetitionsStayBelowTheDefaultThreshold() {
    assertClean("twoIterations()");
  }

  @Test
  void theSameQueryFromDifferentLinesIsNotAnNPlusOne() {
    assertClean("distinctCallSites()");
  }

  @Test
  void deletingDetachedEntitiesOneByOneIsAnNPlusOne() {
    assertNPlusOne("deleteAllDetached()");
  }

  @Test
  void aPagingLoopIsNotAnNPlusOne() {
    assertClean("pagedLoop()");
  }

  @Test
  void loadingTheSameRowRepeatedlyIsNotAnNPlusOne() {
    assertClean("sameRowFiveTimes()");
  }

  private void assertNPlusOne(String method) {
    assertThat(confirmed.get(method)).as(method).containsExactly("n-plus-one");
    assertThat(failures.get(method)).as(method).hasMessageContaining("N+1 Query detected");
  }

  private void assertClean(String method) {
    assertThat(confirmed.get(method)).as(method).isEmpty();
    assertThat(failures).as(method).doesNotContainKey(method);
  }

  @SpringBootTest(classes = TestApplication.class)
  @QueryAudit(autoOpenReport = BooleanOverride.FALSE)
  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  static class Scenarios {
    @Autowired TeamRepository teamRepository;
    @Autowired MemberRepository memberRepository;
    @Autowired TeamQueryService teamQueryService;
    @Autowired PlatformTransactionManager transactionManager;
    @Autowired EntityManager entityManager;
    List<Long> teamIds = new ArrayList<>();

    @BeforeEach
    void seed() {
      teamIds.clear();
      for (int i = 0; i < 5; i++) {
        Team team = teamRepository.save(new Team("Scenario " + i));
        teamIds.add(team.getId());
        for (int j = 0; j < 3; j++) {
          Member member = new Member("Member " + i + j, "scenario" + i + j + "@test.com", "ACTIVE");
          member.setTeam(team);
          memberRepository.save(member);
        }
      }
    }

    @AfterEach
    void cleanUp() {
      memberRepository.deleteAllInBatch();
      teamRepository.deleteAllInBatch();
    }

    @Test
    void lazyManyToOneInLoop() {
      inTransaction(
          () -> {
            for (Member member : memberRepository.findAll()) {
              member.getTeam().getName();
            }
          });
    }

    @Test
    void lazyCollectionInLoop() {
      inTransaction(() -> teamRepository.findAll().forEach(team -> team.getMembers().size()));
    }

    @Test
    void findByIdInLoop() {
      for (Long id : teamIds) {
        teamRepository.findById(id);
      }
    }

    @Test
    void findByIdInStream() {
      teamIds.stream().map(teamRepository::findById).toList();
    }

    @Test
    void transactionalServiceLoop() {
      teamQueryService.memberCounts();
    }

    @Test
    void joinFetch() {
      inTransaction(
          () ->
              entityManager
                  .createQuery("select distinct t from Team t join fetch t.members", Team.class)
                  .getResultList()
                  .forEach(team -> team.getMembers().size()));
    }

    @Test
    void findAllById() {
      teamRepository.findAllById(teamIds);
    }

    @Test
    void twoIterations() {
      for (Long id : teamIds.subList(0, 2)) {
        teamRepository.findById(id);
      }
    }

    @Test
    void distinctCallSites() {
      teamRepository.findById(teamIds.get(0));
      teamRepository.findById(teamIds.get(1));
      teamRepository.findById(teamIds.get(2));
    }

    @Test
    void pagedLoop() {
      for (int page = 0; page < 5; page++) {
        memberRepository.findAll(PageRequest.of(page, 3));
      }
    }

    @Test
    void sameRowFiveTimes() {
      for (int i = 0; i < 5; i++) {
        teamRepository.findById(teamIds.get(0));
      }
    }

    @Test
    void deleteAllDetached() {
      memberRepository.deleteAll(memberRepository.findAll());
    }

    private void inTransaction(Runnable work) {
      new TransactionTemplate(transactionManager).executeWithoutResult(status -> work.run());
    }
  }
}
