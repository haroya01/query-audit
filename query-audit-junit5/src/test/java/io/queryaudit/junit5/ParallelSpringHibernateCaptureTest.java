package io.queryaudit.junit5;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.platform.engine.discovery.DiscoverySelectors.selectClass;

import com.jayway.jsonpath.JsonPath;
import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.config.ReportFormat;
import io.queryaudit.core.config.ReportRedaction;
import io.queryaudit.core.extension.AuditExtensions;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import io.queryaudit.core.reporter.HtmlReportAggregator;
import io.queryaudit.core.reporter.delivery.PublishedAuditRun;
import io.queryaudit.junit5.integration.entity.Member;
import io.queryaudit.junit5.integration.entity.Team;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import net.ttddyy.dsproxy.support.ProxyDataSource;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;
import org.h2.jdbcx.JdbcDataSource;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.event.service.spi.EventListenerRegistry;
import org.hibernate.event.spi.EventType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.platform.engine.TestExecutionResult;
import org.junit.platform.launcher.TestExecutionListener;
import org.junit.platform.launcher.TestIdentifier;
import org.junit.platform.launcher.core.LauncherConfig;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestContextManager;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/** Actual Spring/JPA ownership: one context and SessionFactory, two concurrent transactions. */
@Isolated("Owns nested Launcher properties and its dedicated Spring context")
class ParallelSpringHibernateCaptureTest {
  private static final String ENABLED = "queryaudit.test.parallelHibernateFixture";
  private static final List<String> PROPERTIES =
      List.of(
          ENABLED,
          "queryAudit.reportFormat",
          "queryAudit.reportOutputDir",
          "queryAudit.reportRedaction",
          "queryAudit.autoOpenReport",
          "queryAudit.coverageManifest");
  private static final List<PublishedAuditRun> PUBLICATIONS = new CopyOnWriteArrayList<>();
  private static final AuditExtensions EXTENSIONS =
      AuditExtensions.builder()
          .reportSink("test:parallel-hibernate", true, PUBLICATIONS::add)
          .build();
  private static Scenario scenario;
  private final Map<String, String> originalProperties = new LinkedHashMap<>();

  @BeforeEach
  void prepare() {
    for (String key : PROPERTIES) {
      originalProperties.put(key, System.getProperty(key));
      System.clearProperty(key);
    }
    System.setProperty(ENABLED, "true");
    System.setProperty("queryAudit.autoOpenReport", "false");
    HtmlReportAggregator.getInstance().reset();
    QueryAuditDataSourceStore.clear();
    PUBLICATIONS.clear();
  }

  @AfterEach
  void restore() {
    try {
      // Closing either class while the other is running would invalidate the shared context.
      // Dirty only this dedicated context after the entire nested Launcher has returned.
      for (Class<?> fixture : List.of(FirstFixture.class, SecondFixture.class)) {
        new TestContextManager(fixture)
            .getTestContext()
            .markApplicationContextDirty(DirtiesContext.HierarchyMode.EXHAUSTIVE);
      }
    } finally {
      originalProperties.forEach(
          (key, value) -> {
            if (value == null) System.clearProperty(key);
            else System.setProperty(key, value);
          });
      scenario = null;
      HtmlReportAggregator.getInstance().reset();
      QueryAuditDataSourceStore.clear();
      PUBLICATIONS.clear();
    }
  }

  @Test
  void sharedSpringContextSeparatesSqlConnectionsAndLazyOwners(@TempDir Path output)
      throws Exception {
    verifyConcurrentRun(output, false);
  }

  @Test
  void classCleanupLeavesItsSiblingsHibernateAndJdbcCaptureActive(@TempDir Path output)
      throws Exception {
    verifyConcurrentRun(output, true);
  }

  private void verifyConcurrentRun(Path output, boolean staggered) throws Exception {
    scenario = new Scenario(output, staggered);
    var request =
        LauncherDiscoveryRequestBuilder.request()
            .selectors(selectClass(FirstFixture.class), selectClass(SecondFixture.class))
            .configurationParameter("junit.jupiter.extensions.autodetection.enabled", "false")
            .configurationParameter("junit.jupiter.execution.parallel.enabled", "true")
            .configurationParameter("junit.jupiter.execution.parallel.mode.default", "concurrent")
            .configurationParameter(
                "junit.jupiter.execution.parallel.mode.classes.default", "concurrent")
            .configurationParameter("junit.jupiter.execution.parallel.config.strategy", "fixed")
            .configurationParameter(
                "junit.jupiter.execution.parallel.config.fixed.parallelism", "2")
            .build();
    SummaryGeneratingListener summary = new SummaryGeneratingListener();
    var launcher =
        LauncherFactory.create(
            LauncherConfig.builder().enableTestExecutionListenerAutoRegistration(false).build());
    launcher.execute(
        request,
        summary,
        new TestExecutionListener() {
          @Override
          public void executionFinished(TestIdentifier test, TestExecutionResult result) {
            if (test.getUniqueId().endsWith("[class:" + FirstFixture.class.getName() + "]")) {
              scenario.firstClassFinished.countDown();
            }
          }
        });

    assertThat(summary.getSummary().getFailures()).isEmpty();
    assertThat(summary.getSummary().getTestsSucceededCount()).isEqualTo(2);
    assertThat(scenario.threads).hasSize(2);
    assertThat(scenario.contexts)
        .as("both classes must really share Spring's cached context")
        .hasSize(1);
    assertThat(scenario.factories).hasSize(1).containsExactly(scenario.factory);
    assertThat(scenario.factoryBuilds).hasValue(1);
    assertThat(scenario.entityManagers)
        .as("transactions must not share a non-thread-safe EntityManager")
        .hasSize(2);

    List<QueryAuditReport> reports = HtmlReportAggregator.getInstance().getReports();
    assertThat(reports).hasSize(2);
    Set<String> connectionIds = ConcurrentHashMap.newKeySet();
    for (QueryAuditReport report : reports) {
      boolean first = report.getTestId().contains("[class:" + FirstFixture.class.getName() + "]");
      int owners = first ? 3 : 4;
      String marker = first ? "SELECT 3001" : "SELECT 4001";
      String siblingMarker = first ? "SELECT 4001" : "SELECT 3001";
      assertThat(report.getAllQueries())
          .as(report.getTestId())
          .extracting(QueryRecord::sql)
          .contains(marker)
          .doesNotContain(siblingMarker)
          .hasSize(owners + 2);
      assertThat(report.getTotalQueryCount()).isEqualTo(owners + 2);
      assertThat(
              report.getConfirmedIssues().stream()
                  .filter(issue -> issue.type() == IssueType.N_PLUS_ONE))
          .singleElement()
          .satisfies(
              issue ->
                  assertThat(issue.detail())
                      .contains(
                          "initialized "
                              + owners
                              + " times for "
                              + owners
                              + " different entities"));
      List<Issue> connectionIssues =
          report.getInfoIssues().stream()
              .filter(issue -> issue.type() == IssueType.CONNECTION_HELD_IDLE)
              .toList();
      assertThat(connectionIssues)
          .as("one physical transaction checkout belongs to this test")
          .hasSize(1);
      String connectionDetail = connectionIssues.get(0).detail();
      assertThat(connectionDetail).startsWith("Connection ");
      connectionIds.add(
          connectionDetail.substring("Connection ".length(), connectionDetail.indexOf(" held ")));
    }
    assertThat(connectionIds)
        .as("neither report may inherit the other transaction's connection")
        .hasSize(2);
    assertThat(PUBLICATIONS).hasSize(1);
    Map<String, Object> canonical =
        JsonPath.parse(Files.readString(output.resolve("report.json"))).json();
    assertThat(canonical.get("outcome")).isEqualTo("PASS");
    assertThat((List<?>) canonical.get("incompleteReasons")).isEmpty();
    Map<String, Object> published = JsonPath.parse(PUBLICATIONS.get(0).json()).json();
    assertThat(((Number) published.get("reportedTests")).intValue()).isEqualTo(2);
    assertThat(((Number) published.get("totalQueries")).longValue()).isEqualTo(11);

    assertThat(scenario.proxy.getProxyConfig().getQueryListener().getListeners())
        .isEqualTo(scenario.queryListeners);
    assertThat(scenario.proxy.getProxyConfig().getMethodListener().getListeners())
        .isEqualTo(scenario.methodListeners);
    assertThat(listeners(scenario.factory, EventType.INIT_COLLECTION))
        .isEqualTo(scenario.collectionListeners);
    assertThat(listeners(scenario.factory, EventType.POST_LOAD)).isEqualTo(scenario.loadListeners);
  }

  private static List<Object> listeners(EntityManagerFactory factory, EventType<?> type) {
    EventListenerRegistry registry =
        factory
            .unwrap(SessionFactoryImplementor.class)
            .getServiceRegistry()
            .getService(EventListenerRegistry.class);
    List<Object> listeners = new ArrayList<>();
    registry.getEventListenerGroup(type).listeners().forEach(listeners::add);
    return List.copyOf(listeners);
  }

  private static void loadOwnCollections(
      EntityManagerFactory factory,
      ApplicationContext context,
      String group,
      int owners,
      int marker)
      throws Exception {
    scenario.threads.add(Thread.currentThread());
    scenario.contexts.add(context);
    scenario.factories.add(factory);
    try (EntityManager manager = factory.createEntityManager()) {
      scenario.entityManagers.add(manager);
      manager.getTransaction().begin();
      try {
        scenario.barrier.await(15, TimeUnit.SECONDS);
        manager.createNativeQuery("SELECT " + marker, Integer.class).getSingleResult();
        List<Team> teams =
            manager
                .createQuery(
                    "select t from Team t where t.name like :group order by t.id", Team.class)
                .setParameter("group", group + "-%")
                .getResultList();
        assertThat(teams).hasSize(owners);
        int beforeBoundary = scenario.staggered ? Math.min(3, owners) : owners;
        for (int index = 0; index < beforeBoundary; index++)
          assertThat(teams.get(index).getMembers()).hasSize(1);
        scenario.barrier.await(15, TimeUnit.SECONDS);
        if (scenario.staggered && group.equals("second")) {
          assertThat(scenario.firstClassFinished.await(15, TimeUnit.SECONDS))
              .as("the first class must finish its cleanup before the last lazy load")
              .isTrue();
        }
        for (int index = beforeBoundary; index < owners; index++)
          assertThat(teams.get(index).getMembers()).hasSize(1);
        manager.getTransaction().commit();
      } finally {
        if (manager.getTransaction().isActive()) manager.getTransaction().rollback();
      }
    }
  }

  private static void seed(EntityManagerFactory factory) {
    try (EntityManager manager = factory.createEntityManager()) {
      manager.getTransaction().begin();
      for (String group : List.of("first", "second")) {
        int owners = group.equals("first") ? 3 : 4;
        for (int index = 0; index < owners; index++) {
          Team team = new Team(group + "-" + index);
          manager.persist(team);
          Member member = new Member(group, group + index + "@example.test", "ACTIVE");
          member.setTeam(team);
          manager.persist(member);
        }
      }
      manager.getTransaction().commit();
    }
  }

  private static final class Scenario {
    final Path output;
    final boolean staggered;
    final ProxyDataSource proxy;
    final List<?> queryListeners;
    final List<?> methodListeners;
    final CyclicBarrier barrier = new CyclicBarrier(2);
    final CountDownLatch firstClassFinished = new CountDownLatch(1);
    final Set<Thread> threads = ConcurrentHashMap.newKeySet();
    final Set<ApplicationContext> contexts = ConcurrentHashMap.newKeySet();
    final Set<EntityManagerFactory> factories = ConcurrentHashMap.newKeySet();
    final Set<EntityManager> entityManagers = ConcurrentHashMap.newKeySet();
    final AtomicInteger factoryBuilds = new AtomicInteger();
    EntityManagerFactory factory;
    List<Object> collectionListeners;
    List<Object> loadListeners;

    Scenario(Path output, boolean staggered) {
      this.output = output;
      this.staggered = staggered;
      JdbcDataSource source = new JdbcDataSource();
      source.setURL("jdbc:h2:mem:parallel-hibernate-acceptance;DB_CLOSE_DELAY=-1");
      proxy = ProxyDataSourceBuilder.create(source).build();
      queryListeners = List.copyOf(proxy.getProxyConfig().getQueryListener().getListeners());
      methodListeners = List.copyOf(proxy.getProxyConfig().getMethodListener().getListeners());
    }
  }

  @Configuration(proxyBeanMethods = false)
  static class SharedConfiguration {
    @Bean
    DataSource dataSource() {
      return scenario.proxy;
    }

    @Bean(destroyMethod = "close")
    EntityManagerFactory entityManagerFactory(DataSource source) {
      org.hibernate.cfg.Configuration configuration =
          new org.hibernate.cfg.Configuration()
              .addAnnotatedClass(Team.class)
              .addAnnotatedClass(Member.class)
              .setProperty("hibernate.hbm2ddl.auto", "create-drop")
              .setProperty("hibernate.show_sql", "false");
      configuration.getProperties().put("hibernate.connection.datasource", source);
      EntityManagerFactory factory = configuration.buildSessionFactory();
      seed(factory);
      scenario.factory = factory;
      scenario.factoryBuilds.incrementAndGet();
      scenario.collectionListeners = listeners(factory, EventType.INIT_COLLECTION);
      scenario.loadListeners = listeners(factory, EventType.POST_LOAD);
      return factory;
    }

    @Bean
    QueryAuditConfig queryAuditConfig() {
      QueryAuditConfig config =
          QueryAuditConfig.builder()
              .failOnDetection(false)
              .showInfo(true)
              .nPlusOneThreshold(3)
              .connectionHeldIdleThresholdMs(0)
              .addEnabledRule("connection-held-idle")
              .reportFormat(ReportFormat.JSON)
              .reportRedaction(ReportRedaction.FULL)
              .reportOutputDir(scenario.output.toString())
              .baselinePath(scenario.output.resolve("unused-baseline.txt").toString())
              .autoOpenReport(false)
              .build();
      assertThat(config.isRuleExcluded("connection-held-idle"))
          .as("the zero threshold must exercise connection ownership, not a disabled rule")
          .isFalse();
      return config;
    }

    @Bean
    AuditExtensions extensions() {
      return EXTENSIONS;
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  @SpringJUnitConfig(SharedConfiguration.class)
  @QueryAudit(failOnDetection = BooleanOverride.FALSE, autoOpenReport = BooleanOverride.FALSE)
  @Execution(ExecutionMode.CONCURRENT)
  static class FirstFixture {
    @Autowired EntityManagerFactory factory;
    @Autowired ApplicationContext context;

    @Test
    void firstOwners() throws Exception {
      loadOwnCollections(factory, context, "first", 3, 3001);
    }
  }

  @EnabledIfSystemProperty(named = ENABLED, matches = "true")
  @SpringJUnitConfig(SharedConfiguration.class)
  @QueryAudit(failOnDetection = BooleanOverride.FALSE, autoOpenReport = BooleanOverride.FALSE)
  @Execution(ExecutionMode.CONCURRENT)
  static class SecondFixture {
    @Autowired EntityManagerFactory factory;
    @Autowired ApplicationContext context;

    @Test
    void secondOwners() throws Exception {
      loadOwnCollections(factory, context, "second", 4, 4001);
    }
  }
}
