package io.queryaudit.core.detector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.queryaudit.core.config.QueryAuditConfig;
import io.queryaudit.core.model.IndexInfo;
import io.queryaudit.core.model.IndexMetadata;
import io.queryaudit.core.model.Issue;
import io.queryaudit.core.model.IssueType;
import io.queryaudit.core.model.QueryAuditReport;
import io.queryaudit.core.model.QueryRecord;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * Open-source corpus test: validates that QueryAuditAnalyzer produces zero false positives and zero
 * crashes when analyzing SQL from production-grade open-source Java projects.
 *
 * <p>SQL queries are based on real schemas and query patterns from:
 *
 * <ul>
 *   <li>Spring PetClinic (schema.sql, data.sql, JPA repository queries)
 *   <li>JHipster generated application (jhi_user, jhi_authority, Liquibase)
 *   <li>Keycloak (USER_ENTITY, REALM, CLIENT, role mapping)
 *   <li>Apache SkyWalking (metrics, trace segments, alarm rules)
 *   <li>Quartz Scheduler (QRTZ_TRIGGERS, QRTZ_JOB_DETAILS, locking)
 *   <li>Spring Batch (BATCH_JOB_INSTANCE, BATCH_STEP_EXECUTION)
 *   <li>MyBatis dynamic SQL patterns
 *   <li>Common ORM patterns (soft delete, optimistic locking, audit, tenancy, pagination)
 * </ul>
 *
 * <p>Sources:
 *
 * <ul>
 *   <li>https://github.com/spring-projects/spring-petclinic/blob/main/src/main/resources/db/mysql/schema.sql
 *   <li>https://github.com/spring-projects/spring-petclinic/blob/main/src/main/resources/db/mysql/data.sql
 *   <li>https://github.com/jhipster/generator-jhipster (jhi_user, jhi_authority tables)
 *   <li>https://github.com/keycloak/keycloak/blob/main/model/jpa/src/main/java/org/keycloak/models/jpa/entities/UserEntity.java
 *   <li>https://github.com/keycloak/keycloak/blob/main/model/jpa/src/main/java/org/keycloak/models/jpa/entities/RealmEntity.java
 *   <li>https://github.com/keycloak/keycloak/blob/main/model/jpa/src/main/java/org/keycloak/models/jpa/entities/ClientEntity.java
 *   <li>https://github.com/apache/skywalking (JDBC storage plugin)
 *   <li>https://github.com/quartz-scheduler/quartz/blob/master/quartz/src/main/java/org/quartz/impl/jdbcjobstore/StdJDBCConstants.java
 *   <li>https://github.com/spring-projects/spring-batch/blob/main/spring-batch-core/src/main/resources/org/springframework/batch/core/schema-mysql.sql
 * </ul>
 */
class OpenSourceCorpusTest {

  private static QueryAuditAnalyzer analyzer;

  @BeforeAll
  static void setUp() {
    analyzer = new QueryAuditAnalyzer(QueryAuditConfig.defaults(), List.of());
  }

  // -- Helper methods --

  private static QueryRecord q(String sql) {
    return new QueryRecord(sql, 1_000L, System.currentTimeMillis(), "");
  }

  private static QueryRecord q(String sql, int stackHash) {
    return new QueryRecord(sql, 1_000L, System.currentTimeMillis(), "", stackHash);
  }

  /**
   * Analyze a list of queries with given index metadata, assert no crash, and return the report.
   */
  private static QueryAuditReport analyzeNoCrash(
      String testName, List<QueryRecord> queries, IndexMetadata schema) {
    QueryAuditReport report = analyzer.analyze(testName, queries, schema);
    assertThat(report).as("Report should not be null for: " + testName).isNotNull();
    return report;
  }

  /**
   * Assert that among the confirmed (ERROR/WARNING) issues, none are false positives for
   * well-indexed queries. Returns the report for further assertions.
   */
  private static void assertNoFalsePositivesOnIndexedQueries(
      QueryAuditReport report, Set<IssueType> acceptableDetections, String context) {

    List<Issue> confirmed = report.getConfirmedIssues();
    List<Issue> unexpected =
        confirmed.stream().filter(i -> !acceptableDetections.contains(i.type())).toList();

    if (!unexpected.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      sb.append("Unexpected false positives in ").append(context).append(":\n");
      for (Issue issue : unexpected) {
        sb.append("  [")
            .append(issue.type().getCode())
            .append("] ")
            .append(issue.severity())
            .append(" on ")
            .append(issue.table())
            .append(".")
            .append(issue.column())
            .append(" query=")
            .append(truncate(issue.query(), 80))
            .append("\n");
      }
      assertThat(unexpected).as(sb.toString()).isEmpty();
    }
  }

  private static String truncate(String s, int max) {
    if (s == null) return "<null>";
    return s.length() <= max ? s : s.substring(0, max) + "...";
  }

  // ========================================================================
  // Schema definitions for each open-source project
  // ========================================================================

  /**
   * Spring PetClinic MySQL schema. Source:
   * https://github.com/spring-projects/spring-petclinic/blob/main/src/main/resources/db/mysql/schema.sql
   *
   * <p>Tables: vets, specialties, vet_specialties, types, owners, pets, visits
   */
  private static IndexMetadata petClinicSchema() {
    Map<String, List<IndexInfo>> idx = new HashMap<>();
    idx.put(
        "vets",
        List.of(
            new IndexInfo("vets", "PRIMARY", "id", 1, false, 50),
            new IndexInfo("vets", "idx_last_name", "last_name", 1, true, 50)));
    idx.put(
        "specialties",
        List.of(
            new IndexInfo("specialties", "PRIMARY", "id", 1, false, 10),
            new IndexInfo("specialties", "idx_name", "name", 1, true, 10)));
    idx.put(
        "vet_specialties",
        List.of(
            new IndexInfo("vet_specialties", "uq_vet_specialty", "vet_id", 1, false, 50),
            new IndexInfo("vet_specialties", "uq_vet_specialty", "specialty_id", 2, false, 50),
            new IndexInfo("vet_specialties", "fk_vet_id", "vet_id", 1, true, 50),
            new IndexInfo("vet_specialties", "fk_specialty_id", "specialty_id", 1, true, 10)));
    idx.put(
        "types",
        List.of(
            new IndexInfo("types", "PRIMARY", "id", 1, false, 6),
            new IndexInfo("types", "idx_name", "name", 1, true, 6)));
    idx.put(
        "owners",
        List.of(
            new IndexInfo("owners", "PRIMARY", "id", 1, false, 1000),
            new IndexInfo("owners", "idx_last_name", "last_name", 1, true, 800)));
    idx.put(
        "pets",
        List.of(
            new IndexInfo("pets", "PRIMARY", "id", 1, false, 500),
            new IndexInfo("pets", "idx_name", "name", 1, true, 400),
            new IndexInfo("pets", "fk_owner_id", "owner_id", 1, true, 200),
            new IndexInfo("pets", "fk_type_id", "type_id", 1, true, 6)));
    idx.put(
        "visits",
        List.of(
            new IndexInfo("visits", "PRIMARY", "id", 1, false, 2000),
            new IndexInfo("visits", "fk_pet_id", "pet_id", 1, true, 500)));
    return new IndexMetadata(idx);
  }

  /**
   * JHipster generated app schema. Source: https://github.com/jhipster/generator-jhipster
   * (Liquibase changelogs)
   *
   * <p>Tables: jhi_user, jhi_authority, jhi_user_authority, jhi_persistent_audit_event,
   * jhi_persistent_audit_evt_data, DATABASECHANGELOG, DATABASECHANGELOGLOCK
   */
  private static IndexMetadata jhipsterSchema() {
    Map<String, List<IndexInfo>> idx = new HashMap<>();
    idx.put(
        "jhi_user",
        List.of(
            new IndexInfo("jhi_user", "PRIMARY", "id", 1, false, 5000),
            new IndexInfo("jhi_user", "ux_user_login", "login", 1, false, 5000),
            new IndexInfo("jhi_user", "ux_user_email", "email", 1, false, 5000),
            new IndexInfo("jhi_user", "idx_user_activated", "activated", 1, true, 2)));
    idx.put(
        "jhi_authority", List.of(new IndexInfo("jhi_authority", "PRIMARY", "name", 1, false, 10)));
    idx.put(
        "jhi_user_authority",
        List.of(
            new IndexInfo("jhi_user_authority", "PRIMARY", "user_id", 1, false, 5000),
            new IndexInfo("jhi_user_authority", "PRIMARY", "authority_name", 2, false, 10),
            new IndexInfo(
                "jhi_user_authority", "fk_authority_name", "authority_name", 1, true, 10)));
    idx.put(
        "jhi_persistent_audit_event",
        List.of(
            new IndexInfo("jhi_persistent_audit_event", "PRIMARY", "event_id", 1, false, 100000),
            new IndexInfo(
                "jhi_persistent_audit_event", "idx_audit_principal", "principal", 1, true, 5000),
            new IndexInfo(
                "jhi_persistent_audit_event",
                "idx_audit_event_date",
                "event_date",
                1,
                true,
                50000)));
    idx.put(
        "jhi_persistent_audit_evt_data",
        List.of(
            new IndexInfo("jhi_persistent_audit_evt_data", "PRIMARY", "event_id", 1, false, 100000),
            new IndexInfo("jhi_persistent_audit_evt_data", "PRIMARY", "name", 2, false, 100000)));
    idx.put(
        "DATABASECHANGELOG",
        List.of(
            new IndexInfo("DATABASECHANGELOG", "idx_dcl_id_author", "ID", 1, true, 200),
            new IndexInfo("DATABASECHANGELOG", "idx_dcl_id_author", "AUTHOR", 2, true, 200)));
    idx.put(
        "DATABASECHANGELOGLOCK",
        List.of(new IndexInfo("DATABASECHANGELOGLOCK", "PRIMARY", "ID", 1, false, 1)));
    return new IndexMetadata(idx);
  }

  /**
   * Keycloak JPA schema. Source:
   * https://github.com/keycloak/keycloak/blob/main/model/jpa/src/main/java/org/keycloak/models/jpa/entities/
   *
   * <p>Tables: USER_ENTITY, REALM, CLIENT, USER_ROLE_MAPPING, CREDENTIAL, USER_SESSION,
   * CLIENT_SESSION, USER_ATTRIBUTE, COMPONENT
   */
  private static IndexMetadata keycloakSchema() {
    Map<String, List<IndexInfo>> idx = new HashMap<>();
    idx.put(
        "USER_ENTITY",
        List.of(
            new IndexInfo("USER_ENTITY", "PRIMARY", "ID", 1, false, 50000),
            new IndexInfo("USER_ENTITY", "UK_USERNAME_REALM", "USERNAME", 1, false, 50000),
            new IndexInfo("USER_ENTITY", "UK_USERNAME_REALM", "REALM_ID", 2, false, 10),
            new IndexInfo("USER_ENTITY", "IDX_USER_EMAIL", "EMAIL", 1, true, 50000),
            new IndexInfo("USER_ENTITY", "IDX_USER_REALM", "REALM_ID", 1, true, 10),
            new IndexInfo(
                "USER_ENTITY",
                "IDX_USER_SERVICE_ACCOUNT",
                "SERVICE_ACCOUNT_CLIENT_LINK",
                1,
                true,
                1000)));
    idx.put(
        "REALM",
        List.of(
            new IndexInfo("REALM", "PRIMARY", "ID", 1, false, 10),
            new IndexInfo("REALM", "UK_REALM_NAME", "NAME", 1, false, 10)));
    idx.put(
        "CLIENT",
        List.of(
            new IndexInfo("CLIENT", "PRIMARY", "ID", 1, false, 5000),
            new IndexInfo("CLIENT", "UK_CLIENT_REALM", "REALM_ID", 1, false, 10),
            new IndexInfo("CLIENT", "UK_CLIENT_REALM", "CLIENT_ID", 2, false, 5000)));
    idx.put(
        "USER_ROLE_MAPPING",
        List.of(
            new IndexInfo("USER_ROLE_MAPPING", "PRIMARY", "USER_ID", 1, false, 50000),
            new IndexInfo("USER_ROLE_MAPPING", "PRIMARY", "ROLE_ID", 2, false, 100),
            new IndexInfo(
                "USER_ROLE_MAPPING", "IDX_USER_ROLE_MAPPING", "USER_ID", 1, true, 50000)));
    idx.put(
        "CREDENTIAL",
        List.of(
            new IndexInfo("CREDENTIAL", "PRIMARY", "ID", 1, false, 50000),
            new IndexInfo("CREDENTIAL", "IDX_CREDENTIAL_USER", "USER_ID", 1, true, 50000)));
    idx.put(
        "USER_SESSION",
        List.of(
            new IndexInfo("USER_SESSION", "PRIMARY", "ID", 1, false, 10000),
            new IndexInfo("USER_SESSION", "IDX_US_USER", "USER_ID", 1, true, 10000),
            new IndexInfo("USER_SESSION", "IDX_US_REALM", "REALM_ID", 1, true, 10)));
    idx.put(
        "CLIENT_SESSION",
        List.of(
            new IndexInfo("CLIENT_SESSION", "PRIMARY", "ID", 1, false, 20000),
            new IndexInfo("CLIENT_SESSION", "IDX_CS_SESSION", "SESSION_ID", 1, true, 10000),
            new IndexInfo("CLIENT_SESSION", "IDX_CS_CLIENT", "CLIENT_ID", 1, true, 5000)));
    idx.put(
        "USER_ATTRIBUTE",
        List.of(
            new IndexInfo("USER_ATTRIBUTE", "PRIMARY", "ID", 1, false, 100000),
            new IndexInfo("USER_ATTRIBUTE", "IDX_UA_USER", "USER_ID", 1, true, 50000)));
    idx.put(
        "COMPONENT",
        List.of(
            new IndexInfo("COMPONENT", "PRIMARY", "ID", 1, false, 500),
            new IndexInfo("COMPONENT", "IDX_COMP_REALM", "REALM_ID", 1, true, 10),
            new IndexInfo("COMPONENT", "IDX_COMP_PROVIDER", "PROVIDER_TYPE", 1, true, 50)));
    idx.put(
        "KEYCLOAK_ROLE",
        List.of(
            new IndexInfo("KEYCLOAK_ROLE", "PRIMARY", "ID", 1, false, 200),
            new IndexInfo("KEYCLOAK_ROLE", "UK_ROLE_REALM", "NAME", 1, false, 200),
            new IndexInfo("KEYCLOAK_ROLE", "UK_ROLE_REALM", "REALM_ID", 2, false, 10),
            new IndexInfo(
                "KEYCLOAK_ROLE", "IDX_ROLE_CLIENT", "CLIENT_REALM_CONSTRAINT", 1, true, 100)));
    return new IndexMetadata(idx);
  }

  /**
   * Apache SkyWalking JDBC storage schema. Source: https://github.com/apache/skywalking (JDBC
   * storage plugin, H2/MySQL)
   *
   * <p>Tables: service_inventory, endpoint_inventory, service_instance_inventory, segment,
   * segment_tag, alarm_record, service_traffic, service_cpm_day, endpoint_cpm_day
   */
  private static IndexMetadata skyWalkingSchema() {
    Map<String, List<IndexInfo>> idx = new HashMap<>();
    idx.put(
        "service_traffic",
        List.of(
            new IndexInfo("service_traffic", "PRIMARY", "id", 1, false, 500),
            new IndexInfo("service_traffic", "idx_service_name", "service_name", 1, true, 500),
            new IndexInfo("service_traffic", "idx_service_group", "group_name", 1, true, 50)));
    idx.put(
        "endpoint_traffic",
        List.of(
            new IndexInfo("endpoint_traffic", "PRIMARY", "id", 1, false, 10000),
            new IndexInfo("endpoint_traffic", "idx_endpoint_service", "service_id", 1, true, 500)));
    idx.put(
        "instance_traffic",
        List.of(
            new IndexInfo("instance_traffic", "PRIMARY", "id", 1, false, 5000),
            new IndexInfo("instance_traffic", "idx_instance_service", "service_id", 1, true, 500),
            new IndexInfo(
                "instance_traffic", "idx_instance_last_ping", "last_ping", 1, true, 5000)));
    idx.put(
        "segment",
        List.of(
            new IndexInfo("segment", "PRIMARY", "id", 1, false, 10000000),
            new IndexInfo("segment", "idx_seg_trace", "trace_id", 1, true, 5000000),
            new IndexInfo("segment", "idx_seg_service", "service_id", 1, true, 500),
            new IndexInfo("segment", "idx_seg_time", "time_bucket", 1, true, 1440),
            new IndexInfo("segment", "idx_seg_endpoint", "endpoint_id", 1, true, 10000)));
    idx.put(
        "segment_tag",
        List.of(
            new IndexInfo("segment_tag", "PRIMARY", "id", 1, false, 50000000),
            new IndexInfo("segment_tag", "idx_tag_segment", "segment_id", 1, true, 10000000)));
    idx.put(
        "alarm_record",
        List.of(
            new IndexInfo("alarm_record", "PRIMARY", "id", 1, false, 100000),
            new IndexInfo("alarm_record", "idx_alarm_time", "time_bucket", 1, true, 1440),
            new IndexInfo("alarm_record", "idx_alarm_scope", "scope_id", 1, true, 10)));
    idx.put(
        "service_cpm_day",
        List.of(
            new IndexInfo("service_cpm_day", "PRIMARY", "id", 1, false, 180000),
            new IndexInfo("service_cpm_day", "idx_scpm_entity", "entity_id", 1, true, 500),
            new IndexInfo("service_cpm_day", "idx_scpm_tb", "time_bucket", 1, true, 365)));
    idx.put(
        "endpoint_cpm_day",
        List.of(
            new IndexInfo("endpoint_cpm_day", "PRIMARY", "id", 1, false, 3600000),
            new IndexInfo("endpoint_cpm_day", "idx_ecpm_entity", "entity_id", 1, true, 10000),
            new IndexInfo("endpoint_cpm_day", "idx_ecpm_tb", "time_bucket", 1, true, 365)));
    idx.put(
        "service_resp_time_day",
        List.of(
            new IndexInfo("service_resp_time_day", "PRIMARY", "id", 1, false, 180000),
            new IndexInfo("service_resp_time_day", "idx_srt_entity", "entity_id", 1, true, 500),
            new IndexInfo("service_resp_time_day", "idx_srt_tb", "time_bucket", 1, true, 365)));
    return new IndexMetadata(idx);
  }

  /**
   * Quartz Scheduler schema. Source: https://github.com/quartz-scheduler/quartz
   * (StdJDBCConstants.java, tables_mysql_innodb.sql)
   *
   * <p>Tables: QRTZ_JOB_DETAILS, QRTZ_TRIGGERS, QRTZ_SIMPLE_TRIGGERS, QRTZ_CRON_TRIGGERS,
   * QRTZ_FIRED_TRIGGERS, QRTZ_LOCKS, QRTZ_SCHEDULER_STATE, QRTZ_PAUSED_TRIGGER_GRPS,
   * QRTZ_CALENDARS, QRTZ_BLOB_TRIGGERS
   */
  private static IndexMetadata quartzSchema() {
    Map<String, List<IndexInfo>> idx = new HashMap<>();
    idx.put(
        "QRTZ_JOB_DETAILS",
        List.of(
            new IndexInfo("QRTZ_JOB_DETAILS", "PRIMARY", "SCHED_NAME", 1, false, 1),
            new IndexInfo("QRTZ_JOB_DETAILS", "PRIMARY", "JOB_NAME", 2, false, 10000),
            new IndexInfo("QRTZ_JOB_DETAILS", "PRIMARY", "JOB_GROUP", 3, false, 100),
            new IndexInfo("QRTZ_JOB_DETAILS", "IDX_QRTZ_J_REQ_RECOVERY", "SCHED_NAME", 1, true, 1),
            new IndexInfo(
                "QRTZ_JOB_DETAILS", "IDX_QRTZ_J_REQ_RECOVERY", "REQUESTS_RECOVERY", 2, true, 2),
            new IndexInfo("QRTZ_JOB_DETAILS", "IDX_QRTZ_J_GRP", "SCHED_NAME", 1, true, 1),
            new IndexInfo("QRTZ_JOB_DETAILS", "IDX_QRTZ_J_GRP", "JOB_GROUP", 2, true, 100)));
    idx.put(
        "QRTZ_TRIGGERS",
        List.of(
            new IndexInfo("QRTZ_TRIGGERS", "PRIMARY", "SCHED_NAME", 1, false, 1),
            new IndexInfo("QRTZ_TRIGGERS", "PRIMARY", "TRIGGER_NAME", 2, false, 10000),
            new IndexInfo("QRTZ_TRIGGERS", "PRIMARY", "TRIGGER_GROUP", 3, false, 100),
            new IndexInfo("QRTZ_TRIGGERS", "IDX_QRTZ_T_J", "SCHED_NAME", 1, true, 1),
            new IndexInfo("QRTZ_TRIGGERS", "IDX_QRTZ_T_J", "JOB_NAME", 2, true, 10000),
            new IndexInfo("QRTZ_TRIGGERS", "IDX_QRTZ_T_J", "JOB_GROUP", 3, true, 100),
            new IndexInfo("QRTZ_TRIGGERS", "IDX_QRTZ_T_STATE", "SCHED_NAME", 1, true, 1),
            new IndexInfo("QRTZ_TRIGGERS", "IDX_QRTZ_T_STATE", "TRIGGER_STATE", 2, true, 5),
            new IndexInfo("QRTZ_TRIGGERS", "IDX_QRTZ_T_NFT_ST", "SCHED_NAME", 1, true, 1),
            new IndexInfo("QRTZ_TRIGGERS", "IDX_QRTZ_T_NFT_ST", "TRIGGER_STATE", 2, true, 5),
            new IndexInfo("QRTZ_TRIGGERS", "IDX_QRTZ_T_NFT_ST", "NEXT_FIRE_TIME", 3, true, 10000)));
    idx.put(
        "QRTZ_SIMPLE_TRIGGERS",
        List.of(
            new IndexInfo("QRTZ_SIMPLE_TRIGGERS", "PRIMARY", "SCHED_NAME", 1, false, 1),
            new IndexInfo("QRTZ_SIMPLE_TRIGGERS", "PRIMARY", "TRIGGER_NAME", 2, false, 5000),
            new IndexInfo("QRTZ_SIMPLE_TRIGGERS", "PRIMARY", "TRIGGER_GROUP", 3, false, 50)));
    idx.put(
        "QRTZ_CRON_TRIGGERS",
        List.of(
            new IndexInfo("QRTZ_CRON_TRIGGERS", "PRIMARY", "SCHED_NAME", 1, false, 1),
            new IndexInfo("QRTZ_CRON_TRIGGERS", "PRIMARY", "TRIGGER_NAME", 2, false, 5000),
            new IndexInfo("QRTZ_CRON_TRIGGERS", "PRIMARY", "TRIGGER_GROUP", 3, false, 50)));
    idx.put(
        "QRTZ_BLOB_TRIGGERS",
        List.of(
            new IndexInfo("QRTZ_BLOB_TRIGGERS", "PRIMARY", "SCHED_NAME", 1, false, 1),
            new IndexInfo("QRTZ_BLOB_TRIGGERS", "PRIMARY", "TRIGGER_NAME", 2, false, 1000),
            new IndexInfo("QRTZ_BLOB_TRIGGERS", "PRIMARY", "TRIGGER_GROUP", 3, false, 50)));
    idx.put(
        "QRTZ_FIRED_TRIGGERS",
        List.of(
            new IndexInfo("QRTZ_FIRED_TRIGGERS", "PRIMARY", "SCHED_NAME", 1, false, 1),
            new IndexInfo("QRTZ_FIRED_TRIGGERS", "PRIMARY", "ENTRY_ID", 2, false, 50000),
            new IndexInfo("QRTZ_FIRED_TRIGGERS", "IDX_QRTZ_FT_TRIG", "SCHED_NAME", 1, true, 1),
            new IndexInfo(
                "QRTZ_FIRED_TRIGGERS", "IDX_QRTZ_FT_TRIG", "TRIGGER_NAME", 2, true, 10000),
            new IndexInfo("QRTZ_FIRED_TRIGGERS", "IDX_QRTZ_FT_TRIG", "TRIGGER_GROUP", 3, true, 100),
            new IndexInfo("QRTZ_FIRED_TRIGGERS", "IDX_QRTZ_FT_INST", "SCHED_NAME", 1, true, 1),
            new IndexInfo("QRTZ_FIRED_TRIGGERS", "IDX_QRTZ_FT_INST", "INSTANCE_NAME", 2, true, 10),
            new IndexInfo("QRTZ_FIRED_TRIGGERS", "IDX_QRTZ_FT_JG", "SCHED_NAME", 1, true, 1),
            new IndexInfo("QRTZ_FIRED_TRIGGERS", "IDX_QRTZ_FT_JG", "JOB_GROUP", 2, true, 100)));
    idx.put(
        "QRTZ_LOCKS",
        List.of(
            new IndexInfo("QRTZ_LOCKS", "PRIMARY", "SCHED_NAME", 1, false, 1),
            new IndexInfo("QRTZ_LOCKS", "PRIMARY", "LOCK_NAME", 2, false, 5)));
    idx.put(
        "QRTZ_SCHEDULER_STATE",
        List.of(
            new IndexInfo("QRTZ_SCHEDULER_STATE", "PRIMARY", "SCHED_NAME", 1, false, 1),
            new IndexInfo("QRTZ_SCHEDULER_STATE", "PRIMARY", "INSTANCE_NAME", 2, false, 10)));
    idx.put(
        "QRTZ_PAUSED_TRIGGER_GRPS",
        List.of(
            new IndexInfo("QRTZ_PAUSED_TRIGGER_GRPS", "PRIMARY", "SCHED_NAME", 1, false, 1),
            new IndexInfo("QRTZ_PAUSED_TRIGGER_GRPS", "PRIMARY", "TRIGGER_GROUP", 2, false, 100)));
    idx.put(
        "QRTZ_CALENDARS",
        List.of(
            new IndexInfo("QRTZ_CALENDARS", "PRIMARY", "SCHED_NAME", 1, false, 1),
            new IndexInfo("QRTZ_CALENDARS", "PRIMARY", "CALENDAR_NAME", 2, false, 20)));
    return new IndexMetadata(idx);
  }

  /**
   * Spring Batch schema. Source:
   * https://github.com/spring-projects/spring-batch/blob/main/spring-batch-core/src/main/resources/org/springframework/batch/core/schema-mysql.sql
   *
   * <p>Tables: BATCH_JOB_INSTANCE, BATCH_JOB_EXECUTION, BATCH_JOB_EXECUTION_PARAMS,
   * BATCH_STEP_EXECUTION, BATCH_STEP_EXECUTION_CONTEXT, BATCH_JOB_EXECUTION_CONTEXT
   */
  private static IndexMetadata springBatchSchema() {
    Map<String, List<IndexInfo>> idx = new HashMap<>();
    idx.put(
        "BATCH_JOB_INSTANCE",
        List.of(
            new IndexInfo("BATCH_JOB_INSTANCE", "PRIMARY", "JOB_INSTANCE_ID", 1, false, 100000),
            new IndexInfo("BATCH_JOB_INSTANCE", "JOB_INST_UN", "JOB_NAME", 1, false, 100000),
            new IndexInfo("BATCH_JOB_INSTANCE", "JOB_INST_UN", "JOB_KEY", 2, false, 100000)));
    idx.put(
        "BATCH_JOB_EXECUTION",
        List.of(
            new IndexInfo("BATCH_JOB_EXECUTION", "PRIMARY", "JOB_EXECUTION_ID", 1, false, 200000),
            new IndexInfo(
                "BATCH_JOB_EXECUTION", "JOB_INST_EXEC_FK", "JOB_INSTANCE_ID", 1, true, 100000)));
    idx.put(
        "BATCH_JOB_EXECUTION_PARAMS",
        List.of(
            new IndexInfo(
                "BATCH_JOB_EXECUTION_PARAMS",
                "JOB_EXEC_PARAMS_FK",
                "JOB_EXECUTION_ID",
                1,
                true,
                200000)));
    idx.put(
        "BATCH_STEP_EXECUTION",
        List.of(
            new IndexInfo("BATCH_STEP_EXECUTION", "PRIMARY", "STEP_EXECUTION_ID", 1, false, 500000),
            new IndexInfo(
                "BATCH_STEP_EXECUTION", "JOB_EXEC_STEP_FK", "JOB_EXECUTION_ID", 1, true, 200000)));
    idx.put(
        "BATCH_STEP_EXECUTION_CONTEXT",
        List.of(
            new IndexInfo(
                "BATCH_STEP_EXECUTION_CONTEXT", "PRIMARY", "STEP_EXECUTION_ID", 1, false, 500000)));
    idx.put(
        "BATCH_JOB_EXECUTION_CONTEXT",
        List.of(
            new IndexInfo(
                "BATCH_JOB_EXECUTION_CONTEXT", "PRIMARY", "JOB_EXECUTION_ID", 1, false, 200000)));
    return new IndexMetadata(idx);
  }

  /** MyBatis/ORM patterns schema -- a generic multi-tenant app. */
  private static IndexMetadata mybatisSchema() {
    Map<String, List<IndexInfo>> idx = new HashMap<>();
    idx.put(
        "users",
        List.of(
            new IndexInfo("users", "PRIMARY", "id", 1, false, 50000),
            new IndexInfo("users", "uk_email", "email", 1, false, 50000),
            new IndexInfo("users", "idx_tenant", "tenant_id", 1, true, 100),
            new IndexInfo("users", "idx_deleted", "deleted_at", 1, true, 50000),
            new IndexInfo("users", "idx_status", "status", 1, true, 5)));
    idx.put(
        "orders",
        List.of(
            new IndexInfo("orders", "PRIMARY", "id", 1, false, 200000),
            new IndexInfo("orders", "idx_user", "user_id", 1, true, 50000),
            new IndexInfo("orders", "idx_tenant", "tenant_id", 1, true, 100),
            new IndexInfo("orders", "idx_status", "status", 1, true, 10),
            new IndexInfo("orders", "idx_created", "created_at", 1, true, 200000)));
    idx.put(
        "products",
        List.of(
            new IndexInfo("products", "PRIMARY", "id", 1, false, 10000),
            new IndexInfo("products", "idx_category", "category_id", 1, true, 100),
            new IndexInfo("products", "idx_tenant", "tenant_id", 1, true, 100),
            new IndexInfo("products", "idx_version", "version", 1, true, 10000)));
    idx.put(
        "order_items",
        List.of(
            new IndexInfo("order_items", "PRIMARY", "id", 1, false, 500000),
            new IndexInfo("order_items", "idx_order", "order_id", 1, true, 200000),
            new IndexInfo("order_items", "idx_product", "product_id", 1, true, 10000)));
    idx.put(
        "categories",
        List.of(
            new IndexInfo("categories", "PRIMARY", "id", 1, false, 100),
            new IndexInfo("categories", "idx_parent", "parent_id", 1, true, 50),
            new IndexInfo("categories", "idx_tenant", "tenant_id", 1, true, 100)));
    idx.put(
        "audit_log",
        List.of(
            new IndexInfo("audit_log", "PRIMARY", "id", 1, false, 1000000),
            new IndexInfo("audit_log", "idx_entity", "entity_type", 1, true, 20),
            new IndexInfo("audit_log", "idx_entity", "entity_id", 2, true, 100000),
            new IndexInfo("audit_log", "idx_created", "created_at", 1, true, 1000000),
            new IndexInfo("audit_log", "idx_user", "user_id", 1, true, 50000)));
    return new IndexMetadata(idx);
  }

  // ========================================================================
  // 1. Spring PetClinic
  // ========================================================================

  @Nested
  @DisplayName("1. Spring PetClinic")
  class SpringPetClinic {

    private final IndexMetadata schema = petClinicSchema();

    @Test
    @DisplayName("PetClinic data.sql INSERT statements produce no crashes")
    void dataSqlInserts() {
      // Actual INSERTs from spring-petclinic/src/main/resources/db/mysql/data.sql
      List<QueryRecord> queries =
          List.of(
              q("INSERT IGNORE INTO vets VALUES (1, 'James', 'Carter')"),
              q("INSERT IGNORE INTO vets VALUES (2, 'Helen', 'Leary')"),
              q("INSERT IGNORE INTO vets VALUES (3, 'Linda', 'Douglas')"),
              q("INSERT IGNORE INTO vets VALUES (4, 'Rafael', 'Ortega')"),
              q("INSERT IGNORE INTO vets VALUES (5, 'Henry', 'Stevens')"),
              q("INSERT IGNORE INTO vets VALUES (6, 'Sharon', 'Jenkins')"),
              q("INSERT IGNORE INTO specialties VALUES (1, 'radiology')"),
              q("INSERT IGNORE INTO specialties VALUES (2, 'surgery')"),
              q("INSERT IGNORE INTO specialties VALUES (3, 'dentistry')"),
              q("INSERT IGNORE INTO vet_specialties VALUES (2, 1)"),
              q("INSERT IGNORE INTO vet_specialties VALUES (3, 2)"),
              q("INSERT IGNORE INTO vet_specialties VALUES (3, 3)"),
              q("INSERT IGNORE INTO vet_specialties VALUES (4, 2)"),
              q("INSERT IGNORE INTO vet_specialties VALUES (5, 1)"),
              q("INSERT IGNORE INTO types VALUES (1, 'cat')"),
              q("INSERT IGNORE INTO types VALUES (2, 'dog')"),
              q("INSERT IGNORE INTO types VALUES (3, 'lizard')"),
              q("INSERT IGNORE INTO types VALUES (4, 'snake')"),
              q("INSERT IGNORE INTO types VALUES (5, 'bird')"),
              q("INSERT IGNORE INTO types VALUES (6, 'hamster')"),
              q(
                  "INSERT IGNORE INTO owners VALUES (1, 'George', 'Franklin', '110 W. Liberty St.', 'Madison', '6085551023')"),
              q(
                  "INSERT IGNORE INTO owners VALUES (2, 'Betty', 'Davis', '638 Cardinal Ave.', 'Sun Prairie', '6085551749')"),
              q(
                  "INSERT IGNORE INTO owners VALUES (3, 'Eduardo', 'Rodriquez', '2693 Commerce St.', 'McFarland', '6085558763')"),
              q("INSERT IGNORE INTO pets VALUES (1, 'Leo', '2000-09-07', 1, 1)"),
              q("INSERT IGNORE INTO pets VALUES (2, 'Basil', '2002-08-06', 6, 2)"),
              q("INSERT IGNORE INTO pets VALUES (3, 'Rosy', '2001-04-17', 2, 3)"),
              q("INSERT IGNORE INTO visits VALUES (1, 7, '2010-03-04', 'rabies shot')"),
              q("INSERT IGNORE INTO visits VALUES (2, 8, '2011-03-04', 'rabies shot')"),
              q("INSERT IGNORE INTO visits VALUES (3, 8, '2009-06-04', 'neutered')"),
              q("INSERT IGNORE INTO visits VALUES (4, 7, '2008-09-04', 'spayed')"));

      QueryAuditReport report = analyzeNoCrash("petclinic-data-sql", queries, schema);
      // data.sql inserts are fine -- no false positives expected
      // REPEATED_SINGLE_INSERT is acceptable since data.sql seeds use individual inserts
      assertNoFalsePositivesOnIndexedQueries(
          report, Set.of(IssueType.REPEATED_SINGLE_INSERT), "PetClinic data.sql");
    }

    @Test
    @DisplayName("PetClinic JPA repository queries - indexed lookups")
    void jpaRepositoryQueries() {
      // Queries generated by Spring Data JPA from PetClinic repository interfaces
      List<QueryRecord> queries =
          List.of(
              // OwnerRepository.findByLastName (indexed)
              q(
                  "SELECT id, first_name, last_name, address, city, telephone FROM owners WHERE last_name LIKE ?"),
              // OwnerRepository.findById
              q(
                  "SELECT id, first_name, last_name, address, city, telephone FROM owners WHERE id = ?"),
              // PetRepository.findPetTypes
              q("SELECT id, name FROM types ORDER BY name"),
              // PetRepository.findById
              q("SELECT id, name, birth_date, type_id, owner_id FROM pets WHERE id = ?"),
              // OwnerRepository lazy load pets
              q("SELECT id, name, birth_date, type_id, owner_id FROM pets WHERE owner_id = ?"),
              // VisitRepository.findByPetId
              q("SELECT id, visit_date, description, pet_id FROM visits WHERE pet_id = ?"),
              // VetRepository.findAll with specialties
              q("SELECT v.id, v.first_name, v.last_name FROM vets v ORDER BY v.last_name"),
              // Vet specialties join
              q(
                  "SELECT vs.vet_id, vs.specialty_id, s.id, s.name FROM vet_specialties vs INNER JOIN specialties s ON vs.specialty_id = s.id WHERE vs.vet_id = ?"),
              // Save owner
              q(
                  "INSERT INTO owners (first_name, last_name, address, city, telephone) VALUES (?, ?, ?, ?, ?)"),
              // Update owner
              q(
                  "UPDATE owners SET first_name = ?, last_name = ?, address = ?, city = ?, telephone = ? WHERE id = ?"),
              // Save pet
              q("INSERT INTO pets (name, birth_date, type_id, owner_id) VALUES (?, ?, ?, ?)"),
              // Save visit
              q("INSERT INTO visits (visit_date, description, pet_id) VALUES (?, ?, ?)"),
              // Count owners (exists check)
              q("SELECT COUNT(id) FROM owners WHERE last_name = ?"));

      QueryAuditReport report = analyzeNoCrash("petclinic-jpa", queries, schema);
      // These are well-designed queries with proper indexes.
      // UNBOUNDED_RESULT_SET on "types ORDER BY name" is acceptable (small table).
      // SELECT_ALL never fires because we use explicit columns.
      // REDUNDANT_INDEX is a true positive: vet_specialties has fk_vet_id(vet_id)
      // which is a prefix of uq_vet_specialty(vet_id, specialty_id).
      // The real PetClinic schema does have this redundancy.
      assertNoFalsePositivesOnIndexedQueries(
          report,
          Set.of(
              IssueType.UNBOUNDED_RESULT_SET,
              IssueType.COUNT_INSTEAD_OF_EXISTS,
              IssueType.COVERING_INDEX_OPPORTUNITY,
              IssueType.UNION_WITHOUT_ALL,
              IssueType.REDUNDANT_INDEX),
          "PetClinic JPA queries");
    }
  }

  // ========================================================================
  // 2. JHipster
  // ========================================================================

  @Nested
  @DisplayName("2. JHipster generated app")
  class JHipster {

    private final IndexMetadata schema = jhipsterSchema();

    @Test
    @DisplayName("JHipster authentication queries")
    void authenticationQueries() {
      List<QueryRecord> queries =
          List.of(
              // Login lookup by username
              q(
                  "SELECT id, login, password_hash, first_name, last_name, email, activated, lang_key, image_url, created_by, created_date FROM jhi_user WHERE login = ?"),
              // Login lookup by email
              q(
                  "SELECT id, login, password_hash, first_name, last_name, email, activated, lang_key, image_url FROM jhi_user WHERE email = ?"),
              // Fetch authorities for user
              q("SELECT ua.authority_name FROM jhi_user_authority ua WHERE ua.user_id = ?"),
              // Check if user exists
              q("SELECT COUNT(id) FROM jhi_user WHERE login = ?"),
              // Get all authorities
              q("SELECT name FROM jhi_authority"),
              // Insert new user
              q(
                  "INSERT INTO jhi_user (login, password_hash, first_name, last_name, email, activated, lang_key, image_url, created_by, created_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"),
              // Map user to authority
              q("INSERT INTO jhi_user_authority (user_id, authority_name) VALUES (?, ?)"),
              // Update user
              q(
                  "UPDATE jhi_user SET first_name = ?, last_name = ?, email = ?, activated = ?, lang_key = ?, image_url = ?, last_modified_by = ?, last_modified_date = ? WHERE id = ?"),
              // Activate user
              q("UPDATE jhi_user SET activated = 1 WHERE id = ?"),
              // Delete user authorities
              q("DELETE FROM jhi_user_authority WHERE user_id = ?"));

      QueryAuditReport report = analyzeNoCrash("jhipster-auth", queries, schema);
      assertNoFalsePositivesOnIndexedQueries(
          report,
          Set.of(
              IssueType.UNBOUNDED_RESULT_SET,
              IssueType.COUNT_INSTEAD_OF_EXISTS,
              IssueType.COVERING_INDEX_OPPORTUNITY),
          "JHipster auth queries");
    }

    @Test
    @DisplayName("JHipster audit event queries")
    void auditEventQueries() {
      List<QueryRecord> queries =
          List.of(
              // Insert audit event
              q(
                  "INSERT INTO jhi_persistent_audit_event (principal, event_date, event_type) VALUES (?, ?, ?)"),
              // Insert audit event data
              q(
                  "INSERT INTO jhi_persistent_audit_evt_data (event_id, name, value) VALUES (?, ?, ?)"),
              // Find audit events by principal
              q(
                  "SELECT event_id, principal, event_date, event_type FROM jhi_persistent_audit_event WHERE principal = ? ORDER BY event_date DESC LIMIT 100"),
              // Find audit events between dates
              q(
                  "SELECT event_id, principal, event_date, event_type FROM jhi_persistent_audit_event WHERE event_date BETWEEN ? AND ? ORDER BY event_date DESC LIMIT 100"),
              // Find audit event data
              q(
                  "SELECT event_id, name, value FROM jhi_persistent_audit_evt_data WHERE event_id = ?"),
              // Delete old audit events
              q(
                  "DELETE FROM jhi_persistent_audit_evt_data WHERE event_id IN (SELECT event_id FROM jhi_persistent_audit_event WHERE event_date < ?)"));

      QueryAuditReport report = analyzeNoCrash("jhipster-audit", queries, schema);
      assertNoFalsePositivesOnIndexedQueries(
          report,
          Set.of(
              IssueType.COVERING_INDEX_OPPORTUNITY,
              IssueType.CORRELATED_SUBQUERY,
              IssueType.SUBQUERY_IN_DML),
          "JHipster audit queries");
    }

    @Test
    @DisplayName("JHipster Liquibase DATABASECHANGELOG queries")
    void liquibaseQueries() {
      List<QueryRecord> queries =
          List.of(
              // Liquibase reads changesets
              q(
                  "SELECT ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, TAG, LIQUIBASE, CONTEXTS, LABELS, DEPLOYMENT_ID FROM DATABASECHANGELOG ORDER BY DATEEXECUTED, ORDEREXECUTED"),
              // Liquibase insert new changeset
              q(
                  "INSERT INTO DATABASECHANGELOG (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, TAG, LIQUIBASE, CONTEXTS, LABELS, DEPLOYMENT_ID) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"),
              // Liquibase lock
              q("SELECT ID, LOCKED, LOCKGRANTED, LOCKEDBY FROM DATABASECHANGELOGLOCK WHERE ID = 1"),
              // Liquibase acquire lock
              q(
                  "UPDATE DATABASECHANGELOGLOCK SET LOCKED = 1, LOCKGRANTED = ?, LOCKEDBY = ? WHERE ID = 1 AND LOCKED = 0"),
              // Liquibase release lock
              q(
                  "UPDATE DATABASECHANGELOGLOCK SET LOCKED = 0, LOCKGRANTED = NULL, LOCKEDBY = NULL WHERE ID = 1"));

      QueryAuditReport report = analyzeNoCrash("jhipster-liquibase", queries, schema);
      assertNoFalsePositivesOnIndexedQueries(
          report,
          Set.of(IssueType.UNBOUNDED_RESULT_SET, IssueType.COVERING_INDEX_OPPORTUNITY),
          "JHipster Liquibase queries");
    }
  }

  // ========================================================================
  // 3. Keycloak
  // ========================================================================

  @Nested
  @DisplayName("3. Keycloak")
  class Keycloak {

    private final IndexMetadata schema = keycloakSchema();

    @Test
    @DisplayName("Keycloak UserEntity named queries")
    void userEntityQueries() {
      // Derived from @NamedQuery annotations in UserEntity.java
      // https://github.com/keycloak/keycloak/blob/main/model/jpa/src/main/java/org/keycloak/models/jpa/entities/UserEntity.java
      List<QueryRecord> queries =
          List.of(
              // getRealmUserByUsername
              q(
                  "SELECT u.ID, u.USERNAME, u.EMAIL, u.FIRST_NAME, u.LAST_NAME, u.ENABLED, u.EMAIL_VERIFIED, u.REALM_ID FROM USER_ENTITY u WHERE u.USERNAME = ? AND u.REALM_ID = ?"),
              // getRealmUserByEmail
              q(
                  "SELECT u.ID, u.USERNAME, u.EMAIL, u.FIRST_NAME, u.LAST_NAME, u.ENABLED FROM USER_ENTITY u WHERE u.EMAIL = ? AND u.REALM_ID = ?"),
              // getRealmUserByLastName
              q(
                  "SELECT u.ID, u.USERNAME, u.EMAIL, u.FIRST_NAME, u.LAST_NAME FROM USER_ENTITY u WHERE u.LAST_NAME = ? AND u.REALM_ID = ?"),
              // getRealmUserByFirstLastName
              q(
                  "SELECT u.ID, u.USERNAME, u.EMAIL FROM USER_ENTITY u WHERE u.FIRST_NAME = ? AND u.LAST_NAME = ? AND u.REALM_ID = ?"),
              // getRealmUserByServiceAccount
              q(
                  "SELECT u.ID, u.USERNAME, u.EMAIL FROM USER_ENTITY u WHERE u.SERVICE_ACCOUNT_CLIENT_LINK = ? AND u.REALM_ID = ?"),
              // deleteUsersByRealm
              q("DELETE FROM USER_ENTITY WHERE REALM_ID = ?"),
              // deleteUsersByRealmAndLink
              q("DELETE FROM USER_ENTITY WHERE REALM_ID = ? AND FEDERATION_LINK = ?"),
              // unlinkUsers
              q(
                  "UPDATE USER_ENTITY SET FEDERATION_LINK = NULL WHERE REALM_ID = ? AND FEDERATION_LINK = ?"),
              // getRealmUsersByAttributeNameAndValue (JOIN)
              q(
                  "SELECT u.ID, u.USERNAME, u.EMAIL, u.REALM_ID FROM USER_ENTITY u INNER JOIN USER_ATTRIBUTE attr ON u.ID = attr.USER_ID WHERE attr.NAME = ? AND attr.VALUE = ? AND u.REALM_ID = ?"));

      QueryAuditReport report = analyzeNoCrash("keycloak-user-entity", queries, schema);
      assertNoFalsePositivesOnIndexedQueries(
          report,
          Set.of(IssueType.COVERING_INDEX_OPPORTUNITY, IssueType.UNBOUNDED_RESULT_SET),
          "Keycloak UserEntity queries");
    }

    @Test
    @DisplayName("Keycloak Realm and Client queries")
    void realmAndClientQueries() {
      // Derived from @NamedQuery in RealmEntity.java and ClientEntity.java
      List<QueryRecord> queries =
          List.of(
              // getAllRealmIds
              q("SELECT ID FROM REALM"),
              // getRealmIdByName
              q("SELECT ID FROM REALM WHERE NAME = ?"),
              // getRealmIdsWithProviderType
              q("SELECT DISTINCT c.REALM_ID FROM COMPONENT c WHERE c.PROVIDER_TYPE = ?"),
              // getClientById
              q(
                  "SELECT c.ID, c.CLIENT_ID, c.NAME, c.ENABLED, c.PROTOCOL, c.REALM_ID FROM CLIENT c WHERE c.ID = ? AND c.REALM_ID = ?"),
              // findClientByClientId
              q(
                  "SELECT c.ID, c.CLIENT_ID, c.NAME, c.ENABLED, c.PROTOCOL FROM CLIENT c WHERE c.CLIENT_ID = ? AND c.REALM_ID = ?"),
              // getAlwaysDisplayInConsoleClients
              q(
                  "SELECT c.ID FROM CLIENT c WHERE c.ALWAYS_DISPLAY_IN_CONSOLE = 1 AND c.REALM_ID = ? ORDER BY c.CLIENT_ID"),
              // findClientIdByClientId
              q("SELECT c.ID FROM CLIENT c WHERE c.CLIENT_ID = ? AND c.REALM_ID = ?"));

      QueryAuditReport report = analyzeNoCrash("keycloak-realm-client", queries, schema);
      assertNoFalsePositivesOnIndexedQueries(
          report,
          Set.of(IssueType.UNBOUNDED_RESULT_SET, IssueType.COVERING_INDEX_OPPORTUNITY),
          "Keycloak Realm/Client queries");
    }

    @Test
    @DisplayName("Keycloak role mapping and session queries")
    void roleMappingAndSessionQueries() {
      List<QueryRecord> queries =
          List.of(
              // User role mapping - complex JOIN for role assignment
              q(
                  "SELECT urm.USER_ID, urm.ROLE_ID, kr.NAME, kr.REALM_ID FROM USER_ROLE_MAPPING urm INNER JOIN KEYCLOAK_ROLE kr ON urm.ROLE_ID = kr.ID WHERE urm.USER_ID = ?"),
              // Check if user has role
              q(
                  "SELECT COUNT(urm.USER_ID) FROM USER_ROLE_MAPPING urm WHERE urm.USER_ID = ? AND urm.ROLE_ID = ?"),
              // Grant role to user
              q("INSERT INTO USER_ROLE_MAPPING (USER_ID, ROLE_ID) VALUES (?, ?)"),
              // Remove role from user
              q("DELETE FROM USER_ROLE_MAPPING WHERE USER_ID = ? AND ROLE_ID = ?"),
              // Find user sessions by user
              q("SELECT us.ID, us.USER_ID, us.REALM_ID FROM USER_SESSION us WHERE us.USER_ID = ?"),
              // Find user sessions by realm
              q("SELECT us.ID, us.USER_ID, us.REALM_ID FROM USER_SESSION us WHERE us.REALM_ID = ?"),
              // Find client sessions by session ID
              q(
                  "SELECT cs.ID, cs.CLIENT_ID, cs.SESSION_ID FROM CLIENT_SESSION cs WHERE cs.SESSION_ID = ?"),
              // Delete expired sessions
              q(
                  "DELETE FROM CLIENT_SESSION WHERE SESSION_ID IN (SELECT ID FROM USER_SESSION WHERE REALM_ID = ?)"),
              // Delete user sessions by realm
              q("DELETE FROM USER_SESSION WHERE REALM_ID = ?"),
              // Credential lookup
              q(
                  "SELECT c.ID, c.USER_ID, c.TYPE, c.SECRET_DATA, c.CREDENTIAL_DATA, c.PRIORITY FROM CREDENTIAL c WHERE c.USER_ID = ? ORDER BY c.PRIORITY"),
              // Delete credentials by user
              q("DELETE FROM CREDENTIAL WHERE USER_ID = ?"));

      QueryAuditReport report = analyzeNoCrash("keycloak-roles-sessions", queries, schema);
      // REDUNDANT_INDEX is a true positive: USER_ROLE_MAPPING has
      // IDX_USER_ROLE_MAPPING(user_id) which is a prefix of PRIMARY(user_id, role_id).
      // Keycloak's actual schema has this redundancy for query optimizer hints.
      assertNoFalsePositivesOnIndexedQueries(
          report,
          Set.of(
              IssueType.UNBOUNDED_RESULT_SET,
              IssueType.COVERING_INDEX_OPPORTUNITY,
              IssueType.COUNT_INSTEAD_OF_EXISTS,
              IssueType.CORRELATED_SUBQUERY,
              IssueType.REDUNDANT_INDEX,
              IssueType.SUBQUERY_IN_DML),
          "Keycloak roles/sessions queries");
    }
  }

  // ========================================================================
  // 4. Apache SkyWalking
  // ========================================================================

  @Nested
  @DisplayName("4. Apache SkyWalking")
  class ApacheSkyWalking {

    private final IndexMetadata schema = skyWalkingSchema();

    @Test
    @DisplayName("SkyWalking metrics queries with GROUP BY and time buckets")
    void metricsQueries() {
      List<QueryRecord> queries =
          List.of(
              // Service CPM aggregation by day
              q(
                  "SELECT entity_id, SUM(value) AS total_value FROM service_cpm_day WHERE time_bucket >= ? AND time_bucket <= ? GROUP BY entity_id ORDER BY total_value DESC LIMIT 20"),
              // Endpoint CPM top-N
              q(
                  "SELECT entity_id, AVG(value) AS avg_value FROM endpoint_cpm_day WHERE entity_id = ? AND time_bucket >= ? AND time_bucket <= ? GROUP BY entity_id"),
              // Service response time percentile
              q(
                  "SELECT entity_id, AVG(value) AS avg_resp_time FROM service_resp_time_day WHERE time_bucket >= ? AND time_bucket <= ? GROUP BY entity_id ORDER BY avg_resp_time DESC LIMIT 10"),
              // Service traffic listing
              q(
                  "SELECT id, service_name, group_name FROM service_traffic WHERE service_name LIKE ? LIMIT 50"),
              // Endpoint traffic by service
              q("SELECT id, service_id FROM endpoint_traffic WHERE service_id = ? LIMIT 100"),
              // Instance traffic with last ping
              q(
                  "SELECT id, service_id, last_ping FROM instance_traffic WHERE service_id = ? AND last_ping >= ? LIMIT 100"));

      QueryAuditReport report = analyzeNoCrash("skywalking-metrics", queries, schema);
      // ORDER_BY_LIMIT_WITHOUT_INDEX is a true positive: ORDER BY computed aliases
      // like SUM(value) and AVG(value) cannot use an index. This is expected for
      // aggregation queries and is a valid advisory.
      assertNoFalsePositivesOnIndexedQueries(
          report,
          Set.of(
              IssueType.COVERING_INDEX_OPPORTUNITY,
              IssueType.UNBOUNDED_RESULT_SET,
              IssueType.ORDER_BY_LIMIT_WITHOUT_INDEX,
              IssueType.LIMIT_WITHOUT_ORDER_BY),
          "SkyWalking metrics queries");
    }

    @Test
    @DisplayName("SkyWalking trace segment queries with JOINs")
    void traceSegmentQueries() {
      List<QueryRecord> queries =
          List.of(
              // Query segments by trace ID
              q(
                  "SELECT s.id, s.trace_id, s.service_id, s.endpoint_id, s.time_bucket, s.latency, s.is_error FROM segment s WHERE s.trace_id = ?"),
              // Query segments with tags
              q(
                  "SELECT s.id, s.trace_id, s.service_id, s.endpoint_id, s.latency FROM segment s INNER JOIN segment_tag st ON s.id = st.segment_id WHERE s.service_id = ? AND s.time_bucket >= ? AND s.time_bucket <= ? ORDER BY s.time_bucket DESC LIMIT 20"),
              // Query segments by service and time
              q(
                  "SELECT id, trace_id, service_id, endpoint_id, latency, is_error, time_bucket FROM segment WHERE service_id = ? AND time_bucket >= ? AND time_bucket <= ? ORDER BY time_bucket DESC LIMIT 20"),
              // Query error segments
              q(
                  "SELECT id, trace_id, service_id, endpoint_id, latency, time_bucket FROM segment WHERE service_id = ? AND is_error = 1 AND time_bucket >= ? AND time_bucket <= ? ORDER BY latency DESC LIMIT 20"),
              // Query slow segments
              q(
                  "SELECT id, trace_id, service_id, endpoint_id, latency, time_bucket FROM segment WHERE service_id = ? AND latency >= ? AND time_bucket >= ? AND time_bucket <= ? ORDER BY latency DESC LIMIT 20"),
              // Count segments by service
              q(
                  "SELECT COUNT(id) FROM segment WHERE service_id = ? AND time_bucket >= ? AND time_bucket <= ?"),
              // Delete old segments
              q("DELETE FROM segment WHERE time_bucket < ? LIMIT 5000"));

      QueryAuditReport report = analyzeNoCrash("skywalking-traces", queries, schema);
      // MISSING_WHERE_INDEX and ORDER_BY_LIMIT_WITHOUT_INDEX on 'latency' are
      // true positives: SkyWalking's segment table does not index latency, and
      // queries that filter/sort by latency will do a filesort. This is a known
      // trade-off in SkyWalking's schema design for write performance.
      assertNoFalsePositivesOnIndexedQueries(
          report,
          Set.of(
              IssueType.COVERING_INDEX_OPPORTUNITY,
              IssueType.MISSING_WHERE_INDEX,
              IssueType.ORDER_BY_LIMIT_WITHOUT_INDEX),
          "SkyWalking trace queries");
    }

    @Test
    @DisplayName("SkyWalking alarm record queries")
    void alarmQueries() {
      List<QueryRecord> queries =
          List.of(
              // Query alarm records by time
              q(
                  "SELECT id, scope_id, name, id0, id1, alarm_message, start_time, time_bucket FROM alarm_record WHERE time_bucket >= ? AND time_bucket <= ? ORDER BY time_bucket DESC LIMIT 20"),
              // Query alarm records by scope
              q(
                  "SELECT id, scope_id, name, alarm_message, start_time, time_bucket FROM alarm_record WHERE scope_id = ? AND time_bucket >= ? AND time_bucket <= ? ORDER BY time_bucket DESC LIMIT 20"),
              // Insert alarm record
              q(
                  "INSERT INTO alarm_record (id, scope_id, name, id0, id1, alarm_message, start_time, time_bucket, tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"),
              // Delete old alarms
              q("DELETE FROM alarm_record WHERE time_bucket < ? LIMIT 5000"));

      QueryAuditReport report = analyzeNoCrash("skywalking-alarms", queries, schema);
      assertNoFalsePositivesOnIndexedQueries(
          report, Set.of(IssueType.COVERING_INDEX_OPPORTUNITY), "SkyWalking alarm queries");
    }
  }

  // ========================================================================
  // 5. Quartz Scheduler
  // ========================================================================

  @Nested
  @DisplayName("5. Quartz Scheduler")
  class QuartzScheduler {

    private final IndexMetadata schema = quartzSchema();

    @Test
    @DisplayName("Quartz trigger and job detail queries")
    void triggerAndJobQueries() {
      // Derived from StdJDBCConstants.java in quartz-scheduler/quartz
      List<QueryRecord> queries =
          List.of(
              // SELECT_JOB_DETAIL
              q(
                  "SELECT JOB_NAME, JOB_GROUP, DESCRIPTION, JOB_CLASS_NAME, IS_DURABLE, REQUESTS_RECOVERY, JOB_DATA FROM QRTZ_JOB_DETAILS WHERE SCHED_NAME = ? AND JOB_NAME = ? AND JOB_GROUP = ?"),
              // SELECT_TRIGGER
              q(
                  "SELECT TRIGGER_NAME, TRIGGER_GROUP, JOB_NAME, JOB_GROUP, DESCRIPTION, NEXT_FIRE_TIME, PREV_FIRE_TIME, TRIGGER_TYPE, TRIGGER_STATE, START_TIME, END_TIME, CALENDAR_NAME, MISFIRE_INSTR, PRIORITY FROM QRTZ_TRIGGERS WHERE SCHED_NAME = ? AND TRIGGER_NAME = ? AND TRIGGER_GROUP = ?"),
              // SELECT_TRIGGER_STATE
              q(
                  "SELECT TRIGGER_STATE FROM QRTZ_TRIGGERS WHERE SCHED_NAME = ? AND TRIGGER_NAME = ? AND TRIGGER_GROUP = ?"),
              // SELECT_TRIGGERS_FOR_JOB
              q(
                  "SELECT TRIGGER_NAME, TRIGGER_GROUP FROM QRTZ_TRIGGERS WHERE SCHED_NAME = ? AND JOB_NAME = ? AND JOB_GROUP = ?"),
              // INSERT_JOB_DETAIL
              q(
                  "INSERT INTO QRTZ_JOB_DETAILS (SCHED_NAME, JOB_NAME, JOB_GROUP, DESCRIPTION, JOB_CLASS_NAME, IS_DURABLE, IS_NONCONCURRENT, IS_UPDATE_DATA, REQUESTS_RECOVERY, JOB_DATA) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"),
              // UPDATE_JOB_DETAIL
              q(
                  "UPDATE QRTZ_JOB_DETAILS SET DESCRIPTION = ?, JOB_CLASS_NAME = ?, IS_DURABLE = ?, IS_NONCONCURRENT = ?, IS_UPDATE_DATA = ?, REQUESTS_RECOVERY = ?, JOB_DATA = ? WHERE SCHED_NAME = ? AND JOB_NAME = ? AND JOB_GROUP = ?"),
              // DELETE_JOB_DETAIL
              q(
                  "DELETE FROM QRTZ_JOB_DETAILS WHERE SCHED_NAME = ? AND JOB_NAME = ? AND JOB_GROUP = ?"),
              // SELECT_JOB_GROUPS
              q("SELECT DISTINCT(JOB_GROUP) FROM QRTZ_JOB_DETAILS WHERE SCHED_NAME = ?"),
              // SELECT_NUM_JOBS
              q("SELECT COUNT(JOB_NAME) FROM QRTZ_JOB_DETAILS WHERE SCHED_NAME = ?"),
              // SELECT_JOBS_IN_GROUP
              q(
                  "SELECT JOB_NAME, JOB_GROUP FROM QRTZ_JOB_DETAILS WHERE SCHED_NAME = ? AND JOB_GROUP = ?"));

      QueryAuditReport report = analyzeNoCrash("quartz-triggers-jobs", queries, schema);
      // REDUNDANT_INDEX is a true positive: IDX_QRTZ_T_STATE(sched_name, trigger_state)
      // is a prefix of IDX_QRTZ_T_NFT_ST(sched_name, trigger_state, next_fire_time).
      // Quartz keeps both indexes intentionally for different query patterns.
      assertNoFalsePositivesOnIndexedQueries(
          report,
          Set.of(
              IssueType.UNBOUNDED_RESULT_SET,
              IssueType.COVERING_INDEX_OPPORTUNITY,
              IssueType.COUNT_INSTEAD_OF_EXISTS,
              IssueType.REDUNDANT_INDEX),
          "Quartz trigger/job queries");
    }

    @Test
    @DisplayName("Quartz locking queries with FOR UPDATE")
    void lockingQueries() {
      // Critical locking SQL from Quartz StdRowLockSemaphore
      List<QueryRecord> queries =
          List.of(
              // SELECT ... FOR UPDATE on QRTZ_LOCKS (PK lookup)
              q(
                  "SELECT LOCK_NAME FROM QRTZ_LOCKS WHERE SCHED_NAME = ? AND LOCK_NAME = ? FOR UPDATE"),
              // INSERT lock row
              q("INSERT INTO QRTZ_LOCKS (SCHED_NAME, LOCK_NAME) VALUES (?, ?)"),
              // SELECT_NEXT_TRIGGER_TO_ACQUIRE - the critical scheduling query
              q(
                  "SELECT TRIGGER_NAME, TRIGGER_GROUP, NEXT_FIRE_TIME, PRIORITY FROM QRTZ_TRIGGERS WHERE SCHED_NAME = ? AND TRIGGER_STATE = ? AND NEXT_FIRE_TIME <= ? AND (MISFIRE_INSTR = -1 OR (MISFIRE_INSTR != -1 AND NEXT_FIRE_TIME >= ?)) ORDER BY NEXT_FIRE_TIME, PRIORITY LIMIT ?"),
              // UPDATE_TRIGGER_STATE
              q(
                  "UPDATE QRTZ_TRIGGERS SET TRIGGER_STATE = ? WHERE SCHED_NAME = ? AND TRIGGER_NAME = ? AND TRIGGER_GROUP = ?"),
              // UPDATE_TRIGGER_STATE_FROM_STATE
              q(
                  "UPDATE QRTZ_TRIGGERS SET TRIGGER_STATE = ? WHERE SCHED_NAME = ? AND TRIGGER_NAME = ? AND TRIGGER_GROUP = ? AND TRIGGER_STATE = ?"),
              // INSERT_FIRED_TRIGGER
              q(
                  "INSERT INTO QRTZ_FIRED_TRIGGERS (SCHED_NAME, ENTRY_ID, TRIGGER_NAME, TRIGGER_GROUP, INSTANCE_NAME, FIRED_TIME, SCHED_TIME, PRIORITY, STATE, JOB_NAME, JOB_GROUP, IS_NONCONCURRENT, REQUESTS_RECOVERY) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"),
              // DELETE_FIRED_TRIGGER
              q("DELETE FROM QRTZ_FIRED_TRIGGERS WHERE SCHED_NAME = ? AND ENTRY_ID = ?"),
              // SELECT scheduler state
              q(
                  "SELECT INSTANCE_NAME, LAST_CHECKIN_TIME, CHECKIN_INTERVAL FROM QRTZ_SCHEDULER_STATE WHERE SCHED_NAME = ?"),
              // UPDATE scheduler checkin
              q(
                  "UPDATE QRTZ_SCHEDULER_STATE SET LAST_CHECKIN_TIME = ? WHERE SCHED_NAME = ? AND INSTANCE_NAME = ?"));

      QueryAuditReport report = analyzeNoCrash("quartz-locking", queries, schema);
      // FOR_UPDATE_WITHOUT_INDEX should NOT fire -- QRTZ_LOCKS has PK on (SCHED_NAME, LOCK_NAME)
      // REDUNDANT_INDEX on QRTZ_TRIGGERS is a true positive (see triggerAndJobQueries).
      // ORDER_BY_LIMIT_WITHOUT_INDEX on PRIORITY is a true positive: the
      // SELECT_NEXT_TRIGGER_TO_ACQUIRE query orders by (NEXT_FIRE_TIME, PRIORITY)
      // but PRIORITY has no dedicated index. Quartz accepts this trade-off.
      assertNoFalsePositivesOnIndexedQueries(
          report,
          Set.of(
              IssueType.UNBOUNDED_RESULT_SET,
              IssueType.COVERING_INDEX_OPPORTUNITY,
              IssueType.REDUNDANT_INDEX,
              IssueType.ORDER_BY_LIMIT_WITHOUT_INDEX,
              IssueType.FOR_UPDATE_WITHOUT_TIMEOUT,
              IssueType.LIMIT_WITHOUT_ORDER_BY),
          "Quartz locking queries");
    }

    @Test
    @DisplayName("Quartz cron, simple, and blob trigger CRUD")
    void triggerSubtypeQueries() {
      List<QueryRecord> queries =
          List.of(
              // INSERT_SIMPLE_TRIGGER
              q(
                  "INSERT INTO QRTZ_SIMPLE_TRIGGERS (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP, REPEAT_COUNT, REPEAT_INTERVAL, TIMES_TRIGGERED) VALUES (?, ?, ?, ?, ?, ?)"),
              // UPDATE_SIMPLE_TRIGGER
              q(
                  "UPDATE QRTZ_SIMPLE_TRIGGERS SET REPEAT_COUNT = ?, REPEAT_INTERVAL = ?, TIMES_TRIGGERED = ? WHERE SCHED_NAME = ? AND TRIGGER_NAME = ? AND TRIGGER_GROUP = ?"),
              // SELECT simple trigger
              q(
                  "SELECT REPEAT_COUNT, REPEAT_INTERVAL, TIMES_TRIGGERED FROM QRTZ_SIMPLE_TRIGGERS WHERE SCHED_NAME = ? AND TRIGGER_NAME = ? AND TRIGGER_GROUP = ?"),
              // DELETE simple trigger
              q(
                  "DELETE FROM QRTZ_SIMPLE_TRIGGERS WHERE SCHED_NAME = ? AND TRIGGER_NAME = ? AND TRIGGER_GROUP = ?"),
              // INSERT_CRON_TRIGGER
              q(
                  "INSERT INTO QRTZ_CRON_TRIGGERS (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP, CRON_EXPRESSION, TIME_ZONE_ID) VALUES (?, ?, ?, ?, ?)"),
              // UPDATE_CRON_TRIGGER
              q(
                  "UPDATE QRTZ_CRON_TRIGGERS SET CRON_EXPRESSION = ?, TIME_ZONE_ID = ? WHERE SCHED_NAME = ? AND TRIGGER_NAME = ? AND TRIGGER_GROUP = ?"),
              // SELECT cron trigger
              q(
                  "SELECT CRON_EXPRESSION, TIME_ZONE_ID FROM QRTZ_CRON_TRIGGERS WHERE SCHED_NAME = ? AND TRIGGER_NAME = ? AND TRIGGER_GROUP = ?"),
              // INSERT_BLOB_TRIGGER
              q(
                  "INSERT INTO QRTZ_BLOB_TRIGGERS (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP, BLOB_DATA) VALUES (?, ?, ?, ?)"),
              // SELECT paused trigger groups
              q("SELECT TRIGGER_GROUP FROM QRTZ_PAUSED_TRIGGER_GRPS WHERE SCHED_NAME = ?"),
              // SELECT calendar
              q(
                  "SELECT CALENDAR_NAME, CALENDAR FROM QRTZ_CALENDARS WHERE SCHED_NAME = ? AND CALENDAR_NAME = ?"));

      QueryAuditReport report = analyzeNoCrash("quartz-subtypes", queries, schema);
      assertNoFalsePositivesOnIndexedQueries(
          report,
          Set.of(IssueType.UNBOUNDED_RESULT_SET, IssueType.COVERING_INDEX_OPPORTUNITY),
          "Quartz trigger subtype queries");
    }
  }

  // ========================================================================
  // 6. Spring Batch
  // ========================================================================

  @Nested
  @DisplayName("6. Spring Batch")
  class SpringBatch {

    private final IndexMetadata schema = springBatchSchema();

    @Test
    @DisplayName("Spring Batch job instance and execution queries")
    void jobInstanceQueries() {
      // Derived from JdbcJobInstanceDao, JdbcJobExecutionDao
      List<QueryRecord> queries =
          List.of(
              // CREATE_JOB_INSTANCE
              q(
                  "INSERT INTO BATCH_JOB_INSTANCE (JOB_INSTANCE_ID, JOB_NAME, JOB_KEY, VERSION) VALUES (?, ?, ?, ?)"),
              // FIND_JOBS_WITH_KEY
              q(
                  "SELECT JOB_INSTANCE_ID, JOB_NAME FROM BATCH_JOB_INSTANCE WHERE JOB_NAME = ? AND JOB_KEY = ?"),
              // GET_JOB_FROM_ID
              q(
                  "SELECT JOB_INSTANCE_ID, JOB_NAME, JOB_KEY, VERSION FROM BATCH_JOB_INSTANCE WHERE JOB_INSTANCE_ID = ?"),
              // GET_JOB_FROM_EXECUTION_ID (with join)
              q(
                  "SELECT ji.JOB_INSTANCE_ID, ji.JOB_NAME, ji.JOB_KEY, ji.VERSION FROM BATCH_JOB_INSTANCE ji INNER JOIN BATCH_JOB_EXECUTION je ON ji.JOB_INSTANCE_ID = je.JOB_INSTANCE_ID WHERE je.JOB_EXECUTION_ID = ?"),
              // FIND_LAST_JOBS_BY_NAME
              q(
                  "SELECT JOB_INSTANCE_ID, JOB_NAME FROM BATCH_JOB_INSTANCE WHERE JOB_NAME = ? ORDER BY JOB_INSTANCE_ID DESC LIMIT ?"),
              // COUNT_JOBS_BY_NAME
              q("SELECT COUNT(JOB_INSTANCE_ID) FROM BATCH_JOB_INSTANCE WHERE JOB_NAME = ?"),
              // GET_JOB_NAMES
              q("SELECT DISTINCT JOB_NAME FROM BATCH_JOB_INSTANCE ORDER BY JOB_NAME"),
              // CREATE_JOB_EXECUTION
              q(
                  "INSERT INTO BATCH_JOB_EXECUTION (JOB_EXECUTION_ID, JOB_INSTANCE_ID, START_TIME, END_TIME, STATUS, EXIT_CODE, EXIT_MESSAGE, VERSION, CREATE_TIME, LAST_UPDATED) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"),
              // UPDATE_JOB_EXECUTION
              q(
                  "UPDATE BATCH_JOB_EXECUTION SET START_TIME = ?, END_TIME = ?, STATUS = ?, EXIT_CODE = ?, EXIT_MESSAGE = ?, VERSION = ?, CREATE_TIME = ?, LAST_UPDATED = ? WHERE JOB_EXECUTION_ID = ? AND VERSION = ?"),
              // GET_JOB_EXECUTIONS
              q(
                  "SELECT JOB_EXECUTION_ID, START_TIME, END_TIME, STATUS, EXIT_CODE, EXIT_MESSAGE, CREATE_TIME, LAST_UPDATED, VERSION FROM BATCH_JOB_EXECUTION WHERE JOB_INSTANCE_ID = ? ORDER BY JOB_EXECUTION_ID DESC"),
              // GET_RUNNING_EXECUTIONS
              q(
                  "SELECT je.JOB_EXECUTION_ID, je.START_TIME, je.END_TIME, je.STATUS, je.EXIT_CODE, je.CREATE_TIME, je.LAST_UPDATED, je.VERSION, je.JOB_INSTANCE_ID FROM BATCH_JOB_EXECUTION je WHERE je.END_TIME IS NULL"));

      QueryAuditReport report = analyzeNoCrash("spring-batch-jobs", queries, schema);
      assertNoFalsePositivesOnIndexedQueries(
          report,
          Set.of(
              IssueType.UNBOUNDED_RESULT_SET,
              IssueType.COVERING_INDEX_OPPORTUNITY,
              IssueType.COUNT_INSTEAD_OF_EXISTS,
              IssueType.NULL_COMPARISON),
          "Spring Batch job queries");
    }

    @Test
    @DisplayName("Spring Batch step execution queries")
    void stepExecutionQueries() {
      // Derived from JdbcStepExecutionDao
      List<QueryRecord> queries =
          List.of(
              // SAVE_STEP_EXECUTION
              q(
                  "INSERT INTO BATCH_STEP_EXECUTION (STEP_EXECUTION_ID, VERSION, STEP_NAME, JOB_EXECUTION_ID, START_TIME, END_TIME, STATUS, COMMIT_COUNT, READ_COUNT, FILTER_COUNT, WRITE_COUNT, EXIT_CODE, EXIT_MESSAGE, READ_SKIP_COUNT, WRITE_SKIP_COUNT, PROCESS_SKIP_COUNT, ROLLBACK_COUNT, LAST_UPDATED, CREATE_TIME) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"),
              // UPDATE_STEP_EXECUTION
              q(
                  "UPDATE BATCH_STEP_EXECUTION SET START_TIME = ?, END_TIME = ?, STATUS = ?, COMMIT_COUNT = ?, READ_COUNT = ?, FILTER_COUNT = ?, WRITE_COUNT = ?, EXIT_CODE = ?, EXIT_MESSAGE = ?, READ_SKIP_COUNT = ?, WRITE_SKIP_COUNT = ?, PROCESS_SKIP_COUNT = ?, ROLLBACK_COUNT = ?, LAST_UPDATED = ?, VERSION = ? WHERE STEP_EXECUTION_ID = ? AND VERSION = ?"),
              // GET_STEP_EXECUTIONS
              q(
                  "SELECT STEP_EXECUTION_ID, STEP_NAME, START_TIME, END_TIME, STATUS, COMMIT_COUNT, READ_COUNT, FILTER_COUNT, WRITE_COUNT, EXIT_CODE, EXIT_MESSAGE, READ_SKIP_COUNT, WRITE_SKIP_COUNT, PROCESS_SKIP_COUNT, ROLLBACK_COUNT, LAST_UPDATED, VERSION, CREATE_TIME FROM BATCH_STEP_EXECUTION WHERE JOB_EXECUTION_ID = ? ORDER BY STEP_EXECUTION_ID"),
              // COUNT_STEP_EXECUTIONS
              q(
                  "SELECT COUNT(STEP_EXECUTION_ID) FROM BATCH_STEP_EXECUTION WHERE JOB_EXECUTION_ID = ? AND STEP_NAME = ?"),
              // SAVE_STEP_EXECUTION_CONTEXT
              q(
                  "INSERT INTO BATCH_STEP_EXECUTION_CONTEXT (STEP_EXECUTION_ID, SHORT_CONTEXT, SERIALIZED_CONTEXT) VALUES (?, ?, ?)"),
              // UPDATE_STEP_EXECUTION_CONTEXT
              q(
                  "UPDATE BATCH_STEP_EXECUTION_CONTEXT SET SHORT_CONTEXT = ?, SERIALIZED_CONTEXT = ? WHERE STEP_EXECUTION_ID = ?"),
              // GET_STEP_EXECUTION_CONTEXT
              q(
                  "SELECT SHORT_CONTEXT, SERIALIZED_CONTEXT FROM BATCH_STEP_EXECUTION_CONTEXT WHERE STEP_EXECUTION_ID = ?"),
              // SAVE_JOB_EXECUTION_CONTEXT
              q(
                  "INSERT INTO BATCH_JOB_EXECUTION_CONTEXT (JOB_EXECUTION_ID, SHORT_CONTEXT, SERIALIZED_CONTEXT) VALUES (?, ?, ?)"),
              // GET_JOB_EXECUTION_CONTEXT
              q(
                  "SELECT SHORT_CONTEXT, SERIALIZED_CONTEXT FROM BATCH_JOB_EXECUTION_CONTEXT WHERE JOB_EXECUTION_ID = ?"),
              // GET_EXECUTION_PARAMS
              q(
                  "SELECT JOB_EXECUTION_ID, PARAMETER_NAME, PARAMETER_TYPE, PARAMETER_VALUE, IDENTIFYING FROM BATCH_JOB_EXECUTION_PARAMS WHERE JOB_EXECUTION_ID = ?"));

      QueryAuditReport report = analyzeNoCrash("spring-batch-steps", queries, schema);
      assertNoFalsePositivesOnIndexedQueries(
          report,
          Set.of(
              IssueType.UNBOUNDED_RESULT_SET,
              IssueType.COVERING_INDEX_OPPORTUNITY,
              IssueType.COUNT_INSTEAD_OF_EXISTS),
          "Spring Batch step queries");
    }
  }

  // ========================================================================
  // 7. MyBatis-generated patterns
  // ========================================================================

  @Nested
  @DisplayName("7. MyBatis dynamic SQL patterns")
  class MyBatisPatterns {

    private final IndexMetadata schema = mybatisSchema();

    @Test
    @DisplayName("MyBatis dynamic SQL with CASE WHEN")
    void dynamicSqlCaseWhen() {
      List<QueryRecord> queries =
          List.of(
              // Dynamic ORDER BY with CASE
              q(
                  "SELECT id, email, status, created_at FROM users WHERE tenant_id = ? AND deleted_at IS NULL ORDER BY CASE WHEN status = 'ACTIVE' THEN 0 WHEN status = 'PENDING' THEN 1 ELSE 2 END, created_at DESC LIMIT 20"),
              // Conditional aggregation
              q(
                  "SELECT category_id, COUNT(id) AS total, SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS active_count, SUM(CASE WHEN status = 'INACTIVE' THEN 1 ELSE 0 END) AS inactive_count FROM products WHERE tenant_id = ? GROUP BY category_id"),
              // Dynamic filter with CASE in WHERE
              q(
                  "SELECT id, user_id, status, created_at FROM orders WHERE tenant_id = ? AND status = ? AND created_at >= ? ORDER BY created_at DESC LIMIT 50"),
              // COALESCE default values
              q(
                  "SELECT id, COALESCE(email, 'N/A') AS email, COALESCE(status, 'UNKNOWN') AS status FROM users WHERE tenant_id = ? AND id = ?"));

      QueryAuditReport report = analyzeNoCrash("mybatis-case-when", queries, schema);
      // ORDER_BY_LIMIT_WITHOUT_INDEX on created_at is a true positive:
      // the users table has no index on created_at, so ORDER BY created_at
      // requires a filesort. This is a valid detection.
      assertNoFalsePositivesOnIndexedQueries(
          report,
          Set.of(
              IssueType.COVERING_INDEX_OPPORTUNITY,
              IssueType.UNBOUNDED_RESULT_SET,
              IssueType.ORDER_BY_LIMIT_WITHOUT_INDEX),
          "MyBatis CASE WHEN queries");
    }

    @Test
    @DisplayName("MyBatis nested result map queries")
    void nestedResultMapQueries() {
      List<QueryRecord> queries =
          List.of(
              // Order with items (one-to-many)
              q(
                  "SELECT o.id, o.user_id, o.status, o.created_at, oi.id AS item_id, oi.product_id, oi.quantity, oi.unit_price FROM orders o LEFT JOIN order_items oi ON o.id = oi.order_id WHERE o.id = ?"),
              // Product with category (many-to-one)
              q(
                  "SELECT p.id, p.category_id, p.version, c.id AS cat_id, c.parent_id FROM products p INNER JOIN categories c ON p.category_id = c.id WHERE p.id = ?"),
              // User with orders count
              q(
                  "SELECT u.id, u.email, u.status, COUNT(o.id) AS order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.tenant_id = ? AND u.deleted_at IS NULL GROUP BY u.id, u.email, u.status LIMIT 50"),
              // Category hierarchy (self-join)
              q(
                  "SELECT c.id, c.parent_id, c.tenant_id, p.id AS parent_cat_id FROM categories c LEFT JOIN categories p ON c.parent_id = p.id WHERE c.tenant_id = ?"));

      QueryAuditReport report = analyzeNoCrash("mybatis-nested", queries, schema);
      assertNoFalsePositivesOnIndexedQueries(
          report,
          Set.of(IssueType.COVERING_INDEX_OPPORTUNITY, IssueType.UNBOUNDED_RESULT_SET),
          "MyBatis nested result map queries");
    }

    @Test
    @DisplayName("MyBatis multi-parameter queries")
    void multiParameterQueries() {
      List<QueryRecord> queries =
          List.of(
              // Search with multiple optional filters
              q(
                  "SELECT id, email, status, created_at FROM users WHERE tenant_id = ? AND status = ? AND deleted_at IS NULL ORDER BY created_at DESC LIMIT ? OFFSET ?"),
              // Batch lookup by IDs
              q(
                  "SELECT id, email, status FROM users WHERE id IN (?, ?, ?, ?, ?) AND tenant_id = ?"),
              // Range query
              q(
                  "SELECT id, user_id, status, created_at FROM orders WHERE tenant_id = ? AND created_at BETWEEN ? AND ? AND status IN ('COMPLETED', 'SHIPPED') ORDER BY created_at DESC LIMIT 100"),
              // Update with multiple conditions
              q(
                  "UPDATE orders SET status = ?, updated_at = ? WHERE id = ? AND tenant_id = ? AND status = ?"),
              // Subquery for EXISTS check
              q(
                  "SELECT id, email FROM users WHERE tenant_id = ? AND EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id AND orders.status = 'COMPLETED') LIMIT 100"));

      QueryAuditReport report = analyzeNoCrash("mybatis-multi-param", queries, schema);
      // ORDER_BY_LIMIT_WITHOUT_INDEX on created_at is a true positive
      // (same as CASE WHEN test above -- users table lacks created_at index).
      assertNoFalsePositivesOnIndexedQueries(
          report,
          Set.of(
              IssueType.COVERING_INDEX_OPPORTUNITY,
              IssueType.OFFSET_PAGINATION,
              IssueType.CORRELATED_SUBQUERY,
              IssueType.ORDER_BY_LIMIT_WITHOUT_INDEX,
              IssueType.UNBOUNDED_RESULT_SET,
              IssueType.LIMIT_WITHOUT_ORDER_BY),
          "MyBatis multi-parameter queries");
    }
  }

  // ========================================================================
  // 8. Common ORM patterns
  // ========================================================================

  @Nested
  @DisplayName("8. Common ORM patterns")
  class CommonOrmPatterns {

    private final IndexMetadata schema = mybatisSchema();

    @Test
    @DisplayName("Soft delete pattern: WHERE deleted_at IS NULL")
    void softDeletePattern() {
      List<QueryRecord> queries =
          List.of(
              q(
                  "SELECT id, email, status FROM users WHERE tenant_id = ? AND deleted_at IS NULL ORDER BY id LIMIT 20"),
              q("SELECT id, email, status FROM users WHERE id = ? AND deleted_at IS NULL"),
              q("UPDATE users SET deleted_at = NOW() WHERE id = ? AND tenant_id = ?"),
              q(
                  "SELECT COUNT(id) FROM users WHERE tenant_id = ? AND deleted_at IS NULL AND status = 'ACTIVE'"),
              q("SELECT id, email FROM users WHERE email = ? AND deleted_at IS NULL"));

      QueryAuditReport report = analyzeNoCrash("orm-soft-delete", queries, schema);
      assertNoFalsePositivesOnIndexedQueries(
          report,
          Set.of(
              IssueType.COVERING_INDEX_OPPORTUNITY,
              IssueType.COUNT_INSTEAD_OF_EXISTS,
              IssueType.UNBOUNDED_RESULT_SET),
          "Soft delete pattern");
    }

    @Test
    @DisplayName(
        "Optimistic locking: UPDATE ... SET version = version + 1 WHERE id = ? AND version = ?")
    void optimisticLockingPattern() {
      List<QueryRecord> queries =
          List.of(
              q(
                  "UPDATE products SET name = ?, price = ?, version = version + 1 WHERE id = ? AND version = ?"),
              q(
                  "UPDATE users SET email = ?, status = ?, version = version + 1 WHERE id = ? AND version = ? AND tenant_id = ?"),
              q(
                  "UPDATE orders SET status = ?, updated_at = NOW(), version = version + 1 WHERE id = ? AND version = ?"),
              // JPA @Version style
              q(
                  "UPDATE products SET name = ?, price = ?, version = ? WHERE id = ? AND version = ?"),
              q("SELECT id, name, price, version FROM products WHERE id = ?"));

      QueryAuditReport report = analyzeNoCrash("orm-optimistic-lock", queries, schema);
      // NON_SARGABLE_EXPRESSION on "version = version + 1" should NOT fire
      // because the arithmetic is on the SET side, not the WHERE side
      assertNoFalsePositivesOnIndexedQueries(
          report, Set.of(IssueType.COVERING_INDEX_OPPORTUNITY), "Optimistic locking pattern");
    }

    @Test
    @DisplayName("Audit columns: created_at, updated_at")
    void auditColumnsPattern() {
      List<QueryRecord> queries =
          List.of(
              q(
                  "INSERT INTO audit_log (entity_type, entity_id, action, created_at, user_id) VALUES (?, ?, ?, NOW(), ?)"),
              q(
                  "SELECT id, entity_type, entity_id, action, created_at FROM audit_log WHERE entity_type = ? AND entity_id = ? ORDER BY created_at DESC LIMIT 50"),
              q(
                  "SELECT id, entity_type, entity_id, action, created_at FROM audit_log WHERE user_id = ? AND created_at >= ? ORDER BY created_at DESC LIMIT 100"),
              q(
                  "SELECT entity_type, COUNT(id) AS cnt FROM audit_log WHERE created_at >= ? AND created_at < ? GROUP BY entity_type"),
              q("DELETE FROM audit_log WHERE created_at < ?"));

      QueryAuditReport report = analyzeNoCrash("orm-audit-columns", queries, schema);
      assertNoFalsePositivesOnIndexedQueries(
          report,
          Set.of(IssueType.COVERING_INDEX_OPPORTUNITY, IssueType.UNBOUNDED_RESULT_SET),
          "Audit columns pattern");
    }

    @Test
    @DisplayName("Tenant isolation: WHERE tenant_id = ?")
    void tenantIsolationPattern() {
      List<QueryRecord> queries =
          List.of(
              q(
                  "SELECT id, email, status FROM users WHERE tenant_id = ? AND deleted_at IS NULL LIMIT 100"),
              q(
                  "SELECT id, user_id, status, created_at FROM orders WHERE tenant_id = ? AND status = ? ORDER BY created_at DESC LIMIT 50"),
              q("SELECT p.id, p.category_id FROM products p WHERE p.tenant_id = ? LIMIT 200"),
              q("UPDATE products SET price = ? WHERE id = ? AND tenant_id = ?"),
              q("DELETE FROM categories WHERE id = ? AND tenant_id = ?"),
              q("INSERT INTO products (id, category_id, tenant_id, version) VALUES (?, ?, ?, 1)"),
              q("SELECT id, parent_id FROM categories WHERE tenant_id = ? ORDER BY id"));

      QueryAuditReport report = analyzeNoCrash("orm-tenant-isolation", queries, schema);
      assertNoFalsePositivesOnIndexedQueries(
          report,
          Set.of(
              IssueType.COVERING_INDEX_OPPORTUNITY,
              IssueType.UNBOUNDED_RESULT_SET,
              IssueType.LIMIT_WITHOUT_ORDER_BY),
          "Tenant isolation pattern");
    }

    @Test
    @DisplayName("Pagination: LIMIT ? OFFSET ?")
    void paginationPattern() {
      List<QueryRecord> queries =
          List.of(
              // Standard pagination
              q(
                  "SELECT id, email, status, created_at FROM users WHERE tenant_id = ? AND deleted_at IS NULL ORDER BY created_at DESC LIMIT 20 OFFSET 0"),
              // Small offset is fine
              q(
                  "SELECT id, email, status, created_at FROM users WHERE tenant_id = ? AND deleted_at IS NULL ORDER BY created_at DESC LIMIT 20 OFFSET 20"),
              // Keyset pagination (best practice)
              q(
                  "SELECT id, email, status, created_at FROM users WHERE tenant_id = ? AND deleted_at IS NULL AND created_at < ? ORDER BY created_at DESC LIMIT 20"),
              // Count for pagination
              q("SELECT COUNT(id) FROM users WHERE tenant_id = ? AND deleted_at IS NULL"),
              // Order-based pagination
              q(
                  "SELECT id, user_id, status, created_at FROM orders WHERE tenant_id = ? ORDER BY id DESC LIMIT 50 OFFSET 0"));

      QueryAuditReport report = analyzeNoCrash("orm-pagination", queries, schema);
      // MISSING_WHERE_INDEX and ORDER_BY_LIMIT_WITHOUT_INDEX on created_at
      // are true positives: the users table schema lacks a created_at index,
      // so queries that filter/sort by created_at require a full scan.
      assertNoFalsePositivesOnIndexedQueries(
          report,
          Set.of(
              IssueType.COVERING_INDEX_OPPORTUNITY,
              IssueType.COUNT_INSTEAD_OF_EXISTS,
              IssueType.OFFSET_PAGINATION,
              IssueType.MISSING_WHERE_INDEX,
              IssueType.ORDER_BY_LIMIT_WITHOUT_INDEX),
          "Pagination pattern");
    }
  }

  // ========================================================================
  // Aggregate tests
  // ========================================================================

  @Test
  @DisplayName("All queries across all projects produce zero crashes")
  void zeroCrashesAcrossEntireCorpus() {
    // Collect every single query from every project into one big list
    List<QueryRecord> allQueries = new ArrayList<>();

    // ── PetClinic data.sql (actual INSERT statements from spring-petclinic) ──
    allQueries.addAll(
        List.of(
            q("INSERT IGNORE INTO vets VALUES (1, 'James', 'Carter')"),
            q("INSERT IGNORE INTO vets VALUES (2, 'Helen', 'Leary')"),
            q("INSERT IGNORE INTO vets VALUES (3, 'Linda', 'Douglas')"),
            q("INSERT IGNORE INTO vets VALUES (4, 'Rafael', 'Ortega')"),
            q("INSERT IGNORE INTO vets VALUES (5, 'Henry', 'Stevens')"),
            q("INSERT IGNORE INTO vets VALUES (6, 'Sharon', 'Jenkins')"),
            q("INSERT IGNORE INTO specialties VALUES (1, 'radiology')"),
            q("INSERT IGNORE INTO specialties VALUES (2, 'surgery')"),
            q("INSERT IGNORE INTO specialties VALUES (3, 'dentistry')"),
            q("INSERT IGNORE INTO vet_specialties VALUES (2, 1)"),
            q("INSERT IGNORE INTO vet_specialties VALUES (3, 2)"),
            q("INSERT IGNORE INTO types VALUES (1, 'cat')"),
            q("INSERT IGNORE INTO types VALUES (2, 'dog')"),
            q("INSERT IGNORE INTO types VALUES (3, 'lizard')"),
            q(
                "INSERT IGNORE INTO owners VALUES (1, 'George', 'Franklin', '110 W. Liberty St.', 'Madison', '6085551023')"),
            q(
                "INSERT IGNORE INTO owners VALUES (2, 'Betty', 'Davis', '638 Cardinal Ave.', 'Sun Prairie', '6085551749')"),
            q("INSERT IGNORE INTO pets VALUES (1, 'Leo', '2000-09-07', 1, 1)"),
            q("INSERT IGNORE INTO pets VALUES (2, 'Basil', '2002-08-06', 6, 2)"),
            q("INSERT IGNORE INTO visits VALUES (1, 7, '2010-03-04', 'rabies shot')"),
            q("INSERT IGNORE INTO visits VALUES (2, 8, '2011-03-04', 'rabies shot')")));

    // ── PetClinic JPA repository queries ──
    allQueries.addAll(
        List.of(
            q(
                "SELECT id, first_name, last_name, address, city, telephone FROM owners WHERE last_name LIKE ?"),
            q(
                "SELECT id, first_name, last_name, address, city, telephone FROM owners WHERE id = ?"),
            q("SELECT id, name FROM types ORDER BY name"),
            q("SELECT id, name, birth_date, type_id, owner_id FROM pets WHERE id = ?"),
            q("SELECT id, name, birth_date, type_id, owner_id FROM pets WHERE owner_id = ?"),
            q("SELECT id, visit_date, description, pet_id FROM visits WHERE pet_id = ?"),
            q("SELECT v.id, v.first_name, v.last_name FROM vets v ORDER BY v.last_name"),
            q(
                "SELECT vs.vet_id, vs.specialty_id, s.id, s.name FROM vet_specialties vs INNER JOIN specialties s ON vs.specialty_id = s.id WHERE vs.vet_id = ?"),
            q(
                "INSERT INTO owners (first_name, last_name, address, city, telephone) VALUES (?, ?, ?, ?, ?)"),
            q(
                "UPDATE owners SET first_name = ?, last_name = ?, address = ?, city = ?, telephone = ? WHERE id = ?"),
            q("INSERT INTO pets (name, birth_date, type_id, owner_id) VALUES (?, ?, ?, ?)"),
            q("INSERT INTO visits (visit_date, description, pet_id) VALUES (?, ?, ?)"),
            q("SELECT COUNT(id) FROM owners WHERE last_name = ?")));

    // ── JHipster authentication and audit ──
    allQueries.addAll(
        List.of(
            q(
                "SELECT id, login, password_hash, first_name, last_name, email, activated, lang_key FROM jhi_user WHERE login = ?"),
            q(
                "SELECT id, login, password_hash, first_name, last_name, email, activated FROM jhi_user WHERE email = ?"),
            q("SELECT ua.authority_name FROM jhi_user_authority ua WHERE ua.user_id = ?"),
            q("SELECT COUNT(id) FROM jhi_user WHERE login = ?"),
            q("SELECT name FROM jhi_authority"),
            q(
                "INSERT INTO jhi_user (login, password_hash, first_name, last_name, email, activated, lang_key, created_by, created_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"),
            q("INSERT INTO jhi_user_authority (user_id, authority_name) VALUES (?, ?)"),
            q(
                "UPDATE jhi_user SET first_name = ?, last_name = ?, email = ?, activated = ?, lang_key = ? WHERE id = ?"),
            q("UPDATE jhi_user SET activated = 1 WHERE id = ?"),
            q("DELETE FROM jhi_user_authority WHERE user_id = ?"),
            q(
                "INSERT INTO jhi_persistent_audit_event (principal, event_date, event_type) VALUES (?, ?, ?)"),
            q("INSERT INTO jhi_persistent_audit_evt_data (event_id, name, value) VALUES (?, ?, ?)"),
            q(
                "SELECT event_id, principal, event_date, event_type FROM jhi_persistent_audit_event WHERE principal = ? ORDER BY event_date DESC LIMIT 100"),
            q(
                "SELECT ID, AUTHOR, FILENAME, DATEEXECUTED, MD5SUM FROM DATABASECHANGELOG ORDER BY DATEEXECUTED, ORDEREXECUTED"),
            q(
                "INSERT INTO DATABASECHANGELOG (ID, AUTHOR, FILENAME, DATEEXECUTED, ORDEREXECUTED, MD5SUM, DESCRIPTION, COMMENTS, TAG, LIQUIBASE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"),
            q("SELECT ID, LOCKED, LOCKGRANTED, LOCKEDBY FROM DATABASECHANGELOGLOCK WHERE ID = 1"),
            q(
                "UPDATE DATABASECHANGELOGLOCK SET LOCKED = 1, LOCKGRANTED = ?, LOCKEDBY = ? WHERE ID = 1 AND LOCKED = 0")));

    // ── Keycloak user, realm, client, role, session queries ──
    allQueries.addAll(
        List.of(
            q(
                "SELECT u.ID, u.USERNAME, u.EMAIL, u.FIRST_NAME, u.LAST_NAME, u.ENABLED, u.EMAIL_VERIFIED, u.REALM_ID FROM USER_ENTITY u WHERE u.USERNAME = ? AND u.REALM_ID = ?"),
            q(
                "SELECT u.ID, u.USERNAME, u.EMAIL, u.FIRST_NAME, u.LAST_NAME, u.ENABLED FROM USER_ENTITY u WHERE u.EMAIL = ? AND u.REALM_ID = ?"),
            q(
                "SELECT u.ID, u.USERNAME, u.EMAIL FROM USER_ENTITY u WHERE u.SERVICE_ACCOUNT_CLIENT_LINK = ? AND u.REALM_ID = ?"),
            q("DELETE FROM USER_ENTITY WHERE REALM_ID = ?"),
            q("DELETE FROM USER_ENTITY WHERE REALM_ID = ? AND FEDERATION_LINK = ?"),
            q(
                "UPDATE USER_ENTITY SET FEDERATION_LINK = NULL WHERE REALM_ID = ? AND FEDERATION_LINK = ?"),
            q("SELECT ID FROM REALM"),
            q("SELECT ID FROM REALM WHERE NAME = ?"),
            q("SELECT DISTINCT c.REALM_ID FROM COMPONENT c WHERE c.PROVIDER_TYPE = ?"),
            q(
                "SELECT c.ID, c.CLIENT_ID, c.NAME, c.ENABLED, c.PROTOCOL FROM CLIENT c WHERE c.ID = ? AND c.REALM_ID = ?"),
            q("SELECT c.ID FROM CLIENT c WHERE c.CLIENT_ID = ? AND c.REALM_ID = ?"),
            q(
                "SELECT urm.USER_ID, urm.ROLE_ID, kr.NAME, kr.REALM_ID FROM USER_ROLE_MAPPING urm INNER JOIN KEYCLOAK_ROLE kr ON urm.ROLE_ID = kr.ID WHERE urm.USER_ID = ?"),
            q("INSERT INTO USER_ROLE_MAPPING (USER_ID, ROLE_ID) VALUES (?, ?)"),
            q("DELETE FROM USER_ROLE_MAPPING WHERE USER_ID = ? AND ROLE_ID = ?"),
            q("SELECT us.ID, us.USER_ID, us.REALM_ID FROM USER_SESSION us WHERE us.USER_ID = ?"),
            q(
                "SELECT cs.ID, cs.CLIENT_ID, cs.SESSION_ID FROM CLIENT_SESSION cs WHERE cs.SESSION_ID = ?"),
            q(
                "SELECT c.ID, c.USER_ID, c.TYPE, c.SECRET_DATA, c.CREDENTIAL_DATA, c.PRIORITY FROM CREDENTIAL c WHERE c.USER_ID = ? ORDER BY c.PRIORITY"),
            q("DELETE FROM CREDENTIAL WHERE USER_ID = ?")));

    // ── SkyWalking metrics, traces, alarms ──
    allQueries.addAll(
        List.of(
            q(
                "SELECT entity_id, SUM(value) AS total_value FROM service_cpm_day WHERE time_bucket >= ? AND time_bucket <= ? GROUP BY entity_id ORDER BY total_value DESC LIMIT 20"),
            q(
                "SELECT entity_id, AVG(value) AS avg_value FROM endpoint_cpm_day WHERE entity_id = ? AND time_bucket >= ? AND time_bucket <= ? GROUP BY entity_id"),
            q(
                "SELECT id, service_name, group_name FROM service_traffic WHERE service_name LIKE ? LIMIT 50"),
            q("SELECT id, service_id FROM endpoint_traffic WHERE service_id = ? LIMIT 100"),
            q(
                "SELECT id, trace_id, service_id, endpoint_id, latency FROM segment s INNER JOIN segment_tag st ON s.id = st.segment_id WHERE s.service_id = ? AND s.time_bucket >= ? ORDER BY s.time_bucket DESC LIMIT 20"),
            q(
                "SELECT id, trace_id, service_id, endpoint_id, latency, is_error, time_bucket FROM segment WHERE service_id = ? AND time_bucket >= ? AND time_bucket <= ? ORDER BY time_bucket DESC LIMIT 20"),
            q(
                "SELECT COUNT(id) FROM segment WHERE service_id = ? AND time_bucket >= ? AND time_bucket <= ?"),
            q("DELETE FROM segment WHERE time_bucket < ? LIMIT 5000"),
            q(
                "SELECT id, scope_id, name, alarm_message, time_bucket FROM alarm_record WHERE time_bucket >= ? AND time_bucket <= ? ORDER BY time_bucket DESC LIMIT 20"),
            q(
                "INSERT INTO alarm_record (id, scope_id, name, alarm_message, start_time, time_bucket, tags) VALUES (?, ?, ?, ?, ?, ?, ?)"),
            q("DELETE FROM alarm_record WHERE time_bucket < ? LIMIT 5000")));

    // ── Quartz Scheduler: jobs, triggers, locks ──
    allQueries.addAll(
        List.of(
            q(
                "SELECT JOB_NAME, JOB_GROUP, DESCRIPTION, JOB_CLASS_NAME, IS_DURABLE, REQUESTS_RECOVERY, JOB_DATA FROM QRTZ_JOB_DETAILS WHERE SCHED_NAME = ? AND JOB_NAME = ? AND JOB_GROUP = ?"),
            q(
                "SELECT TRIGGER_NAME, TRIGGER_GROUP, JOB_NAME, JOB_GROUP, DESCRIPTION, NEXT_FIRE_TIME, TRIGGER_STATE FROM QRTZ_TRIGGERS WHERE SCHED_NAME = ? AND TRIGGER_NAME = ? AND TRIGGER_GROUP = ?"),
            q(
                "SELECT TRIGGER_STATE FROM QRTZ_TRIGGERS WHERE SCHED_NAME = ? AND TRIGGER_NAME = ? AND TRIGGER_GROUP = ?"),
            q(
                "INSERT INTO QRTZ_JOB_DETAILS (SCHED_NAME, JOB_NAME, JOB_GROUP, DESCRIPTION, JOB_CLASS_NAME, IS_DURABLE, IS_NONCONCURRENT, IS_UPDATE_DATA, REQUESTS_RECOVERY, JOB_DATA) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"),
            q(
                "UPDATE QRTZ_JOB_DETAILS SET DESCRIPTION = ?, JOB_CLASS_NAME = ?, IS_DURABLE = ? WHERE SCHED_NAME = ? AND JOB_NAME = ? AND JOB_GROUP = ?"),
            q(
                "DELETE FROM QRTZ_JOB_DETAILS WHERE SCHED_NAME = ? AND JOB_NAME = ? AND JOB_GROUP = ?"),
            q("SELECT LOCK_NAME FROM QRTZ_LOCKS WHERE SCHED_NAME = ? AND LOCK_NAME = ? FOR UPDATE"),
            q("INSERT INTO QRTZ_LOCKS (SCHED_NAME, LOCK_NAME) VALUES (?, ?)"),
            q(
                "SELECT TRIGGER_NAME, TRIGGER_GROUP, NEXT_FIRE_TIME, PRIORITY FROM QRTZ_TRIGGERS WHERE SCHED_NAME = ? AND TRIGGER_STATE = ? AND NEXT_FIRE_TIME <= ? ORDER BY NEXT_FIRE_TIME, PRIORITY LIMIT ?"),
            q(
                "UPDATE QRTZ_TRIGGERS SET TRIGGER_STATE = ? WHERE SCHED_NAME = ? AND TRIGGER_NAME = ? AND TRIGGER_GROUP = ?"),
            q(
                "INSERT INTO QRTZ_FIRED_TRIGGERS (SCHED_NAME, ENTRY_ID, TRIGGER_NAME, TRIGGER_GROUP, INSTANCE_NAME, FIRED_TIME, SCHED_TIME, PRIORITY, STATE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"),
            q("DELETE FROM QRTZ_FIRED_TRIGGERS WHERE SCHED_NAME = ? AND ENTRY_ID = ?"),
            q(
                "SELECT INSTANCE_NAME, LAST_CHECKIN_TIME, CHECKIN_INTERVAL FROM QRTZ_SCHEDULER_STATE WHERE SCHED_NAME = ?"),
            q(
                "UPDATE QRTZ_SCHEDULER_STATE SET LAST_CHECKIN_TIME = ? WHERE SCHED_NAME = ? AND INSTANCE_NAME = ?"),
            q(
                "INSERT INTO QRTZ_SIMPLE_TRIGGERS (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP, REPEAT_COUNT, REPEAT_INTERVAL, TIMES_TRIGGERED) VALUES (?, ?, ?, ?, ?, ?)"),
            q(
                "INSERT INTO QRTZ_CRON_TRIGGERS (SCHED_NAME, TRIGGER_NAME, TRIGGER_GROUP, CRON_EXPRESSION, TIME_ZONE_ID) VALUES (?, ?, ?, ?, ?)")));

    // ── Spring Batch: job instances, executions, steps ──
    allQueries.addAll(
        List.of(
            q(
                "INSERT INTO BATCH_JOB_INSTANCE (JOB_INSTANCE_ID, JOB_NAME, JOB_KEY, VERSION) VALUES (?, ?, ?, ?)"),
            q(
                "SELECT JOB_INSTANCE_ID, JOB_NAME FROM BATCH_JOB_INSTANCE WHERE JOB_NAME = ? AND JOB_KEY = ?"),
            q(
                "SELECT JOB_INSTANCE_ID, JOB_NAME, JOB_KEY, VERSION FROM BATCH_JOB_INSTANCE WHERE JOB_INSTANCE_ID = ?"),
            q(
                "SELECT ji.JOB_INSTANCE_ID, ji.JOB_NAME, ji.JOB_KEY, ji.VERSION FROM BATCH_JOB_INSTANCE ji INNER JOIN BATCH_JOB_EXECUTION je ON ji.JOB_INSTANCE_ID = je.JOB_INSTANCE_ID WHERE je.JOB_EXECUTION_ID = ?"),
            q("SELECT COUNT(JOB_INSTANCE_ID) FROM BATCH_JOB_INSTANCE WHERE JOB_NAME = ?"),
            q("SELECT DISTINCT JOB_NAME FROM BATCH_JOB_INSTANCE ORDER BY JOB_NAME"),
            q(
                "INSERT INTO BATCH_JOB_EXECUTION (JOB_EXECUTION_ID, JOB_INSTANCE_ID, START_TIME, END_TIME, STATUS, EXIT_CODE, EXIT_MESSAGE, VERSION, CREATE_TIME, LAST_UPDATED) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"),
            q(
                "UPDATE BATCH_JOB_EXECUTION SET STATUS = ?, EXIT_CODE = ?, EXIT_MESSAGE = ?, VERSION = ?, LAST_UPDATED = ? WHERE JOB_EXECUTION_ID = ? AND VERSION = ?"),
            q(
                "SELECT JOB_EXECUTION_ID, START_TIME, END_TIME, STATUS, EXIT_CODE, CREATE_TIME, LAST_UPDATED, VERSION FROM BATCH_JOB_EXECUTION WHERE JOB_INSTANCE_ID = ? ORDER BY JOB_EXECUTION_ID DESC"),
            q(
                "INSERT INTO BATCH_STEP_EXECUTION (STEP_EXECUTION_ID, VERSION, STEP_NAME, JOB_EXECUTION_ID, START_TIME, END_TIME, STATUS, COMMIT_COUNT, READ_COUNT, WRITE_COUNT, EXIT_CODE, EXIT_MESSAGE, LAST_UPDATED, CREATE_TIME) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"),
            q(
                "UPDATE BATCH_STEP_EXECUTION SET STATUS = ?, COMMIT_COUNT = ?, READ_COUNT = ?, WRITE_COUNT = ?, EXIT_CODE = ?, EXIT_MESSAGE = ?, VERSION = ?, LAST_UPDATED = ? WHERE STEP_EXECUTION_ID = ? AND VERSION = ?"),
            q(
                "SELECT STEP_EXECUTION_ID, STEP_NAME, STATUS, COMMIT_COUNT, READ_COUNT, WRITE_COUNT, EXIT_CODE, LAST_UPDATED, VERSION FROM BATCH_STEP_EXECUTION WHERE JOB_EXECUTION_ID = ? ORDER BY STEP_EXECUTION_ID"),
            q(
                "INSERT INTO BATCH_STEP_EXECUTION_CONTEXT (STEP_EXECUTION_ID, SHORT_CONTEXT, SERIALIZED_CONTEXT) VALUES (?, ?, ?)"),
            q(
                "SELECT SHORT_CONTEXT, SERIALIZED_CONTEXT FROM BATCH_STEP_EXECUTION_CONTEXT WHERE STEP_EXECUTION_ID = ?"),
            q(
                "INSERT INTO BATCH_JOB_EXECUTION_CONTEXT (JOB_EXECUTION_ID, SHORT_CONTEXT, SERIALIZED_CONTEXT) VALUES (?, ?, ?)"),
            q(
                "SELECT JOB_EXECUTION_ID, PARAMETER_NAME, PARAMETER_TYPE, PARAMETER_VALUE FROM BATCH_JOB_EXECUTION_PARAMS WHERE JOB_EXECUTION_ID = ?")));

    // ── MyBatis / ORM patterns ──
    allQueries.addAll(
        List.of(
            q(
                "SELECT id, email, status FROM users WHERE tenant_id = ? AND deleted_at IS NULL ORDER BY CASE WHEN status = 'ACTIVE' THEN 0 ELSE 1 END LIMIT 20"),
            q(
                "SELECT category_id, COUNT(id) AS total FROM products WHERE tenant_id = ? GROUP BY category_id"),
            q(
                "SELECT o.id, o.user_id, oi.product_id, oi.quantity FROM orders o LEFT JOIN order_items oi ON o.id = oi.order_id WHERE o.id = ?"),
            q(
                "SELECT p.id, p.category_id, c.id AS cat_id FROM products p INNER JOIN categories c ON p.category_id = c.id WHERE p.id = ?"),
            q("SELECT id, email, status FROM users WHERE id IN (?, ?, ?) AND tenant_id = ?"),
            q(
                "SELECT id, user_id, status FROM orders WHERE tenant_id = ? AND created_at BETWEEN ? AND ? AND status IN ('COMPLETED', 'SHIPPED') ORDER BY created_at DESC LIMIT 100"),
            q(
                "UPDATE orders SET status = ?, updated_at = ? WHERE id = ? AND tenant_id = ? AND status = ?"),
            q("SELECT id, email FROM users WHERE tenant_id = ? AND deleted_at IS NULL LIMIT 100"),
            q("SELECT id, email, status FROM users WHERE id = ? AND deleted_at IS NULL"),
            q("UPDATE users SET deleted_at = NOW() WHERE id = ? AND tenant_id = ?"),
            q(
                "UPDATE products SET name = ?, price = ?, version = version + 1 WHERE id = ? AND version = ?"),
            q(
                "UPDATE users SET email = ?, status = ?, version = version + 1 WHERE id = ? AND version = ? AND tenant_id = ?"),
            q("SELECT id, name, price, version FROM products WHERE id = ?"),
            q(
                "INSERT INTO audit_log (entity_type, entity_id, action, created_at, user_id) VALUES (?, ?, ?, NOW(), ?)"),
            q(
                "SELECT id, entity_type, entity_id, action, created_at FROM audit_log WHERE entity_type = ? AND entity_id = ? ORDER BY created_at DESC LIMIT 50"),
            q(
                "SELECT id, entity_type, entity_id, action, created_at FROM audit_log WHERE user_id = ? AND created_at >= ? ORDER BY created_at DESC LIMIT 100"),
            q("DELETE FROM audit_log WHERE created_at < ?"),
            q(
                "SELECT id, email, status, created_at FROM users WHERE tenant_id = ? AND deleted_at IS NULL ORDER BY created_at DESC LIMIT 20 OFFSET 0"),
            q(
                "SELECT id, email, status, created_at FROM users WHERE tenant_id = ? AND deleted_at IS NULL AND created_at < ? ORDER BY created_at DESC LIMIT 20"),
            q("SELECT COUNT(id) FROM users WHERE tenant_id = ? AND deleted_at IS NULL"),
            q(
                "SELECT id, user_id, status FROM orders WHERE tenant_id = ? ORDER BY id DESC LIMIT 50 OFFSET 0"),
            q("DELETE FROM categories WHERE id = ? AND tenant_id = ?"),
            q("INSERT INTO products (id, category_id, tenant_id, version) VALUES (?, ?, ?, 1)"),
            q("SELECT id, parent_id FROM categories WHERE tenant_id = ? ORDER BY id")));

    // Verify total count: at least 100 queries
    assertThat(allQueries.size())
        .as("Total query count should be at least 100")
        .isGreaterThanOrEqualTo(100);

    System.out.println("Total queries in open-source corpus: " + allQueries.size());

    // Run all through analyzer with a combined schema -- should not crash on any
    IndexMetadata combinedSchema = combinedSchema();

    assertThatCode(() -> analyzer.analyze("open-source-corpus-all", allQueries, combinedSchema))
        .as("Analyzer must not crash on any open-source query")
        .doesNotThrowAnyException();

    QueryAuditReport report =
        analyzer.analyze("open-source-corpus-all", allQueries, combinedSchema);
    assertThat(report).isNotNull();

    System.out.println("=== Open Source Corpus Aggregate Report ===");
    System.out.println("Total queries: " + report.getTotalQueryCount());
    System.out.println("Unique patterns: " + report.getUniquePatternCount());
    System.out.println("Confirmed issues (ERROR/WARNING): " + report.getConfirmedIssues().size());
    System.out.println("Info issues: " + report.getInfoIssues().size());
    if (!report.getConfirmedIssues().isEmpty()) {
      System.out.println("Confirmed issue breakdown:");
      report.getConfirmedIssues().stream()
          .collect(Collectors.groupingBy(i -> i.type().getCode(), Collectors.counting()))
          .forEach((code, count) -> System.out.println("  " + code + ": " + count));
    }
    System.out.println("=".repeat(50));
  }

  @Test
  @DisplayName("Each project's queries analyzed independently produce no crashes")
  void eachProjectNoCrash() {
    record ProjectCorpus(String name, List<QueryRecord> queries, IndexMetadata schema) {}

    List<ProjectCorpus> corpora =
        List.of(
            new ProjectCorpus(
                "petclinic",
                List.of(
                    q("SELECT id, first_name, last_name FROM owners WHERE last_name LIKE ?"),
                    q("SELECT id, name FROM types ORDER BY name"),
                    q(
                        "SELECT id, name, birth_date, type_id, owner_id FROM pets WHERE owner_id = ?"),
                    q("INSERT INTO visits (visit_date, description, pet_id) VALUES (?, ?, ?)")),
                petClinicSchema()),
            new ProjectCorpus(
                "jhipster",
                List.of(
                    q("SELECT id, login, email FROM jhi_user WHERE login = ?"),
                    q("INSERT INTO jhi_user_authority (user_id, authority_name) VALUES (?, ?)"),
                    q("SELECT ID, LOCKED FROM DATABASECHANGELOGLOCK WHERE ID = 1")),
                jhipsterSchema()),
            new ProjectCorpus(
                "keycloak",
                List.of(
                    q(
                        "SELECT u.ID, u.USERNAME FROM USER_ENTITY u WHERE u.USERNAME = ? AND u.REALM_ID = ?"),
                    q("SELECT c.ID FROM CLIENT c WHERE c.CLIENT_ID = ? AND c.REALM_ID = ?"),
                    q("SELECT ID FROM REALM WHERE NAME = ?")),
                keycloakSchema()),
            new ProjectCorpus(
                "skywalking",
                List.of(
                    q(
                        "SELECT entity_id, SUM(value) FROM service_cpm_day WHERE time_bucket >= ? AND time_bucket <= ? GROUP BY entity_id LIMIT 20"),
                    q("SELECT id, trace_id, service_id FROM segment WHERE trace_id = ?"),
                    q(
                        "INSERT INTO alarm_record (id, scope_id, name, time_bucket) VALUES (?, ?, ?, ?)")),
                skyWalkingSchema()),
            new ProjectCorpus(
                "quartz",
                List.of(
                    q(
                        "SELECT LOCK_NAME FROM QRTZ_LOCKS WHERE SCHED_NAME = ? AND LOCK_NAME = ? FOR UPDATE"),
                    q(
                        "SELECT TRIGGER_NAME, TRIGGER_GROUP FROM QRTZ_TRIGGERS WHERE SCHED_NAME = ? AND JOB_NAME = ? AND JOB_GROUP = ?"),
                    q(
                        "INSERT INTO QRTZ_JOB_DETAILS (SCHED_NAME, JOB_NAME, JOB_GROUP) VALUES (?, ?, ?)")),
                quartzSchema()),
            new ProjectCorpus(
                "spring-batch",
                List.of(
                    q(
                        "INSERT INTO BATCH_JOB_INSTANCE (JOB_INSTANCE_ID, JOB_NAME, JOB_KEY, VERSION) VALUES (?, ?, ?, ?)"),
                    q(
                        "SELECT JOB_INSTANCE_ID FROM BATCH_JOB_INSTANCE WHERE JOB_NAME = ? AND JOB_KEY = ?"),
                    q(
                        "UPDATE BATCH_STEP_EXECUTION SET STATUS = ? WHERE STEP_EXECUTION_ID = ? AND VERSION = ?")),
                springBatchSchema()),
            new ProjectCorpus(
                "mybatis",
                List.of(
                    q(
                        "SELECT id, email FROM users WHERE tenant_id = ? AND deleted_at IS NULL LIMIT 20"),
                    q("UPDATE products SET version = version + 1 WHERE id = ? AND version = ?"),
                    q(
                        "SELECT id, user_id FROM orders WHERE tenant_id = ? ORDER BY created_at DESC LIMIT 50 OFFSET 0")),
                mybatisSchema()));

    for (ProjectCorpus corpus : corpora) {
      assertThatCode(() -> analyzer.analyze(corpus.name(), corpus.queries(), corpus.schema()))
          .as("Analyzer must not crash on " + corpus.name() + " queries")
          .doesNotThrowAnyException();
    }
  }

  // -- Combined schema for aggregate tests --

  private static IndexMetadata combinedSchema() {
    Map<String, List<IndexInfo>> combined = new HashMap<>();

    // Add all project schemas
    Stream.of(
            petClinicSchema(),
            jhipsterSchema(),
            keycloakSchema(),
            skyWalkingSchema(),
            quartzSchema(),
            springBatchSchema(),
            mybatisSchema())
        .forEach(
            schema -> {
              // Merge indexes from each schema
              for (String table :
                  List.of(
                      "vets",
                      "specialties",
                      "vet_specialties",
                      "types",
                      "owners",
                      "pets",
                      "visits",
                      "jhi_user",
                      "jhi_authority",
                      "jhi_user_authority",
                      "jhi_persistent_audit_event",
                      "jhi_persistent_audit_evt_data",
                      "DATABASECHANGELOG",
                      "DATABASECHANGELOGLOCK",
                      "USER_ENTITY",
                      "REALM",
                      "CLIENT",
                      "USER_ROLE_MAPPING",
                      "CREDENTIAL",
                      "USER_SESSION",
                      "CLIENT_SESSION",
                      "USER_ATTRIBUTE",
                      "COMPONENT",
                      "KEYCLOAK_ROLE",
                      "service_traffic",
                      "endpoint_traffic",
                      "instance_traffic",
                      "segment",
                      "segment_tag",
                      "alarm_record",
                      "service_cpm_day",
                      "endpoint_cpm_day",
                      "service_resp_time_day",
                      "QRTZ_JOB_DETAILS",
                      "QRTZ_TRIGGERS",
                      "QRTZ_SIMPLE_TRIGGERS",
                      "QRTZ_CRON_TRIGGERS",
                      "QRTZ_BLOB_TRIGGERS",
                      "QRTZ_FIRED_TRIGGERS",
                      "QRTZ_LOCKS",
                      "QRTZ_SCHEDULER_STATE",
                      "QRTZ_PAUSED_TRIGGER_GRPS",
                      "QRTZ_CALENDARS",
                      "BATCH_JOB_INSTANCE",
                      "BATCH_JOB_EXECUTION",
                      "BATCH_JOB_EXECUTION_PARAMS",
                      "BATCH_STEP_EXECUTION",
                      "BATCH_STEP_EXECUTION_CONTEXT",
                      "BATCH_JOB_EXECUTION_CONTEXT",
                      "users",
                      "orders",
                      "products",
                      "order_items",
                      "categories",
                      "audit_log")) {
                List<IndexInfo> indexes = schema.getIndexesForTable(table);
                if (indexes != null && !indexes.isEmpty()) {
                  combined.putIfAbsent(table, indexes);
                }
              }
            });
    return new IndexMetadata(combined);
  }
}
