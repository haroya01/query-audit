package io.queryaudit.spring.slice;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.junit5.BooleanOverride;
import io.queryaudit.junit5.ExpectQueries;
import io.queryaudit.junit5.QueryAudit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.jdbc.core.JdbcTemplate;

@JdbcTest
@QueryAudit(autoOpenReport = BooleanOverride.FALSE)
class JdbcSliceCaptureTest {

  @Autowired JdbcTemplate jdbc;

  @Test
  @ExpectQueries(select = 1, exact = true)
  void capturesQueriesWithoutImportingTheAutoConfiguration() {
    assertThat(jdbc.queryForObject("select 1", Integer.class)).isEqualTo(1);
  }

  @SpringBootConfiguration
  static class SliceApplication {}
}
