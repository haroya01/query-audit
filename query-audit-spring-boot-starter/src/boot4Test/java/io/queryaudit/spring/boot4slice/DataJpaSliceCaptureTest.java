package io.queryaudit.spring.boot4slice;

import static org.assertj.core.api.Assertions.assertThat;

import io.queryaudit.junit5.BooleanOverride;
import io.queryaudit.junit5.ExpectQueries;
import io.queryaudit.junit5.QueryAudit;
import io.queryaudit.spring.QueryAuditAutoConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.annotation.ImportCandidates;
import org.springframework.boot.data.jpa.test.autoconfigure.AutoConfigureDataJpa;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureJdbc;
import org.springframework.boot.jpa.test.autoconfigure.TestEntityManager;

@DataJpaTest
@QueryAudit(autoOpenReport = BooleanOverride.FALSE)
class DataJpaSliceCaptureTest {

  @Autowired NoteRepository notes;
  @Autowired TestEntityManager entityManager;

  @BeforeEach
  void seed() {
    notes.save(new Note("first"));
    entityManager.flush();
    entityManager.clear();
  }

  @Test
  @ExpectQueries(select = 1, exact = true)
  void capturesQueriesWithoutImportingTheAutoConfiguration() {
    assertThat(notes.count()).isEqualTo(1);
  }

  @Test
  void boot4SlicesImportTheAutoConfiguration() {
    ClassLoader loader = getClass().getClassLoader();
    assertThat(ImportCandidates.load(AutoConfigureDataJpa.class, loader))
        .contains(QueryAuditAutoConfiguration.class.getName());
    assertThat(ImportCandidates.load(AutoConfigureJdbc.class, loader))
        .contains(QueryAuditAutoConfiguration.class.getName());
  }
}
