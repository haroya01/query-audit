package io.queryaudit.spring.boot4;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

/**
 * The reported victim of issue #134: runs last (class-name order), reuses the context cached by
 * {@link RepoTest}, and in the report failed with {@code HikariDataSource has been closed} because
 * a sibling context's shutdown had cascaded into this context's pool.
 */
@SpringBootTest
@ActiveProfiles("test")
@Transactional
class ServiceTest {

  @Autowired private ItemService service;

  @Test
  void createAndList() {
    service.create("svc");
    assertThat(service.findAll()).isNotEmpty();
  }

  @Test
  void listAgainOnReusedContext() {
    assertThat(service.findAll()).isNotNull();
  }
}
