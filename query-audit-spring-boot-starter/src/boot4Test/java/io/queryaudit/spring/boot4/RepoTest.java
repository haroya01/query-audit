package io.queryaudit.spring.boot4;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

/**
 * Context signature 3 of the issue #134 reproduction: {@code @Transactional} test around the
 * repository. Runs before {@link ServiceTest} (class-name order) and shares its cached context.
 */
@SpringBootTest
@ActiveProfiles("test")
@Transactional
class RepoTest {

  @Autowired private ItemRepository repository;

  @Test
  void saveAndLoad() {
    repository.save(new Item("repo"));
    assertThat(repository.findAll()).isNotEmpty();
  }
}
