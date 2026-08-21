package io.queryaudit.spring.boot4;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;

/**
 * Context signature 2 of the issue #134 reproduction: {@code @AutoConfigureMockMvc} changes the
 * TestContext cache key, so this class gets its own ApplicationContext and its own connection pool.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("test")
@Transactional
class ControllerTest {

  @Autowired private MockMvc mockMvc;

  @Test
  void itemsEndpointHitsTheDatabase() throws Exception {
    mockMvc.perform(get("/items")).andExpect(status().isOk());
  }
}
