package example.audit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.queryaudit.junit5.EnableQueryInspector;
import io.queryaudit.junit5.ExpectQueries;
import javax.sql.DataSource;
import net.ttddyy.dsproxy.support.ProxyDataSourceBuilder;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

@EnableQueryInspector
class FirstAuditTest {

  static final DataSource DATA_SOURCE = dataSource();

  @Test
  @ExpectQueries(select = 1, insert = 0, update = 0, delete = 0)
  void readsOnce() throws Exception {
    readOne();
    if (Boolean.getBoolean("queryAudit.demo.extraQuery")) {
      readOne();
    }
    if (Boolean.getBoolean("queryAudit.demo.extraWrite")) {
      writeOnReadPath();
    }
  }

  private void readOne() throws Exception {
    try (var connection = DATA_SOURCE.getConnection();
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT status FROM orders WHERE id = 1 LIMIT 1")) {
      assertTrue(result.next());
      assertEquals("NEW", result.getString("status"));
    }
  }

  private void writeOnReadPath() throws Exception {
    try (var connection = DATA_SOURCE.getConnection();
        var statement = connection.createStatement()) {
      // The returned order is unchanged, but the read path now executes a write.
      assertEquals(1, statement.executeUpdate("UPDATE orders SET status = 'NEW' WHERE id = 1"));
    }
  }

  private static DataSource dataSource() {
    var h2 = new JdbcDataSource();
    h2.setURL("jdbc:h2:mem:firstaudit;DB_CLOSE_DELAY=-1");
    // Prepare the fixture before the audited test starts.
    try (var connection = h2.getConnection();
        var statement = connection.createStatement()) {
      statement.execute("CREATE TABLE orders (id INT PRIMARY KEY, status VARCHAR(20))");
      statement.executeUpdate("INSERT INTO orders (id, status) VALUES (1, 'NEW')");
    } catch (java.sql.SQLException exception) {
      throw new IllegalStateException("Cannot prepare the order fixture", exception);
    }
    return ProxyDataSourceBuilder.create(h2).name("first-audit").build();
  }
}
