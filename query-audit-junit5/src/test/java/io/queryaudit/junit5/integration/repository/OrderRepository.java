package io.queryaudit.junit5.integration.repository;

import io.queryaudit.junit5.integration.entity.Order;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<Order, Long> {

  List<Order> findByStatus(String status);
}
