package io.queryaudit.junit5.integration.repository;

import io.queryaudit.junit5.integration.entity.Product;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductRepository extends JpaRepository<Product, Long> {}
