package io.queryaudit.junit5.integration.repository;

import io.queryaudit.junit5.integration.entity.Team;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TeamRepository extends JpaRepository<Team, Long> {}
