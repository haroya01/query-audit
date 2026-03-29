package io.queryaudit.junit5.integration.repository;

import io.queryaudit.junit5.integration.entity.Member;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface MemberRepository extends JpaRepository<Member, Long> {

  List<Member> findByStatus(String status);

  @Query(value = "SELECT * FROM members WHERE email = ?1", nativeQuery = true)
  Member findByEmailNative(String email);

  List<Member> findByNameContaining(String name);

  boolean existsByEmail(String email);

  void deleteByStatus(String status);
}
