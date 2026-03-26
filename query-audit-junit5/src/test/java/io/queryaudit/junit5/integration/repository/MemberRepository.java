package io.queryaudit.junit5.integration.repository;

import io.queryaudit.junit5.integration.entity.Member;
import java.util.List;
import java.util.Optional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface MemberRepository extends JpaRepository<Member, Long> {

  List<Member> findByStatus(String status);

  @Query(value = "SELECT * FROM members WHERE email = ?1", nativeQuery = true)
  Member findByEmailNative(String email);

  List<Member> findByNameContaining(String name);

  /** Derived query: Spring Data adds LIMIT automatically for Optional return type. */
  Optional<Member> findFirstByStatusOrderByNameDesc(String status);

  /** Custom @Query without LIMIT: SQL is executed as-is, no LIMIT added. */
  @Query("SELECT m FROM Member m WHERE m.status = :status ORDER BY m.name DESC")
  Optional<Member> findByStatusCustom(@Param("status") String status);
}
