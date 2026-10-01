package io.queryaudit.junit5.integration;

import io.queryaudit.junit5.integration.entity.Team;
import io.queryaudit.junit5.integration.repository.TeamRepository;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TeamQueryService {
  private final TeamRepository teams;

  public TeamQueryService(TeamRepository teams) {
    this.teams = teams;
  }

  @Transactional(readOnly = true)
  public List<Integer> memberCounts() {
    List<Integer> counts = new ArrayList<>();
    for (Team team : teams.findAll()) {
      counts.add(team.getMembers().size());
    }
    return counts;
  }
}
