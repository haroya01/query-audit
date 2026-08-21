package io.queryaudit.spring.boot4;

import java.util.List;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class ItemService {

  private final ItemRepository repository;

  public ItemService(ItemRepository repository) {
    this.repository = repository;
  }

  @Transactional
  public Item create(String name) {
    return repository.save(new Item(name));
  }

  @Transactional(readOnly = true)
  public List<Item> findAll() {
    return repository.findAll();
  }
}
