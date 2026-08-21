package io.queryaudit.spring.boot4;

import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ItemController {

  private final ItemService service;

  public ItemController(ItemService service) {
    this.service = service;
  }

  @GetMapping("/items")
  public List<Item> items() {
    return service.findAll();
  }
}
