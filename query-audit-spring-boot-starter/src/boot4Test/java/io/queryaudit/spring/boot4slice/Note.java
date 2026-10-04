package io.queryaudit.spring.boot4slice;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;

@Entity
public class Note {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private Long id;

  private String text;

  protected Note() {}

  public Note(String text) {
    this.text = text;
  }

  public Long getId() {
    return id;
  }
}
