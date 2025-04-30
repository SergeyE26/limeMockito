package ru.serg26t.limeMockito.dto;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import java.util.UUID;
import lombok.Data;
import lombok.extern.jackson.Jacksonized;

@Data
@Entity
@Jacksonized
public class Fruit {

    @Id
    UUID id;
    String name;
    String description;
}
