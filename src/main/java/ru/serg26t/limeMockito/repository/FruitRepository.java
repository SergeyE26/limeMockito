package ru.serg26t.limeMockito.repository;

import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.serg26t.limeMockito.dto.Fruit;

@Repository
public interface FruitRepository extends JpaRepository<Fruit, UUID> {

    Optional<Fruit> findByName(String name);
}
