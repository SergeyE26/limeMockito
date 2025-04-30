package ru.serg26t.limeMockito.service;

import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.serg26t.limeMockito.dto.Fruit;
import ru.serg26t.limeMockito.repository.FruitRepository;

@Slf4j
@Service
@RequiredArgsConstructor
public class FruitService {

    private final FruitRepository fruitRepository;
    private final KafkaTemplate<String, Fruit> kafkaTemplate;

    @Value("${app.kafka.fruits.outbox.topic}")
    private String outboxTopic;

    @Value("${app.kafka.enabled:false}")
    private boolean isKafkaEnabled;

    public Optional<Fruit> findByName(String name) {
        log.info("Find Fruit by name: {}", name);
        return fruitRepository.findByName(name);
    }

    @Transactional
    public void save(Fruit fruit) {
        log.info("New fruit discovered: {}", fruit.getName());
        fruitRepository.save(fruit);
        if (isKafkaEnabled) {
            kafkaTemplate.send(outboxTopic, fruit.getName(), fruit);
        }
        else {
            log.info("Kafka disabled");
        }
    }
}
