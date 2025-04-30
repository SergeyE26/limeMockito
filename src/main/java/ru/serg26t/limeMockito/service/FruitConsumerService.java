package ru.serg26t.limeMockito.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.serg26t.limeMockito.dto.Fruit;

@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true")
public class FruitConsumerService {

    private final FruitService fruitService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "${app.kafka.fruits.inbox.topic}",
            containerFactory = "fruitListenerContainerFactory"
    )
    public void consume(String message) {
        try {
            Fruit fruit = objectMapper.readValue(message, Fruit.class);
            fruitService.save(fruit);
        } catch (Exception e) {
            log.error("Error processing new fruit: {}", message, e);
        }
    }
}
