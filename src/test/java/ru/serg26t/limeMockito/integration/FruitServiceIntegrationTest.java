package ru.serg26t.limeMockito.integration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import static org.awaitility.Awaitility.await;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import ru.serg26t.limeMockito.dto.Fruit;

@EmbeddedKafka(
        partitions = 1,
        brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"},
        topics = {"fruits_inbox_topic", "fruits_outbox_topic"})
public class FruitServiceIntegrationTest {

    private final KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
    private final KafkaConsumer<String, Fruit> responseConsumer = new KafkaConsumer<>(consumerProperties());;

    private static Properties consumerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "lime");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");
        return props;
    }

    private static ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @BeforeEach
    void init() {
        responseConsumer.subscribe(Collections.singletonList("fruits_outbox_topic"));
    }

    @AfterEach
    void cleanup() {
        responseConsumer.unsubscribe();
    }

    @Test
    void test() throws JsonProcessingException {

        Fruit fruit = new Fruit();
        fruit.setId(UUID.randomUUID());
        fruit.setName("Апельсин");
        fruit.setDescription("Оранжевый фрукт с витамином Ц");

        ObjectMapper objectMapper = new ObjectMapper();

        kafkaTemplate.send("fruits_inbox_topic", objectMapper.writeValueAsString(fruit));

        await().atMost(5, TimeUnit.SECONDS)
                .until(() -> KafkaTestUtils.getRecords(responseConsumer).count() == 2);
    }
}
