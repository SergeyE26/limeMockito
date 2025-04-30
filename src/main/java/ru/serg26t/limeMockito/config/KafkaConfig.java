package ru.serg26t.limeMockito.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import ru.serg26t.limeMockito.dto.Fruit;

@EnableKafka
@Configuration
public class KafkaConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;
    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String autoOffsetReset;
    @Value("${spring.kafka.consumer.enable-auto-commit}")
    private boolean autoCommit;


    @Bean(name = "fruitListenerContainerFactory")
    @ConditionalOnProperty(name = "app.kafka.enabled", havingValue = "true")
    public KafkaListenerContainerFactory<?> fruitListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Fruit> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(fruitConsumerFactory());
        factory.setBatchListener(false);
        factory.getContainerProperties().setAuthExceptionRetryInterval(Duration.ofSeconds(10));
        return factory;
    }

    @Bean(name = "fruitConsumerFactory")
    public DefaultKafkaConsumerFactory<String, Fruit> fruitConsumerFactory() {
        Map<String, Object> properties = new HashMap<>();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
        return new DefaultKafkaConsumerFactory<>(properties);
    }
}
