package net.benfro.lab.reactive_kafka.sec16_spring_stuff;

import java.util.List;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;

import lombok.extern.slf4j.Slf4j;
import reactor.kafka.receiver.ReceiverOptions;

@SuppressWarnings("removal")
@Slf4j
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public ReceiverOptions<String, OrderEvent> receiverOptions(KafkaProperties kafkaProperties) {
        return ReceiverOptions.<String, OrderEvent>create(kafkaProperties.buildConsumerProperties())
            .subscription(List.of("order-events"));
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, OrderEvent> reactiveKafkaConsumerTemplate(ReceiverOptions<String, OrderEvent> options) {
        return new ReactiveKafkaConsumerTemplate<>(options);
    }

}
