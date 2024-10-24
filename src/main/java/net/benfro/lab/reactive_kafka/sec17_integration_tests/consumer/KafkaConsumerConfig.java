package net.benfro.lab.reactive_kafka.sec17_integration_tests.consumer;

import java.util.List;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import lombok.extern.slf4j.Slf4j;
import reactor.kafka.receiver.ReceiverOptions;

@SuppressWarnings("removal")
@Slf4j
@Configuration
public class KafkaConsumerConfig {

    // TODO Have a look at ErrorHandlingDeserializer!!

    @Bean
    public ReceiverOptions<String, DummyOrder> receiverOptions(KafkaProperties kafkaProperties) {
        return ReceiverOptions.<String, DummyOrder>create(kafkaProperties.buildConsumerProperties())
            // Keep header information
            // These properties can be set in *.yaml file as well
            .consumerProperty(JsonDeserializer.REMOVE_TYPE_INFO_HEADERS, false)
            .consumerProperty(JsonDeserializer.USE_TYPE_INFO_HEADERS, false)
            .consumerProperty(JsonDeserializer.VALUE_DEFAULT_TYPE, DummyOrder.class)
            .subscription(List.of("order-events"));
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, DummyOrder> reactiveKafkaConsumerTemplate(ReceiverOptions<String, DummyOrder> options) {
        return new ReactiveKafkaConsumerTemplate<>(options);
    }

}
