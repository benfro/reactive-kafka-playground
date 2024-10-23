package net.benfro.lab.reactive_kafka.sec16_spring_stuff;

import java.util.List;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;

import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.SenderOptions;

@SuppressWarnings("removal")
@Configuration
public class KafkaProducerConfig {

    @Bean
    public SenderOptions<String, DummyOrder> senderOptions(KafkaProperties kafkaProperties) {
        return SenderOptions.<String, DummyOrder>create(kafkaProperties.buildProducerProperties());
    }

    @Bean
    public ReactiveKafkaProducerTemplate<String, DummyOrder> reactiveKafkaProducerTemplate(SenderOptions<String, DummyOrder> options) {
        return new ReactiveKafkaProducerTemplate<>(options);
    }
}
