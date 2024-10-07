package net.benfro.lab.reactive_kafka.consumer;

import java.util.Map;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import lombok.extern.slf4j.Slf4j;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

@Slf4j
public class Lec02Consumer {

    public static void main(String[] args) {

        var consumerConfig = Map.<String, Object>of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.GROUP_ID_CONFIG, "inventory-service-group",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "4077"
        );

        var options = ReceiverOptions.create(consumerConfig)
            .subscription(Pattern.compile("order.*"));
//            .subscription(List.of("order-events"));
        //.consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, "demo-group")

        KafkaReceiver.create(options)
            .receive()
            .doOnNext(r -> log.info("topic: {}, key: {}, value: {}", r.topic(), r.key(), r.value()))
            .doOnNext(r -> r.receiverOffset().acknowledge())
            .subscribe();
    }
}
