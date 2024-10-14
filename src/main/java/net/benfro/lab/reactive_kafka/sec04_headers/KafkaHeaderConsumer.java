package net.benfro.lab.reactive_kafka.sec04_headers;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import lombok.extern.slf4j.Slf4j;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

@Slf4j
public class KafkaHeaderConsumer {
    // TODO This lab doesn't work...
    public static void main(String[] args) {

        var consumerConfig = Map.<String, Object>of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "4077"
        );

        var options = ReceiverOptions.create(consumerConfig)
            .subscription(List.of("order-events"));

        KafkaReceiver.create(options)
            .receive()
            .doOnNext(r -> log.info("r.key: {}, r.value: {}", r.key(), r.value()))
            .doOnNext(r -> r.headers().forEach(h -> log.info("Header key {}, value {}", h.key(), h.value())))
            .doOnNext(r -> r.receiverOffset().acknowledge() )
            .subscribe();


    }
}
