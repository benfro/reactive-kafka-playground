package net.benfro.lab.reactive_kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

@Slf4j
public class Lec01Consumer {

    public static void main(String[] args) {

        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group"
        );

        var options = ReceiverOptions.create(consumerConfig)
                .subscription(List.of("order-events"));
                //.consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, "demo-group")

        KafkaReceiver.create(options)
                .receive()
                .doOnNext(r -> log.info("r.key: {}, r.value: {}", r.key(), r.value()))
                .subscribe();
    }
}
 