package net.benfro.lab.reactive_kafka.sec07;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import lombok.extern.slf4j.Slf4j;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

/**
 * Seek position
 */
@Slf4j
public class KafkaConsumer {

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
            .addAssignListener(c -> {
                c.forEach(r -> log.info("Assigned: {}", r.position()));
                c.forEach(r -> r.seek(r.position() - 2));
                c.stream()
                    .filter(r -> r.topicPartition().partition() == 2)
                    .findFirst()
                    .ifPresent(r -> r.seek(r.position() - 2));
            })
            .subscription(List.of("order-events"));
        //.consumerProperty(ConsumerConfig.GROUP_ID_CONFIG, "demo-group")

        KafkaReceiver.create(options)
            .receive()
//            .take(3) // Will stop after three items
            .doOnNext(r -> log.info("r.key: {}, r.value: {}", r.key(), r.value()))
            .doOnNext(r -> r.receiverOffset().acknowledge() )
            .subscribe();
    }
}
