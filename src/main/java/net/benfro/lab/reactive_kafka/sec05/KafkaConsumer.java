package net.benfro.lab.reactive_kafka.sec05;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.common.serialization.StringDeserializer;

import lombok.extern.slf4j.Slf4j;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

@Slf4j
public class KafkaConsumer {

    public static void start(String instanceId) {

        var consumerConfig = Map.<String, Object>of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, instanceId,
            // Assign in the order they are achieved, Default is RangeAssignor.class
            ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class
        );

        var options = ReceiverOptions.create(consumerConfig)
            .subscription(List.of("triple-events"));

        KafkaReceiver.create(options)
            .receive()
            .doOnNext(r -> log.info("r.key: {}, r.value: {}", r.key(), r.value()))
            .doOnNext(r -> r.receiverOffset().acknowledge() )
            .subscribe();
    }
}
