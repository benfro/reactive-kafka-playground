package net.benfro.lab.reactive_kafka.sec09_kafka_transaction;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import lombok.extern.slf4j.Slf4j;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

// Didn't work: console producer wasn't parsing the key-value correctly, and key was NULL
// 14:30:40.307 [reactive-kafka-demo-group-1] INFO  n.b.l.r.s.TransferEventConsumer - key: null, value: 1:a,b,10

@Slf4j
public class TransferDemo {

    public static void main(String[] args) {

        var transferEventConsumer = new TransferEventConsumer(kafkaReceiver());
        var transferEventProcessor = new TransferEventProcessor(kafkaSender());

        transferEventConsumer
            .receive()
            .transform(transferEventProcessor::consume)
            .doOnNext(r -> log.info("done {}", r.correlationMetadata()))
            .doOnError(ex -> log.error(ex.getMessage()))
            .subscribe();
    }

    private static KafkaReceiver<String, String> kafkaReceiver() {
        var consumerConfig = Map.<String, Object>of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
            ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
            ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );

        var options = ReceiverOptions.<String, String>create(consumerConfig)
            .subscription(List.of("transfer-requests"));

        return KafkaReceiver.create(options);
    }

    private static KafkaSender<String, String> kafkaSender() {
        var producerConfig = Map.<String, Object>of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.TRANSACTIONAL_ID_CONFIG, "money-transfer" // When doing transactions!
        );
        var options = SenderOptions.<String, String>create(producerConfig);
        return KafkaSender.create(options);
    }
}
