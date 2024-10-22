package net.benfro.lab.reactive_kafka.sec13_dead_letter;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class DeadLetterTopicConsumer {

    public static void main(String[] args) {

        var dltProducer = deadLetterTopicProducer();
        var processor = new OrderEventProcessor(dltProducer);
        var receiver = receiver();

        receiver.receive()
                .concatMap(processor::process)
                .subscribe();
    }


    private static ReactiveDeadLetterTopicProducer<String, String> deadLetterTopicProducer() {
        var producerConfig = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1 :9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        var options = SenderOptions.<String, String>create(producerConfig);
        var sender = KafkaSender.create(options);
        return new ReactiveDeadLetterTopicProducer<>(
                sender,
                Retry.fixedDelay(2, Duration.ofSeconds(1))
        );
    }

    private static KafkaReceiver<String, String> receiver() {
        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );

        var options = ReceiverOptions.<String, String>create(consumerConfig)
                .subscription(List.of("order-events"));

        return KafkaReceiver.create(options);
    }
}