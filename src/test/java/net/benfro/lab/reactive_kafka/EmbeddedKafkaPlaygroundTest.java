package net.benfro.lab.reactive_kafka;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.test.StepVerifier;

@EmbeddedKafka(
//    ports = 9092, // Random port
    partitions = 1,
    brokerProperties = {"auto.create.topics.enable=false"},
    topics = {"order-events"}
)
@Slf4j
class EmbeddedKafkaPlaygroundTest {

    @Test
    void embeddedKafkaDemo() {

        // Random port holder
        var brokers = EmbeddedKafkaCondition.getBroker().getBrokersAsString();

        StepVerifier.create(Producer.run(brokers))
            .verifyComplete();

        StepVerifier.create(Consumer.run(brokers))
            .verifyComplete();
    }

    @Slf4j
    private static class Consumer {
        public static Mono<Void> run(String brokers) {

            var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "4077"
            );

            var options = ReceiverOptions.create(consumerConfig)
                .subscription(List.of("order-events"));

            return KafkaReceiver.create(options)
                .receive()
                .take(10)
                .doOnNext(r -> log.info("r.key: {}, r.value: {}", r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .then();
        }
    }


    @Slf4j
    private static class Producer {
        public static Mono<Void> run(String brokers) {

            Map<String, Object> config = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
            );

            var options = SenderOptions.<String, String>create(config);

            var flux = Flux.range(1,10)
                .delayElements(Duration.ofMillis(10))
                .map(i -> new ProducerRecord<>("order-events", i.toString(), "order-" + i))
                .map(pr -> SenderRecord.create(pr, pr.key()));

            var sender = KafkaSender.create(options);

            return sender.send(flux)
                .doOnNext(record -> log.info("Correlation: {}", record.correlationMetadata()))
                .doOnComplete(sender::close)
                .then();
        }
    }

}
