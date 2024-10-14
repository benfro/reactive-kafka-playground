package net.benfro.lab.reactive_kafka.sec03_producer;

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;



@Slf4j
public class KafkaProducer {

    public static void main(String[] args) {

        Map<String, Object> config = Map.<String, Object>of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        var options = SenderOptions.<String, String>create(config);

        var flux = Flux.interval(Duration.ofMillis(100))
            .take(100)
            .map(i -> new ProducerRecord<>("order-events", i.toString(), "order-" + i))
            .map(pr -> SenderRecord.create(pr, pr.key()));

        var sender = KafkaSender.create(options);
//        sender.close();
        sender.send(flux)
            .doOnNext(record -> log.info("Correlation: {}", record.correlationMetadata()))
            .doOnComplete(sender::close)
            .subscribe();
    }


}
