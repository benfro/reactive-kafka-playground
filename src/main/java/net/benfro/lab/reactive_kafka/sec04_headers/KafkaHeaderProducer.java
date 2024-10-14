package net.benfro.lab.reactive_kafka.sec04_headers;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

@Slf4j
public class KafkaHeaderProducer {

    public static void main(String[] args) {

        Map<String, Object> config = Map.<String, Object>of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        var options = SenderOptions.<String, String>create(config);

        var flux = Flux.range(1, 10).map(KafkaHeaderProducer::createSenderRecord);

        var sender = KafkaSender.create(options);
        sender.send(flux)
            .doOnNext(r -> log.info("Sender correlation: {}", r.correlationMetadata()))
            .doOnComplete(sender::close)
            .subscribe();
    }

    static SenderRecord<String, String, String> createSenderRecord(Integer i) {
        var headers = new RecordHeaders();
        headers.add("client-id", "some-client".getBytes());
        headers.add("tracing-id", "tracing-client".getBytes());
        var pr = new ProducerRecord<>("order-events", null, i.toString(), "order-" + i, headers);
        return SenderRecord.create(pr, pr.key());
    }


}
