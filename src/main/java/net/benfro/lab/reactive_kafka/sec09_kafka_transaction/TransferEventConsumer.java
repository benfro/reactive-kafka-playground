package net.benfro.lab.reactive_kafka.sec09_kafka_transaction;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;

@Slf4j
@RequiredArgsConstructor
public class TransferEventConsumer {

    private final KafkaReceiver<String, String> receiver;

    public Flux<TransferEvent> receive() {
        return this.receiver.receive()
            .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
            .map(this::toTransferEvent);
    }

    private TransferEvent toTransferEvent(ReceiverRecord<String, String> record) {
        String[] split = record.value().split(",");
        var runnable = record.key().equals("5") ? fail() : ack(record);
        return new TransferEvent(
            record.key(),
            split[0],
            split[1],
            split[2],
            runnable
        );
    }

    private Runnable ack(ReceiverRecord<String, String> record) {
        return () -> record.receiverOffset().acknowledge();
    }

    private Runnable fail() {
        return () -> {
            throw new RuntimeException("Such failure");
        };
    }

}
