package net.benfro.lab.reactive_kafka.sec09_kafka_transaction;

import java.time.Duration;
import java.util.function.Predicate;

import org.apache.kafka.clients.producer.ProducerRecord;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

@Slf4j
@RequiredArgsConstructor
public class TransferEventProcessor {

    private final KafkaSender<String, String> sender;

    public Flux<SenderResult<String>> consume(Flux<TransferEvent> flux) {
        return flux.concatMap(this::validate)
            .concatMap(this::sendTransaction);
    }

    private Mono<SenderResult<String>> sendTransaction(TransferEvent transferEvent) {
        var senderRecords = this.toSenderRecord(transferEvent);
        var manager = this.sender.transactionManager();
        return manager.begin()
            .then(this.sender.send(senderRecords)
                .concatWith(
                    Mono.delay(Duration.ofSeconds(1)) // For demo purposes
                        .then(Mono.fromRunnable(transferEvent.acknowledge())))
                .concatWith(manager.commit())
                // Here you could define a ResultObject to harbor the multiple sender records
                .last())
            .doOnError(ex -> log.error(ex.getMessage()))
            .onErrorResume(ex -> manager.abort());
    }

    private Mono<TransferEvent> validate(TransferEvent event) {
        // Database call or alike IRL
        return Mono.just(event)
            .filter(Predicate.not(e -> e.key().equals("5")))
            .switchIfEmpty(
                Mono.<TransferEvent>fromRunnable(event.acknowledge())
                    .doFirst(() -> log.info("fails validation: {}", event.key()))
            );
    }

    private Flux<SenderRecord<String, String, String>> toSenderRecord(TransferEvent event) {
        var prodRecordTp = new ProducerRecord<>("transaction-events",
            event.key(),
            String.format("%s+%s",event.to(), event.amount()));
        var prodRecordFrom = new ProducerRecord<>("transaction-events",
            event.key(),
            String.format("%s-%s", event.from(), event.amount()));

        var senderRecordTo = SenderRecord.create(prodRecordTp, prodRecordTp.key());
        var senderRecordFrom = SenderRecord.create(prodRecordFrom, prodRecordFrom.key());

        return Flux.just(senderRecordTo, senderRecordFrom);
    }

}
