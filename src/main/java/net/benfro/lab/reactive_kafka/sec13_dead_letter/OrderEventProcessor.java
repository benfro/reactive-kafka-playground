package net.benfro.lab.reactive_kafka.sec13_dead_letter;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;

@Slf4j
@RequiredArgsConstructor
public class OrderEventProcessor {

    private final ReactiveDeadLetterTopicProducer<String, String> deadLetterTopicProducer;

    public Mono<Void> process(ReceiverRecord<String, String> record) {
        return Mono.just(record)
                .doOnNext(r -> {
                    if (r.key().endsWith("5")) {
                        throw new RuntimeException("processing exception");
                    }
                    log.info("key: {}, value: {}", r.key(), r.value());
                    r.receiverOffset().acknowledge();
                })
                .onErrorMap(ex -> new RecordProcessingException(record, ex))
                .transform(this.deadLetterTopicProducer.recordProcessingErrorHandler())
                .then();
    }
}
