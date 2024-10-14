package net.benfro.lab.reactive_kafka.sec08_error_handling;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import io.netty.handler.timeout.ReadTimeoutException;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

/**
 * Processors for Consumer
 */
@Slf4j
public class Processors {

    /**
     * Isolate handling and retries to a separate inner processing pipeline
     */
    static Mono<Void> process(ReceiverRecord<Object, Object> record) {
        return Mono.just(record)
            .doOnNext(r -> {
                var index = ThreadLocalRandom.current().nextInt(1, 10);
                log.info("key: {}, index {}, value: {}", r.key(), index, r.value().toString().toCharArray()[index]);
            })
            .retryWhen(Retry.fixedDelay(3L, Duration.ofSeconds(1)).onRetryExhaustedThrow((spec,signal) -> signal.failure() ))
            .doOnError(ex -> log.error("error: {}", ex.getMessage()))
            .doFinally(s -> record.receiverOffset().acknowledge())
            .onErrorComplete()
            .then();
    }

    /**
     * Different behaviour retry wise with different errors
     */
    static Mono<Void> processWithRetry(ReceiverRecord<Object, Object> record) {
        return Mono.just(record)
            .doOnNext(r -> {
//                if(r.key().toString().equals("5")) {
//                    throw new RuntimeException("DB is down");
//                }
                var index = ThreadLocalRandom.current().nextInt(1, 20);
                log.info("key: {}, index {}, value: {}", r.key(), index, r.value().toString().toCharArray()[index]);
                record.receiverOffset().acknowledge();
            })
            .retryWhen(retrySpec())
            .doOnError(ex -> log.error("error: {}", ex.getMessage()))
            .onErrorResume(IndexOutOfBoundsException.class, ex -> Mono.fromRunnable(() -> record.receiverOffset().acknowledge()))
            .then();
    }

    private static Retry retrySpec() {
       return Retry.fixedDelay(3L, Duration.ofSeconds(1))
           .filter(IndexOutOfBoundsException.class::isInstance)
           .onRetryExhaustedThrow((spec, signal) -> signal.failure() );
    }


}
