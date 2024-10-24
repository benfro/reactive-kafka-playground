package net.benfro.lab.reactive_kafka.sec17_integration_tests.producer;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Duration;

import org.junit.jupiter.api.Test;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import lombok.extern.slf4j.Slf4j;
import net.benfro.lab.AbstractIT;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.test.StepVerifier;

@Slf4j
@TestPropertySource(properties = {"app=producer"})
@DirtiesContext // Clears context after each test
class OrderEventProducerTest extends AbstractIT {

    @Test
    void producerTest() {
        KafkaReceiver<String, OrderEvent> receiver = createReceiver("order-events");
        var orderEvents = receiver.receive()
            .take(10)
            .doOnNext(r -> log.info("Key: {}, value: {}", r.key(), r.value()));

        StepVerifier.create(orderEvents)
            .consumeNextWith(r -> assertNotNull(r.value().customerId()))
            .expectNextCount(9)
            .expectComplete()
            .verify(Duration.ofSeconds(10));

    }
}
