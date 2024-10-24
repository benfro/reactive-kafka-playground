package net.benfro.lab.reactive_kafka.sec17_integration_tests.producer;

import java.time.Duration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;
import net.benfro.lab.reactive_kafka.sec17_integration_tests.consumer.DummyOrder;
import reactor.core.publisher.Flux;

@Slf4j
@Service
public class ProducerRunner implements CommandLineRunner {

    @Autowired
    private ReactiveKafkaProducerTemplate<String, OrderEvent> template;

    @Override
    public void run(String... args) throws Exception {
        this.orderFlux()
            .flatMap(oe -> this.template.send("order-events", oe.id().toString(), oe))
            .doOnNext(r -> log.info("result: {}", r.recordMetadata()))
            .subscribe();
    }

    private Flux<OrderEvent> orderFlux() {
        return Flux.interval(Duration.ofMillis(500))
            .take(1000)
            .map(OrderEvent::of);
    }

}
