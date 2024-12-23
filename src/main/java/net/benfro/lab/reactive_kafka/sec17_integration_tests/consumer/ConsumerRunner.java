package net.benfro.lab.reactive_kafka.sec17_integration_tests.consumer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ConsumerRunner implements CommandLineRunner {

    @Autowired
    private ReactiveKafkaConsumerTemplate<String, DummyOrder> template;

    @Override
    public void run(String... args) throws Exception {
        this.template.receive()
//            .doOnNext(r -> r.headers().forEach(h -> log.info("Header key {}, value {}", h.key(), new String(h.value()))))
//            .doOnNext(r -> log.info("Key: {}, value: {}", r.key(), r.value()))
            .doOnNext(r -> log.info("Key: {}, value: {}", r.key(), r.value().customerId()))
            .subscribe();
    }
}
