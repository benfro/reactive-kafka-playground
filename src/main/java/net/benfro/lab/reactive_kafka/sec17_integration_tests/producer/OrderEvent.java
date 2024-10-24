package net.benfro.lab.reactive_kafka.sec17_integration_tests.producer;

import java.time.LocalDateTime;
import java.util.UUID;

public record OrderEvent(
    UUID id,
    Long customerId,
    LocalDateTime orderEvent
) {
    public static OrderEvent of(Long customerId) {
        return new OrderEvent(UUID.randomUUID(), customerId, LocalDateTime.now());
    }
}
