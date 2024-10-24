package net.benfro.lab.reactive_kafka.sec17_integration_tests.consumer;

public record DummyOrder(
    String orderId,
    String customerId
) {
}
