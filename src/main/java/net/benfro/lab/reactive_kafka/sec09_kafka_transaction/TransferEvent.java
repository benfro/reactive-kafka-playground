package net.benfro.lab.reactive_kafka.sec09_kafka_transaction;

public record TransferEvent(
    String key,
    String from,
    String to,
    String amount,
    Runnable acknowledge
) {
}
