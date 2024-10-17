package net.benfro.lab.reactive_kafka.sec13_dead_letter;

import lombok.Getter;
import reactor.kafka.receiver.ReceiverRecord;

public class RecordProcessingException extends RuntimeException {

    @Getter
    private ReceiverRecord<?, ?> record;

    public RecordProcessingException(ReceiverRecord<?, ?> record, Throwable cause) {
        super(cause);
        this.record = record;
    }

    @SuppressWarnings("unchecked")
    public <K, V> ReceiverRecord<K, V> getRecord() {
        return (ReceiverRecord<K, V>) record;
    }
}
