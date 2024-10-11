package net.benfro.lab.reactive_kafka.sec05;

/**
 * 1) Create group with 3 partitions
 * 2) Start consumers, one at a time and read logs.
 * 3) Now stop consumers, one at a time and read logs
 *
 * Topic name: triple-events
 */
public class KafkaConsumerGroup {

    private static class Consumer1 {
        public static void main(String[] args) {
            KafkaConsumer.start("1");
        }
    }

    private static class Consumer2 {
        public static void main(String[] args) {
            KafkaConsumer.start("2");
        }
    }

    private static class Consumer3 {
        public static void main(String[] args) {
            KafkaConsumer.start("3");
        }
    }

}
