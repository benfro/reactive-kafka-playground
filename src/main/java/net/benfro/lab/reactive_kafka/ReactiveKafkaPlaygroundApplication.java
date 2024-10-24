package net.benfro.lab.reactive_kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"net.benfro.lab.reactive_kafka.sec17_integration_tests.${app}"})
public class ReactiveKafkaPlaygroundApplication {

	public static void main(String[] args) {
		SpringApplication.run(ReactiveKafkaPlaygroundApplication.class, args);
	}

}
