package com.netflix.encodingservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class EncodingServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(EncodingServiceApplication.class, args);
	}
}