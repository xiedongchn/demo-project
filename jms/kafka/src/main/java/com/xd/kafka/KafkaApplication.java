package com.xd.kafka;

import com.xd.kafka.producer.Producer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/**
 * kafka-console-producer --broker-list localhost:9092 --topic test
 * kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning
 */
@SpringBootApplication
public class KafkaApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext ctx = SpringApplication.run(KafkaApplication.class, args);

        Producer producer = ctx.getBean(Producer.class);
        producer.sendMsg("test", String.valueOf(System.currentTimeMillis()), "Application started!");
    }

}
