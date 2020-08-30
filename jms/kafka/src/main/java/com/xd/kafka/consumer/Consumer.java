package com.xd.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 消费者
 *
 * @author xd
 * Created on 八月/30 19:45
 */
@Component
public class Consumer {

    @KafkaListener(topics = "test")
    public void listener(ConsumerRecord<String, String> record) {
        String value = record.value();
        System.out.println("receive:" + value);
    }
}