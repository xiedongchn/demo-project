package com.xd.kafka.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Description
 *
 * @author xd
 * Created on 八月/30 19:40
 */
@Component
public class Producer {

    @Autowired
    private KafkaTemplate<String, String> template;

    public void sendMsg(String topic, String msg) {
        template.send(topic, msg);
    }

    public void sendMsg(String topic, String key, String msg) {
        template.send(topic, key, msg);
    }

}
