package com.xd.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * Description
 *
 * @author xd
 * Created on 九月/7 22:03
 */
public class Producer {

    // MQ的生产者类
    public static DefaultMQProducer defaultMQProducer;
    static {
        // 创建mq生产者对象
        defaultMQProducer = new DefaultMQProducer("producer_group");
        // 设置nameserver地址，让生产者能拉取路由信息
        // 这样才知道每隔topic数据分散在哪些Broker机器上
        // 然后才能把消息发送到broker上去
        defaultMQProducer.setNamesrvAddr("localhost:9876");
        try {
            defaultMQProducer.start();
            // 设置异步发送失败重试次数为0
            defaultMQProducer.setRetryTimesWhenSendAsyncFailed(0);
            // 自动容错机制，比如如果某次访问一个Broker发现网络延迟有500ms，然后还无法访问，
            // 那么就会自动回避访问这个Broker一段时间，比如接下来3000ms内，就不会访问这个Broker了。
            // 这样的话，就可以避免一个Broker故障之后，短时间内生产者频繁的发送消息到这个故障的Broker上去，
            // 出现较多次数的异常。而是在一个Broker故障之后，自动回避一段时间不要访问这个Broker，过段时间再去访问他。
            // 那么这样过一段时间之后，可能这个Master Broker就已经恢复好了，比如他的Slave Broker切换为了Master可以让别人访问了。
            defaultMQProducer.setSendLatencyFaultEnable(true);
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }

    /**
     * 同步发送消息
     */
    public static void sendMsg(String topic, String message) throws UnsupportedEncodingException, InterruptedException, RemotingException, MQClientException, MQBrokerException {
        // 构建一个消息对象
        // Message()构造方法参数解析
        //  topic：指定消息的topic
        //  消息的tag
        //  消息的字节数组
        Message msg = new Message(topic, "", message.getBytes(RemotingHelper.DEFAULT_CHARSET));
        // 使用Producer发送消息
        SendResult sendResult = defaultMQProducer.send(msg);
        System.out.printf("SendResult%s%n", sendResult);
    }

    /**
     * 异步发送消息，消息发送成功之后回调SendCallback方法
     */
    public static void sendMsgAsync(String topic, String message) throws UnsupportedEncodingException, InterruptedException, RemotingException, MQClientException, MQBrokerException {
        // 构建一个消息对象
        // Message()构造方法参数解析
        //  topic：指定消息的topic
        //  消息的tag
        //  消息的字节数组
        Message msg = new Message(topic, "", message.getBytes(RemotingHelper.DEFAULT_CHARSET));
        // 使用Producer发送消息
        defaultMQProducer.send(msg, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {

            }

            @Override
            public void onException(Throwable e) {

            }
        });
    }

    /**
     * 单向发送消息，不管消息发送结果，允许消息丢失
     */
    public static void sendOneWay(String topic, String message) throws UnsupportedEncodingException, InterruptedException, RemotingException, MQClientException, MQBrokerException {
        // 构建一个消息对象
        // Message()构造方法参数解析
        //  topic：指定消息的topic
        //  消息的tag
        //  消息的字节数组
        Message msg = new Message(topic, "", message.getBytes(RemotingHelper.DEFAULT_CHARSET));
        // 使用Producer发送消息
        defaultMQProducer.sendOneway(msg);
    }

    /**
     * 发送一条具备指定tag或属性的消息
     */
    public static void sendByTagAndProp() throws UnsupportedEncodingException, RemotingException, MQClientException, InterruptedException {
        // 构建一个消息对象
        // Message()构造方法参数解析
        //  topic：指定消息的topic
        //  消息的tag
        //  消息的字节数组

        Message msg = new Message("Test_Topic", "TagA", "testMsg".getBytes(RemotingHelper.DEFAULT_CHARSET));
        msg.putUserProperty("a", "10");
        msg.putUserProperty("b", "11");
        // 使用Producer发送消息
        defaultMQProducer.sendOneway(msg);
    }

    public static void main(String[] args) throws MQClientException, InterruptedException {
        final DefaultMQProducer producer = new DefaultMQProducer("test_producer");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();
        for (int i = 0; i < 10; i++) {
            new Thread(() -> {
                while (true) {
                    try {
                        Message message = new Message("TopicTest", "TagA", "test".getBytes(RemotingHelper.DEFAULT_CHARSET));
                        SendResult sendResult = producer.send(message);
                        System.out.println(sendResult.toString());
                    } catch (UnsupportedEncodingException | MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        while (true) {
            Thread.sleep(1000);
        }
    }
}
