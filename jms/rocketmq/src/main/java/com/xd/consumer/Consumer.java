package com.xd.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.Set;

/**
 * Description
 *
 * @author xd
 * Created on 九月/7 22:12
 */
public class Consumer {

    /**
     * DefaultMQPushConsumer，被动消费消息，Broker会主动把消息推送过来
     */
    public static void pushConsumer() {
        new Thread(() -> {
            try {
                // 消费者对象，传入group对消费者进行分组，例如多个订单系统：order_consumer_group，数据中心：analyse_consumer_group
                // 不同的系统使用不同的分组
                DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer_group");
                // 设置NameServer地址，拉取路由信息，就可以知道哪些Topic在那个Broker上，然后从对应的Broker拉取数据
                consumer.setNamesrvAddr("localhost:9876");
                // 订阅topic，消费哪些topic的消息，从这个topic的机器上拉取消息
                consumer.subscribe("TopicTest", "*");
                // 注册监听器，当consumer拉取到了订单消息，就会回调这个方法进行处理
                consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                    try {

                        // 对获取到的消息进行处理，实现系统之间的解耦，生产者系统与消费者系统解耦，生产者系统只要把消息
                        // 放到消息队列中即可，不必等待消费者去消费
                        // 例如用户在平台上借款，在放款成功之后，需要通知到大数据中心、订单系统、短信平台、微信推送系统，
                        // 那么此时只需要发送一个消息到消息队列，其余的事情就不用关心了
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    } catch (Exception e) {
                        e.printStackTrace();

                        // 本地落库异常的情况下，对消息处理失败了，此时返回一个稍后重试消费的状态
                        return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                    }
                });
            } catch (MQClientException e) {
                e.printStackTrace();
            }
        }).start();

    }

    /**
     * DefaultMQPullConsumer，被动消费消息，Broker会主动把消息推送过来
     */
    public static void pullConsumer() throws MQClientException {
        DefaultMQPullConsumer pullConsumer = new DefaultMQPullConsumer("consumer_group");
        Set<MessageQueue> mqs = pullConsumer.fetchSubscribeMessageQueues("TopicTest");
        for (MessageQueue mq : mqs) {
            System.out.printf("Consume from the queue %s%n", mq);
            SINGLE_MQ:
            while (true) {
                try {
                    PullResult result = pullConsumer.pullBlockIfNotFound(mq, null, 0L, 32);
                    System.out.printf("%s,%n", result);
                } catch (InterruptedException | RemotingException | MQBrokerException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 消费指定tag或属性的消息
     */
    public static void consumeByTagAndProp() throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("consumer_group");
        // 消费指定Tag的消息,多个tag用||分隔
        consumer.subscribe("Test_Topic", "TagA || TagB");
        // 通过消息选择器过滤消息,功能比tag更强大也更复杂,支持类sql查询
        consumer.subscribe("Test_Topic", MessageSelector.bySql("a > 5 and b='abd'"));
    }

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_consumer");
        // 设置NameServer地址
        consumer.setNamesrvAddr("localhost:9876");
        // 订阅topic，消费哪些topic的消息
        consumer.subscribe("TopicTest", "*");
        //注册一个回调接口，去接受获取到的消息
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            System.out.println(msgs);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        consumer.start();
    }
}
