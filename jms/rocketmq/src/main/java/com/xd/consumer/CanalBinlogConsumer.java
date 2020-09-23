package com.xd.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;

/**
 * @author xd
 * Created on 2020/9/23
 */
public class CanalBinlogConsumer {

    public static void main(String[] args) {
        pushConsumer();
    }

    /**
     * DefaultMQPushConsumer，被动消费消息，Broker会主动把消息推送过来
     */
    public static void pushConsumer() {
        new Thread(() -> {
            try {
                // 消费者对象，传入group对消费者进行分组，例如多个订单系统：order_consumer_group，数据中心：analyse_consumer_group
                // 不同的系统使用不同的分组
                DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("canal_consumer_group");
                // 设置NameServer地址，拉取路由信息，就可以知道哪些Topic在那个Broker上，然后从对应的Broker拉取数据
                consumer.setNamesrvAddr("172.30.60.128:9876");
                // 订阅topic，消费哪些topic的消息，从这个topic的机器上拉取消息
                consumer.subscribe("Canal_Topic", "*");
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
                consumer.start();
            } catch (MQClientException e) {
                e.printStackTrace();
            }
        }).start();

    }
}
