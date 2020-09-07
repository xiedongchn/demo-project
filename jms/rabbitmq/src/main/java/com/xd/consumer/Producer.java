package com.xd.consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

/**
 * Description
 *
 * @author xd
 * Created on 八月/31 23:35
 */
public class Producer {

    //队列名称
    private final static String QUEUE_NAME = "helloMQ";

    public static void main(String[] argv) {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            //设置RabbitMQ所在主机ip或者主机名
            factory.setHost("localhost");
            //创建一个连接
            Connection connection = factory.newConnection();
            //创建一个频道
            Channel channel = connection.createChannel();
            //指定一个队列
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            //告诉RabbitMQ不要在同一时间给一个消费者超过一条消息。换句话说，只有在消费者空闲的时候会发送下一条信息。
            int prefetchCount = 1;
            channel.basicQos(prefetchCount);
            //发送的消息
            String message = "hello world!";
            //往队列中发出一条消息
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            System.out.println(" [x] Sent '" + message + "'");
            //关闭频道和连接
            channel.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
