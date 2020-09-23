package com.xd.producer;

import com.xd.service.impl.TransactionProducerImpl;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.*;

/**
 * @author xd
 * Created on 2020/9/22
 */
public class TransactionProducer {

    public static void main(String[] args) throws UnsupportedEncodingException, MQClientException {
        // 接收RocketMQ回调的监听器端口
        // 这里会实现订单本地事务，commit、rollback，回调查询等逻辑
        TransactionListener transactionListener = new TransactionProducerImpl();

        // 创建一个支持事务的Producer，先随意制定一个分组
        TransactionMQProducer producer = new TransactionMQProducer("TestProducerGroup");
        // 制定一个线程池，这个线程池就是用来处理mq回调请求的
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("TestThread");
                return thread;
            }
        });

        // 给事务消息生产者设置线程池
        producer.setExecutorService(executorService);
        // 设置对应的回调函数
        producer.setTransactionListener(transactionListener);
        // 启动事务消息生产者
        producer.start();

        // 构造一条消息
        Message message = new Message("PayOrderSuccessTopic", "TestTag", "TestKey", "订单支付消息".getBytes(RemotingHelper.DEFAULT_CHARSET));
        try {
            // 将消息消息用half模式发送出去
            SendResult result = producer.sendMessageInTransaction(message, null);
        } catch (MQClientException e) {
            e.printStackTrace();
            // 如果half消息发送失败
            // 订单系统执行回滚逻辑，比如说触发支付退款，更新订单状态为已关闭
        }
    }
}
