package com.xd.service.impl;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * @author xd
 * Created on 2020/9/22
 */
public class TransactionProducerImpl implements TransactionListener {

    // 如果half消息发送成功了，就会回调这个函数
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        // 执行订单本地事务
        // 接着根据本地一连串事务结果，去选择执行commit或rollback
        try {
            // 如果本地事务都执行成功了，返回commit
            return LocalTransactionState.COMMIT_MESSAGE;
        } catch (Exception e) {
            // 如果本地事务执行失败，回滚执行过的操作，返回rollback，标记half消息无效
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
    }

    // 如果因为各种原因，没有返回commit或rollback
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        // 根据消息事务id查询本地事务是否执行成功了
        Integer status = Integer.valueOf(msg.getTransactionId());
        // 根据本地事务情况选择执行commit or rollback
        switch (status) {
            case 1: return LocalTransactionState.COMMIT_MESSAGE;
            case 2: return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        return LocalTransactionState.UNKNOW;
    }
}
