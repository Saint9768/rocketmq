/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.client.impl.consumer;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.body.ProcessQueueInfo;

/**
 * Queue consumption snapshot -- 队列消费快照
 * <p>
 * 每次对msgTreeMap、msgCount、msgSize进行操作时，都会加上读写锁
 */
public class ProcessQueue {
    public final static long REBALANCE_LOCK_MAX_LIVE_TIME =
            Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockMaxLiveTime", "30000"));
    public final static long REBALANCE_LOCK_INTERVAL = Long.parseLong(System.getProperty("rocketmq.client.rebalance.lockInterval", "20000"));
    private final static long PULL_MAX_IDLE_TIME = Long.parseLong(System.getProperty("rocketmq.client.pull.pullMaxIdleTime", "120000"));
    private final InternalLogger log = ClientLogger.getLog();
    /**
     * 读写锁：对消息的操作都要使用
     */
    private final ReadWriteLock lockTreeMap = new ReentrantReadWriteLock();
    /**
     * 临时存放消息用的
     */
    private final TreeMap<Long, MessageExt> msgTreeMap = new TreeMap<Long, MessageExt>();
    /**
     * 消息数量
     */
    private final AtomicLong msgCount = new AtomicLong();
    /**
     * 整个ProcessQueue处理单元的总消息长度
     */
    private final AtomicLong msgSize = new AtomicLong();
    private final Lock lockConsume = new ReentrantLock();
    /**
     * A subset of msgTreeMap, will only be used when orderly consume
     * 一个临时的TreeMap，仅在顺序消费模式下使用
     */
    private final TreeMap<Long, MessageExt> consumingMsgOrderlyTreeMap = new TreeMap<Long, MessageExt>();
    private final AtomicLong tryUnlockTimes = new AtomicLong(0);
    /**
     * 整个ProcessQueue处理单元的offset最大边界
     */
    private volatile long queueOffsetMax = 0L;
    private volatile boolean dropped = false;
    private volatile long lastPullTimestamp = System.currentTimeMillis();
    private volatile long lastConsumeTimestamp = System.currentTimeMillis();
    private volatile boolean locked = false;
    private volatile long lastLockTimestamp = System.currentTimeMillis();
    /**
     * 是否正在消费
     */
    private volatile boolean consuming = false;
    /**
     * broker端还有多少条消息没被处理（拉取消息的那一刻）
     */
    private volatile long msgAccCnt = 0;

    /**
     * 处理消息时，拥有的锁是否过期，默认过期时间30s
     */
    public boolean isLockExpired() {
        return (System.currentTimeMillis() - this.lastLockTimestamp) > REBALANCE_LOCK_MAX_LIVE_TIME;
    }

    /**
     * 拉取消息是否过期，默认两分钟
     */
    public boolean isPullExpired() {
        return (System.currentTimeMillis() - this.lastPullTimestamp) > PULL_MAX_IDLE_TIME;
    }

    /**
     * 清理过期的消息
     * 1、最多16条一处理；
     * 2、消息在客户端存在超过15分钟就被认为已过期，然后从本地缓存中移除，以10s的延时消息方式发送会Broker
     *
     * @param pushConsumer
     */
    public void cleanExpiredMsg(DefaultMQPushConsumer pushConsumer) {
        log.info("start clean  msg");
        // 顺序消费不清理过期消息
        if (pushConsumer.getDefaultMQPushConsumerImpl().isConsumeOrderly()) {
            return;
        }

        // 每次最多清理16条消息
        int loop = msgTreeMap.size() < 16 ? msgTreeMap.size() : 16;
        for (int i = 0; i < loop; i++) {
            MessageExt msg = null;
            try {
                // 处理消息之前，先拿到读锁
                this.lockTreeMap.readLock().lockInterruptibly();
                try {
                    // 临时存放消息的treeMap不为空，并且 判断当前时间 - TreeMap里第一条消息的开始消费时间 > 15分钟
                    if (!msgTreeMap.isEmpty() && System.currentTimeMillis() - Long.parseLong(MessageAccessor.getConsumeStartTimeStamp(msgTreeMap.firstEntry().getValue())) > pushConsumer.getConsumeTimeout() * 60 * 1000) {
                        // 把第一条消息拿出来
                        msg = msgTreeMap.firstEntry().getValue();
                    } else {

                        break;
                    }
                } finally {
                    // 释放读锁
                    this.lockTreeMap.readLock().unlock();
                }
            } catch (InterruptedException e) {
                log.error("getExpiredMsg exception", e);
            }

            try {

                // 把过期消息以延时消息方式重新发给broker，10s之后才能消费。
                pushConsumer.sendMessageBack(msg, 3);
                log.info("send expire msg back. topic={}, msgId={}, storeHost={}, queueId={}, queueOffset={}", msg.getTopic(), msg.getMsgId(), msg.getStoreHost(), msg.getQueueId(), msg.getQueueOffset());
                String msgBody = new String(msg.getBody(), "UTF-8");
                Date currDate = new Date();
                System.out.println("Time is : " + currDate + ", send expire msg back. msgId=" + msg.getMsgId() + ", msgBody=" + msgBody + ", queueId=" + msg.getQueueId()  + ",queueOffset=" + msg.getQueueOffset());
                try {
                    // 获取写锁
                    this.lockTreeMap.writeLock().lockInterruptibly();
                    try {
                        if (!msgTreeMap.isEmpty() && msg.getQueueOffset() == msgTreeMap.firstKey()) {
                            try {
                                // 将过期消息从本地缓存中的消息列表中移除掉，  Collections.singletonList表示只有一个元素的List集合
                                removeMessage(Collections.singletonList(msg));
                            } catch (Exception e) {
                                log.error("send expired msg exception", e);
                            }
                        }
                    } finally {
                        // 释放写锁
                        this.lockTreeMap.writeLock().unlock();
                    }
                } catch (InterruptedException e) {
                    log.error("getExpiredMsg exception", e);
                }
                msgTreeMap.values().stream().forEach((msgInTree) -> {
                    try {
                        System.out.println("Msg OffSet is : " + msgInTree.getQueueOffset() + ", msgBody is : " + new String(msgInTree.getBody(), "UTF-8"));
                    } catch (UnsupportedEncodingException e) {
                        System.out.println("遍历msgTreeMap出现异常了！");
                    }
                });
                System.out.println("------------");

            } catch (Exception e) {
                log.error("send expired msg exception", e);
            }
        }
    }

    /**
     * 初始化属性：将消息放入到ProcessQueue的msgTreeMap中
     * MQClientInstance#start()方法中调用pullMessageService#start()
     * PullMessageService本身是一个ServiceThread，它的run()方法中会调用DefaultMQPushConsumerImpl#pullMessage()方法，
     * 然后在其中new出的PullCallBack的onSuccess()方法中：
     * 1. 当传入的pullResult的pullStatus是FOUND时，并且PullRequest的msgFoundList不为空时 会调用该putMessage()方法。
     *
     * @param msgs pullRequest的msgFoundList
     */
    public boolean putMessage(final List<MessageExt> msgs) {
        // 这个只有在顺序消费的时候才会遇到，并发消费不会用到
        boolean dispatchToConsume = false;
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                //有效消息数量
                int validMsgCnt = 0;
                for (MessageExt msg : msgs) {
                    // 把传过来的消息都都放在msgTreeMap中，以消息在queue中的offset作为key，msg做为value
                    MessageExt old = msgTreeMap.put(msg.getQueueOffset(), msg);
                    // 正常情况，说明原本msgTreeMap中不包含此条消息
                    if (null == old) {
                        validMsgCnt++;
                        // 将最后一个消息的offset赋值给queueOffsetMax
                        this.queueOffsetMax = msg.getQueueOffset();
                        // 把当前消息的长度加到msgSize中
                        msgSize.addAndGet(msg.getBody().length);
                    }
                }
                // 增加有效消息数量
                msgCount.addAndGet(validMsgCnt);

                // msgTreeMap不为空(含有消息)，并且不是正在消费状态
                // // 这个值在放消息的时候会设置为true，在顺序消费模式，取不到消息则设置为false
                if (!msgTreeMap.isEmpty() && !this.consuming) {
                    // 将ProcessQueue置为正在被消费状态
                    // 有消息，且为未消费状态，则顺序消费模式可以消费
                    dispatchToConsume = true;
                    this.consuming = true;
                }

                if (!msgs.isEmpty()) {
                    // 拿到最后一条消息
                    MessageExt messageExt = msgs.get(msgs.size() - 1);
                    // 获取broker端（拉取消息时）queue里最大的offset，maxOffset会存在每条消息里
                    String property = messageExt.getProperty(MessageConst.PROPERTY_MAX_OFFSET);
                    // 计算broker端还有多少条消息没有被消费
                    if (property != null) {
                        // broker端的最大偏移量 - 当前ProcessQueue中处理的最大消息偏移量
                        long accTotal = Long.parseLong(property) - messageExt.getQueueOffset();
                        if (accTotal > 0) {
                            this.msgAccCnt = accTotal;
                        }
                    }
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("putMessage exception", e);
        }

        return dispatchToConsume;
    }

    /**
     * processQueue中处理的一批消息中最大offset和最小offset之间的差距
     * <p>
     * DefaultMQPushConsumerImpl#pullMessage()方法中，会根据这个判断当前还有多少消息未处理，如果大于某个值(默认2000)，则进行流控处理
     */
    public long getMaxSpan() {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    // msgTreeMap中最后一个消息的offset - 第一个消息的offset
                    return this.msgTreeMap.lastKey() - this.msgTreeMap.firstKey();
                }
            } finally {
                this.lockTreeMap.readLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("getMaxSpan exception", e);
        }

        return 0;
    }

    /**
     * ProcessQueue移除消息逻辑
     * <p>
     * ConsumeMessageConcurrentlyService#processConsumeResult()中调用
     *
     * @return 这里的返回值和消费进度有很大的关系
     */
    public long removeMessage(final List<MessageExt> msgs) {
        // 当前消费进度的下一个offset
        long result = -1;
        final long now = System.currentTimeMillis();
        try {
            // 获取写锁
            this.lockTreeMap.writeLock().lockInterruptibly();
            // 最后消费的时间
            this.lastConsumeTimestamp = now;
            try {
                if (!msgTreeMap.isEmpty()) {
                    result = this.queueOffsetMax + 1;
                    // 删除的消息个数
                    int removedCnt = 0;
                    // 遍历消息，将其从Treemap中移除
                    for (MessageExt msg : msgs) {
                        MessageExt prev = msgTreeMap.remove(msg.getQueueOffset());
                        // 不为null，说明删除成功
                        if (prev != null) {
                            // 已经移除的消息数量
                            removedCnt--;
                            // ProcessQueue中消息的长度 - 当前消息长度
                            msgSize.addAndGet(0 - msg.getBody().length);
                        }
                    }
                    // ProcessQueue中的消息数量 - 删除的消息数量，即加上removedCnt（是一个负数）
                    msgCount.addAndGet(removedCnt);

                    // 如果还有消息存在，则使用msgTreeMap中的第一个消息的offset（即最小offset）作为消费进度
                    if (!msgTreeMap.isEmpty()) {
                        result = msgTreeMap.firstKey();
                    }
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (Throwable t) {
            log.error("removeMessage exception", t);
        }

        return result;
    }

    public TreeMap<Long, MessageExt> getMsgTreeMap() {
        return msgTreeMap;
    }

    public AtomicLong getMsgCount() {
        return msgCount;
    }

    public AtomicLong getMsgSize() {
        return msgSize;
    }

    public boolean isDropped() {
        return dropped;
    }

    public void setDropped(boolean dropped) {
        this.dropped = dropped;
    }

    public boolean isLocked() {
        return locked;
    }

    public void setLocked(boolean locked) {
        this.locked = locked;
    }

    /**
     * ConsumerMessageOrderlyService#processConsumeResult()中使用
     * 该方法已经过时了
     */
    public void rollback() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                // 把临时TreeMap中的消息全部放到msgTreeMap中，等待下一次消费
                this.msgTreeMap.putAll(this.consumingMsgOrderlyTreeMap);
                this.consumingMsgOrderlyTreeMap.clear();
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    /**
     * 在顺序消费模式下，调用takeMessages从msgTreeMap中获取消息：其内部会将消息都放在一个临时的TreeMap中，然后进行消费。
     * 消费完消息之后，我们需要调用commit()方法将这个临时的TreeMap清除。
     */
    public long commit() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                // 获取临时TreeMap中最后一个消息的offset
                Long offset = this.consumingMsgOrderlyTreeMap.lastKey();
                // 消费完成之后，减去该批次的消息数量
                msgCount.addAndGet(0 - this.consumingMsgOrderlyTreeMap.size());

                // 维护ProcessQueue中的总消息长度
                for (MessageExt msg : this.consumingMsgOrderlyTreeMap.values()) {
                    // 减去每条已消费消息的长度
                    msgSize.addAndGet(0 - msg.getBody().length);
                }
                // 清除临时TreeMap中的所有消息
                this.consumingMsgOrderlyTreeMap.clear();
                // 返回下一个消费进度
                if (offset != null) {
                    return offset + 1;
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("commit exception", e);
        }

        return -1;
    }

    /**
     * 顺序消费模式下，使消息可以被重新消费
     * ConsumerMessageOrderlyService#processConsumeResult()中使用
     */
    public void makeMessageToConsumeAgain(List<MessageExt> msgs) {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                for (MessageExt msg : msgs) {
                    // 从临时TreeMap中取出消息，
                    this.consumingMsgOrderlyTreeMap.remove(msg.getQueueOffset());
                    //再将消息放到msgTreeMap中
                    this.msgTreeMap.put(msg.getQueueOffset(), msg);
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("makeMessageToCosumeAgain exception", e);
        }
    }

    /**
     * 该方法在顺序消费模式下使用，取到消息后，就会调用我们定义的MessageListener进行消费。
     *
     * consumer.start()流程，即DefaultMQPushConsumerImpl流程，如果是顺序消费会实例化ConsumeMessageOrderlyService。
     * 在ConsumeMessageOrderlyService#submitConsumeRequest()方法中会new一个ConsumeRequest（是一个Runnable实现类），然后将其放到线程中执行；即将消息丢入消费服务线程中进行消费
     * 而submitConsumeRequest()方法被调用的场景有两种：
     * 1. 集群消费模式下调用submitConsumeRequestLater方法重新提交client端消费请求时，
     * 2.DefaultMQPushConsumerImpl#putMessage()中拉取消息成功之后，将消息丢入消费服务线程中进行消费。
     */
    public List<MessageExt> takeMessages(final int batchSize) {
        List<MessageExt> result = new ArrayList<MessageExt>(batchSize);
        final long now = System.currentTimeMillis();
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            this.lastConsumeTimestamp = now;
            try {
                if (!this.msgTreeMap.isEmpty()) {
                    // 从 msgTreeMap中获取batchSize条数据，每次都返回offset最小的那条消息并从msgTreeMap中移除
                    for (int i = 0; i < batchSize; i++) {
                        Map.Entry<Long, MessageExt> entry = this.msgTreeMap.pollFirstEntry();
                        if (entry != null) {
                            // 把消息放到返回列表
                            result.add(entry.getValue());
                            // 把消息的offset和消息体msg，放到顺序消费TreeMap中
                            consumingMsgOrderlyTreeMap.put(entry.getKey(), entry.getValue());
                        } else {
                            break;
                        }
                    }
                }

                // 没有取到消息，说明不需要消费，即将consuming置为FALSE。
                if (result.isEmpty()) {
                    consuming = false;
                }
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("take Messages exception", e);
        }

        return result;
    }

    public boolean hasTempMessage() {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();
            try {
                return !this.msgTreeMap.isEmpty();
            } finally {
                this.lockTreeMap.readLock().unlock();
            }
        } catch (InterruptedException e) {
        }

        return true;
    }

    public void clear() {
        try {
            this.lockTreeMap.writeLock().lockInterruptibly();
            try {
                this.msgTreeMap.clear();
                this.consumingMsgOrderlyTreeMap.clear();
                this.msgCount.set(0);
                this.msgSize.set(0);
                this.queueOffsetMax = 0L;
            } finally {
                this.lockTreeMap.writeLock().unlock();
            }
        } catch (InterruptedException e) {
            log.error("rollback exception", e);
        }
    }

    public long getLastLockTimestamp() {
        return lastLockTimestamp;
    }

    public void setLastLockTimestamp(long lastLockTimestamp) {
        this.lastLockTimestamp = lastLockTimestamp;
    }

    public Lock getLockConsume() {
        return lockConsume;
    }

    public long getLastPullTimestamp() {
        return lastPullTimestamp;
    }

    public void setLastPullTimestamp(long lastPullTimestamp) {
        this.lastPullTimestamp = lastPullTimestamp;
    }

    public long getMsgAccCnt() {
        return msgAccCnt;
    }

    public void setMsgAccCnt(long msgAccCnt) {
        this.msgAccCnt = msgAccCnt;
    }

    public long getTryUnlockTimes() {
        return this.tryUnlockTimes.get();
    }

    public void incTryUnlockTimes() {
        this.tryUnlockTimes.incrementAndGet();
    }

    public void fillProcessQueueInfo(final ProcessQueueInfo info) {
        try {
            this.lockTreeMap.readLock().lockInterruptibly();

            if (!this.msgTreeMap.isEmpty()) {
                info.setCachedMsgMinOffset(this.msgTreeMap.firstKey());
                info.setCachedMsgMaxOffset(this.msgTreeMap.lastKey());
                info.setCachedMsgCount(this.msgTreeMap.size());
                info.setCachedMsgSizeInMiB((int) (this.msgSize.get() / (1024 * 1024)));
            }

            if (!this.consumingMsgOrderlyTreeMap.isEmpty()) {
                info.setTransactionMsgMinOffset(this.consumingMsgOrderlyTreeMap.firstKey());
                info.setTransactionMsgMaxOffset(this.consumingMsgOrderlyTreeMap.lastKey());
                info.setTransactionMsgCount(this.consumingMsgOrderlyTreeMap.size());
            }

            info.setLocked(this.locked);
            info.setTryUnlockTimes(this.tryUnlockTimes.get());
            info.setLastLockTimestamp(this.lastLockTimestamp);

            info.setDroped(this.dropped);
            info.setLastPullTimestamp(this.lastPullTimestamp);
            info.setLastConsumeTimestamp(this.lastConsumeTimestamp);
        } catch (Exception e) {
        } finally {
            this.lockTreeMap.readLock().unlock();
        }
    }

    public long getLastConsumeTimestamp() {
        return lastConsumeTimestamp;
    }

    public void setLastConsumeTimestamp(long lastConsumeTimestamp) {
        this.lastConsumeTimestamp = lastConsumeTimestamp;
    }

}
