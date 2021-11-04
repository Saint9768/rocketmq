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
package org.apache.rocketmq.client.consumer.rebalance;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;

/**
 * Average Hashing queue algorithm
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
    private final InternalLogger log = ClientLogger.getLog();

    @Override
    public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
        List<String> cidAll) {
        if (currentCID == null || currentCID.length() < 1) {
            throw new IllegalArgumentException("currentCID is empty");
        }
        if (mqAll == null || mqAll.isEmpty()) {
            throw new IllegalArgumentException("mqAll is null or mqAll empty");
        }
        if (cidAll == null || cidAll.isEmpty()) {
            throw new IllegalArgumentException("cidAll is null or cidAll empty");
        }

        List<MessageQueue> result = new ArrayList<MessageQueue>();
        // 如果消费者列表中不包含当前客户端ID，返回给空集合；
        // 为啥消费者列表中会不包含当前客户端ID？？？？ 因为有可能消费者还没有注册到Broker中，即broker还不知道这个客户端的存在，也就不用这个客户端干活了。
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                consumerGroup,
                currentCID,
                cidAll);
            return result;
        }

        // 获取当前客户端ID 在 消费者集合中的索引位置。
        int index = cidAll.indexOf(currentCID);
        // MessageQueue个数 % 消费者个数，判断有多个MessageQueue无法平均分配。
        int mod = mqAll.size() % cidAll.size();
        // 计算当前消费者可以分几个队列去消费。
        int averageSize =
                // 队列个数 是否小于等于 消费者个数
            mqAll.size() <= cidAll.size() ?
                    1 // 队列个数小于消费者个数时，一个消费者只能分到一个queue
                    : (mod > 0 && index < mod ? // 队列个数大约消费者个数时，如果queue还剩'零头' 并且 当前client在消费者集合中的索引 小于 '零头'
                            mqAll.size() / cidAll.size() + 1 // 当前client会多分到一个queue
                    : mqAll.size() / cidAll.size()); // 没有'零头' 或 当前client在消费者集合中的索引 大于等于 '零头' ，则平均分配（不管"零头"）
            // 假如 8个queue 3个consumer（0、1、2），假设当前client在consumer集合的0索引处。
            // 此时，0可以分3个queue,   1可以分3个queue，  3可以分2个

        // 计算开始的位置
        // 计算出 集合0位置client在MessageQueue中开始的位置时：0 * 3 = 0；  1位置： 1 * 3 = 3；   2位置： 2 * 2 + 2 = 6；
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        // 计算出当前消费者可消费queue在MessageQueue集合中的范围
        // 消费者集合0位置的 range = min(3, 8)=3; 1位置的range = min(3, 5)=3; 2位置的range = min(6，2)=2。
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        // 将计算出的MessageQueue加入到结果中并返回。
        for (int i = 0; i < range; i++) {
            // 当前client所在消费者集合的0位置，则： 第一次添加 (0 + 0) % 8,  第二次(0 + 1) % 8，  第三次(0 + 2) % 8
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }
}
