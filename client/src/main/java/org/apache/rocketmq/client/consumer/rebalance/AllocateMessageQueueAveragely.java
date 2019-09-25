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
        if (!cidAll.contains(currentCID)) {
            log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
                consumerGroup,
                currentCID,
                cidAll);
            return result;
        }

        //当前队列的下标
        int index = cidAll.indexOf(currentCID);
        //mod代表多出来的队列数
        int mod = mqAll.size() % cidAll.size();
        //averageSize 奇数
        // mqAll.size() <= cidAll.size() 消费者数量超过了Mq队列永远返回1
        // mod>0 && index < mod，说明：不能整除，index小于mod说明有富裕的mq可分配mqAll.size() / cidAll.size()+1，否则mqAll.size() / cidAll.size()
        int averageSize =
            mqAll.size() <= cidAll.size() ? 1 : (mod > 0 && index < mod ? mqAll.size() / cidAll.size()
                + 1 : mqAll.size() / cidAll.size());

        //startIndex：
        //不能整除：并且有富裕的多出来的，index*avarageSize
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.min(averageSize, mqAll.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(mqAll.get((startIndex + i) % mqAll.size()));
        }
        return result;
    }

    @Override
    public String getName() {
        return "AVG";
    }

    public static void main(String[] args) {
        int index=2;
        int mqSize=10;
        int cId=7;
        int mod=mqSize%cId;

        int averageSize =
                mqSize <= cId ? 1 : (mod > 0 && index < mod ? mqSize/ cId
                        + 1 : mqSize / cId);
        System.out.println(averageSize);
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        System.out.println(startIndex);

        int range = Math.min(averageSize, mqSize - startIndex);
        System.out.println(range);

        System.out.println("=====");
        for (int i = 0; i < range; i++) {
            System.out.println((startIndex + i) % mqSize);
        }

    }
}
