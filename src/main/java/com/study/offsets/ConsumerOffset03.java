package com.study.offsets;

import com.study.domain.Message;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;

/**
 * @Auther: lipengchao
 * @Date: 2020/11/12 -- 19:53
 * @Description: com.study.quickstart
 * @version: 1.0
 */
public class ConsumerOffset03 {

    public static void main(String[] args) {
        //创建kafka消费者
        // 1.设置kafka 消费者属性 反序列的配置  消费者组
        Properties kp = new Properties();
        kp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node02:9092,node03:9092,node04:9092");
        kp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kp.put(ConsumerConfig.GROUP_ID_CONFIG, "g2");

        // 是否自动提交偏移量 如果设置时间过长 将会造成多次重复消费
        // 不自动提交 采用手动提交的策略
        // kp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 如果关闭自动提交策略,需要手动写提交策略
        kp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // kp.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 10000);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(kp);
        // 2.消费对应topic
        // 可以是集合或者正则表达式
        consumer.subscribe(Arrays.asList("topic04"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (!records.isEmpty()) {
                Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
                // 在拿到对应的结果遍历消费之前,创建一个用于提交offset的hashmap
                HashMap<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                while (iterator.hasNext()) {
                    ConsumerRecord<String, String> next = iterator.next();
                    Message message = new Message(next.topic(), next.partition(), next.offset(), next.key(), next.value(), next.timestamp());
                    System.out.println(message);
                    // 消费完成后 需要提交偏移量
                    // 注: 偏移量永远是kafka下次读取的偏移量 所以提交的偏移量为当前偏移量+1
                    offsets.put(new TopicPartition(next.topic(), next.partition()), new OffsetAndMetadata(next.offset()+1));
                    // 进行偏移量的提交 分为异步和同步 并且可以指定回调
                    consumer.commitAsync(offsets, new OffsetCommitCallback() {
                        @Override
                        public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                            System.out.println("map:" + map);
                            System.out.println("Exception:" + e);
                        }
                    });
                    // 同步提交可以指定提交的周期
//                    consumer.commitSync();
                }
            }
        }


    }
}
