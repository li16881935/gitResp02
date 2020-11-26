package com.study.offsets;

import com.study.domain.Message;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

/**
 * @Auther: lipengchao
 * @Date: 2020/11/12 -- 19:53
 * @Description: com.study.quickstart
 * @version: 1.0
 */
public class ConsumerOffset02 {

    public static void main(String[] args) {
        //创建kafka消费者
        // 1.设置kafka 消费者属性 反序列的配置  消费者组
        Properties kp = new Properties();
        kp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node02:9092,node03:9092,node04:9092");
        kp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kp.put(ConsumerConfig.GROUP_ID_CONFIG, "g4");

        // 是否自动提交偏移量 如果设置时间过长 将会造成多次重复消费
        // 不自动提交 采用手动提交的策略
        kp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        kp.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 10000);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(kp);
        // 2.消费对应topic
        // 可以是集合或者正则表达式
        consumer.subscribe(Arrays.asList("topic03"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (!records.isEmpty()) {
                Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
                while (iterator.hasNext()) {
                    ConsumerRecord<String, String> next = iterator.next();
                    Message message = new Message(next.topic(),next.partition(),next.offset(),next.key(),next.value(), next.timestamp());
                    System.out.println(message);
                }
            }
        }


    }
}
