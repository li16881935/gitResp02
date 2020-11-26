package com.study.serialize;

import com.study.domain.Message;
import com.study.domain.User;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * @Auther: lipengchao
 * @Date: 2020/11/12 -- 19:53
 * @Description: com.study.quickstart
 * @version: 1.0
 */
public class Consumer {
    public static void main(String[] args) {
        //创建kafka消费者
        // 1.设置kafka 消费者属性 反序列的配置  消费者组
        Properties kp = new Properties();
        kp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node02:9092,node03:9092,node04:9092");
        kp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MyDeSerializable.class.getName());
        kp.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");
        KafkaConsumer<String, User> consumer = new KafkaConsumer<>(kp);
        // 2.消费对应topic
        // 可以是集合或者正则表达式
        consumer.subscribe(Arrays.asList("topic03"));
        while (true) {
            ConsumerRecords<String, User> records = consumer.poll(Duration.ofSeconds(1));
            if (!records.isEmpty()) {
                Iterator<ConsumerRecord<String, User>> iterator = records.iterator();
                while (iterator.hasNext()) {
                    ConsumerRecord<String, User> next = iterator.next();
                    System.out.println(next.value());
                }
            }
        }

    }
}
