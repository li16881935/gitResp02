package com.study.quickstart;

import com.study.domain.Message;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @Auther: lipengchao
 * @Date: 2020/11/12 -- 19:53
 * @Description: com.study.quickstart
 * @version: 1.0
 */
public class Consumer_Group_1 {
    public static final Pattern pattern = Pattern.compile("^topic.*");

    public static void main(String[] args) {
        //创建kafka消费者
        // 1.设置kafka 消费者属性 反序列的配置  消费者组
        Properties kp = new Properties();
        kp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node02:9092,node03:9092,node04:9092");
        kp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kp.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(kp);
        // 2.消费对应topic
        // 可以是集合或者正则表达式
        consumer.subscribe(pattern);
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
