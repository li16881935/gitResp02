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
public class ConsumerOffset {

    public static void main(String[] args) {
        //创建kafka消费者
        // 1.设置kafka 消费者属性 反序列的配置  消费者组
        Properties kp = new Properties();
        kp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node02:9092,node03:9092,node04:9092");
        kp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kp.put(ConsumerConfig.GROUP_ID_CONFIG, "g3");

        // 注: 默认的offset配置是latest类型
        // 常规类型有:
        // -latest  默认  没有偏移量的时候从最近的消费偏移量开始消费
        // -earliest    没有偏移量的时候从最早的消费偏移量开始消费
        // -none    没有偏移量直接报错
        kp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // 是否自动提交偏移量 如果设置时间过长 将会造成多次重复消费
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
