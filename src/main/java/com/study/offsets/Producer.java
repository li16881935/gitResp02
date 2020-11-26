package com.study.offsets;

import com.study.interceptor.MyInterceptor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @Auther: lipengchao
 * @Date: 2020/11/12 -- 19:53
 * @Description: com.study.quickstart
 * @version: 1.0
 */
public class Producer {

    public static void main(String[] args) {
        Properties kp = new Properties();
        kp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"node02:9092,node03:9092,node04:9092");
        kp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // kp.put(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, MyInterceptor.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kp);
        for (int i = 10; i < 15; i++) {
            // 注: ProduceRecord 可以四个参数的第二个可以指定发送到topic 下的某个分区
            producer.send(new ProducerRecord<String, String>("topic04", "key" + i, "value" + i));
        }
        producer.close();
    }
}
