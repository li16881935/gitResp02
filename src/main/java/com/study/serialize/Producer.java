package com.study.serialize;

import com.study.domain.User;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Date;
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
        kp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MySerializable.class.getName());
        KafkaProducer<String, User> producer = new KafkaProducer<>(kp);
        for (int i = 0; i < 10; i++) {
            User user = new User(i, "name" + i, new Date());
            producer.send(new ProducerRecord<String, User>("topic03", "key" + i, user));
        }
        producer.close();
    }
}
