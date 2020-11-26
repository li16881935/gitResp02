package com.study.ack;

import com.study.quickstart.Producer;
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
public class ProducerAcks {
    /**
     * kafka 默认应答为 ack =  1
     * ack = 1 : Leader会将Record记录到本地日志文件中,但是不会等待其他follower的确认,如果leader确认记录后立即失败
     *存在消息丢失
     * ack = 0 : 生产者不会等待服务器的任何确认.该记录立即添加到套接字缓冲区中,并视为已发送,不能保证消息一定发送到kafka
     * 但是效率非常高 ,适用于日志等不是很重要的场景
     * ack = all/-1 : 意味着leader将等待所有副本的确认,这将保证消息不会丢失,但是存在多次消费的问题,以下有例子说明
     */
    public static void main(String[] args) {

        Properties kp = new Properties();
        kp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"node02:9092,node03:9092,node04:9092");
        kp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置ack的模式
        kp.put(ProducerConfig.ACKS_CONFIG, "-1");
        // 设置失败重试次数 和 设置消息发送超时时间
        // request.timeout.ms = 30000  默认
        // retries = 2147483647 默认
        kp.put(ProducerConfig.RETRIES_CONFIG, 3);
        kp.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kp);
        producer.send(new ProducerRecord<String, String>("topic04", "ack", "ack test" ));
        producer.flush();
        producer.close();
    }
}
