package com.study.transaction;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

/**
 * @Auther: lipengchao
 * @Date: 2020/11/12 -- 19:53
 * @Description: com.study.quickstart
 * @version: 1.0
 */
public class ProducerTransaction {
    /**
     * kafka 生产者幂等
     * 前提 需设置 ack模式为all/-1和失败重试机制 也就是 leader会等待所有副本因子的确认,可以保证消息不会丢失,但是有重复消费的问题
     *  区分重复有两点
     *  1.消息幂等 2.消息是否消费过
     *
     * 生产者会生成一个PID pid 此id是从零开始单调递增 只有提交的pid 等于之前提交的pid+1 broker才会接受此消息,否则broker会认为此消息是
     * 失败从新发送的 ,不会接受此消息
     *
     */
    public static void main(String[] args) {

        Properties kp = new Properties();
        kp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"node02:9092,node03:9092,node04:9092");
        kp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置事务的id,必须是唯一的
        kp.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction-id"+UUID.randomUUID().toString());
        // 此项设置代表批处理的大小为1024
        kp.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);
        // 此项设置代表如果批处理的消息不够上面设置的量 会延迟多少毫秒,
        kp.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        // 此项开启消息幂等 默认为false
        kp.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        kp.put(ProducerConfig.ACKS_CONFIG, "all");
        // 默认请求失败的时间是30000ms
        kp.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1000000);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kp);

        //初始化事务
        producer.initTransactions();
        try {
            producer.beginTransaction();
            for (int i = 0; i < 10; i++) {
                if (i == 8) {
                    int j = i / 0;
                }
                producer.send(new ProducerRecord<String, String>("topic04","transaction" + i, "right transaction test"  + i));
                producer.flush();
            }
            producer.commitTransaction();
        } catch (Exception e) {
            System.err.println(e.getMessage());
            producer.abortTransaction();
            e.printStackTrace();
        }

    }
}
