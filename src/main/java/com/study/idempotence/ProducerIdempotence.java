package com.study.idempotence;

import com.sun.xml.internal.ws.runtime.config.TubelineFeatureReader;
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
public class ProducerIdempotence {
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
        // 设置ack的模式 -1为broker需要等待所有节点确认
        kp.put(ProducerConfig.ACKS_CONFIG, "-1");
        // 设置失败重试次数 和 设置消息发送超时时间
        // request.timeout.ms = 30000  默认
        // retries = 2147483647 默认
        kp.put(ProducerConfig.RETRIES_CONFIG, 3);
        kp.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 1);
        // 次项开启消息幂等 默认为false
        kp.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        // 默认值是5 设置为1能保证严格有序
        // 官方解释:在阻塞之前，客户端将在单个连接上发送的未确认请求的最大数量。
        // 注意，如果这个设置设置为大于1并且有失败的发送，那么重试(即它启用了重试)会有消息重新排序的风险。
        // 设置为1 会阻塞其他pid的提交,进而保证pid的有序性
        kp.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(kp);
        producer.send(new ProducerRecord<String, String>("topic04", "idempotence", "idempotence test" ));
        producer.flush();
        producer.close();
    }
}
