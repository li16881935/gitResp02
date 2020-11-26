package com.study.transaction;

import com.study.domain.Message;
import com.study.quickstart.Producer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;

/**
 * @Auther: lipengchao
 * @Date: 2020/11/15 -- 19:47
 * @Description: com.study.transaction
 * @version: 1.0
 */
public class PCTransaction {
    public static void main(String[] args) {
        KafkaProducer producer = getProducerInstance();
        KafkaConsumer consumer = getConsumerInstance("g1");
        consumer.subscribe(Arrays.asList("topic04"));
        //初始化事务
        producer.initTransactions();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            if (!records.isEmpty()) {
                Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
                // 在拿到对应的结果遍历消费之前,创建一个用于提交offset的hashmap
                HashMap<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                while (iterator.hasNext()) {
                    ConsumerRecord<String, String> next = iterator.next();
                    try {
                        producer.beginTransaction();
//                        int j = 10 / 0;
                        producer.send(new ProducerRecord<String, String>("topic05", next.key(), next.value()+"pc事务测试!!!"));
                        // producer.flush();
                        offsets.put(new TopicPartition(next.topic(), next.partition()), new OffsetAndMetadata(next.offset() + 1));
                        // 注:以下方法将偏移量和生产者only事务原子化
                        producer.sendOffsetsToTransaction(offsets,"g1");
                        producer.commitTransaction();

                    } catch(Exception e){
                        System.err.println("error: "+e.getMessage());
                        producer.abortTransaction();
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private static KafkaProducer getProducerInstance() {
        Properties kp = new Properties();
        kp.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node02:9092,node03:9092,node04:9092");
        kp.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kp.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置事务的id,必须是唯一的
        kp.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transaction-id" + UUID.randomUUID().toString());
        // 此项设置代表批处理的大小为1024
        kp.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);
        // 此项设置代表如果批处理的消息不够上面设置的量 会延迟多少毫秒,
        kp.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        // 此项开启消息幂等 默认为false
        kp.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        kp.put(ProducerConfig.ACKS_CONFIG, "all");
        // 默认请求失败的时间是30000ms
        kp.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000);
        return new KafkaProducer<String, String>(kp);
    }

    private static KafkaConsumer getConsumerInstance(String groupId) {
        //创建kafka消费者
        // 1.设置kafka 消费者属性 反序列的配置  消费者组
        Properties kp = new Properties();
        kp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node02:9092,node03:9092,node04:9092");
        kp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kp.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kp.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        // 关闭自动提交策略,需要手动写提交策略
        kp.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new KafkaConsumer<String, String>(kp);

    }
}
