package com.study.interceptor;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @Auther: lipengchao
 * @Date: 2020/11/13 -- 17:59
 * @Description: com.study.interceptor
 * @version: 1.0
 */
public class MyInterceptor implements ProducerInterceptor{

    /**
     *
     * @param record
     * @return
     */
    @Override
    public ProducerRecord onSend(ProducerRecord record) {
        // 此处对拦截的消息进行封装
        ProducerRecord record2 = new ProducerRecord(record.topic(), record.key(), record.value() + "my interceptor");
        System.out.println("producerRecord: "+ record2);
        return record2;
    }

    /**
     * 成功或者失败都会调用此方法
     * @param recordMetadata
     * @param e
     */
    @Override
    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        System.out.println("recordMetadata: "+recordMetadata +";\t"+"Exception: "+ e);
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
