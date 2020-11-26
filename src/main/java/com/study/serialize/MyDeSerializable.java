package com.study.serialize;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

/**
 * @Auther: lipengchao
 * @Date: 2020/11/13 -- 11:40
 * @Description: com.study.serialize
 * @version: 1.0
 */
public class MyDeSerializable implements Deserializer<Object> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Object deserialize(String s, byte[] bytes) {
        return SerializationUtils.deserialize(bytes);
    }

    @Override
    public void close() {

    }
}
