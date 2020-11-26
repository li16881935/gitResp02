package com.study.serialize;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

/**
 * @Auther: lipengchao
 * @Date: 2020/11/13 -- 11:40
 * @Description: com.study.serialize
 * @version: 1.0
 */
public class MySerializable implements Serializer<Object> {


    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Object o) {
        return SerializationUtils.serialize((Serializable) o);
    }

    @Override
    public void close() {

    }
}
