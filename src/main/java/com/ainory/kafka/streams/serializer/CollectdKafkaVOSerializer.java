package com.ainory.kafka.streams.serializer;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Serializer;

import java.io.*;
import java.util.Map;

/**
 * @author ainory on 2018. 3. 20..
 */
public class CollectdKafkaVOSerializer<CollectdKafkaVO> implements Serializer<CollectdKafkaVO>{

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, CollectdKafkaVO collectdKafkaVO) {
        return SerializationUtils.serialize((Serializable) collectdKafkaVO);
    }

    @Override
    public void close() {

    }
}
