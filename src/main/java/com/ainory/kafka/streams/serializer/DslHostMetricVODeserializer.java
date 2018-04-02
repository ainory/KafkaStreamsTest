package com.ainory.kafka.streams.serializer;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * @author ainory on 2018. 3. 20..
 */
public class DslHostMetricVODeserializer<DslHostMetricVO> implements Deserializer<DslHostMetricVO> {


    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public DslHostMetricVO deserialize(String s, byte[] bytes) {
        if(bytes == null){
            return null;
        }

        return (DslHostMetricVO) SerializationUtils.deserialize(bytes);
    }

    @Override
    public void close() {

    }
}