package com.ainory.kafka.streams.serializer;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * @author ainory on 2018. 3. 20..
 */
public class HostMetricVODeserializer<HostMetricVO> implements Deserializer<HostMetricVO> {


    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public HostMetricVO deserialize(String s, byte[] bytes) {
        if(bytes == null){
            return null;
        }

        return (HostMetricVO) SerializationUtils.deserialize(bytes);
    }

    @Override
    public void close() {

    }
}
