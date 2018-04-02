package com.ainory.kafka.streams.serializer;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

/**
 * @author ainory on 2018. 3. 20..
 */
public class HostMetricVOSerializer<HostMetricVO> implements Serializer<HostMetricVO> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, HostMetricVO hostMetricVO) {
        return SerializationUtils.serialize((Serializable) hostMetricVO);
    }

    @Override
    public void close() {

    }
}
