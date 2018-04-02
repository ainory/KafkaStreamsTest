package com.ainory.kafka.streams.serializer;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Serializer;

import java.io.Serializable;
import java.util.Map;

/**
 * @author ainory on 2018. 3. 20..
 */
public class DslHostMetricVOSerializer<DslHostMetricVO> implements Serializer<DslHostMetricVO> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, DslHostMetricVO dslHostMetricVO) {
        return SerializationUtils.serialize((Serializable) dslHostMetricVO);
    }

    @Override
    public void close() {

    }
}