package com.ainory.kafka.streams.serializer;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Map;

/**
 * @author ainory on 2018. 3. 27..
 */
public class NumberDeserializer implements Deserializer<Number> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Number deserialize(String topic, byte[] bytes) {
        if(bytes == null){
            return null;
        }

        return (Number) SerializationUtils.deserialize(bytes);
    }

    @Override
    public void close() {

    }
}
