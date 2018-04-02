package com.ainory.kafka.streams.serializer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @author ainory on 2018. 3. 27..
 */
public class NumberSerializer implements Serializer<Number> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String topic, Number number) {

        if(number instanceof Integer){
            return Serdes.Integer().serializer().serialize(topic, number.intValue());
        }else if(number instanceof Long){
            return Serdes.Long().serializer().serialize(topic, number.longValue());
        }else if(number instanceof Float){
            return Serdes.Float().serializer().serialize(topic, number.floatValue());
        }else if(number instanceof Double){
            return Serdes.Double().serializer().serialize(topic, number.doubleValue());
        }else {
            return new byte[0];
        }
    }

    @Override
    public void close() {

    }
}
