package com.ainory.kafka.streams.serializer;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

/**
 * @author ainory on 2018. 3. 20..
 */
public class CollectdKafkaVODeserializer<CollectdKafkaVO> implements Deserializer<CollectdKafkaVO>{

    /*private Class<CollectdKafkaVO> deserializedClass;

    public CollectdKafkaVODeserializer(Class<CollectdKafkaVO> deserializedClass) {
        this.deserializedClass = deserializedClass;
    }*/

    public CollectdKafkaVODeserializer() {
    }


    @Override
    public void configure(Map<String, ?> map, boolean b) {
//        if(deserializedClass == null) {
//            deserializedClass = (Class<CollectdKafkaVO>) map.get("serializedClass");
//        }
    }

    @Override
    public CollectdKafkaVO deserialize(String s, byte[] bytes) {
        if(bytes == null){
            return null;
        }

        return (CollectdKafkaVO) SerializationUtils.deserialize(bytes);
    }

    @Override
    public void close() {

    }
}
