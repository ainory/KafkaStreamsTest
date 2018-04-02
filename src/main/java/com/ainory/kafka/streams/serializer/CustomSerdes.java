package com.ainory.kafka.streams.serializer;

import com.ainory.kafka.streams.entity.HostMetricVO;
import com.ainory.kafka.streams.entity.DslHostMetricVO;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @author ainory on 2018. 3. 30..
 */
public class CustomSerdes {


    public CustomSerdes() {
    }

    public static Serde<HostMetricVO> HostMetricVO(){
        return new CustomSerdes.HostMetricVOSerde();
    }

    public static Serde<DslHostMetricVO> DslHostMetricVO(){
        return new CustomSerdes.DslHostMetricVOSerde();
    }

    public static final class HostMetricVOSerde extends CustomSerdes.HostMetricWrapperSerde<HostMetricVO>{
        public HostMetricVOSerde(){
            super(new HostMetricVOSerializer<>(), new HostMetricVODeserializer<>());
        }
    }

    public static final class DslHostMetricVOSerde extends CustomSerdes.DslHostMetricWrapperSerde<DslHostMetricVO>{
        public DslHostMetricVOSerde(){
            super(new DslHostMetricVOSerializer<>(), new DslHostMetricVODeserializer<>());
        }
    }

    protected static class HostMetricWrapperSerde<HostMetricVO> implements Serde<HostMetricVO> {

        private final Serializer<HostMetricVO> serializer;
        private final Deserializer<HostMetricVO> deserializer;

        public HostMetricWrapperSerde(Serializer<HostMetricVO> serializer, Deserializer<HostMetricVO> deserializer) {
            this.serializer = serializer;
            this.deserializer = deserializer;
        }

        @Override
        public void configure(Map<String, ?> map, boolean b) {
            serializer.configure(map, b);
            deserializer.configure(map, b);
        }

        @Override
        public void close() {
            serializer.close();
            deserializer.close();

        }

        @Override
        public Serializer<HostMetricVO> serializer() {
            return serializer;
        }

        @Override
        public Deserializer<HostMetricVO> deserializer() {
            return deserializer;
        }
    }

    protected static class DslHostMetricWrapperSerde<DslHostMetricVO> implements Serde<DslHostMetricVO> {

        private final Serializer<DslHostMetricVO> serializer;
        private final Deserializer<DslHostMetricVO> deserializer;

        public DslHostMetricWrapperSerde(Serializer<DslHostMetricVO> serializer, Deserializer<DslHostMetricVO> deserializer) {
            this.serializer = serializer;
            this.deserializer = deserializer;
        }

        @Override
        public void configure(Map<String, ?> map, boolean b) {
            serializer.configure(map, b);
            deserializer.configure(map, b);
        }

        @Override
        public void close() {
            serializer.close();
            deserializer.close();

        }

        @Override
        public Serializer<DslHostMetricVO> serializer() {
            return serializer;
        }

        @Override
        public Deserializer<DslHostMetricVO> deserializer() {
            return deserializer;
        }
    }
}
