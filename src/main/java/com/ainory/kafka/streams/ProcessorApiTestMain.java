package com.ainory.kafka.streams;

import com.ainory.kafka.streams.entity.HostMetricVO;
import com.ainory.kafka.streams.process.ProcessorSupplierTest;
import com.ainory.kafka.streams.serializer.HostMetricVODeserializer;
import com.ainory.kafka.streams.serializer.HostMetricVOSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

/**
 * @author ainory on 2018. 4. 2..
 */
public class ProcessorApiTestMain {

    private void processorApiTest() {

        try {
//            CollectdKafkaVOSerializer collectdKafkaVOSerializer = new CollectdKafkaVOSerializer();
//            CollectdKafkaVODeserializer collectdKafkaVODeserializer = new CollectdKafkaVODeserializer();

            HostMetricVOSerializer hostMetricVOSerializer = new HostMetricVOSerializer();
            HostMetricVODeserializer hostMetricVODeserializer = new HostMetricVODeserializer();

            Topology topology = new Topology();

            topology.addSource("Source", new StringDeserializer(), new StringDeserializer(), "COLLECTD_DATA");

            topology.addProcessor("PROCESS1", new ProcessorSupplierTest(), "Source");

            StoreBuilder<KeyValueStore<String, HostMetricVO>> hostMetricStoreSupplier =
                    Stores.keyValueStoreBuilder(
                            Stores.inMemoryKeyValueStore("HostMetric"),
                            Serdes.String(), Serdes.serdeFrom(hostMetricVOSerializer, hostMetricVODeserializer));

            topology.addStateStore(hostMetricStoreSupplier, "PROCESS1");

            topology.addSink("SINK1", "ainory_kafka_summary", "PROCESS1");

            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ainory-stream-test10");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "spanal-app:9092,spanal-1:9092,spanal-2:9092,spanal-3:9092");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

            StreamsConfig config = new StreamsConfig(props);

            KafkaStreams streams = new KafkaStreams(topology, config);
            streams.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void processorApiRun() {
        try {
            processorApiTest();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        ProcessorApiTestMain processorApiTestMain = new ProcessorApiTestMain();

        processorApiTestMain.processorApiRun();
    }
}
