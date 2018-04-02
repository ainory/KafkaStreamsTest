package com.ainory.kafka.streams;

import com.ainory.kafka.streams.entity.CollectdKafkaVO;
import com.ainory.kafka.streams.entity.HostMetricVO;
import com.ainory.kafka.streams.process.ProcessorSupplierTest;
import com.ainory.kafka.streams.serializer.HostMetricVODeserializer;
import com.ainory.kafka.streams.serializer.HostMetricVOSerializer;
import com.ainory.kafka.streams.timestamp.extractor.CollectdTimestampExtractor;
import com.ainory.kafka.streams.util.JsonUtil;
import com.ainory.kafka.streams.entity.DslHostMetricVO;
import com.ainory.kafka.streams.keyvalue.mapper.DslKeyValueMapper;
import com.ainory.kafka.streams.serializer.CustomSerdes;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author ainory on 2018. 3. 6..
 */
public class KafkaTest {

    public static int current_sum = 0;
    public static long current_time = 0L;

    public static int WINDOW_SECONDS = 60;

    public static Long previousValue = null;
    public static Long sumValue = null;

    private void streamWindowTest1() {
        try {
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-tumbling-window");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

            StreamsBuilder builder = new StreamsBuilder();

            KStream<String, String> source = builder.stream("longs11");

            KStream<Windowed<String>, String> sum = source
                    // temperature values are sent without a key (null), so in order
                    // to group and reduce them, a key is needed ("temp" has been chosen)
                    .selectKey(new KeyValueMapper<String, String, String>() {
                        @Override
                        public String apply(String key, String value) {
                            return "num";
                        }
                    })
                    .groupByKey()
                    .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(WINDOW_SECONDS)).advanceBy(TimeUnit.SECONDS.toMillis(WINDOW_SECONDS)))
                    .reduce(new Reducer<String>() {
                        @Override
                        public String apply(String value1, String value2) {
                            System.out.println(value1 + " : " + value2);

                            try {
                                Integer.parseInt(value1);
                                Integer.parseInt(value2);
                            } catch (Exception e) {
                                return "0";
                            }
                            return String.valueOf(Integer.parseInt(value1) + Integer.parseInt(value2));
                        }
                    })
                    .toStream()
                    .filter(new Predicate<Windowed<String>, String>() {
                        @Override
                        public boolean test(Windowed<String> key, String value) {
                            System.out.println(key.window().start() + " : " + key.window().end() + " : " + value);


                            // time init
                            if (current_time == 0) {
                                current_time = key.window().start();
                                System.out.println("init : " + current_time);
                                return false;
                            }

                            if ((current_time + (WINDOW_SECONDS * 1000)) == key.window().start()) {
                                System.out.println("match --> " + current_time + " : " + key.window().start());
                                current_time = key.window().start();
                                return true;
                            } else {
                                return false;
                            }
                        }
                    });

            WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(Serdes.String().serializer());
            WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(Serdes.String().deserializer(), WINDOW_SECONDS);
            Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);

            sum.to("longs-sum", Produced.with(windowedSerde, Serdes.String()));


            final KafkaStreams streams = new KafkaStreams(builder.build(), props);
//            final CountDownLatch latch = new CountDownLatch(1);

            // attach shutdown handler to catch control-c
            Runtime.getRuntime().addShutdownHook(new Thread("streams-tumbling-window-shutdown-hook") {
                @Override
                public void run() {
                    streams.close();
//                    latch.countDown();
                }
            });

            try {
                streams.start();
//                latch.await();
            } catch (Throwable e) {
                System.exit(1);
            }


            // Now generate the data and write to the topic.
            Properties producerConfig = new Properties();
            producerConfig.put("bootstrap.servers", "spanal-app:9092,spanal-1:9092,spanal-2:9092,spanal-3:9092");
            producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            KafkaProducer producer = new KafkaProducer<String, String>(producerConfig);

            Random rng = new Random();

            String value;
            while (true) {

                value = String.valueOf(rng.nextInt(100));
//                System.out.println(value);
                producer.send(new ProducerRecord<>("longs11", "num", value));
                try {
                    Thread.sleep(1000L);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            } // Close infinite data generating loop.

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void streamWindowTest2() {

        try {


            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-tumbling-window33");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "spanal-app:9092,spanal-1:9092,spanal-2:9092,spanal-3:9092");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

            StreamsBuilder builder = new StreamsBuilder();

            KStream<String, Long> source = builder.stream("longs");

            KGroupedStream<String, Long> groupedSource = source.groupByKey();

            KTable<Windowed<String>, Long> windowTable = groupedSource.windowedBy(TimeWindows.of(10000))
                    .aggregate(new Initializer<Long>() {
                        @Override
                        public Long apply() {
                            return 0L;
                        }
                    }, new Aggregator<String, Long, Long>() {
                        @Override
                        public Long apply(String aggKey, Long newValue, Long aggValue) {
                            return aggValue + newValue;
                        }
                    }, Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-stream-store").withValueSerde(Serdes.Long()));

            WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(Serdes.String().serializer());
            WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(Serdes.String().deserializer(), WINDOW_SECONDS);
            Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);

            windowTable.toStream().filter(new Predicate<Windowed<String>, Long>() {
                @Override
                public boolean test(Windowed<String> stringWindowed, Long aLong) {

//                    System.out.println("[DATA] " + previousValue +" : "+aLong);

                    if (previousValue == null) {
                        System.out.println("[INIT] " + previousValue + " : " + aLong);
                        previousValue = aLong;

                        return false;
                    }

                    if (sumValue == null) {
                        sumValue = previousValue;
                    }

                    if (previousValue > aLong) {
                        System.out.println("[SEND] " + previousValue + " : " + aLong);
                        sumValue = previousValue;
                        previousValue = aLong;
                        return true;
                    } else {
                        System.out.println("[SKIP] " + previousValue + " : " + aLong);
                        sumValue = previousValue;
                        previousValue = aLong;
                        return false;
                    }
                }
            }).map(new KeyValueMapper<Windowed<String>, Long, KeyValue<Windowed<String>, Long>>() {
                @Override
                public KeyValue<Windowed<String>, Long> apply(Windowed<String> stringWindowed, Long aLong) {
                    return new KeyValue<>(stringWindowed, sumValue);
                }
            }).to("long-count-all", Produced.with(windowedSerde, Serdes.Long()));



            /*
            .filter(new Predicate<Windowed<String>, Long>() {
                @Override
                public boolean test(Windowed<String> stringWindowed, Long aLong) {

                    if(previousValue == null){
                        System.out.println("!!! " + previousValue +" : "+aLong);
                        previousValue = aLong;

                        return false;
                    }

                    if(previousValue > aLong){
                        System.out.println(previousValue +" : "+aLong);
                        previousValue = aLong;
                        return true;
                    }

                    return false;
                }
            })
             */

            /*groupedSource.windowedBy(TimeWindows.of(10000))
                    .aggregate(new Initializer<Long>() {
                        @Override
                        public Long apply() {
                            return 0L;
                        }
                    }, new Aggregator<String, Long, Long>() {
                        @Override
                        public Long apply(String aggKey, Long newValue, Long aggValue) {
                            return aggValue + newValue;
                        }
                    }, Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("time-windowed-aggregated-stream-store").withValueSerde(Serdes.Long()));


            WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(Serdes.String().serializer());
            WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(Serdes.String().deserializer(), WINDOW_SECONDS);
            Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);

            groupedSource.*/


            final KafkaStreams streams = new KafkaStreams(builder.build(), props);
//            final CountDownLatch latch = new CountDownLatch(1);

            // attach shutdown handler to catch control-c
            Runtime.getRuntime().addShutdownHook(new Thread("streams-tumbling-window-shutdown-hook") {
                @Override
                public void run() {
                    streams.close();
//                    latch.countDown();
                }
            });

            try {
                streams.start();
//                latch.await();
            } catch (Throwable e) {
                System.exit(1);
            }

            // Now generate the data and write to the topic.
            Properties producerConfig = new Properties();
            producerConfig.put("bootstrap.servers", "localhost:9092");
            producerConfig.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerConfig.put("value.serializer", "org.apache.kafka.common.serialization.LongSerializer");

            KafkaProducer producer = new KafkaProducer<String, String>(producerConfig);

            Random rng = new Random();

            Long value;
            while (true) {

                value = rng.nextLong();
//                System.out.println(value);
                producer.send(new ProducerRecord<>("longs", "num", value));
                try {
                    Thread.sleep(1000L);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            } // Close infinite data generating loop.


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void streamWindowTest3() {

        try {

            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ainory-stream-test");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "spanal-app:9092,spanal-1:9092,spanal-2:9092,spanal-3:9092");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

            StreamsBuilder builder = new StreamsBuilder();
            KStream<String, String> collectd = builder.stream("COLLECTD_DATA");

            StoreBuilder<KeyValueStore<String, String>> countStoreSupplier =
                    Stores.keyValueStoreBuilder(
                            Stores.inMemoryKeyValueStore("inmemory-counts"),
                            Serdes.String(),
                            Serdes.String());

            builder.addStateStore(countStoreSupplier);


            collectd.selectKey(new KeyValueMapper<String, String, String>() {
                @Override
                public String apply(String key, String value) {
                    CollectdKafkaVO[] arrCollectdKafkaVO = (CollectdKafkaVO[]) JsonUtil.jsonStringToObject(value, CollectdKafkaVO[].class);
                    System.out.println(arrCollectdKafkaVO[0].getHost() + " : " + arrCollectdKafkaVO[0].getTime() + " : " + arrCollectdKafkaVO[0].getPlugin() + " : " + arrCollectdKafkaVO[0].getType_instance());
                    return arrCollectdKafkaVO[0].getHost();
                }
            }).groupByKey();


            final KafkaStreams streams = new KafkaStreams(builder.build(), props);
            streams.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

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

    private void dslTumblingWindowTest() {

        try {

            // consumer
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-tumbling-window10");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "spanal-app:9092,spanal-1:9092,spanal-2:9092,spanal-3:9092");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

            // store ser & deser
            HostMetricVOSerializer hostMetricVOSerializer = new HostMetricVOSerializer();
            HostMetricVODeserializer hostMetricVODeserializer = new HostMetricVODeserializer();

            // store
            StoreBuilder<KeyValueStore<String, HostMetricVO>> hostMetricStoreSupplier =
                    Stores.keyValueStoreBuilder(
                            Stores.inMemoryKeyValueStore("HostMetric"),
                            Serdes.String(), Serdes.serdeFrom(hostMetricVOSerializer, hostMetricVODeserializer));


            // store
            StoreBuilder<KeyValueStore<String, String>> metricStoreSupplier =
                    Stores.keyValueStoreBuilder(
                            Stores.inMemoryKeyValueStore("metric"),
                            Serdes.String(), Serdes.String());

            StreamsBuilder builder = new StreamsBuilder();
//            KStream<String, String> source = builder.stream("COLLECTD_DATA");
            KStream<Windowed<String>, String> source = builder.stream("COLLECTD_DATA");

            // make key
            /*source.selectKey(new KeyValueMapper<String, String, String>() {
                @Override
                public String apply(String key, String value) {

                    CollectdKafkaVO[] arrCollectdKafkaVO = (CollectdKafkaVO[]) JsonUtil.jsonStringToObject(value, CollectdKafkaVO[].class);
                    CollectdKafkaVO collectdKafkaVO = arrCollectdKafkaVO[0];

                    StringBuffer resultKey = new StringBuffer();
                    // make key
                    resultKey.append(collectdKafkaVO.getHost()).append("^").append(collectdKafkaVO.getPlugin()).append("^").append(collectdKafkaVO.getPlugin_instance()).append("^").append(collectdKafkaVO.getType()).append("^").append(collectdKafkaVO.getType_instance());
                    return resultKey.toString();
                }
            });*/

            // make value (map)
            /*source.map(new KeyValueMapper<String, String, KeyValue<String, Number>>() {
                @Override
                public KeyValue<String, Number> apply(String key, String value) {


                    CollectdKafkaVO[] arrCollectdKafkaVO = (CollectdKafkaVO[]) JsonUtil.jsonStringToObject(value, CollectdKafkaVO[].class);
                    CollectdKafkaVO collectdKafkaVO = arrCollectdKafkaVO[0];

                    StringBuffer resultKey = new StringBuffer();
                    // make key
                    resultKey.append(collectdKafkaVO.getHost()).append("^").append(collectdKafkaVO.getPlugin()).append("^").append(collectdKafkaVO.getPlugin_instance()).append("^").append(collectdKafkaVO.getType()).append("^").append(collectdKafkaVO.getType_instance());
                    System.out.println(resultKey.toString() + " : " + value);


                    KeyValue<String, Number> resultKeyValue = new KeyValue<String, Number>(key, 10);


                    return resultKeyValue;
                }
            });*/

            // make value (flatmap)
//            source.flatMap(new KeyValueMapper<String, String, Iterable<KeyValue<String, String>>>() {
            source.flatMap(new KeyValueMapper<Windowed<String>, String, Iterable<KeyValue<Windowed<String>, String>>>() {

                @Override
                public Iterable<KeyValue<Windowed<String>, String>> apply(Windowed<String> key, String value) {

                    //TODO: Loop... dsnames
                    ArrayList<KeyValue<Windowed<String>, String>> resultList = new ArrayList<>();
//                    ArrayList<KeyValue<String, String>> resultList = new ArrayList<>();

                    CollectdKafkaVO[] arrCollectdKafkaVO = (CollectdKafkaVO[]) JsonUtil.jsonStringToObject(value, CollectdKafkaVO[].class);
                    CollectdKafkaVO collectdKafkaVO = arrCollectdKafkaVO[0];

//                    System.out.println(value);

                    for (int i = 0; i < collectdKafkaVO.getDsnames().size(); i++) {

                        StringBuffer resultKey = new StringBuffer();
                        if (StringUtils.equals(collectdKafkaVO.getDsnames().get(i), "value")) {
                            // make key
                            resultKey.append(collectdKafkaVO.getHost()).append("^").append(collectdKafkaVO.getPlugin()).append("^").append(collectdKafkaVO.getPlugin_instance()).append("^").append(collectdKafkaVO.getType()).append("^").append(collectdKafkaVO.getType_instance());

                            Number nValue;
                            if (collectdKafkaVO.getValues().get(i) instanceof Integer) {
                                nValue = new Integer((Integer) collectdKafkaVO.getValues().get(i));
                            } else if (collectdKafkaVO.getValues().get(i) instanceof Long) {
                                nValue = new Long((Long) collectdKafkaVO.getValues().get(i));
                            } else if (collectdKafkaVO.getValues().get(i) instanceof Float) {
                                nValue = new Float((Float) collectdKafkaVO.getValues().get(i));
                            } else if (collectdKafkaVO.getValues().get(i) instanceof Double) {
                                nValue = new Double((Double) collectdKafkaVO.getValues().get(i));
                            } else {
                                continue;
                            }
                            Windowed<String> windowed = new Windowed<>(resultKey.toString(), new Window(60000, 60000) {
                                @Override
                                public boolean overlap(Window window) {
                                    return true;
                                }
                            });
                            resultList.add(new KeyValue<>(windowed, nValue.toString()));
//                            resultList.add(new KeyValue<>(resultKey.toString(), nValue.toString()));

//                            System.out.println(resultKey.toString() + " : " + nValue.toString());
                        } else {
                            // make key
//                            resultKey.append(collectdKafkaVO.getHost()).append("^").append(collectdKafkaVO.getPlugin()).append("^").append(collectdKafkaVO.getPlugin_instance()).append("^").append(collectdKafkaVO.getType()).append("^").append(collectdKafkaVO.getType_instance());
                            resultKey.append(collectdKafkaVO.getHost()).append("^").append(collectdKafkaVO.getPlugin()).append("^").append(collectdKafkaVO.getPlugin_instance()).append("^").append(collectdKafkaVO.getType());
                            resultKey.append("_").append(collectdKafkaVO.getDsnames().get(i));

                            Number nValue;
                            if (collectdKafkaVO.getValues().get(i) instanceof Integer) {
                                nValue = new Integer((Integer) collectdKafkaVO.getValues().get(i));
                            } else if (collectdKafkaVO.getValues().get(i) instanceof Long) {
                                nValue = new Long((Long) collectdKafkaVO.getValues().get(i));
                            } else if (collectdKafkaVO.getValues().get(i) instanceof Float) {
                                nValue = new Float((Float) collectdKafkaVO.getValues().get(i));
                            } else if (collectdKafkaVO.getValues().get(i) instanceof Double) {
                                nValue = new Double((Double) collectdKafkaVO.getValues().get(i));
                            } else {
                                continue;
                            }


                            Windowed<String> windowed = new Windowed<>(resultKey.toString(), new Window(60000, 60000) {
                                @Override
                                public boolean overlap(Window window) {
                                    return true;
                                }
                            });
                            resultList.add(new KeyValue<>(windowed, nValue.toString()));
//                            resultList.add(new KeyValue<>(resultKey.toString(), nValue.toString()));

//                            System.out.println(resultKey.toString() + " : " + nValue.toString());
                        }
                    }
                    return resultList;
                }
            });


            WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(Serdes.String().serializer());
            WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(Serdes.String().deserializer());
            Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);

            source.groupByKey().windowedBy(TimeWindows.of(60 * 1000)).count().toStream().to("ainory_kafka_summary");

            /*source.to("");

                    .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(WINDOW_SECONDS)).advanceBy(TimeUnit.SECONDS.toMillis(WINDOW_SECONDS)))
                    .reduce(new Reducer<String>() {
                        @Override
                        public String apply(String value1, String value2) {
                            System.out.println(value1 + " : " + value2);

                            try{
                                return NumberUtils.createBigDecimal(value1).add(NumberUtils.createBigDecimal(value2)).toString();
                            }catch (Exception e){
                                e.printStackTrace();
                                return "0";
                            }
                        }
                    })
                    .toStream()*/

            final KafkaStreams streams = new KafkaStreams(builder.build(), props);
            streams.start();

//            }).groupByKey().windowedBy(TimeWindows.of(60 * 1000)).count().toStream();

                    /*.windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(WINDOW_SECONDS)).advanceBy(TimeUnit.SECONDS.toMillis(WINDOW_SECONDS)))
                    .reduce(new Reducer<String>() {
                        @Override
                        public String apply(String value1, String value2) {
                            System.out.println(value1 + " : " + value2);

                            try{
                                return NumberUtils.createBigDecimal(value1).add(NumberUtils.createBigDecimal(value2)).toString();
                            }catch (Exception e){
                                e.printStackTrace();
                                return "0";
                            }
                        }
                    })
                    .toStream().filter(new Predicate<Windowed<String>, String>() {
                @Override
                public boolean test(Windowed<String> key, String value) {
                    System.out.println(key.window().start() + " : " + key.window().end() + " : " + value);


                    // time init
                    if (current_time == 0) {
                        current_time = key.window().start();
                        System.out.println("init : " + current_time);
                        return false;
                    }

                    if ((current_time + (WINDOW_SECONDS * 1000)) == key.window().start()) {
                        System.out.println("match --> " + current_time + " : " + key.window().start());
                        current_time = key.window().start();
                        return true;
                    } else {
                        return false;
                    }
                }
            });*/

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void dslTumblingWindowTest2() {

        try {
            // consumer
            Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-tumbling-window10");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "spanal-app:9092,spanal-1:9092,spanal-2:9092,spanal-3:9092");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
            props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
            props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);


            WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(Serdes.String().serializer());
            WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(Serdes.String().deserializer(), TimeUnit.SECONDS.toMillis(WINDOW_SECONDS));
//            WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(Serdes.String().deserializer());
            Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);


            StreamsBuilder builder = new StreamsBuilder();
            KStream<String, String> source = builder.stream("COLLECTD_DATA");
//            KStream<Windowed<String>, String> source = builder.stream("COLLECTD_DATA");

            // make value (map)
//            KStream<Windowed<String>, String> sum  =
            source.map(new KeyValueMapper<String, String, KeyValue<String, String>>() {
                @Override
                public KeyValue<String, String> apply(String key, String value) {

                    CollectdKafkaVO[] arrCollectdKafkaVO = (CollectdKafkaVO[]) JsonUtil.jsonStringToObject(value, CollectdKafkaVO[].class);
                    CollectdKafkaVO collectdKafkaVO = arrCollectdKafkaVO[0];

                    StringBuffer resultKey = new StringBuffer();
                    if (StringUtils.equals(collectdKafkaVO.getDsnames().get(0), "value")) {
                        // make key
                        resultKey.append(collectdKafkaVO.getHost()).append("^").append(collectdKafkaVO.getPlugin()).append("^").append(collectdKafkaVO.getPlugin_instance()).append("^").append(collectdKafkaVO.getType()).append("^").append(collectdKafkaVO.getType_instance());

                        Number nValue;
                        if (collectdKafkaVO.getValues().get(0) instanceof Integer) {
                            nValue = new Integer((Integer) collectdKafkaVO.getValues().get(0));
                        } else if (collectdKafkaVO.getValues().get(0) instanceof Long) {
                            nValue = new Long((Long) collectdKafkaVO.getValues().get(0));
                        } else if (collectdKafkaVO.getValues().get(0) instanceof Float) {
                            nValue = new Float((Float) collectdKafkaVO.getValues().get(0));
                        } else if (collectdKafkaVO.getValues().get(0) instanceof Double) {
                            nValue = new Double((Double) collectdKafkaVO.getValues().get(0));
                        } else {
                            return null;
                        }
//                        return new KeyValue<>(windowed, nValue.toString());
//                        System.out.println(resultKey.toString() + " : " + nValue.toString());
                        return new KeyValue<>(resultKey.toString(), nValue.toString());

                    } else {
                        // make key
//                            resultKey.append(collectdKafkaVO.getHost()).append("^").append(collectdKafkaVO.getPlugin()).append("^").append(collectdKafkaVO.getPlugin_instance()).append("^").append(collectdKafkaVO.getType()).append("^").append(collectdKafkaVO.getType_instance());
                        resultKey.append(collectdKafkaVO.getHost()).append("^").append(collectdKafkaVO.getPlugin()).append("^").append(collectdKafkaVO.getPlugin_instance()).append("^").append(collectdKafkaVO.getType());
                        resultKey.append("_").append(collectdKafkaVO.getDsnames().get(0));

                        Number nValue;
                        if (collectdKafkaVO.getValues().get(0) instanceof Integer) {
                            nValue = new Integer((Integer) collectdKafkaVO.getValues().get(0));
                        } else if (collectdKafkaVO.getValues().get(0) instanceof Long) {
                            nValue = new Long((Long) collectdKafkaVO.getValues().get(0));
                        } else if (collectdKafkaVO.getValues().get(0) instanceof Float) {
                            nValue = new Float((Float) collectdKafkaVO.getValues().get(0));
                        } else if (collectdKafkaVO.getValues().get(0) instanceof Double) {
                            nValue = new Double((Double) collectdKafkaVO.getValues().get(0));
                        } else {
                            return null;
                        }

//                        System.out.println(resultKey.toString() + " : " + nValue.toString());
                        return new KeyValue<>(resultKey.toString(), nValue.toString());

                    }
                }
            }).groupByKey()
//                    .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(WINDOW_SECONDS)).advanceBy(TimeUnit.SECONDS.toMillis(WINDOW_SECONDS)))
                    .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(WINDOW_SECONDS)).until(TimeUnit.SECONDS.toMillis(WINDOW_SECONDS)))
//                    .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(WINDOW_SECONDS)))
                    .reduce(new Reducer<String>() {
                        @Override
                        public String apply(String value1, String value2) {

//                            System.out.println(NumberUtils.createBigDecimal(value1).add(NumberUtils.createBigDecimal(value2)).toString());

                            return NumberUtils.createBigDecimal(value1).add(NumberUtils.createBigDecimal(value2)).toString();
                        }
                    })
                    .toStream((k, v) -> k.key()).to("ainory_kafka_summary", Produced.<String, String>with(Serdes.String(), Serdes.String()));
                    /*.toStream().map(new KeyValueMapper<Windowed<String>, String, KeyValue<? extends Windowed<String>, ? extends String>>() {
                        @Override
                        public KeyValue<? extends Windowed<String>, ? extends String> apply(Windowed<String> stringWindowed, String s) {
                            return new KeyValue<>(stringWindowed, s);
                        }
                    }).filter(new Predicate<Windowed<String>, String>() {
                        @Override
                        public boolean test(Windowed<String> stringWindowed, String s) {
                            return true;
                        }
                    }).to("ainory_kafka_summary", Produced.with(windowedSerde, Serdes.String()));*/
                    /*.toStream()
                    .filter(new Predicate<Windowed<String>, String>() {
                        @Override
                        public boolean test(Windowed<String> key, String value) {
//                            System.out.println(key.window().start() + " : " + key.window().end() + " : " + key.key() +" -> "+value);


                            // time init
                            if (current_time == 0) {
                                current_time = key.window().start();
                                System.out.println("init : " + current_time);
                                return false;
                            }

                            if ((current_time + (WINDOW_SECONDS * 1000)) == key.window().start()) {
                                System.out.println("match --> " + current_time + " : " + key.window().start());
                                System.out.println(key.window().start() + " : " + key.window().end() + " [ " + key.key() +" -> "+ value + " ]");
                                current_time = key.window().start();
                                return true;
                            } else {
                                System.out.println(key.window().start() + " : " + key.window().end() + " : " + key.key() +" -> "+value);
                                return false;
                            }
                        }
                    });*/


            final KafkaStreams streams = new KafkaStreams(builder.build(), props);
            streams.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void dslTumblingWindowTest3() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "sum-dsl-application-ainory2");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "spanal-app:9092,spanal-1:9092,spanal-2:9092,spanal-3:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
//        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CollectdTimestampExtractor.class);

        // windowMs <= commit
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 60 * 1000L);



        /*HostMetricVOSerializer hostMetricVOSerializer = new HostMetricVOSerializer();
        HostMetricVODeserializer hostMetricVODeserializer = new HostMetricVODeserializer();
        StoreBuilder<KeyValueStore<String, HostMetricVO>> hostMetricStoreSupplier =
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("HostMetric"),
                        Serdes.String(), Serdes.serdeFrom(hostMetricVOSerializer, hostMetricVODeserializer));*/

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> collectdMsg = builder.stream("COLLECTD_DATA");

        //sum
        KTable<Windowed<String>, String> sum = collectdMsg
                // key & value setting
                .map(new KeyValueMapper<String, String, KeyValue<String, String>>() {
                    @Override
                    public KeyValue<String, String> apply(String key, String value) {

                        CollectdKafkaVO[] arrCollectdKafkaVO = (CollectdKafkaVO[]) JsonUtil.jsonStringToObject(value, CollectdKafkaVO[].class);
                        CollectdKafkaVO collectdKafkaVO = arrCollectdKafkaVO[0];

                        StringBuffer resultKey = new StringBuffer();
                        if (collectdKafkaVO.getDsnames().get(0).equals("value")) {
                            // make key - single value ( ex -> [{"values":[0],"dstypes":["gauge"],"dsnames":["value"],"time":1522299234.188,"interval":10.000,"host":"spanal-3","plugin":"disk","plugin_instance":"dm-2","type":"pending_operations","type_instance":"","meta":{"network:received":true}}] )
                            resultKey.append(collectdKafkaVO.getHost()).append("^").append(collectdKafkaVO.getPlugin()).append("^").append(collectdKafkaVO.getPlugin_instance()).append("^").append(collectdKafkaVO.getType()).append("^").append(collectdKafkaVO.getType_instance());

                            Number nValue;
                            if (collectdKafkaVO.getValues().get(0) instanceof Integer) {
                                nValue = new Integer((Integer) collectdKafkaVO.getValues().get(0));
                            } else if (collectdKafkaVO.getValues().get(0) instanceof Long) {
                                nValue = new Long((Long) collectdKafkaVO.getValues().get(0));
                            } else if (collectdKafkaVO.getValues().get(0) instanceof Float) {
                                nValue = new Float((Float) collectdKafkaVO.getValues().get(0));
                            } else if (collectdKafkaVO.getValues().get(0) instanceof Double) {
                                nValue = new Double((Double) collectdKafkaVO.getValues().get(0));
                            } else {
                                return null;
                            }
                            return new KeyValue<>(resultKey.toString(), nValue.toString());

                        } else {
                            // make key - multi value ( ex -> [{"values":[0,0.699869398389539],"dstypes":["derive","derive"],"dsnames":["read","write"],"time":1522299234.188,"interval":10.000,"host":"spanal-3","plugin":"disk","plugin_instance":"dm-2","type":"disk_time","type_instance":"","meta":{"network:received":true}}] )
//                            resultKey.append(collectdKafkaVO.getHost()).append("^").append(collectdKafkaVO.getPlugin()).append("^").append(collectdKafkaVO.getPlugin_instance()).append("^").append(collectdKafkaVO.getType()).append("^").append(collectdKafkaVO.getType_instance());
                            resultKey.append(collectdKafkaVO.getHost()).append("^").append(collectdKafkaVO.getPlugin()).append("^").append(collectdKafkaVO.getPlugin_instance()).append("^").append(collectdKafkaVO.getType());
                            resultKey.append("_").append(collectdKafkaVO.getDsnames().get(0));

                            Number nValue;
                            if (collectdKafkaVO.getValues().get(0) instanceof Integer) {
                                nValue = new Integer((Integer) collectdKafkaVO.getValues().get(0));
                            } else if (collectdKafkaVO.getValues().get(0) instanceof Long) {
                                nValue = new Long((Long) collectdKafkaVO.getValues().get(0));
                            } else if (collectdKafkaVO.getValues().get(0) instanceof Float) {
                                nValue = new Float((Float) collectdKafkaVO.getValues().get(0));
                            } else if (collectdKafkaVO.getValues().get(0) instanceof Double) {
                                nValue = new Double((Double) collectdKafkaVO.getValues().get(0));
                            } else {
                                return null;
                            }

                            return new KeyValue<>(resultKey.toString(), nValue.toString());

                        }
                    }
                })
                .groupByKey()
//                .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(WINDOW_SECONDS)).advanceBy(TimeUnit.SECONDS.toMillis(WINDOW_SECONDS)))
//                .windowedBy(TimeWindows.of(60 * 1000L).advanceBy(60 * 1000L).until(60 * 1000L))
//                .windowedBy(TimeWindows.of(60 * 1000L).until(60 * 1000L))
                .windowedBy(TimeWindows.of(60 * 1000L).advanceBy(60 * 1000L))
//                .windowedBy(TimeWindows.of(60 * 1000L))
                .aggregate(new Initializer<String>() {

                    @Override
                    public String apply() {
                        return "0";
                    }
                }, new Aggregator<String, String, String>() {
                    @Override
                    public String apply(String groupBykey, String value, String applyValue) {

                        try {
                            // sum
                            return NumberUtils.createBigDecimal(String.valueOf(value)).add(NumberUtils.createBigDecimal(applyValue)).toString();
                        } catch (Exception e) {
                            e.printStackTrace();
                            return "0";
                        }
                    }
                });

        sum.toStream().map(new KeyValueMapper<Windowed<String>, String, KeyValue<String, String>>() {
            @Override
            public KeyValue<String, String> apply(Windowed<String> stringWindowed, String aLong) {

                StringBuffer key = new StringBuffer();
                key.append("[").append(DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss")).append("]").append("[").append(DateFormatUtils.format(stringWindowed.window().start(), "yyyy-MM-dd HH:mm:ss")).append(" ~ ").append(DateFormatUtils.format(stringWindowed.window().end(), "yyyy-MM-dd HH:mm:ss")).append("] ").append(stringWindowed.key());
                KeyValue<String, String> resultValue = new KeyValue<>(key.toString(), aLong);
                System.out.println(key.toString() + " : " + resultValue.value);
                return resultValue;
            }
        }).to("ainory_kafka_summary", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);

        // zero seconds start
        while (true) {
            Calendar calendar = Calendar.getInstance();
            if (calendar.get(Calendar.SECOND) == 0) {
                break;
            }
            try {
                Thread.sleep(100);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        streams.start();
    }

    private void dslTumblingWindowTest4() {


        try {
            Properties config = new Properties();
            config.put(StreamsConfig.APPLICATION_ID_CONFIG, "sum-dsl-application-ainory56");
            config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "spanal-app:9092,spanal-1:9092,spanal-2:9092,spanal-3:9092");
            config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
//            config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, CustomSerdes.DslHostMetricVO().getClass());
            config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
//            config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
            config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CollectdTimestampExtractor.class);

            // windowMs <= commit
            config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, WINDOW_SECONDS * 1000L);

            // process guarantee
//            config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once");

            StreamsBuilder builder = new StreamsBuilder();
            KStream<String, String> collectdMsg = builder.stream("COLLECTD_DATA", Consumed.with(Serdes.String(), Serdes.String()));

            collectdMsg
                    .map(new DslKeyValueMapper())
                    .groupByKey()
                    .windowedBy(TimeWindows.of(WINDOW_SECONDS * 1000L).advanceBy(WINDOW_SECONDS * 1000L))
//                    .windowedBy(TimeWindows.of(WINDOW_SECONDS * 1000L))
                    .aggregate(new Initializer<DslHostMetricVO>() {

                        @Override
                        public DslHostMetricVO apply() {
                            return new DslHostMetricVO();
                        }
                    }, new Aggregator<String, DslHostMetricVO, DslHostMetricVO>() {
                        @Override
                        public DslHostMetricVO apply(String groupBykey, DslHostMetricVO currentVO, DslHostMetricVO aggregationVO) {

                            try {

                                //setting statistics
                                DslHostMetricVO resultVO = new DslHostMetricVO();

                                resultVO.setHostname(currentVO.getHostname());
                                resultVO.setAggregationCount(aggregationVO.getAggregationCount() + 1);
                                resultVO.setMax(resultVO.getMax(currentVO.getValue(), aggregationVO.getMax()));
                                resultVO.setMin(resultVO.getMin(currentVO.getValue(), aggregationVO.getMin()));
                                resultVO.setSum(resultVO.getSum(currentVO.getValue(), aggregationVO.getSum()));

                                resultVO.setAvg(resultVO.getAvg(resultVO.getSum(), resultVO.getAggregationCount()));

                                return resultVO;
                            } catch (Exception e) {
                                e.printStackTrace();
                                return new DslHostMetricVO();
                            }
                        }
                    })
                    .toStream().map(new KeyValueMapper<Windowed<String>, DslHostMetricVO, KeyValue<String, String>>() {
                                        @Override
                                        public KeyValue<String, String> apply(Windowed<String> stringWindowed, DslHostMetricVO valueVO) {

                                            /*StringBuffer key = new StringBuffer();
                                            key.append("[").append(DateFormatUtils.format(System.currentTimeMillis(), "yyyy-MM-dd HH:mm:ss")).append("]").append("[").append(DateFormatUtils.format(stringWindowed.window().start(), "yyyy-MM-dd HH:mm:ss")).append(" ~ ").append(DateFormatUtils.format(stringWindowed.window().end(), "yyyy-MM-dd HH:mm:ss")).append("] ").append(stringWindowed.key());
                                            KeyValue<String, String> resultValue = new KeyValue<>(key.toString(), aLong);
                                            System.out.println(key.toString() + " : " + resultValue.value);*/

                                            valueVO.setStartTimestamp(stringWindowed.window().start());
                                            valueVO.setEndTimestamp(stringWindowed.window().end());

                                            KeyValue<String, String> resultMapper = new KeyValue<>(stringWindowed.key(), valueVO.toSimpleString());

                                            System.out.println(stringWindowed.key() + " => " + valueVO.toSimpleString());

                                            return resultMapper;
                                        }
                    })
                    .to("ainory_kafka_summary", Produced.with(Serdes.String(), Serdes.String()));

            KafkaStreams streams = new KafkaStreams(builder.build(), config);

            // zero seconds start
            while (true) {
                Calendar calendar = Calendar.getInstance();
                if (calendar.get(Calendar.SECOND) == 0) {
                    break;
                }
                try {
                    Thread.sleep(100);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
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

    public void dslRun() {
        try {
            dslTumblingWindowTest4();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        KafkaTest kafkaTest = new KafkaTest();

        kafkaTest.dslRun();

//        kafkaTest.processorApiRun();


    }

}