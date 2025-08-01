# Kafka Streams Test - Usage Guide

## Overview

This guide provides comprehensive instructions for setting up, configuring, and using the Kafka Streams Test project. The project demonstrates real-time processing of collectd monitoring data using Apache Kafka Streams with both DSL and Processor API approaches.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation & Setup](#installation--setup)
3. [Configuration](#configuration)
4. [Quick Start](#quick-start)
5. [DSL-Based Processing](#dsl-based-processing)
6. [Processor API Usage](#processor-api-usage)
7. [Data Formats](#data-formats)
8. [Examples](#examples)
9. [Monitoring & Troubleshooting](#monitoring--troubleshooting)
10. [Performance Tuning](#performance-tuning)

---

## Prerequisites

### System Requirements

- **Java:** JDK 8 or higher
- **Apache Kafka:** Version 1.0.0 or compatible
- **Maven:** 3.6+ for building the project
- **Memory:** Minimum 2GB RAM recommended

### Required Dependencies

The project uses the following key dependencies (managed via Maven):

- `kafka-streams: 1.0.0` - Core streaming functionality
- `kafka-clients: 1.0.0` - Kafka client libraries
- `commons-lang3: 3.5` - String and utility operations
- `commons-math3: 3.6.1` - Mathematical operations
- `jackson-databind` - JSON serialization (transitive dependency)

---

## Installation & Setup

### 1. Clone and Build

```bash
# Clone the repository
git clone <repository-url>
cd KafkaStreamsTest

# Build the project
mvn clean compile

# Create executable JAR with dependencies
mvn package
```

This creates:
- `target/KafkaStreamsTest.jar` - Main application JAR
- `target/KafkaStreamsTest-jar-with-dependencies.jar` - Standalone executable
- `target/libs/` - Dependencies directory

### 2. Kafka Setup

#### Start Kafka Services

```bash
# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka broker
bin/kafka-server-start.sh config/server.properties
```

#### Create Required Topics

```bash
# Input topic for collectd data
bin/kafka-topics.sh --create \
  --topic COLLECTD_DATA \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Output topic for DSL results
bin/kafka-topics.sh --create \
  --topic COLLECTD_DATA_TUMBLING_WINDOW \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Output topic for Processor API results
bin/kafka-topics.sh --create \
  --topic ainory_kafka_summary \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### 3. Verify Setup

```bash
# List topics
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# Test producer
bin/kafka-console-producer.sh --topic COLLECTD_DATA --bootstrap-server localhost:9092

# Test consumer
bin/kafka-console-consumer.sh --topic COLLECTD_DATA --bootstrap-server localhost:9092 --from-beginning
```

---

## Configuration

### Application Configuration

#### Kafka Streams Properties

Create a `streams.properties` file:

```properties
# Application identity
application.id=kafka-streams-collectd-processor
bootstrap.servers=localhost:9092

# Processing guarantees
processing.guarantee=at_least_once
num.stream.threads=2

# Offset management
auto.offset.reset=latest
enable.auto.commit=true

# Memory and performance
cache.max.bytes.buffering=0
max.poll.records=1000
```

#### Server Configuration

Update broker addresses in source code or via environment variables:

```java
// In DslTestMain.java and ProcessorApiTestMain.java
String KAFKA_BROKERS = System.getenv("KAFKA_BROKERS") != null ? 
    System.getenv("KAFKA_BROKERS") : 
    "spanal-app:9092,spanal-1:9092,spanal-2:9092,spanal-3:9092";

props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
```

### Environment Variables

```bash
export KAFKA_BROKERS="localhost:9092"
export APP_ID="collectd-processor"
export WINDOW_SIZE_SECONDS="60"
```

---

## Quick Start

### 1. Run DSL Processing

```bash
# Execute DSL-based stream processing
java -cp target/KafkaStreamsTest-jar-with-dependencies.jar \
  com.ainory.kafka.streams.DslTestMain

# Or using Maven
mvn exec:java -Dexec.mainClass="com.ainory.kafka.streams.DslTestMain"
```

### 2. Run Processor API

```bash
# Execute Processor API-based processing
java -cp target/KafkaStreamsTest-jar-with-dependencies.jar \
  com.ainory.kafka.streams.ProcessorApiTestMain
```

### 3. Send Test Data

```bash
# Send sample collectd data
echo '[{"values":[45.2],"dstypes":["gauge"],"dsnames":["value"],"time":"1522299234.188","interval":10.0,"host":"test-server","plugin":"cpu","plugin_instance":"0","type":"cpu","type_instance":"idle","meta":{"network:received":true}}]' | \
  bin/kafka-console-producer.sh --topic COLLECTD_DATA --bootstrap-server localhost:9092
```

### 4. Monitor Output

```bash
# Monitor DSL output
bin/kafka-console-consumer.sh \
  --topic COLLECTD_DATA_TUMBLING_WINDOW \
  --bootstrap-server localhost:9092 \
  --from-beginning

# Monitor Processor API output
bin/kafka-console-consumer.sh \
  --topic ainory_kafka_summary \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

---

## DSL-Based Processing

### Overview

The DSL approach provides high-level abstractions for stream processing with automatic windowing and aggregation.

### Key Features

- **Tumbling Windows:** 60-second non-overlapping time windows
- **Automatic Aggregation:** Min, max, average, and sum calculations
- **Type Safety:** Strongly typed operations with custom serdes
- **Fault Tolerance:** Built-in error handling and recovery

### Implementation Example

```java
public class CustomDslProcessor {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "custom-dsl-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        
        StreamsBuilder builder = new StreamsBuilder();
        
        // Input stream
        KStream<String, String> input = builder.stream("COLLECTD_DATA");
        
        // Transform and aggregate
        KStream<String, DslHostMetricVO> processed = input
            .map(new DslKeyValueMapper())
            .filter((key, value) -> value != null);
            
        KTable<Windowed<String>, DslHostMetricVO> aggregated = processed
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofSeconds(60)))
            .aggregate(
                DslHostMetricVO::new,
                (key, value, aggregate) -> {
                    // Aggregation logic
                    aggregate.setValue(aggregate.getValue().add(value.getValue()));
                    aggregate.setAggregationCount(aggregate.getAggregationCount() + 1);
                    return aggregate;
                },
                Materialized.with(Serdes.String(), CustomSerdes.DslHostMetricVO())
            );
            
        // Output stream
        aggregated.toStream().to("output-topic");
        
        // Start processing
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        
        // Graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
```

### Window Operations

#### Tumbling Windows

```java
TimeWindows tumblingWindow = TimeWindows.of(Duration.ofSeconds(60));
```

- **Use Case:** Non-overlapping periodic summaries
- **Example:** Hourly CPU utilization reports

#### Hopping Windows

```java
TimeWindows hoppingWindow = TimeWindows
    .of(Duration.ofSeconds(60))
    .advanceBy(Duration.ofSeconds(30));
```

- **Use Case:** Overlapping analysis for smoother trends
- **Example:** Moving averages with 30-second shifts

#### Session Windows

```java
SessionWindows sessionWindow = SessionWindows.with(Duration.ofMinutes(5));
```

- **Use Case:** Activity-based grouping
- **Example:** User session analysis

---

## Processor API Usage

### Overview

The Processor API provides low-level control over stream processing with custom business logic.

### Key Components

1. **Processor:** Custom processing logic implementation
2. **State Store:** Local storage for aggregation state
3. **Punctuator:** Scheduled processing tasks
4. **Topology:** Stream processing graph definition

### Implementation Pattern

```java
public class CustomProcessor extends AbstractProcessor<String, String> {
    private ProcessorContext context;
    private KeyValueStore<String, HostMetricVO> store;
    
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.store = (KeyValueStore) context.getStateStore("metrics-store");
        
        // Schedule periodic output
        context.schedule(Duration.ofSeconds(60), PunctuationType.WALL_CLOCK_TIME, 
            this::punctuate);
    }
    
    @Override
    public void process(String key, String value) {
        try {
            // Parse input data
            CollectdKafkaVO[] data = JsonUtil.jsonStringToObject(value, CollectdKafkaVO[].class);
            
            // Process and store
            HostMetricVO metric = store.get(key);
            if (metric == null) {
                metric = new HostMetricVO();
                metric.setHostname(key);
            }
            
            // Update metrics
            updateMetrics(metric, data[0]);
            store.put(key, metric);
            
        } catch (Exception e) {
            // Error handling
            context.forward(key, "ERROR: " + e.getMessage());
        }
    }
    
    private void punctuate(long timestamp) {
        try (KeyValueIterator<String, HostMetricVO> iter = store.all()) {
            while (iter.hasNext()) {
                KeyValue<String, HostMetricVO> entry = iter.next();
                
                // Output aggregated results
                String output = JsonUtil.objectToJsonString(entry.value);
                context.forward(entry.key, output);
            }
        }
    }
}
```

### State Store Configuration

```java
// In topology setup
StoreBuilder<KeyValueStore<String, HostMetricVO>> storeBuilder = 
    Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore("metrics-store"),  // or inMemoryKeyValueStore
        Serdes.String(),
        CustomSerdes.HostMetricVO()
    );

topology.addStateStore(storeBuilder, "PROCESSOR_NAME");
```

---

## Data Formats

### Input Format (CollectdKafkaVO)

Collectd JSON structure as received from Kafka:

```json
[{
  "dsnames": ["value"],
  "dstypes": ["gauge"],
  "host": "web-server-01",
  "interval": 10.0,
  "meta": {"network:received": true},
  "plugin": "cpu",
  "plugin_instance": "0",
  "time": "1522299234.188",
  "type": "cpu",
  "type_instance": "idle",
  "values": [45.2]
}]
```

### Output Formats

#### DSL Output (DslHostMetricVO)

```json
{
  "hostname": "web-server-01",
  "startTimestamp": 1522299180000,
  "endTimestamp": 1522299240000,
  "value": 45.2,
  "avg": 47.8,
  "min": 42.1,
  "max": 52.3,
  "sum": 956.0,
  "aggregationCount": 20
}
```

#### Processor API Output (HostMetricVO)

```json
{
  "hostname": "web-server-01",
  "startTimestamp": 1522299180000,
  "endTimestamp": 1522299240000,
  "cpu_avg": 47.8,
  "cpu_min": 42.1,
  "cpu_max": 52.3,
  "memory_avg": 8589934592.0,
  "memory_min": 8000000000,
  "memory_max": 9000000000,
  "cpu_list": [45.2, 46.1, 47.3],
  "memory_list": [8000000000, 8500000000, 9000000000]
}
```

---

## Examples

### Example 1: Basic CPU Monitoring

#### Input Data
```bash
# CPU usage data
echo '[{"values":[45.2],"dstypes":["gauge"],"dsnames":["value"],"time":"1522299234.188","interval":10.0,"host":"server-01","plugin":"cpu","plugin_instance":"0","type":"cpu","type_instance":"user","meta":{}}]' | \
  bin/kafka-console-producer.sh --topic COLLECTD_DATA --bootstrap-server localhost:9092
```

#### Expected Output
```json
{
  "hostname": "server-01",
  "value": 45.2,
  "avg": 45.2,
  "min": 45.2,
  "max": 45.2,
  "aggregationCount": 1
}
```

### Example 2: Memory Monitoring

#### Input Data
```bash
# Memory usage data
echo '[{"values":[8589934592],"dstypes":["gauge"],"dsnames":["value"],"time":"1522299234.188","interval":10.0,"host":"server-01","plugin":"memory","plugin_instance":"","type":"memory","type_instance":"used","meta":{}}]' | \
  bin/kafka-console-producer.sh --topic COLLECTD_DATA --bootstrap-server localhost:9092
```

### Example 3: Disk I/O Monitoring

#### Input Data
```bash
# Disk operations
echo '[{"values":[1250],"dstypes":["counter"],"dsnames":["value"],"time":"1522299234.188","interval":10.0,"host":"server-01","plugin":"disk","plugin_instance":"sda","type":"disk_ops","type_instance":"read","meta":{}}]' | \
  bin/kafka-console-producer.sh --topic COLLECTD_DATA --bootstrap-server localhost:9092
```

### Example 4: Multi-value Load Average

#### Input Data
```bash
# Load average (short, mid, long term)
echo '[{"values":[2.07,2.07,2.04],"dstypes":["gauge","gauge","gauge"],"dsnames":["shortterm","midterm","longterm"],"time":"1522299234.188","interval":60.0,"host":"server-01","plugin":"load","plugin_instance":"","type":"load","type_instance":"","meta":{}}]' | \
  bin/kafka-console-producer.sh --topic COLLECTD_DATA --bootstrap-server localhost:9092
```

---

## Monitoring & Troubleshooting

### Application Monitoring

#### JMX Metrics

Enable JMX for monitoring:

```bash
export JMX_OPTS="-Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.port=9999"

java $JMX_OPTS -cp target/KafkaStreamsTest-jar-with-dependencies.jar \
  com.ainory.kafka.streams.DslTestMain
```

#### Log Configuration

```xml
<!-- logback.xml -->
<configuration>
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder>
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>
    
    <logger name="org.apache.kafka.streams" level="INFO"/>
    <logger name="com.ainory.kafka.streams" level="DEBUG"/>
    
    <root level="INFO">
        <appender-ref ref="STDOUT"/>
    </root>
</configuration>
```

### Common Issues

#### 1. Topic Not Found

```
Error: Topic 'COLLECTD_DATA' not found
```

**Solution:**
```bash
bin/kafka-topics.sh --create --topic COLLECTD_DATA --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

#### 2. Serialization Errors

```
Error: Cannot deserialize value
```

**Solution:** Verify JSON format and custom serde configuration:
```java
// Ensure proper serde configuration
props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
```

#### 3. Window Processing Delays

```
Warning: Processing lag detected
```

**Solution:** Tune window size and buffering:
```java
props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
```

#### 4. Memory Issues

```
Error: OutOfMemoryError
```

**Solution:** Increase heap size:
```bash
export JAVA_OPTS="-Xmx2g -Xms1g"
```

### Debugging Tips

1. **Enable Debug Logging:** Set log levels to DEBUG for detailed processing information
2. **Monitor Consumer Lag:** Check offset lag using Kafka tools
3. **Verify Data Format:** Use console consumers to inspect message formats
4. **Check State Stores:** Monitor state store sizes and contents
5. **Network Connectivity:** Verify broker connectivity and DNS resolution

---

## Performance Tuning

### JVM Tuning

```bash
export JAVA_OPTS="-Xmx4g -Xms2g \
  -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=100 \
  -XX:+UseStringDeduplication"
```

### Kafka Streams Configuration

```java
Properties props = new Properties();

// Parallelism
props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);

// Buffering
props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 100 * 1024 * 1024); // 100MB

// Commit frequency
props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000);

// Processing guarantee
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
```

### Topic Configuration

```bash
# Increase partitions for parallelism
bin/kafka-topics.sh --alter \
  --topic COLLECTD_DATA \
  --partitions 12 \
  --bootstrap-server localhost:9092

# Configure retention
bin/kafka-configs.sh --alter \
  --entity-type topics \
  --entity-name COLLECTD_DATA \
  --add-config retention.ms=86400000 \
  --bootstrap-server localhost:9092
```

### Monitoring Performance

#### Key Metrics to Monitor

1. **Processing Rate:** Records processed per second
2. **Latency:** End-to-end processing latency
3. **Memory Usage:** Heap and off-heap memory consumption
4. **Network I/O:** Bandwidth utilization
5. **State Store Size:** Local storage consumption

#### Performance Benchmarks

Typical performance characteristics:

- **Throughput:** 10,000-50,000 records/second (depending on complexity)
- **Latency:** 50-200ms end-to-end
- **Memory:** 1-4GB heap recommended
- **Storage:** 100MB-1GB for state stores

---

This comprehensive usage guide provides all the necessary information to successfully deploy and operate the Kafka Streams Test project. For additional support, refer to the API documentation and source code comments.