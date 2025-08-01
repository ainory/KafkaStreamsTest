# Kafka Streams Test - API Documentation

## Overview

The Kafka Streams Test project provides real-time stream processing capabilities for collectd monitoring data using Apache Kafka Streams. This documentation covers all public APIs, functions, and components with usage examples.

## Table of Contents

1. [Main Entry Points](#main-entry-points)
2. [Entity Classes](#entity-classes)
3. [Serialization Components](#serialization-components)
4. [Stream Processors](#stream-processors)
5. [Utility Classes](#utility-classes)
6. [Configuration](#configuration)

---

## Main Entry Points

### DslTestMain

**Package:** `com.ainory.kafka.streams`

The main class for Kafka Streams DSL-based processing with tumbling window operations.

#### Public Constants

```java
public static int WINDOW_SECONDS = 60
```
- **Description:** Default window size for tumbling window aggregations (60 seconds)
- **Type:** `int`
- **Usage:** Used in windowing operations for data aggregation

#### Key Methods

##### dslTumblingWindowTest()
```java
private void dslTumblingWindowTest()
```
- **Description:** Sets up and runs Kafka Streams topology using DSL API with tumbling windows
- **Functionality:**
  - Configures Kafka Streams properties
  - Creates stream topology for collectd data processing
  - Performs windowed aggregations (min, max, avg, sum)
  - Outputs aggregated results to target topic

**Example Usage:**
```java
DslTestMain dslMain = new DslTestMain();
// Run via main method - starts tumbling window processing
```

**Configuration Properties:**
- Application ID: `streams-tumbling-window10`
- Bootstrap Servers: `spanal-app:9092,spanal-1:9092,spanal-2:9092,spanal-3:9092`
- Input Topic: `COLLECTD_DATA`
- Output Topic: `COLLECTD_DATA_TUMBLING_WINDOW`

---

### ProcessorApiTestMain

**Package:** `com.ainory.kafka.streams`

Entry point for Kafka Streams Processor API-based implementation.

#### Key Methods

##### processorApiTest()
```java
private void processorApiTest()
```
- **Description:** Sets up Kafka Streams topology using low-level Processor API
- **Components:**
  - Source processor for `COLLECTD_DATA` topic
  - Custom processor (`ProcessorSupplierTest`)
  - State store for metric data
  - Sink processor for output

##### processorApiRun()
```java
public void processorApiRun()
```
- **Description:** Public wrapper method to run processor API test
- **Usage:** Entry point for starting processor-based stream processing

**Example Usage:**
```java
ProcessorApiTestMain processorMain = new ProcessorApiTestMain();
processorMain.processorApiRun();
```

**Topology Configuration:**
- Source: `COLLECTD_DATA` topic
- Processor: Custom aggregation logic
- State Store: In-memory key-value store (`HostMetric`)
- Sink: `ainory_kafka_summary` topic

---

### KafkaTest

**Package:** `com.ainory.kafka.streams`

Comprehensive test class with multiple stream processing patterns and producer functionality.

#### Public Constants

```java
public static int current_sum = 0
public static long current_time = 0L
public static int WINDOW_SECONDS = 60
public static Long previousValue = null
public static Long sumValue = null
```

#### Key Functionality
- **Data Generation:** Producer methods for generating test data
- **Stream Processing:** Multiple processing patterns (DSL and Processor API)
- **Window Operations:** Tumbling and hopping window implementations
- **Aggregation:** Min, max, average, and sum calculations

---

## Entity Classes

### CollectdKafkaVO

**Package:** `com.ainory.kafka.streams.entity`

Data transfer object representing collectd monitoring data structure.

#### Fields

| Field | Type | Description |
|-------|------|-------------|
| `dsnames` | `ArrayList<String>` | Data source names (e.g., "value", "shortterm", "midterm") |
| `dstypes` | `ArrayList<String>` | Data source types (e.g., "gauge", "counter") |
| `host` | `String` | Hostname where metric was collected |
| `interval` | `Double` | Collection interval in seconds |
| `meta` | `HashMap` | Metadata information |
| `plugin` | `String` | Collectd plugin name (e.g., "cpu", "memory", "disk") |
| `plugin_instance` | `String` | Plugin instance identifier |
| `time` | `String` | Timestamp of data collection |
| `type` | `String` | Metric type (e.g., "cpu", "load", "df_complex") |
| `type_instance` | `String` | Type instance identifier |
| `values` | `ArrayList` | Metric values array |

#### Example JSON Structure
```json
[{
  "dsnames": ["value"],
  "dstypes": ["gauge"],
  "host": "spanal-3",
  "interval": 10.0,
  "meta": {"network:received": true},
  "plugin": "disk",
  "plugin_instance": "dm-2",
  "type": "pending_operations",
  "type_instance": "",
  "time": "1522299234.188",
  "values": [0]
}]
```

#### Usage Example
```java
String jsonData = "..."; // JSON string
CollectdKafkaVO[] metrics = (CollectdKafkaVO[]) JsonUtil.jsonStringToObject(jsonData, CollectdKafkaVO[].class);
CollectdKafkaVO metric = metrics[0];
String hostname = metric.getHost();
String plugin = metric.getPlugin();
```

---

### HostMetricVO

**Package:** `com.ainory.kafka.streams.entity`

Aggregated host metric data for processor API operations.

#### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `hostname` | `String` | Host identifier |
| `startTimestamp` | `long` | Aggregation window start time |
| `endTimestamp` | `long` | Aggregation window end time |
| `cpu_list` | `ArrayList<Double>` | CPU usage values |
| `memory_list` | `ArrayList<Long>` | Memory usage values |
| `cpu_avg` | `double` | Average CPU usage |
| `cpu_min` | `double` | Minimum CPU usage |
| `cpu_max` | `double` | Maximum CPU usage |
| `memory_avg` | `double` | Average memory usage |
| `memory_min` | `long` | Minimum memory usage |
| `memory_max` | `long` | Maximum memory usage |

#### Methods

##### addCpuData(double)
```java
public void addCpuData(double cpuValue)
```
- **Description:** Adds CPU data point and recalculates statistics
- **Parameters:** `cpuValue` - CPU usage percentage
- **Side Effects:** Updates min, max, and average calculations

##### addMemoryData(long)
```java
public void addMemoryData(long memoryValue)
```
- **Description:** Adds memory data point and recalculates statistics
- **Parameters:** `memoryValue` - Memory usage in bytes
- **Side Effects:** Updates min, max, and average calculations

#### Usage Example
```java
HostMetricVO hostMetric = new HostMetricVO();
hostMetric.setHostname("server-01");
hostMetric.addCpuData(45.2);
hostMetric.addMemoryData(8589934592L); // 8GB in bytes

// Access aggregated data
double avgCpu = hostMetric.getCpu_avg();
long maxMemory = hostMetric.getMemory_max();
```

---

### DslHostMetricVO

**Package:** `com.ainory.kafka.streams.entity`

Simplified metric data object for DSL-based stream operations with BigDecimal precision.

#### Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `hostname` | `String` | Host identifier |
| `value` | `BigDecimal` | Current metric value |
| `avg` | `BigDecimal` | Average value |
| `min` | `BigDecimal` | Minimum value |
| `max` | `BigDecimal` | Maximum value |
| `sum` | `BigDecimal` | Sum of values |
| `aggregationCount` | `int` | Number of aggregated values |

#### Methods

##### getAvg(BigDecimal, int)
```java
public BigDecimal getAvg(BigDecimal sumValue, int aggregationCount)
```
- **Description:** Calculates average from sum and count
- **Parameters:** 
  - `sumValue` - Total sum of values
  - `aggregationCount` - Number of data points
- **Returns:** Average value as BigDecimal

##### getMin(BigDecimal, BigDecimal)
```java
public BigDecimal getMin(BigDecimal leftValue, BigDecimal rightValue)
```
- **Description:** Returns minimum of two values
- **Returns:** Smaller of the two input values

#### Usage Example
```java
DslHostMetricVO metric = new DslHostMetricVO();
metric.setHostname("web-server-01");
metric.setValue(new BigDecimal("75.5"));
metric.setAggregationCount(1);

// Calculate average
BigDecimal avg = metric.getAvg(metric.getSum(), metric.getAggregationCount());
```

---

## Serialization Components

### CustomSerdes

**Package:** `com.ainory.kafka.streams.serializer`

Factory class providing custom Kafka Serdes for entity objects.

#### Static Methods

##### HostMetricVO()
```java
public static Serde<HostMetricVO> HostMetricVO()
```
- **Description:** Creates Serde for HostMetricVO objects
- **Returns:** Configured Serde instance
- **Usage:** For Kafka Streams topology configuration

##### DslHostMetricVO()
```java
public static Serde<DslHostMetricVO> DslHostMetricVO()
```
- **Description:** Creates Serde for DslHostMetricVO objects
- **Returns:** Configured Serde instance

#### Usage Example
```java
// In stream topology configuration
StreamsBuilder builder = new StreamsBuilder();
KStream<String, DslHostMetricVO> stream = builder.stream("input-topic", 
    Consumed.with(Serdes.String(), CustomSerdes.DslHostMetricVO()));
```

---

### Individual Serializers

#### CollectdKafkaVOSerializer/Deserializer
- **Purpose:** JSON serialization for CollectdKafkaVO objects
- **Implementation:** Uses Jackson ObjectMapper

#### HostMetricVOSerializer/Deserializer
- **Purpose:** JSON serialization for HostMetricVO objects
- **Implementation:** Uses Jackson ObjectMapper

#### NumberSerializer/Deserializer
- **Purpose:** Serialization for numeric values
- **Implementation:** Custom byte array conversion

---

## Stream Processors

### ProcessTest1

**Package:** `com.ainory.kafka.streams.process`

Custom processor for aggregating collectd metrics using Processor API.

#### Configuration Constants

```java
private final long CHECK_INTERVAL_SEC = 1
private final long COLLECTD_COLLECT_INTERVAL_SEC = 10
private final int SUMMARY_INTERVAL_SEC = 60
```

#### Key Methods

##### init(ProcessorContext)
```java
public void init(ProcessorContext context)
```
- **Description:** Initializes processor with context and state store
- **Parameters:** `context` - Processor execution context
- **Side Effects:** Sets up periodic punctuation for aggregation

##### process(String, String)
```java
public void process(String key, String value)
```
- **Description:** Processes incoming collectd data records
- **Parameters:**
  - `key` - Record key (typically hostname)
  - `value` - JSON string containing collectd data
- **Functionality:**
  - Parses JSON data
  - Extracts CPU and memory metrics
  - Updates running aggregations in state store

#### Usage Pattern
```java
// Used within Kafka Streams topology
topology.addProcessor("PROCESS1", new ProcessorSupplierTest(), "Source");

// State store configuration
StoreBuilder<KeyValueStore<String, HostMetricVO>> storeBuilder = 
    Stores.keyValueStoreBuilder(
        Stores.inMemoryKeyValueStore("HostMetric"),
        Serdes.String(), 
        CustomSerdes.HostMetricVO());
```

---

### ProcessorSupplierTest

**Package:** `com.ainory.kafka.streams.process`

Factory for creating ProcessTest1 processor instances.

#### Methods

##### get()
```java
public Processor get()
```
- **Description:** Creates new ProcessTest1 instance
- **Returns:** New processor instance
- **Usage:** Required by Kafka Streams Processor API

---

## Utility Classes

### JsonUtil

**Package:** `com.ainory.kafka.streams.util`

JSON serialization and deserialization utilities using Jackson.

#### Static Methods

##### objectToJsonString(Object)
```java
public static String objectToJsonString(Object obj)
```
- **Description:** Converts Java object to JSON string
- **Parameters:** `obj` - Object to serialize
- **Returns:** JSON string representation
- **Error Handling:** Returns null on serialization errors

##### jsonStringToObject(String)
```java
public static Object jsonStringToObject(String jsonString)
```
- **Description:** Converts JSON string to generic Object
- **Parameters:** `jsonString` - JSON string to parse
- **Returns:** Parsed object
- **Error Handling:** Returns null on parsing errors

##### jsonStringToObject(String, Class)
```java
public static Object jsonStringToObject(String jsonString, Class aClass)
```
- **Description:** Converts JSON string to specific class type
- **Parameters:**
  - `jsonString` - JSON string to parse
  - `aClass` - Target class for deserialization
- **Returns:** Typed object instance

#### Usage Examples
```java
// Serialize object to JSON
HostMetricVO metric = new HostMetricVO();
String json = JsonUtil.objectToJsonString(metric);

// Deserialize JSON to object
CollectdKafkaVO[] metrics = (CollectdKafkaVO[]) JsonUtil.jsonStringToObject(
    jsonData, CollectdKafkaVO[].class);

// Generic deserialization
Object parsed = JsonUtil.jsonStringToObject(jsonString);
```

---

### CollectdTimestampExtractor

**Package:** `com.ainory.kafka.streams.timestamp.extractor`

Custom timestamp extraction for collectd data records.

#### Methods

##### extract(ConsumerRecord, long)
```java
public long extract(ConsumerRecord<Object, Object> consumerRecord, long l)
```
- **Description:** Extracts timestamp from collectd JSON data
- **Parameters:**
  - `consumerRecord` - Kafka consumer record
  - `l` - Previous timestamp (fallback)
- **Returns:** Extracted timestamp in milliseconds
- **Logic:**
  - Parses collectd JSON structure
  - Extracts 'time' field
  - Converts decimal timestamp to milliseconds
  - Falls back to current time on errors

#### Usage Example
```java
// In stream configuration
props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, 
    CollectdTimestampExtractor.class);
```

---

### DslKeyValueMapper

**Package:** `com.ainory.kafka.streams.keyvalue.mapper`

Maps collectd JSON data to typed key-value pairs for DSL operations.

#### Methods

##### apply(String, String)
```java
public KeyValue<String, DslHostMetricVO> apply(String key, String value)
```
- **Description:** Transforms collectd JSON to DslHostMetricVO
- **Parameters:**
  - `key` - Original key
  - `value` - Collectd JSON string
- **Returns:** KeyValue pair with composite key and typed value
- **Key Format:** `host^plugin^plugin_instance^type^type_instance`

#### Data Type Handling
Supports multiple numeric types:
- `Integer`
- `Long`
- `Float`
- `Double`

#### Usage Example
```java
// In stream processing
KStream<String, String> input = builder.stream("collectd-topic");
KStream<String, DslHostMetricVO> mapped = input.map(new DslKeyValueMapper());
```

---

## Configuration

### Kafka Streams Properties

#### Common Configuration
```java
Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "your-app-id");
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092,kafka2:9092");
props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
```

#### DSL-Specific Configuration
```java
props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, 
    CollectdTimestampExtractor.class);
```

### Topic Configuration

| Topic | Purpose | Key Type | Value Type |
|-------|---------|----------|------------|
| `COLLECTD_DATA` | Input collectd metrics | String | JSON String |
| `COLLECTD_DATA_TUMBLING_WINDOW` | Aggregated DSL output | String | DslHostMetricVO JSON |
| `ainory_kafka_summary` | Processor API output | String | HostMetricVO JSON |

### Window Configuration

#### Tumbling Windows
```java
TimeWindows.of(Duration.ofSeconds(WINDOW_SECONDS))
```
- **Purpose:** Non-overlapping time-based aggregation
- **Default Size:** 60 seconds
- **Use Case:** Periodic summary statistics

#### Hopping Windows
```java
TimeWindows.of(Duration.ofSeconds(windowSize))
    .advanceBy(Duration.ofSeconds(advanceSize))
```
- **Purpose:** Overlapping time-based aggregation
- **Configuration:** Customizable window and advance sizes

---

## Error Handling

### Common Patterns

1. **JSON Parsing Errors**
   - Caught and logged in JsonUtil methods
   - Default fallback values returned
   - Graceful degradation

2. **Timestamp Extraction Errors**
   - Falls back to current system time
   - Prevents stream processing interruption

3. **Serialization Errors**
   - Logged via printStackTrace()
   - Returns null for failed operations

### Best Practices

1. **Null Checks:** Always verify objects before processing
2. **Exception Handling:** Wrap risky operations in try-catch blocks
3. **Fallback Values:** Provide sensible defaults for missing data
4. **Logging:** Use appropriate logging levels for different error types

---

This documentation provides comprehensive coverage of all public APIs, functions, and components in the Kafka Streams Test project. For additional implementation details, refer to the source code and inline comments.