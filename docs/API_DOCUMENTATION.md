# Kafka Streams 테스트 - API 문서

## 개요

Kafka Streams 테스트 프로젝트는 Apache Kafka Streams를 사용하여 collectd 모니터링 데이터에 대한 실시간 스트림 처리 기능을 제공합니다. 이 문서는 사용 예제와 함께 모든 public API, 함수 및 컴포넌트를 다룹니다.

## 목차

1. [메인 진입점](#메인-진입점)
2. [엔티티 클래스](#엔티티-클래스)
3. [직렬화 컴포넌트](#직렬화-컴포넌트)
4. [스트림 프로세서](#스트림-프로세서)
5. [유틸리티 클래스](#유틸리티-클래스)
6. [설정](#설정)

---

## 메인 진입점

### DslTestMain

**패키지:** `com.ainory.kafka.streams`

텀블링 윈도우 연산을 포함한 Kafka Streams DSL 기반 처리의 메인 클래스입니다.

#### Public 상수

```java
public static int WINDOW_SECONDS = 60
```
- **설명:** 텀블링 윈도우 집계를 위한 기본 윈도우 크기 (60초)
- **타입:** `int`
- **사용:** 데이터 집계를 위한 윈도우잉 연산에 사용

#### 주요 메서드

##### dslTumblingWindowTest()
```java
private void dslTumblingWindowTest()
```
- **설명:** DSL API를 사용하여 텀블링 윈도우와 함께 Kafka Streams 토폴로지를 설정하고 실행
- **기능:**
  - Kafka Streams 속성 설정
  - collectd 데이터 처리를 위한 스트림 토폴로지 생성
  - 윈도우 집계 수행 (최솟값, 최댓값, 평균, 합계)
  - 집계된 결과를 대상 토픽으로 출력

**사용 예제:**
```java
DslTestMain dslMain = new DslTestMain();
// main 메서드를 통해 실행 - 텀블링 윈도우 처리 시작
```

**설정 속성:**
- Application ID: `streams-tumbling-window10`
- Bootstrap Servers: `spanal-app:9092,spanal-1:9092,spanal-2:9092,spanal-3:9092`
- 입력 토픽: `COLLECTD_DATA`
- 출력 토픽: `COLLECTD_DATA_TUMBLING_WINDOW`

---

### ProcessorApiTestMain

**패키지:** `com.ainory.kafka.streams`

Kafka Streams Processor API 기반 구현의 진입점입니다.

#### 주요 메서드

##### processorApiTest()
```java
private void processorApiTest()
```
- **설명:** 저수준 Processor API를 사용하여 Kafka Streams 토폴로지 설정
- **컴포넌트:**
  - `COLLECTD_DATA` 토픽용 소스 프로세서
  - 커스텀 프로세서 (`ProcessorSupplierTest`)
  - 메트릭 데이터용 상태 저장소
  - 출력용 싱크 프로세서

##### processorApiRun()
```java
public void processorApiRun()
```
- **설명:** 프로세서 API 테스트를 실행하는 public 래퍼 메서드
- **사용:** 프로세서 기반 스트림 처리를 시작하는 진입점

**사용 예제:**
```java
ProcessorApiTestMain processorMain = new ProcessorApiTestMain();
processorMain.processorApiRun();
```

**토폴로지 설정:**
- 소스: `COLLECTD_DATA` 토픽
- 프로세서: 커스텀 집계 로직
- 상태 저장소: 인메모리 키-값 저장소 (`HostMetric`)
- 싱크: `ainory_kafka_summary` 토픽

---

### KafkaTest

**패키지:** `com.ainory.kafka.streams`

다양한 스트림 처리 패턴과 프로듀서 기능을 포함한 포괄적인 테스트 클래스입니다.

#### Public 상수

```java
public static int current_sum = 0
public static long current_time = 0L
public static int WINDOW_SECONDS = 60
public static Long previousValue = null
public static Long sumValue = null
```

#### 주요 기능
- **데이터 생성:** 테스트 데이터 생성을 위한 프로듀서 메서드
- **스트림 처리:** 다양한 처리 패턴 (DSL 및 Processor API)
- **윈도우 연산:** 텀블링 및 호핑 윈도우 구현
- **집계:** 최솟값, 최댓값, 평균, 합계 계산

---

## 엔티티 클래스

### CollectdKafkaVO

**패키지:** `com.ainory.kafka.streams.entity`

collectd 모니터링 데이터 구조를 나타내는 데이터 전송 객체입니다.

#### 필드

| 필드 | 타입 | 설명 |
|-------|------|-------------|
| `dsnames` | `ArrayList<String>` | 데이터 소스 이름 (예: "value", "shortterm", "midterm") |
| `dstypes` | `ArrayList<String>` | 데이터 소스 타입 (예: "gauge", "counter") |
| `host` | `String` | 메트릭이 수집된 호스트명 |
| `interval` | `Double` | 수집 간격 (초) |
| `meta` | `HashMap` | 메타데이터 정보 |
| `plugin` | `String` | Collectd 플러그인 이름 (예: "cpu", "memory", "disk") |
| `plugin_instance` | `String` | 플러그인 인스턴스 식별자 |
| `time` | `String` | 데이터 수집 타임스탬프 |
| `type` | `String` | 메트릭 타입 (예: "cpu", "load", "df_complex") |
| `type_instance` | `String` | 타입 인스턴스 식별자 |
| `values` | `ArrayList` | 메트릭 값 배열 |

#### JSON 구조 예제
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

#### 사용 예제
```java
String jsonData = "..."; // JSON 문자열
CollectdKafkaVO[] metrics = (CollectdKafkaVO[]) JsonUtil.jsonStringToObject(jsonData, CollectdKafkaVO[].class);
CollectdKafkaVO metric = metrics[0];
String hostname = metric.getHost();
String plugin = metric.getPlugin();
```

---

### HostMetricVO

**패키지:** `com.ainory.kafka.streams.entity`

프로세서 API 연산을 위한 집계된 호스트 메트릭 데이터입니다.

#### 주요 필드

| 필드 | 타입 | 설명 |
|-------|------|-------------|
| `hostname` | `String` | 호스트 식별자 |
| `startTimestamp` | `long` | 집계 윈도우 시작 시간 |
| `endTimestamp` | `long` | 집계 윈도우 종료 시간 |
| `cpu_list` | `ArrayList<Double>` | CPU 사용률 값들 |
| `memory_list` | `ArrayList<Long>` | 메모리 사용량 값들 |
| `cpu_avg` | `double` | 평균 CPU 사용률 |
| `cpu_min` | `double` | 최소 CPU 사용률 |
| `cpu_max` | `double` | 최대 CPU 사용률 |
| `memory_avg` | `double` | 평균 메모리 사용량 |
| `memory_min` | `long` | 최소 메모리 사용량 |
| `memory_max` | `long` | 최대 메모리 사용량 |

#### 메서드

##### addCpuData(double)
```java
public void addCpuData(double cpuValue)
```
- **설명:** CPU 데이터 포인트를 추가하고 통계를 재계산
- **매개변수:** `cpuValue` - CPU 사용률 백분율
- **부작용:** 최솟값, 최댓값, 평균 계산 업데이트

##### addMemoryData(long)
```java
public void addMemoryData(long memoryValue)
```
- **설명:** 메모리 데이터 포인트를 추가하고 통계를 재계산
- **매개변수:** `memoryValue` - 메모리 사용량 (바이트)
- **부작용:** 최솟값, 최댓값, 평균 계산 업데이트

#### 사용 예제
```java
HostMetricVO hostMetric = new HostMetricVO();
hostMetric.setHostname("server-01");
hostMetric.addCpuData(45.2);
hostMetric.addMemoryData(8589934592L); // 8GB (바이트)

// 집계된 데이터 접근
double avgCpu = hostMetric.getCpu_avg();
long maxMemory = hostMetric.getMemory_max();
```

---

### DslHostMetricVO

**패키지:** `com.ainory.kafka.streams.entity`

BigDecimal 정밀도를 가진 DSL 기반 스트림 연산을 위한 간소화된 메트릭 데이터 객체입니다.

#### 주요 필드

| 필드 | 타입 | 설명 |
|-------|------|-------------|
| `hostname` | `String` | 호스트 식별자 |
| `value` | `BigDecimal` | 현재 메트릭 값 |
| `avg` | `BigDecimal` | 평균값 |
| `min` | `BigDecimal` | 최솟값 |
| `max` | `BigDecimal` | 최댓값 |
| `sum` | `BigDecimal` | 값들의 합계 |
| `aggregationCount` | `int` | 집계된 값의 개수 |

#### 메서드

##### getAvg(BigDecimal, int)
```java
public BigDecimal getAvg(BigDecimal sumValue, int aggregationCount)
```
- **설명:** 합계와 개수로부터 평균을 계산
- **매개변수:** 
  - `sumValue` - 값들의 총합
  - `aggregationCount` - 데이터 포인트 개수
- **반환값:** BigDecimal로 된 평균값

##### getMin(BigDecimal, BigDecimal)
```java
public BigDecimal getMin(BigDecimal leftValue, BigDecimal rightValue)
```
- **설명:** 두 값 중 최솟값을 반환
- **반환값:** 두 입력값 중 더 작은 값

#### 사용 예제
```java
DslHostMetricVO metric = new DslHostMetricVO();
metric.setHostname("web-server-01");
metric.setValue(new BigDecimal("75.5"));
metric.setAggregationCount(1);

// 평균 계산
BigDecimal avg = metric.getAvg(metric.getSum(), metric.getAggregationCount());
```

---

## 직렬화 컴포넌트

### CustomSerdes

**패키지:** `com.ainory.kafka.streams.serializer`

엔티티 객체용 커스텀 Kafka Serdes를 제공하는 팩토리 클래스입니다.

#### 정적 메서드

##### HostMetricVO()
```java
public static Serde<HostMetricVO> HostMetricVO()
```
- **설명:** HostMetricVO 객체용 Serde 생성
- **반환값:** 설정된 Serde 인스턴스
- **사용:** Kafka Streams 토폴로지 설정용

##### DslHostMetricVO()
```java
public static Serde<DslHostMetricVO> DslHostMetricVO()
```
- **설명:** DslHostMetricVO 객체용 Serde 생성
- **반환값:** 설정된 Serde 인스턴스
- **사용:** DSL 스트림 토폴로지용

##### CollectdKafkaVO()
```java
public static Serde<CollectdKafkaVO> CollectdKafkaVO()
```
- **설명:** CollectdKafkaVO 배열용 Serde 생성
- **반환값:** 설정된 Serde 인스턴스
- **사용:** 입력 데이터 역직렬화용

#### 사용 예제
```java
// 스트림 설정에서 Serde 사용
StreamsBuilder builder = new StreamsBuilder();
KStream<String, CollectdKafkaVO[]> stream = builder.stream("COLLECTD_DATA", 
    Consumed.with(Serdes.String(), CustomSerdes.CollectdKafkaVO()));

// 출력에 커스텀 serde 사용
stream.to("output-topic", Produced.with(Serdes.String(), CustomSerdes.HostMetricVO()));
```

---

### 개별 직렬화기/역직렬화기

#### HostMetricVOSerializer
**패키지:** `com.ainory.kafka.streams.serializer`

```java
public class HostMetricVOSerializer implements Serializer<HostMetricVO>
```
- **목적:** HostMetricVO 객체를 JSON 바이트 배열로 직렬화
- **구현:** JsonUtil을 사용한 JSON 변환

#### HostMetricVODeserializer
**패키지:** `com.ainory.kafka.streams.serializer`

```java
public class HostMetricVODeserializer implements Deserializer<HostMetricVO>
```
- **목적:** JSON 바이트 배열을 HostMetricVO 객체로 역직렬화
- **오류 처리:** 역직렬화 실패 시 null 반환

#### DslHostMetricVOSerializer/Deserializer
유사한 패턴으로 DslHostMetricVO 객체 처리

#### CollectdKafkaVOSerializer/Deserializer
CollectdKafkaVO[] 배열 객체 처리

---

## 스트림 프로세서

### ProcessTest1

**패키지:** `com.ainory.kafka.streams.process`

Processor API를 위한 메인 프로세서 로직 구현입니다.

#### 주요 메서드

##### init(ProcessorContext)
```java
public void init(ProcessorContext context)
```
- **설명:** 프로세서 초기화 및 상태 저장소 설정
- **기능:**
  - 프로세서 컨텍스트 저장
  - 상태 저장소 참조 획득
  - 60초 간격 스케줄링 설정

##### process(String, CollectdKafkaVO[])
```java
public void process(String key, CollectdKafkaVO[] value)
```
- **설명:** 들어오는 collectd 데이터 처리
- **로직:**
  - 각 메트릭에 대해 반복
  - 호스트별 메트릭 데이터 집계
  - 상태 저장소에 업데이트된 메트릭 저장

##### punctuate(long)
```java
public void punctuate(long timestamp)
```
- **설명:** 주기적 집계 계산 및 출력
- **동작:**
  - 모든 저장된 메트릭 반복
  - 최종 통계 계산
  - 결과를 다운스트림으로 전달

#### 사용 예제
```java
// 토폴로지에서 프로세서 사용
Topology topology = new Topology();
topology.addSource("source", "COLLECTD_DATA")
        .addProcessor("process", ProcessorSupplierTest::new, "source")
        .addStateStore(Stores.keyValueStoreBuilder(...)
        .addSink("sink", "output-topic", "process");
```

---

### ProcessorSupplierTest

**패키지:** `com.ainory.kafka.streams.process`

ProcessTest1 인스턴스 생성을 위한 프로세서 공급자입니다.

#### 메서드

##### get()
```java
public Processor<String, CollectdKafkaVO[]> get()
```
- **설명:** 새로운 ProcessTest1 인스턴스 생성
- **반환값:** 설정된 프로세서 인스턴스
- **사용:** Kafka Streams 토폴로지 구성에서

---

## 유틸리티 클래스

### JsonUtil

**패키지:** `com.ainory.kafka.streams.util`

JSON 직렬화/역직렬화를 위한 유틸리티 클래스입니다.

#### 정적 메서드

##### objectToJsonBytes(Object)
```java
public static byte[] objectToJsonBytes(Object object)
```
- **설명:** 객체를 JSON 바이트 배열로 변환
- **매개변수:** `object` - 직렬화할 객체
- **반환값:** JSON 바이트 배열
- **예외처리:** IOException 시 null 반환

##### jsonStringToObject(String, Class)
```java
public static Object jsonStringToObject(String jsonString, Class<?> clazz)
```
- **설명:** JSON 문자열을 지정된 클래스 객체로 변환
- **매개변수:**
  - `jsonString` - JSON 문자열
  - `clazz` - 대상 클래스
- **반환값:** 역직렬화된 객체
- **예외처리:** IOException 시 null 반환

##### jsonBytesToObject(byte[], Class)
```java
public static Object jsonBytesToObject(byte[] jsonBytes, Class<?> clazz)
```
- **설명:** JSON 바이트 배열을 객체로 변환
- **사용:** Kafka 메시지 역직렬화

#### 사용 예제
```java
// 객체를 JSON으로 직렬화
HostMetricVO metric = new HostMetricVO();
byte[] jsonBytes = JsonUtil.objectToJsonBytes(metric);

// JSON에서 객체로 역직렬화
String jsonString = "{\"hostname\":\"server-01\"}";
HostMetricVO deserializedMetric = (HostMetricVO) JsonUtil.jsonStringToObject(jsonString, HostMetricVO.class);
```

---

### DslKeyValueMapper

**패키지:** `com.ainory.kafka.streams.keyvalue.mapper`

DSL 스트림에서 키-값 변환을 위한 매퍼입니다.

#### 메서드

##### apply(String, DslHostMetricVO)
```java
public KeyValue<String, DslHostMetricVO> apply(String key, DslHostMetricVO value)
```
- **설명:** 스트림 레코드를 새로운 키-값 쌍으로 변환
- **매개변수:**
  - `key` - 원본 키
  - `value` - DslHostMetricVO 값
- **반환값:** 변환된 KeyValue 쌍

---

### CollectdTimestampExtractor

**패키지:** `com.ainory.kafka.streams.timestamp.extractor`

collectd 데이터에서 타임스탬프 추출을 위한 커스텀 타임스탬프 추출기입니다.

#### 메서드

##### extract(ConsumerRecord, long)
```java
public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp)
```
- **설명:** 레코드에서 타임스탬프 추출
- **로직:**
  - CollectdKafkaVO 배열에서 시간 필드 파싱
  - 문자열 타임스탬프를 밀리초로 변환
  - 윈도우 집계를 위한 정확한 타이밍 보장

#### 사용 예제
```java
// 스트림 설정에서 타임스탬프 추출기 사용
StreamsConfig config = new StreamsConfig(props);
config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, 
           CollectdTimestampExtractor.class);
```

---

## 설정

### Kafka Streams 설정

#### 필수 속성
```java
Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-collectd");
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
```

#### 성능 튜닝 속성
```java
// 처리 보장
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);

// 스레드 수
props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);

// 버퍼링
props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

// 상태 저장소
props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
```

#### 윈도우 설정
```java
// 텀블링 윈도우 크기
Duration windowSize = Duration.ofSeconds(60);

// 보유 시간
Duration retentionPeriod = Duration.ofHours(24);
```

### 토픽 설정

#### 토픽 생성 명령어
```bash
# 입력 토픽
kafka-topics.sh --create --topic COLLECTD_DATA \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

# DSL 출력 토픽
kafka-topics.sh --create --topic COLLECTD_DATA_TUMBLING_WINDOW \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1

# Processor API 출력 토픽
kafka-topics.sh --create --topic ainory_kafka_summary \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
```

#### 토픽 구성 권장사항
```bash
# 압축 활성화
kafka-configs.sh --alter --entity-type topics --entity-name COLLECTD_DATA \
  --add-config compression.type=lz4

# 보유 정책
kafka-configs.sh --alter --entity-type topics --entity-name COLLECTD_DATA \
  --add-config retention.ms=86400000  # 24시간
```

---

### 오류 처리 및 모니터링

#### 오류 처리 구성
```java
// 역직렬화 오류 처리
props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
          LogAndContinueExceptionHandler.class);

// 프로덕션 예외 처리
props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
          DefaultProductionExceptionHandler.class);
```

#### 모니터링 메트릭
- **처리량:** `records-processed-rate`
- **지연시간:** `commit-latency-avg`
- **오류율:** `skipped-records-rate`

#### 로깅 설정
```xml
<!-- logback.xml -->
<logger name="org.apache.kafka.streams" level="INFO"/>
<logger name="com.ainory.kafka.streams" level="DEBUG"/>
```

---

**이 API 문서는 Kafka Streams 테스트 프로젝트의 모든 구성 요소에 대한 완전한 참조를 제공합니다. 각 클래스와 메서드는 실제 사용 예제와 함께 문서화되어 있습니다.**