# Kafka Streams Test - API 문서

## 개요

Kafka Streams Test 프로젝트는 Apache Kafka Streams를 사용하여 collectd 모니터링 데이터를 실시간으로 스트림 처리하는 기능을 제공합니다. 이 문서는 사용 예제와 함께 모든 공개 API, 함수, 컴포넌트를 다룹니다.

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

Tumbling Window 연산을 사용하는 Kafka Streams DSL 기반 처리의 메인 클래스입니다.

#### 공개 상수

```java
public static int WINDOW_SECONDS = 60
```
- **설명:** Tumbling Window 집계의 기본 윈도우 크기 (60초)
- **타입:** `int`
- **사용법:** 데이터 집계를 위한 윈도우 연산에 사용

#### 주요 메서드

##### dslTumblingWindowTest()
```java
private void dslTumblingWindowTest()
```
- **설명:** Tumbling Window를 사용하는 DSL API로 Kafka Streams 토폴로지를 설정하고 실행
- **기능:**
  - Kafka Streams 속성 설정
  - collectd 데이터 처리를 위한 스트림 토폴로지 생성
  - 윈도우 집계 수행 (최솟값, 최댓값, 평균, 합계)
  - 대상 토픽으로 집계 결과 출력

**사용 예제:**
```java
DslTestMain dslMain = new DslTestMain();
// main 메서드를 통해 실행 - tumbling window 처리 시작
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
- **설명:** Low-level Processor API를 사용하여 Kafka Streams 토폴로지 설정
- **구성 요소:**
  - `COLLECTD_DATA` 토픽을 위한 Source 프로세서
  - 사용자 정의 프로세서 (`ProcessorSupplierTest`)
  - 메트릭 데이터를 위한 State Store
  - 출력을 위한 Sink 프로세서

##### processorApiRun()
```java
public void processorApiRun()
```
- **설명:** Processor API 테스트를 실행하는 공개 래퍼 메서드
- **사용법:** 프로세서 기반 스트림 처리 시작을 위한 진입점

**사용 예제:**
```java
ProcessorApiTestMain processorMain = new ProcessorApiTestMain();
processorMain.processorApiRun();
```

**토폴로지 설정:**
- Source: `COLLECTD_DATA` 토픽
- Processor: 사용자 정의 집계 로직
- State Store: 인메모리 키-값 저장소 (`HostMetric`)
- Sink: `ainory_kafka_summary` 토픽

---

### KafkaTest

**패키지:** `com.ainory.kafka.streams`

다양한 스트림 처리 패턴과 프로듀서 기능을 포함한 종합 테스트 클래스입니다.

#### 공개 상수

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
- **윈도우 연산:** Tumbling 및 Hopping 윈도우 구현
- **집계:** 최솟값, 최댓값, 평균, 합계 계산

---

## 엔티티 클래스

### CollectdKafkaVO

**패키지:** `com.ainory.kafka.streams.entity`

collectd 모니터링 데이터 구조를 나타내는 데이터 전송 객체입니다.

#### 필드

| 필드 | 타입 | 설명 |
|------|------|------|
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

Processor API 연산을 위한 집계된 호스트 메트릭 데이터입니다.

#### 주요 필드

| 필드 | 타입 | 설명 |
|------|------|------|
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
hostMetric.addMemoryData(8589934592L); // 8GB in bytes

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
|------|------|------|
| `hostname` | `String` | 호스트 식별자 |
| `value` | `BigDecimal` | 현재 메트릭 값 |
| `avg` | `BigDecimal` | 평균 값 |
| `min` | `BigDecimal` | 최솟값 |
| `max` | `BigDecimal` | 최댓값 |
| `sum` | `BigDecimal` | 합계 |
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
- **반환값:** BigDecimal로서의 평균값

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

엔티티 객체를 위한 사용자 정의 Kafka Serdes를 제공하는 팩토리 클래스입니다.

#### 정적 메서드

##### HostMetricVO()
```java
public static Serde<HostMetricVO> HostMetricVO()
```
- **설명:** HostMetricVO 객체를 위한 Serde 생성
- **반환값:** 설정된 Serde 인스턴스
- **사용법:** Kafka Streams 토폴로지 설정용

##### DslHostMetricVO()
```java
public static Serde<DslHostMetricVO> DslHostMetricVO()
```
- **설명:** DslHostMetricVO 객체를 위한 Serde 생성
- **반환값:** 설정된 Serde 인스턴스

#### 사용 예제
```java
// 스트림 토폴로지 설정에서
StreamsBuilder builder = new StreamsBuilder();
KStream<String, DslHostMetricVO> stream = builder.stream("input-topic", 
    Consumed.with(Serdes.String(), CustomSerdes.DslHostMetricVO()));
```

---

### 개별 직렬화기

#### CollectdKafkaVOSerializer/Deserializer
- **목적:** CollectdKafkaVO 객체의 JSON 직렬화
- **구현:** Jackson ObjectMapper 사용

#### HostMetricVOSerializer/Deserializer
- **목적:** HostMetricVO 객체의 JSON 직렬화
- **구현:** Jackson ObjectMapper 사용

#### NumberSerializer/Deserializer
- **목적:** 숫자 값의 직렬화
- **구현:** 사용자 정의 바이트 배열 변환

---

## 스트림 프로세서

### ProcessTest1

**패키지:** `com.ainory.kafka.streams.process`

Processor API를 사용하여 collectd 메트릭을 집계하는 사용자 정의 프로세서입니다.

#### 설정 상수

```java
private final long CHECK_INTERVAL_SEC = 1
private final long COLLECTD_COLLECT_INTERVAL_SEC = 10
private final int SUMMARY_INTERVAL_SEC = 60
```

#### 주요 메서드

##### init(ProcessorContext)
```java
public void init(ProcessorContext context)
```
- **설명:** 컨텍스트와 state store로 프로세서 초기화
- **매개변수:** `context` - 프로세서 실행 컨텍스트
- **부작용:** 집계를 위한 주기적 punctuation 설정

##### process(String, String)
```java
public void process(String key, String value)
```
- **설명:** 들어오는 collectd 데이터 레코드 처리
- **매개변수:**
  - `key` - 레코드 키 (일반적으로 호스트명)
  - `value` - collectd 데이터를 포함한 JSON 문자열
- **기능:**
  - JSON 데이터 파싱
  - CPU 및 메모리 메트릭 추출
  - state store의 실행 중인 집계 업데이트

#### 사용 패턴
```java
// Kafka Streams 토폴로지 내에서 사용
topology.addProcessor("PROCESS1", new ProcessorSupplierTest(), "Source");

// State store 설정
StoreBuilder<KeyValueStore<String, HostMetricVO>> storeBuilder = 
    Stores.keyValueStoreBuilder(
        Stores.inMemoryKeyValueStore("HostMetric"),
        Serdes.String(), 
        CustomSerdes.HostMetricVO());
```

---

### ProcessorSupplierTest

**패키지:** `com.ainory.kafka.streams.process`

ProcessTest1 프로세서 인스턴스를 생성하는 팩토리입니다.

#### 메서드

##### get()
```java
public Processor get()
```
- **설명:** 새로운 ProcessTest1 인스턴스 생성
- **반환값:** 새로운 프로세서 인스턴스
- **사용법:** Kafka Streams Processor API에서 필요

---

## 유틸리티 클래스

### JsonUtil

**패키지:** `com.ainory.kafka.streams.util`

Jackson을 사용한 JSON 직렬화 및 역직렬화 유틸리티입니다.

#### 정적 메서드

##### objectToJsonString(Object)
```java
public static String objectToJsonString(Object obj)
```
- **설명:** Java 객체를 JSON 문자열로 변환
- **매개변수:** `obj` - 직렬화할 객체
- **반환값:** JSON 문자열 표현
- **에러 처리:** 직렬화 오류 시 null 반환

##### jsonStringToObject(String)
```java
public static Object jsonStringToObject(String jsonString)
```
- **설명:** JSON 문자열을 일반 Object로 변환
- **매개변수:** `jsonString` - 파싱할 JSON 문자열
- **반환값:** 파싱된 객체
- **에러 처리:** 파싱 오류 시 null 반환

##### jsonStringToObject(String, Class)
```java
public static Object jsonStringToObject(String jsonString, Class aClass)
```
- **설명:** JSON 문자열을 특정 클래스 타입으로 변환
- **매개변수:**
  - `jsonString` - 파싱할 JSON 문자열
  - `aClass` - 역직렬화를 위한 대상 클래스
- **반환값:** 타입이 지정된 객체 인스턴스

#### 사용 예제
```java
// 객체를 JSON으로 직렬화
HostMetricVO metric = new HostMetricVO();
String json = JsonUtil.objectToJsonString(metric);

// JSON을 객체로 역직렬화
CollectdKafkaVO[] metrics = (CollectdKafkaVO[]) JsonUtil.jsonStringToObject(
    jsonData, CollectdKafkaVO[].class);

// 일반 역직렬화
Object parsed = JsonUtil.jsonStringToObject(jsonString);
```

---

### CollectdTimestampExtractor

**패키지:** `com.ainory.kafka.streams.timestamp.extractor`

collectd 데이터 레코드를 위한 사용자 정의 타임스탬프 추출기입니다.

#### 메서드

##### extract(ConsumerRecord, long)
```java
public long extract(ConsumerRecord<Object, Object> consumerRecord, long l)
```
- **설명:** collectd JSON 데이터에서 타임스탬프 추출
- **매개변수:**
  - `consumerRecord` - Kafka 컨슈머 레코드
  - `l` - 이전 타임스탬프 (폴백)
- **반환값:** 밀리초 단위로 추출된 타임스탬프
- **로직:**
  - collectd JSON 구조 파싱
  - 'time' 필드 추출
  - 소수 타임스탬프를 밀리초로 변환
  - 오류 시 현재 시간으로 폴백

#### 사용 예제
```java
// 스트림 설정에서
props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, 
    CollectdTimestampExtractor.class);
```

---

### DslKeyValueMapper

**패키지:** `com.ainory.kafka.streams.keyvalue.mapper`

DSL 연산을 위해 collectd JSON 데이터를 타입이 지정된 키-값 쌍으로 매핑합니다.

#### 메서드

##### apply(String, String)
```java
public KeyValue<String, DslHostMetricVO> apply(String key, String value)
```
- **설명:** collectd JSON을 DslHostMetricVO로 변환
- **매개변수:**
  - `key` - 원본 키
  - `value` - Collectd JSON 문자열
- **반환값:** 복합 키와 타입이 지정된 값을 가진 KeyValue 쌍
- **키 형식:** `host^plugin^plugin_instance^type^type_instance`

#### 데이터 타입 처리
다양한 숫자 타입 지원:
- `Integer`
- `Long`
- `Float`
- `Double`

#### 사용 예제
```java
// 스트림 처리에서
KStream<String, String> input = builder.stream("collectd-topic");
KStream<String, DslHostMetricVO> mapped = input.map(new DslKeyValueMapper());
```

---

## 설정

### Kafka Streams 속성

#### 공통 설정
```java
Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "your-app-id");
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka1:9092,kafka2:9092");
props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
```

#### DSL 전용 설정
```java
props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, 
    CollectdTimestampExtractor.class);
```

### 토픽 설정

| 토픽 | 목적 | 키 타입 | 값 타입 |
|------|------|---------|---------|
| `COLLECTD_DATA` | collectd 메트릭 입력 | String | JSON String |
| `COLLECTD_DATA_TUMBLING_WINDOW` | DSL 집계 출력 | String | DslHostMetricVO JSON |
| `ainory_kafka_summary` | Processor API 출력 | String | HostMetricVO JSON |

### 윈도우 설정

#### Tumbling Windows
```java
TimeWindows.of(Duration.ofSeconds(WINDOW_SECONDS))
```
- **목적:** 겹치지 않는 시간 기반 집계
- **기본 크기:** 60초
- **사용 사례:** 주기적 요약 통계

#### Hopping Windows
```java
TimeWindows.of(Duration.ofSeconds(windowSize))
    .advanceBy(Duration.ofSeconds(advanceSize))
```
- **목적:** 겹치는 시간 기반 집계
- **설정:** 사용자 정의 가능한 윈도우 및 advance 크기

---

## 에러 처리

### 공통 패턴

1. **JSON 파싱 오류**
   - JsonUtil 메서드에서 포착 및 로깅
   - 기본 폴백 값 반환
   - 우아한 성능 저하

2. **타임스탬프 추출 오류**
   - 현재 시스템 시간으로 폴백
   - 스트림 처리 중단 방지

3. **직렬화 오류**
   - printStackTrace()를 통한 로깅
   - 실패한 연산에 대해 null 반환

### 모범 사례

1. **Null 체크:** 처리 전 항상 객체 검증
2. **예외 처리:** 위험한 연산을 try-catch 블록으로 감싸기
3. **폴백 값:** 누락된 데이터에 대한 합리적인 기본값 제공
4. **로깅:** 다양한 오류 타입에 적절한 로깅 레벨 사용

---

이 문서는 Kafka Streams Test 프로젝트의 모든 공개 API, 함수, 컴포넌트에 대한 포괄적인 커버리지를 제공합니다. 추가 구현 세부사항은 소스 코드와 인라인 주석을 참조하십시오.