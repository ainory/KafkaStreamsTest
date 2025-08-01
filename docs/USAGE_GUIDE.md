# Kafka Streams 테스트 - 사용 가이드

## 개요

이 가이드는 Kafka Streams 테스트 프로젝트의 설정, 구성 및 사용에 대한 포괄적인 지침을 제공합니다. 이 프로젝트는 DSL과 Processor API 두 가지 접근 방식을 사용하여 collectd 모니터링 데이터의 실시간 처리를 보여줍니다.

## 목차

1. [전제 조건](#전제-조건)
2. [설치 및 설정](#설치-및-설정)
3. [설정](#설정)
4. [빠른 시작](#빠른-시작)
5. [DSL 기반 처리](#dsl-기반-처리)
6. [Processor API 사용](#processor-api-사용)
7. [데이터 형식](#데이터-형식)
8. [예제](#예제)
9. [모니터링 및 문제 해결](#모니터링-및-문제-해결)
10. [성능 튜닝](#성능-튜닝)

---

## 전제 조건

### 시스템 요구사항

- **Java:** JDK 8 이상
- **Apache Kafka:** 버전 1.0.0 이상 호환
- **Maven:** 3.6+ (프로젝트 빌드용)
- **메모리:** 최소 2GB RAM 권장

### 필수 종속성

프로젝트는 다음 주요 종속성을 사용합니다 (Maven을 통해 관리):

- `kafka-streams: 1.0.0` - 핵심 스트리밍 기능
- `kafka-clients: 1.0.0` - Kafka 클라이언트 라이브러리
- `commons-lang3: 3.5` - 문자열 및 유틸리티 연산
- `commons-math3: 3.6.1` - 수학적 연산
- `jackson-databind` - JSON 직렬화 (전이 종속성)

---

## 설치 및 설정

### 1. 클론 및 빌드

```bash
# 저장소 클론
git clone <repository-url>
cd KafkaStreamsTest

# 프로젝트 빌드
mvn clean compile

# 종속성이 포함된 실행 가능한 JAR 생성
mvn package
```

다음이 생성됩니다:
- `target/KafkaStreamsTest.jar` - 메인 애플리케이션 JAR
- `target/KafkaStreamsTest-jar-with-dependencies.jar` - 독립 실행형 실행 파일
- `target/libs/` - 종속성 디렉토리

### 2. Kafka 설정

#### Kafka 서비스 시작

```bash
# Zookeeper 시작
bin/zookeeper-server-start.sh config/zookeeper.properties

# Kafka 브로커 시작
bin/kafka-server-start.sh config/server.properties
```

#### 필수 토픽 생성

```bash
# collectd 데이터용 입력 토픽
bin/kafka-topics.sh --create \
  --topic COLLECTD_DATA \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# DSL 결과용 출력 토픽
bin/kafka-topics.sh --create \
  --topic COLLECTD_DATA_TUMBLING_WINDOW \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Processor API 결과용 출력 토픽
bin/kafka-topics.sh --create \
  --topic ainory_kafka_summary \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

#### 토픽 확인

```bash
# 생성된 토픽 나열
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# 토픽 세부 정보 확인
bin/kafka-topics.sh --describe --topic COLLECTD_DATA --bootstrap-server localhost:9092
```

---

## 설정

### 기본 Kafka Streams 설정

```java
Properties props = new Properties();

// 필수 속성
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-collectd-processor");
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

// 성능 튜닝
props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);

// 오류 처리
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
```

### 고급 설정 옵션

#### 타임스탬프 추출

```java
props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, 
    CollectdTimestampExtractor.class);
```

#### 상태 저장소 설정

```java
props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
```

#### 처리 보장

```java
// 적어도 한 번 처리
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);

// 정확히 한 번 처리 (성능 트레이드오프 있음)
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
```

---

## 빠른 시작

### 1. 애플리케이션 실행

#### DSL 기반 처리

```bash
java -cp target/KafkaStreamsTest-jar-with-dependencies.jar \
  com.ainory.kafka.streams.DslTestMain
```

#### Processor API 처리

```bash
java -cp target/KafkaStreamsTest-jar-with-dependencies.jar \
  com.ainory.kafka.streams.ProcessorApiTestMain
```

### 2. 테스트 데이터 전송

```bash
# 샘플 CPU 메트릭
echo '[{"values":[45.2],"dstypes":["gauge"],"dsnames":["value"],"time":"1522299234.188","interval":10.0,"host":"test-server","plugin":"cpu","plugin_instance":"0","type":"cpu","type_instance":"user","meta":{}}]' | \
  bin/kafka-console-producer.sh --topic COLLECTD_DATA --bootstrap-server localhost:9092
```

### 3. 결과 모니터링

```bash
# DSL 출력 확인
bin/kafka-console-consumer.sh --topic COLLECTD_DATA_TUMBLING_WINDOW \
  --bootstrap-server localhost:9092 --from-beginning

# Processor API 출력 확인
bin/kafka-console-consumer.sh --topic ainory_kafka_summary \
  --bootstrap-server localhost:9092 --from-beginning
```

---

## DSL 기반 처리

### 개요

DSL(Domain Specific Language) API는 함수형 프로그래밍 스타일을 사용하여 스트림 처리를 위한 고수준 추상화를 제공합니다.

### 주요 구성 요소

1. **KStream:** 무한한 레코드 스트림
2. **KTable:** 변경 로그 스트림 (업데이트 가능)
3. **윈도우:** 시간 기반 집계
4. **집계:** 그룹화된 데이터에 대한 계산

### 구현 패턴

```java
StreamsBuilder builder = new StreamsBuilder();

// 입력 스트림 정의
KStream<String, String> source = builder.stream("COLLECTD_DATA");

// 데이터 변환 및 키 변경
KStream<String, DslHostMetricVO> mappedStream = source
    .map(new DslKeyValueMapper())
    .filter((key, value) -> value != null);

// 윈도우 집계
KTable<Windowed<String>, DslHostMetricVO> windowedTable = mappedStream
    .groupByKey()
    .windowedBy(TimeWindows.of(Duration.ofSeconds(60)))
    .aggregate(
        () -> new DslHostMetricVO(),           // 초기화자
        (key, value, aggregate) -> {           // 집계자
            // 집계 로직 구현
            return updateAggregate(aggregate, value);
        },
        Materialized.with(Serdes.String(), CustomSerdes.DslHostMetricVO())
    );

// 결과를 출력 토픽으로 전송
windowedTable.toStream()
    .map((windowedKey, value) -> new KeyValue<>(windowedKey.key(), value))
    .to("COLLECTD_DATA_TUMBLING_WINDOW", 
        Produced.with(Serdes.String(), CustomSerdes.DslHostMetricVO()));
```

### 윈도우 유형

#### 텀블링 윈도우
```java
TimeWindows.of(Duration.ofSeconds(60))
```
- **목적:** 겹치지 않는 시간 기반 집계
- **사용 사례:** 주기적 요약 통계

#### 호핑 윈도우
```java
TimeWindows.of(Duration.ofSeconds(60))
    .advanceBy(Duration.ofSeconds(30))
```
- **목적:** 겹치는 시간 기반 집계
- **사용 사례:** 사용자 세션 분석

---

## Processor API 사용

### 개요

Processor API는 커스텀 비즈니스 로직으로 스트림 처리에 대한 저수준 제어를 제공합니다.

### 주요 구성 요소

1. **Processor:** 커스텀 처리 로직 구현
2. **상태 저장소:** 집계 상태를 위한 로컬 저장소
3. **Punctuator:** 예약된 처리 작업
4. **토폴로지:** 스트림 처리 그래프 정의

### 구현 패턴

```java
public class CustomProcessor extends AbstractProcessor<String, String> {
    private ProcessorContext context;
    private KeyValueStore<String, HostMetricVO> store;
    
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.store = (KeyValueStore) context.getStateStore("metrics-store");
        
        // 주기적 출력 스케줄링
        context.schedule(Duration.ofSeconds(60), PunctuationType.WALL_CLOCK_TIME, 
            this::punctuate);
    }
    
    @Override
    public void process(String key, String value) {
        try {
            // 입력 데이터 파싱
            CollectdKafkaVO[] data = JsonUtil.jsonStringToObject(value, CollectdKafkaVO[].class);
            
            // 처리 및 저장
            HostMetricVO metric = store.get(key);
            if (metric == null) {
                metric = new HostMetricVO();
                metric.setHostname(key);
            }
            
            // 메트릭 업데이트
            updateMetrics(metric, data[0]);
            store.put(key, metric);
            
        } catch (Exception e) {
            // 오류 처리
            context.forward(key, "ERROR: " + e.getMessage());
        }
    }
    
    private void punctuate(long timestamp) {
        try (KeyValueIterator<String, HostMetricVO> iter = store.all()) {
            while (iter.hasNext()) {
                KeyValue<String, HostMetricVO> entry = iter.next();
                
                // 집계된 결과 출력
                String output = JsonUtil.objectToJsonString(entry.value);
                context.forward(entry.key, output);
            }
        }
    }
}
```

### 상태 저장소 설정

```java
// 토폴로지 설정에서
StoreBuilder<KeyValueStore<String, HostMetricVO>> storeBuilder = 
    Stores.keyValueStoreBuilder(
        Stores.persistentKeyValueStore("metrics-store"),  // 또는 inMemoryKeyValueStore
        Serdes.String(),
        CustomSerdes.HostMetricVO()
    );

topology.addStateStore(storeBuilder, "PROCESSOR_NAME");
```

---

## 데이터 형식

### 입력 형식 (CollectdKafkaVO)

Kafka에서 수신한 Collectd JSON 구조:

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

### 출력 형식

#### DSL 출력 (DslHostMetricVO)

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

#### Processor API 출력 (HostMetricVO)

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

## 예제

### 예제 1: 기본 CPU 모니터링

#### 입력 데이터
```bash
# CPU 사용량 데이터
echo '[{"values":[45.2],"dstypes":["gauge"],"dsnames":["value"],"time":"1522299234.188","interval":10.0,"host":"server-01","plugin":"cpu","plugin_instance":"0","type":"cpu","type_instance":"user","meta":{}}]' | \
  bin/kafka-console-producer.sh --topic COLLECTD_DATA --bootstrap-server localhost:9092
```

#### 예상 출력
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

### 예제 2: 메모리 모니터링

#### 입력 데이터
```bash
# 메모리 사용량 데이터
echo '[{"values":[8589934592],"dstypes":["gauge"],"dsnames":["value"],"time":"1522299234.188","interval":10.0,"host":"server-01","plugin":"memory","plugin_instance":"","type":"memory","type_instance":"used","meta":{}}]' | \
  bin/kafka-console-producer.sh --topic COLLECTD_DATA --bootstrap-server localhost:9092
```

### 예제 3: 디스크 I/O 모니터링

#### 입력 데이터
```bash
# 디스크 작업
echo '[{"values":[1250],"dstypes":["counter"],"dsnames":["value"],"time":"1522299234.188","interval":10.0,"host":"server-01","plugin":"disk","plugin_instance":"sda","type":"disk_ops","type_instance":"read","meta":{}}]' | \
  bin/kafka-console-producer.sh --topic COLLECTD_DATA --bootstrap-server localhost:9092
```

### 예제 4: 다중 값 로드 평균

#### 입력 데이터
```bash
# 로드 평균 (단기, 중기, 장기)
echo '[{"values":[2.07,2.07,2.04],"dstypes":["gauge","gauge","gauge"],"dsnames":["shortterm","midterm","longterm"],"time":"1522299234.188","interval":60.0,"host":"server-01","plugin":"load","plugin_instance":"","type":"load","type_instance":"","meta":{}}]' | \
  bin/kafka-console-producer.sh --topic COLLECTD_DATA --bootstrap-server localhost:9092
```

---

## 모니터링 및 문제 해결

### 애플리케이션 모니터링

#### JMX 메트릭

모니터링을 위한 JMX 활성화:

```bash
export JMX_OPTS="-Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -Dcom.sun.management.jmxremote.port=9999"

java $JMX_OPTS -cp target/KafkaStreamsTest-jar-with-dependencies.jar \
  com.ainory.kafka.streams.DslTestMain
```

#### 로그 설정

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

### 일반적인 문제

#### 1. 토픽을 찾을 수 없음

```
Error: Topic 'COLLECTD_DATA' not found
```

**해결책:**
```bash
bin/kafka-topics.sh --create --topic COLLECTD_DATA --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

#### 2. 직렬화 오류

```
Error: Cannot deserialize value
```

**해결책:** JSON 형식 및 커스텀 serde 설정 확인:
```java
// 적절한 serde 설정 확인
props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
```

#### 3. 윈도우 처리 지연

```
Warning: Processing lag detected
```

**해결책:** 윈도우 크기 및 버퍼링 튜닝:
```java
props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
```

#### 4. 메모리 문제

```
Error: OutOfMemoryError
```

**해결책:** 힙 크기 증가:
```bash
export JAVA_OPTS="-Xmx2g -Xms1g"
```

### 디버깅 팁

1. **디버그 로깅 활성화:** 상세한 처리 정보를 위해 로그 레벨을 DEBUG로 설정
2. **컨슈머 지연 모니터링:** Kafka 도구를 사용하여 오프셋 지연 확인
3. **데이터 형식 확인:** 콘솔 컨슈머를 사용하여 메시지 형식 검사
4. **상태 저장소 확인:** 상태 저장소 크기 및 내용 모니터링
5. **네트워크 연결:** 브로커 연결 및 DNS 해석 확인

---

## 성능 튜닝

### JVM 튜닝

```bash
export JAVA_OPTS="-Xmx4g -Xms2g \
  -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=100 \
  -XX:+UseStringDeduplication"
```

### Kafka Streams 설정

```java
Properties props = new Properties();

// 병렬성
props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);

// 버퍼링
props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 100 * 1024 * 1024); // 100MB

// 커밋 빈도
props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000);

// 처리 보장
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
```

### 토픽 설정

```bash
# 병렬성을 위한 파티션 수 증가
bin/kafka-topics.sh --alter \
  --topic COLLECTD_DATA \
  --partitions 12 \
  --bootstrap-server localhost:9092

# 보존 설정
bin/kafka-configs.sh --alter \
  --entity-type topics \
  --entity-name COLLECTD_DATA \
  --add-config retention.ms=86400000 \
  --bootstrap-server localhost:9092
```

### 성능 모니터링

#### 모니터링해야 할 주요 메트릭

1. **처리율:** 초당 처리된 레코드 수
2. **지연시간:** 종단간 처리 지연시간
3. **메모리 사용량:** 힙 및 오프힙 메모리 소비
4. **네트워크 I/O:** 대역폭 사용률
5. **상태 저장소 크기:** 로컬 저장소 소비

#### 성능 벤치마크

일반적인 성능 특성:

- **처리량:** 10,000-50,000 레코드/초 (복잡도에 따라)
- **지연시간:** 50-200ms 종단간
- **메모리:** 1-4GB 힙 권장
- **저장소:** 상태 저장소용 100MB-1GB

---

이 포괄적인 사용 가이드는 Kafka Streams 테스트 프로젝트를 성공적으로 배포하고 운영하는 데 필요한 모든 정보를 제공합니다. 추가 지원이 필요하면 API 문서 및 소스 코드 주석을 참조하세요.