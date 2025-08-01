# Kafka Streams Test - 사용 가이드

## 개요

이 가이드는 Kafka Streams Test 프로젝트를 설정, 구성 및 사용하는 포괄적인 지침을 제공합니다. 이 프로젝트는 DSL과 Processor API 방식 모두를 사용하여 collectd 모니터링 데이터의 실시간 처리를 보여줍니다.

## 목차

1. [필수 사항](#필수-사항)
2. [설치 및 설정](#설치-및-설정)
3. [구성](#구성)
4. [빠른 시작](#빠른-시작)
5. [DSL 기반 처리](#dsl-기반-처리)
6. [Processor API 사용법](#processor-api-사용법)
7. [데이터 형식](#데이터-형식)
8. [예제](#예제)
9. [모니터링 및 문제해결](#모니터링-및-문제해결)
10. [성능 튜닝](#성능-튜닝)

---

## 필수 사항

### 시스템 요구사항

- **Java:** JDK 8 이상
- **Apache Kafka:** 1.0.0 버전 이상 호환
- **Maven:** 프로젝트 빌드를 위한 3.6+
- **메모리:** 최소 2GB RAM 권장

### 필요한 의존성

프로젝트는 다음 주요 의존성을 사용합니다 (Maven으로 관리):

- `kafka-streams: 1.0.0` - 핵심 스트리밍 기능
- `kafka-clients: 1.0.0` - Kafka 클라이언트 라이브러리
- `commons-lang3: 3.5` - 문자열 및 유틸리티 연산
- `commons-math3: 3.6.1` - 수학적 연산
- `jackson-databind` - JSON 직렬화 (전이 의존성)

---

## 설치 및 설정

### 1. 클론 및 빌드

```bash
# 저장소 클론
git clone <repository-url>
cd KafkaStreamsTest

# 프로젝트 빌드
mvn clean compile

# 의존성을 포함한 실행 가능한 JAR 생성
mvn package
```

다음이 생성됩니다:
- `target/KafkaStreamsTest.jar` - 메인 애플리케이션 JAR
- `target/KafkaStreamsTest-jar-with-dependencies.jar` - 독립 실행형 실행 파일
- `target/libs/` - 의존성 디렉토리

### 2. Kafka 설정

#### Kafka 서비스 시작

```bash
# Zookeeper 시작
bin/zookeeper-server-start.sh config/zookeeper.properties

# Kafka 브로커 시작
bin/kafka-server-start.sh config/server.properties
```

#### 필요한 토픽 생성

```bash
# collectd 데이터를 위한 입력 토픽
bin/kafka-topics.sh --create \
  --topic COLLECTD_DATA \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# DSL 결과를 위한 출력 토픽
bin/kafka-topics.sh --create \
  --topic COLLECTD_DATA_TUMBLING_WINDOW \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Processor API 결과를 위한 출력 토픽
bin/kafka-topics.sh --create \
  --topic ainory_kafka_summary \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

### 3. 설정 확인

```bash
# 토픽 목록
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

# 프로듀서 테스트
bin/kafka-console-producer.sh --topic COLLECTD_DATA --bootstrap-server localhost:9092

# 컨슈머 테스트
bin/kafka-console-consumer.sh --topic COLLECTD_DATA --bootstrap-server localhost:9092 --from-beginning
```

---

## 구성

### 애플리케이션 구성

#### Kafka Streams 속성

`streams.properties` 파일 생성:

```properties
# 애플리케이션 식별
application.id=kafka-streams-collectd-processor
bootstrap.servers=localhost:9092

# 처리 보장
processing.guarantee=at_least_once
num.stream.threads=2

# 오프셋 관리
auto.offset.reset=latest
enable.auto.commit=true

# 메모리 및 성능
cache.max.bytes.buffering=0
max.poll.records=1000
```

#### 서버 구성

소스 코드에서 브로커 주소 업데이트 또는 환경 변수 사용:

```java
// DslTestMain.java 및 ProcessorApiTestMain.java에서
String KAFKA_BROKERS = System.getenv("KAFKA_BROKERS") != null ? 
    System.getenv("KAFKA_BROKERS") : 
    "spanal-app:9092,spanal-1:9092,spanal-2:9092,spanal-3:9092";

props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
```

### 환경 변수

```bash
export KAFKA_BROKERS="localhost:9092"
export APP_ID="collectd-processor"
export WINDOW_SIZE_SECONDS="60"
```

---

## 빠른 시작

### 1. DSL 처리 실행

```bash
# DSL 기반 스트림 처리 실행
java -cp target/KafkaStreamsTest-jar-with-dependencies.jar \
  com.ainory.kafka.streams.DslTestMain

# 또는 Maven 사용
mvn exec:java -Dexec.mainClass="com.ainory.kafka.streams.DslTestMain"
```

### 2. Processor API 실행

```bash
# Processor API 기반 처리 실행
java -cp target/KafkaStreamsTest-jar-with-dependencies.jar \
  com.ainory.kafka.streams.ProcessorApiTestMain
```

### 3. 테스트 데이터 전송

```bash
# 샘플 collectd 데이터 전송
echo '[{"values":[45.2],"dstypes":["gauge"],"dsnames":["value"],"time":"1522299234.188","interval":10.0,"host":"test-server","plugin":"cpu","plugin_instance":"0","type":"cpu","type_instance":"idle","meta":{"network:received":true}}]' | \
  bin/kafka-console-producer.sh --topic COLLECTD_DATA --bootstrap-server localhost:9092
```

### 4. 출력 모니터링

```bash
# DSL 출력 모니터링
bin/kafka-console-consumer.sh \
  --topic COLLECTD_DATA_TUMBLING_WINDOW \
  --bootstrap-server localhost:9092 \
  --from-beginning

# Processor API 출력 모니터링
bin/kafka-console-consumer.sh \
  --topic ainory_kafka_summary \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

---

## DSL 기반 처리

### 개요

DSL 방식은 자동 윈도우잉과 집계를 가진 스트림 처리를 위한 고수준 추상화를 제공합니다.

### 주요 기능

- **Tumbling Windows:** 60초 비중첩 시간 윈도우
- **자동 집계:** 최솟값, 최댓값, 평균, 합계 계산
- **타입 안전성:** 사용자 정의 serdes를 사용한 강타입 연산
- **장애 허용성:** 내장된 오류 처리 및 복구

### 구현 예제

```java
public class CustomDslProcessor {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "custom-dsl-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        
        StreamsBuilder builder = new StreamsBuilder();
        
        // 입력 스트림
        KStream<String, String> input = builder.stream("COLLECTD_DATA");
        
        // 변환 및 집계
        KStream<String, DslHostMetricVO> processed = input
            .map(new DslKeyValueMapper())
            .filter((key, value) -> value != null);
            
        KTable<Windowed<String>, DslHostMetricVO> aggregated = processed
            .groupByKey()
            .windowedBy(TimeWindows.of(Duration.ofSeconds(60)))
            .aggregate(
                DslHostMetricVO::new,
                (key, value, aggregate) -> {
                    // 집계 로직
                    aggregate.setValue(aggregate.getValue().add(value.getValue()));
                    aggregate.setAggregationCount(aggregate.getAggregationCount() + 1);
                    return aggregate;
                },
                Materialized.with(Serdes.String(), CustomSerdes.DslHostMetricVO())
            );
            
        // 출력 스트림
        aggregated.toStream().to("output-topic");
        
        // 처리 시작
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        
        // 우아한 종료
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
```

### 윈도우 연산

#### Tumbling Windows

```java
TimeWindows tumblingWindow = TimeWindows.of(Duration.ofSeconds(60));
```

- **사용 사례:** 겹치지 않는 주기적 요약
- **예제:** 시간별 CPU 사용률 보고서

#### Hopping Windows

```java
TimeWindows hoppingWindow = TimeWindows
    .of(Duration.ofSeconds(60))
    .advanceBy(Duration.ofSeconds(30));
```

- **사용 사례:** 더 부드러운 추세를 위한 중첩 분석
- **예제:** 30초 시프트가 있는 이동 평균

#### Session Windows

```java
SessionWindows sessionWindow = SessionWindows.with(Duration.ofMinutes(5));
```

- **사용 사례:** 활동 기반 그룹화
- **예제:** 사용자 세션 분석

---

## Processor API 사용법

### 개요

Processor API는 사용자 정의 비즈니스 로직을 가진 스트림 처리에 대한 저수준 제어를 제공합니다.

### 주요 구성 요소

1. **Processor:** 사용자 정의 처리 로직 구현
2. **State Store:** 집계 상태를 위한 로컬 저장소
3. **Punctuator:** 예약된 처리 작업
4. **Topology:** 스트림 처리 그래프 정의

### 구현 패턴

```java
public class CustomProcessor extends AbstractProcessor<String, String> {
    private ProcessorContext context;
    private KeyValueStore<String, HostMetricVO> store;
    
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.store = (KeyValueStore) context.getStateStore("metrics-store");
        
        // 주기적 출력 예약
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

### State Store 구성

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

Kafka에서 받은 Collectd JSON 구조:

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
# CPU 사용률 데이터
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
# 디스크 연산
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

## 모니터링 및 문제해결

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

#### 로그 구성

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
오류: 토픽 'COLLECTD_DATA'를 찾을 수 없습니다
```

**해결책:**
```bash
bin/kafka-topics.sh --create --topic COLLECTD_DATA --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

#### 2. 직렬화 오류

```
오류: 값을 역직렬화할 수 없습니다
```

**해결책:** JSON 형식과 사용자 정의 serde 구성 확인:
```java
// 적절한 serde 구성 확인
props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
```

#### 3. 윈도우 처리 지연

```
경고: 처리 지연이 감지되었습니다
```

**해결책:** 윈도우 크기와 버퍼링 조정:
```java
props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
```

#### 4. 메모리 문제

```
오류: OutOfMemoryError
```

**해결책:** 힙 크기 증가:
```bash
export JAVA_OPTS="-Xmx2g -Xms1g"
```

### 디버깅 팁

1. **디버그 로깅 활성화:** 상세한 처리 정보를 위해 로그 레벨을 DEBUG로 설정
2. **컨슈머 지연 모니터링:** Kafka 도구를 사용하여 오프셋 지연 확인
3. **데이터 형식 확인:** 콘솔 컨슈머를 사용하여 메시지 형식 검사
4. **State Store 확인:** State Store 크기와 내용 모니터링
5. **네트워크 연결성:** 브로커 연결성 및 DNS 해결 확인

---

## 성능 튜닝

### JVM 튜닝

```bash
export JAVA_OPTS="-Xmx4g -Xms2g \
  -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=100 \
  -XX:+UseStringDeduplication"
```

### Kafka Streams 구성

```java
Properties props = new Properties();

// 병렬 처리
props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);

// 버퍼링
props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 100 * 1024 * 1024); // 100MB

// 커밋 빈도
props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000);

// 처리 보장
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
```

### 토픽 구성

```bash
# 병렬 처리를 위한 파티션 증가
bin/kafka-topics.sh --alter \
  --topic COLLECTD_DATA \
  --partitions 12 \
  --bootstrap-server localhost:9092

# 보존 기간 구성
bin/kafka-configs.sh --alter \
  --entity-type topics \
  --entity-name COLLECTD_DATA \
  --add-config retention.ms=86400000 \
  --bootstrap-server localhost:9092
```

### 성능 모니터링

#### 모니터링할 주요 메트릭

1. **처리 속도:** 초당 처리된 레코드 수
2. **지연 시간:** 종단 간 처리 지연 시간
3. **메모리 사용량:** 힙 및 오프힙 메모리 소비
4. **네트워크 I/O:** 대역폭 사용률
5. **State Store 크기:** 로컬 저장소 소비

#### 성능 벤치마크

일반적인 성능 특성:

- **처리량:** 초당 10,000-50,000 레코드 (복잡성에 따라)
- **지연 시간:** 종단 간 50-200ms
- **메모리:** 1-4GB 힙 권장
- **저장소:** State Store용 100MB-1GB

---

이 포괄적인 사용 가이드는 Kafka Streams Test 프로젝트를 성공적으로 배포하고 운영하는 데 필요한 모든 정보를 제공합니다. 추가 지원은 API 문서와 소스 코드 주석을 참조하십시오.