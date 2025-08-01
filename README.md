# Kafka Streams 테스트

Apache Kafka Streams를 사용한 collectd 모니터링 데이터의 실시간 스트림 처리를 종합적으로 보여주는 프로젝트입니다. 이 프로젝트는 확장 가능한 데이터 처리 파이프라인 구축을 위한 DSL(Domain Specific Language)과 Processor API 두 가지 접근 방식을 모두 제시합니다.

## 🎯 개요

Kafka Streams 테스트 프로젝트는 collectd 모니터링 데이터를 실시간으로 처리하며, 시간 윈도우를 통한 집계 및 계산을 수행합니다. 다음과 같은 내용을 보여줍니다:

- **실시간 데이터 처리:** 모니터링 메트릭(CPU, 메모리, 디스크 I/O)의 스트림 처리
- **윈도우 집계:** 텀블링 윈도우와 호핑 윈도우를 통한 시간 기반 그룹화
- **다양한 처리 패턴:** 고수준 DSL과 저수준 Processor API 구현
- **확장 가능한 아키텍처:** 상태 관리가 포함된 수평 확장 가능한 스트림 처리

## 📋 목차

- [아키텍처](#-아키텍처)
- [기능](#-기능)
- [빠른 시작](#-빠른-시작)
- [문서](#-문서)
- [프로젝트 구조](#-프로젝트-구조)
- [설정](#-설정)
- [예제](#-예제)
- [성능](#-성능)
- [기여하기](#-기여하기)
- [라이선스](#-라이선스)

## 🏗 아키텍처

### 데이터 흐름

```
collectd → Kafka Topic → Kafka Streams → 집계 결과 → 출력 Topic
```

**상세 흐름:**
1. **collectd**가 시스템 메트릭을 수집 (10초 간격)
2. **Kafka 플러그인**이 JSON 데이터를 `COLLECTD_DATA` 토픽으로 전송
3. **Kafka Streams**가 60초 집계 윈도우로 데이터 처리
4. **결과**가 최솟값/최댓값/평균/합계 계산과 함께 대상 토픽으로 출력

### 처리 방식

#### DSL (Domain Specific Language)
- 고수준 함수형 프로그래밍 스타일
- 자동 윈도우잉 및 집계
- 커스텀 serdes를 통한 타입 안전 연산
- 내장 장애 허용성

#### Processor API
- 처리 로직에 대한 저수준 제어
- 커스텀 상태 관리
- 유연한 스케줄링 및 펑처에이션
- 세밀한 오류 처리

## ✨ 기능

### 핵심 기능

- **📊 실시간 모니터링:** 도착하는 collectd 메트릭을 실시간으로 처리
- **⏱️ 윈도우 집계:** 주기적 요약을 위한 60초 텀블링 윈도우
- **📈 통계 연산:** 최솟값, 최댓값, 평균, 합계 계산
- **🔄 다중 API:** DSL과 Processor API 구현 모두 제공
- **💾 상태 관리:** 영구 및 인메모리 상태 저장소
- **⚡ 고성능:** 처리량과 저지연시간에 최적화

### 지원 메트릭

- **CPU 사용량:** 코어별 및 전체 CPU 사용률
- **메모리 통계:** 메모리 사용 패턴 및 트렌드
- **디스크 I/O:** 읽기/쓰기 작업 및 처리량
- **로드 평균:** 시스템 로드 메트릭 (1분, 5분, 15분)
- **네트워크 활동:** 인터페이스 통계 및 대역폭

## 🚀 빠른 시작

### 전제 조건

- Java 8+ (JDK)
- Apache Kafka 1.0.0+
- Maven 3.6+

### 설치

```bash
# 저장소 클론
git clone <repository-url>
cd KafkaStreamsTest

# 프로젝트 빌드
mvn clean package

# 다음이 생성됩니다:
# - target/KafkaStreamsTest.jar
# - target/KafkaStreamsTest-jar-with-dependencies.jar
```

### Kafka 설정

```bash
# 서비스 시작
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties

# 토픽 생성
bin/kafka-topics.sh --create --topic COLLECTD_DATA --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
bin/kafka-topics.sh --create --topic COLLECTD_DATA_TUMBLING_WINDOW --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
bin/kafka-topics.sh --create --topic ainory_kafka_summary --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

### 애플리케이션 실행

#### DSL 기반 처리
```bash
java -cp target/KafkaStreamsTest-jar-with-dependencies.jar com.ainory.kafka.streams.DslTestMain
```

#### Processor API 처리
```bash
java -cp target/KafkaStreamsTest-jar-with-dependencies.jar com.ainory.kafka.streams.ProcessorApiTestMain
```

### 테스트 데이터 전송

```bash
# CPU 메트릭 예제
echo '[{"values":[45.2],"dstypes":["gauge"],"dsnames":["value"],"time":"1522299234.188","interval":10.0,"host":"test-server","plugin":"cpu","plugin_instance":"0","type":"cpu","type_instance":"user","meta":{}}]' | \
  bin/kafka-console-producer.sh --topic COLLECTD_DATA --bootstrap-server localhost:9092
```

### 결과 모니터링

```bash
# DSL 출력 확인
bin/kafka-console-consumer.sh --topic COLLECTD_DATA_TUMBLING_WINDOW --bootstrap-server localhost:9092 --from-beginning

# Processor API 출력 확인
bin/kafka-console-consumer.sh --topic ainory_kafka_summary --bootstrap-server localhost:9092 --from-beginning
```

## 📚 문서

### 완전한 문서 세트

- **[API 문서](docs/API_DOCUMENTATION.md)** - 예제가 포함된 포괄적인 API 참조
- **[사용 가이드](docs/USAGE_GUIDE.md)** - 상세한 설정, 구성 및 사용 지침
- **[JavaDoc](target/site/apidocs/)** - `mvn javadoc:javadoc` 실행 후 생성

### 주요 컴포넌트 문서

| 컴포넌트 | 목적 | 문서 |
|-----------|---------|---------------|
| `DslTestMain` | DSL 기반 스트림 처리 | [API 문서](docs/API_DOCUMENTATION.md#dsltestmain) |
| `ProcessorApiTestMain` | Processor API 구현 | [API 문서](docs/API_DOCUMENTATION.md#processorapitestmain) |
| `CollectdKafkaVO` | 입력 데이터 구조 | [API 문서](docs/API_DOCUMENTATION.md#collectdkafkavo) |
| `HostMetricVO` | 집계된 출력 형식 | [API 문서](docs/API_DOCUMENTATION.md#hostmetricvo) |
| `CustomSerdes` | 직렬화 유틸리티 | [API 문서](docs/API_DOCUMENTATION.md#customserdes) |
| `JsonUtil` | JSON 처리 유틸리티 | [API 문서](docs/API_DOCUMENTATION.md#jsonutil) |

## 📁 프로젝트 구조

```
KafkaStreamsTest/
├── src/main/java/com/ainory/kafka/streams/
│   ├── DslTestMain.java                    # DSL 기반 메인 클래스
│   ├── ProcessorApiTestMain.java           # Processor API 메인 클래스
│   ├── KafkaTest.java                      # 포괄적인 테스트 시나리오
│   ├── entity/                             # 데이터 전송 객체
│   │   ├── CollectdKafkaVO.java           # 입력 데이터 구조
│   │   ├── HostMetricVO.java              # Processor API 출력
│   │   └── DslHostMetricVO.java           # DSL 출력 구조
│   ├── serializer/                         # 커스텀 직렬화
│   │   ├── CustomSerdes.java              # Serde 팩토리
│   │   ├── *Serializer.java               # 개별 직렬화기
│   │   └── *Deserializer.java             # 개별 역직렬화기
│   ├── process/                            # 스트림 프로세서
│   │   ├── ProcessTest1.java              # 메인 프로세서 로직
│   │   └── ProcessorSupplierTest.java     # 프로세서 팩토리
│   ├── keyvalue/mapper/                    # 키-값 변환
│   │   └── DslKeyValueMapper.java         # DSL 키 매핑
│   ├── timestamp/extractor/                # 타임스탬프 추출
│   │   └── CollectdTimestampExtractor.java # 커스텀 타임스탬프 로직
│   └── util/                               # 유틸리티 클래스
│       └── JsonUtil.java                  # JSON 처리
├── docs/                                   # 문서
│   ├── API_DOCUMENTATION.md               # 완전한 API 참조
│   └── USAGE_GUIDE.md                     # 설정 및 사용 가이드
├── pom.xml                                 # Maven 설정
├── README.md                               # 이 파일
└── LICENSE                                 # 라이선스 정보
```

## ⚙️ 설정

### Kafka Streams 속성

**주요 설정 옵션:**

```java
// 애플리케이션 식별
application.id=kafka-streams-collectd-processor
bootstrap.servers=localhost:9092

// 처리 설정
processing.guarantee=at_least_once
num.stream.threads=2

// 윈도우 설정
window.size.seconds=60
cache.max.bytes.buffering=0
```

### 토픽 설정

| 토픽 | 목적 | 파티션 | 보존 기간 |
|-------|---------|------------|-----------|
| `COLLECTD_DATA` | collectd의 입력 메트릭 | 3 | 24시간 |
| `COLLECTD_DATA_TUMBLING_WINDOW` | DSL 집계 출력 | 3 | 7일 |
| `ainory_kafka_summary` | Processor API 출력 | 3 | 7일 |

### 환경 변수

```bash
export KAFKA_BROKERS="localhost:9092"
export APP_ID="collectd-processor"
export WINDOW_SIZE_SECONDS="60"
export JAVA_OPTS="-Xmx2g -Xms1g"
```

## 💡 예제

### 입력 데이터 형식 (Collectd JSON)

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
  "type_instance": "user",
  "values": [45.2]
}]
```

### 출력 데이터 형식

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
  "cpu_avg": 47.8,
  "cpu_min": 42.1,
  "cpu_max": 52.3,
  "memory_avg": 8589934592.0,
  "memory_min": 8000000000,
  "memory_max": 9000000000,
  "startTimestamp": 1522299180000,
  "endTimestamp": 1522299240000
}
```

### 사용 사례 예제

#### 1. CPU 모니터링
```bash
# 여러 코어의 CPU 사용량 모니터링
echo '[{"values":[45.2],"dstypes":["gauge"],"dsnames":["value"],"time":"1522299234.188","interval":10.0,"host":"server-01","plugin":"cpu","plugin_instance":"0","type":"cpu","type_instance":"user","meta":{}}]'
```

#### 2. 메모리 추적
```bash
# 메모리 사용률 추적
echo '[{"values":[8589934592],"dstypes":["gauge"],"dsnames":["value"],"time":"1522299234.188","interval":10.0,"host":"server-01","plugin":"memory","plugin_instance":"","type":"memory","type_instance":"used","meta":{}}]'
```

#### 3. 로드 평균 모니터링
```bash
# 다중 값 로드 평균 (1분, 5분, 15분)
echo '[{"values":[2.07,2.07,2.04],"dstypes":["gauge","gauge","gauge"],"dsnames":["shortterm","midterm","longterm"],"time":"1522299234.188","interval":60.0,"host":"server-01","plugin":"load","plugin_instance":"","type":"load","type_instance":"","meta":{}}]'
```

## 📊 성능

### 벤치마크

**일반적인 성능 특성:**

- **처리량:** 초당 10,000-50,000 레코드
- **지연시간:** 50-200ms 종단간
- **메모리 사용량:** 1-4GB 힙 권장
- **상태 저장소:** 일반적으로 100MB-1GB 크기

### 최적화 팁

1. **병렬성 증가:** 더 많은 파티션 및 스트림 스레드 추가
2. **JVM 튜닝:** 더 나은 일시 정지 시간을 위해 G1GC 사용
3. **버퍼링 설정:** 워크로드에 맞게 캐시 크기 조정
4. **지연 모니터링:** 컨슈머 지연 및 처리 지연 확인

```bash
# 성능 튜닝 예제
export JAVA_OPTS="-Xmx4g -Xms2g -XX:+UseG1GC -XX:MaxGCPauseMillis=100"
java $JAVA_OPTS -cp target/KafkaStreamsTest-jar-with-dependencies.jar com.ainory.kafka.streams.DslTestMain
```

## 🛠 개발

### 소스에서 빌드

```bash
# 클린 빌드
mvn clean compile

# 테스트 실행
mvn test

# 종속성과 함께 패키지
mvn package

# 문서 생성
mvn javadoc:javadoc
```

### 개발 환경에서 실행

```bash
# Maven으로 실행
mvn exec:java -Dexec.mainClass="com.ainory.kafka.streams.DslTestMain"

# 디버그 모드
mvn exec:java -Dexec.mainClass="com.ainory.kafka.streams.DslTestMain" -Dexec.args="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"
```

### 코드 스타일

- **Java 8+** 기능 및 패턴
- 적절한 곳에 **함수형 프로그래밍**
- 점진적 저하를 통한 **포괄적인 오류 처리**
- 모니터링 및 디버깅을 위한 **상세한 로깅**

## 🔧 문제 해결

### 일반적인 문제

#### 토픽을 찾을 수 없음
```bash
# 누락된 토픽 생성
bin/kafka-topics.sh --create --topic COLLECTD_DATA --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

#### 직렬화 오류
```java
// serde 설정 확인
props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
```

#### 메모리 문제
```bash
# 힙 크기 증가
export JAVA_OPTS="-Xmx4g -Xms2g"
```

### 디버깅

1. 상세한 처리 정보를 위한 **디버그 로깅 활성화**
2. Kafka 도구를 사용한 **컨슈머 지연 모니터링**
3. 콘솔 컨슈머로 **데이터 형식 확인**
4. **상태 저장소 크기** 및 내용 확인
5. Kafka 브로커에 대한 **네트워크 연결 검증**

## 🤝 기여하기

기여를 환영합니다! 기여 가이드라인을 참조해 주세요:

1. 저장소를 **포크**합니다
2. 기능 브랜치를 **생성**합니다
3. 테스트와 함께 **변경사항을 작성**합니다
4. **풀 리퀘스트를 제출**합니다

### 개발 설정

```bash
git clone <your-fork>
cd KafkaStreamsTest
mvn clean compile
# 변경사항 작성
mvn test
```

## 📄 라이선스

이 프로젝트는 [LICENSE](LICENSE) 파일에 명시된 조건에 따라 라이선스가 부여됩니다.

## 🆘 지원

질문 및 지원을 위해:

- **문서:** [docs/](docs/) 디렉토리 확인
- **이슈:** GitHub 이슈 열기
- **API 참조:** [API_DOCUMENTATION.md](docs/API_DOCUMENTATION.md) 참조
- **설정 도움:** [USAGE_GUIDE.md](docs/USAGE_GUIDE.md) 참조

---

## 📝 버전 히스토리

### 현재 버전: 1.0-SNAPSHOT

**기술 스택:**
- Java 8+
- Apache Kafka Streams 1.0.0
- Apache Kafka Clients 1.0.0
- Apache Commons Lang3 3.5
- Apache Commons Math3 3.6.1
- Jackson (JSON 처리용)

### 구현된 주요 기능:
- ✅ DSL 기반 스트림 처리
- ✅ Processor API 구현
- ✅ 윈도우 집계 (텀블링 윈도우)
- ✅ 커스텀 직렬화/역직렬화
- ✅ Collectd 데이터 형식 지원
- ✅ 통계 계산 (최솟값/최댓값/평균/합계)
- ✅ 상태 저장소 관리
- ✅ 오류 처리 및 복구
- ✅ 포괄적인 문서

---

**대규모 실시간 모니터링 데이터 처리 준비 완료!** 🚀

