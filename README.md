# Goka Pipeline

Goka를 사용한 실시간 스트리밍 파이프라인 프로젝트입니다. 이 프로젝트는 센서 데이터를 처리하고 분석하는 스트리밍 파이프라인을 구현합니다.

## 프로젝트 구조

```
goka-pipeline/
├── cmd/                    # 실행 파일 디렉토리
│   ├── processor/         # 프로세서 실행 파일
│   └── service/          # 서비스 실행 파일
├── config/                # 설정 파일 디렉토리
├── inference/            # 추론 관련 코드
├── topicinit/           # 토픽 초기화 관련 코드
├── validFilter/         # 유효성 검사 필터
├── sensorMessage.go     # 센서 메시지 정의
├── go.mod              # Go 모듈 정의
└── go.sum              # Go 모듈 의존성 체크섬
```

## 주요 기능

- 센서 데이터 스트리밍 처리
- 화재 감지 이벤트 처리
- 메시지 유효성 검사
- 실시간 상태 관리

## 의존성

- github.com/lovoo/goka v1.1.14
- github.com/IBM/sarama v1.45.1
- github.com/gorilla/mux v1.8.1
- gopkg.in/yaml.v2 v2.4.0

## 실행 방법

프로세서는 다음과 같은 옵션으로 실행할 수 있습니다:

```bash
# 필터 프로세서 실행
go run cmd/processor/main.go -filter

# 화재 전송 프로세서 실행
go run cmd/processor/main.go -fire

# 화재 상태 프로세서 실행
go run cmd/processor/main.go -fireState
```

## 웹 서비스

### 서비스 구성
- `cmd/service/main.go`: 웹 서비스 메인 실행 파일
- Kafka 브로커와 연동하여 실시간 데이터 처리
- 화재 감지 스트림(`FireDetectionStream`) 모니터링

### 서비스 실행
```bash
# 웹 서비스 실행
go run cmd/service/main.go
```

### 주요 기능
- 실시간 화재 감지 이벤트 모니터링
- Kafka 스트림 데이터 처리
- RESTful API 엔드포인트 제공

## 메시지 구조

### Origin Message
- MessageID: 메시지 식별자
- MessageTimestamp: 메시지 생성 시간
- SourceTimestamp: 소스 타임스탬프
- SourceWidth/Height: 소스 이미지 크기
- InferenceType: 추론 타입
- InferenceResult: 추론 결과

### Fire Message
- SensorID: 센서 식별자
- MessageMetadata: 메시지 메타데이터
- Fire: 화재 감지 정보 배열
