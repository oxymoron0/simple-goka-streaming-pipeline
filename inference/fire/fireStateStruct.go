package fire

import (
	"encoding/json"
	"time"

	messaging "github.com/oxymoron0/simple-goka-streaming-pipeline"
)

// 실시간 모니터링: 현재 화재 상태 | N 분 내 화재 알림 여부
// 이력, 통계: 누적 화재 감지 메세지 수 | 최근 화재 감지 시각
// 추론 상태 => How to collect?
type FireState struct {
	SensorID           string
	LastFireDetection  time.Time         // 최근 화재 감지 시각
	FireDetectionCount int               // 센서 별 누적 화재 감지 메시지 수
	RecentFireMessages []FireMessageInfo // 최근 10분 내 화재 메시지
}

type FireMessageInfo struct {
	Timestamp   time.Time
	FireMessage messaging.FireInfo
}

type FireStateCodec struct{}

func (c *FireStateCodec) Encode(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}

func (c *FireStateCodec) Decode(data []byte) (interface{}, error) {
	var fireState FireState
	err := json.Unmarshal(data, &fireState)
	return fireState, err
}
