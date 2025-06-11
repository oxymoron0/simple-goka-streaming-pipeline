package state

import (
	"context"
	"encoding/json"

	"github.com/IBM/sarama"
	"github.com/lovoo/goka"
	messaging "github.com/oxymoron0/simple-goka-streaming-pipeline"
	"github.com/oxymoron0/simple-goka-streaming-pipeline/topicinit"
)

var (
	group goka.Group = "fireState"
	Table goka.Table = goka.GroupTable(group)
)

// 센서 ID 별 State
// 최근 10초 내 화재 알림 여부
// 10초 간 화재 관련 데이터
func collect(ctx goka.Context, msg interface{}) {
	// Context에서 메세지 읽고 ml에 업데이트
	var ml []messaging.FireMessageCodec
	if v := ctx.Value(); v != nil {
		ml = v.([]messaging.FireMessageCodec)
	}

	// msg를 Message로 타입 변환(타입 단언)
	m := msg.(*messaging.FireMessageCodec)

	// 메세지 갱신 및 ml 추가
	ml = append(ml, *m)

	// 갱신된 ml을 Context에 덮어쓰기
	ctx.SetValue(ml)
}

func PrepareTopics(brokers []string, config *sarama.Config) {
	topicinit.EnsureStreamExists(string(messaging.FireDetectionStream), brokers, config)
}

// 실시간 모니터링: 현재 화재 상태 | N 분 내 화재 알림 여부
// 이력, 통계: 누적 화재 감지 메세지 수 | 최근 화재 감지 시각
// 센서 상태 => How to collect?
type FireState struct {
	SensorID      string
	FireDetection bool
	FireData      []messaging.FireMessageCodec
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

func Run(ctx context.Context, brokers []string) func() error {
	return func() error {
		g := goka.DefineGroup(group,
			goka.Input(messaging.FireDetectionStream, new(messaging.FireMessageCodec), collect),
			goka.Persist(new(FireStateCodec)),
		)
		// Processor 생성
		p, err := goka.NewProcessor(brokers, g)
		if err != nil {
			return err
		}
		return p.Run(ctx)
	}
}
