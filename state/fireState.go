package state

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/lovoo/goka"
	messaging "github.com/oxymoron0/simple-goka-streaming-pipeline"
	"github.com/oxymoron0/simple-goka-streaming-pipeline/topicinit"
)

var (
	group          goka.Group = "fireState"
	FireStateTable goka.Table = goka.GroupTable(group)
)

// 이진 탐색으로 5분 이내의 첫 번째 메시지 인덱스 찾기
func findCutoffIndex(messages []FireMessageInfo, cutoffTime time.Time) int {
	left, right := 0, len(messages)-1

	// 모든 메시지가 5분 이전이면
	if len(messages) == 0 || messages[right].Timestamp.Before(cutoffTime) {
		return len(messages)
	}

	// 모든 메시지가 5분 이내면
	if messages[left].Timestamp.After(cutoffTime) {
		return 0
	}

	// 이진 탐색
	for left < right {
		mid := (left + right) / 2

		if messages[mid].Timestamp.After(cutoffTime) {
			right = mid
		} else {
			left = mid + 1
		}
	}

	return left
}

func collect(ctx goka.Context, msg interface{}) {
	// 현재 메시지를 FireMessage로 타입 단언
	fireMsg, ok := msg.(messaging.FireMessage)
	if !ok {
		return
	}
	fmt.Println(fireMsg)

	// 현재 상태 가져오기
	var state FireState
	if v := ctx.Value(); v != nil {
		state = v.(FireState)
	} else {
		// 최초 상태 초기화
		state = FireState{
			SensorID:           fireMsg.SensorID,
			LastFireDetection:  fireMsg.MessageMetadata.SourceTimestamp,
			FireDetectionCount: 0,
			RecentFireMessages: make([]FireMessageInfo, 0),
		}
	}

	// 상태 업데이트
	state.LastFireDetection = fireMsg.MessageMetadata.SourceTimestamp
	state.FireDetectionCount++

	// RecentFireMessages 업데이트
	// 1. 새로운 메시지 추가
	for _, fireInfo := range fireMsg.Fire {
		newMsg := FireMessageInfo{
			Timestamp:   fireMsg.MessageMetadata.SourceTimestamp,
			FireMessage: fireInfo,
		}
		state.RecentFireMessages = append(state.RecentFireMessages, newMsg)
	}

	// 2. 5분이 지난 메시지 제거 (이진 탐색 사용)
	fiveMinutesAgo := time.Now().Add(-5 * time.Minute)
	cutoffIndex := findCutoffIndex(state.RecentFireMessages, fiveMinutesAgo)
	state.RecentFireMessages = state.RecentFireMessages[cutoffIndex:]

	// 상태 저장
	fmt.Println(state)
	ctx.SetValue(state)
}

func PrepareTopics(brokers []string, config *sarama.Config) {
	topicinit.EnsureStreamExists(string(messaging.FireDetectionStream), brokers, config)
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
