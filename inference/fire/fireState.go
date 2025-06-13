package fire

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
	stateGroup     goka.Group = "fire-state"
	FireStateTable goka.Table = goka.GroupTable(stateGroup)
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
	// fmt.Println(fireMsg)
	// {AST_DS7.1_TEST {f0304aa9-a832-4701-9c51-db4cfe3107ef 2025-06-13 00:22:27.062 +0000 UTC 2025-06-13 00:22:26.452299889 +0000 UTC 1920 1080} [{0 false {1052 374 17 27} 0.90625}]}

	// 현재 상태 가져오기
	var state FireState
	if v := ctx.Value(); v != nil {
		state = v.(FireState)
		// state = FireState{
		// 	SensorID:           fireMsg.SensorID,
		// 	LastFireDetection:  fireMsg.MessageMetadata.SourceTimestamp,
		// 	FireDetectionCount: 0,
		// 	RecentFireMessages: make([]FireMessageInfo, 0),
		// }
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

	// // 2. 5분이 지난 메시지 제거 (이진 탐색 사용)
	fiveMinutesAgo := time.Now().Add(-5 * time.Minute)
	cutoffIndex := findCutoffIndex(state.RecentFireMessages, fiveMinutesAgo)
	state.RecentFireMessages = state.RecentFireMessages[cutoffIndex:]

	// 상태 저장
	fmt.Println("time", fiveMinutesAgo)
	fmt.Println("cutoffIndex", cutoffIndex)
	fmt.Println("state", state)
	ctx.SetValue(state)
}

func PrepareTopics(brokers []string, config *sarama.Config) {
	topicinit.EnsureStreamExists(string(messaging.FireDetectionStream), brokers, config)
}

func RunState(ctx context.Context, brokers []string) func() error {
	return func() error {
		g := goka.DefineGroup(stateGroup,
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
