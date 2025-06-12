package transporter

import (
	"context"
	"errors"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/lovoo/goka"
	messaging "github.com/oxymoron0/simple-goka-streaming-pipeline"
	"github.com/oxymoron0/simple-goka-streaming-pipeline/topicinit"
)

var (
	group goka.Group = "fire-transporter"
)

func shouldDrop(ctx goka.Context) bool {
	fmt.Println("ctx.Key()", ctx.Key())
	return ctx.Key() != "fire detection"
}

func convertToFireMessage(validMsg messaging.OriginMessage) (messaging.FireMessage, error) {
	// DetectionInfo를 []interface{}로 타입 단언
	detectionArray, ok := validMsg.InferenceResult.DetectionInfo.([]interface{})
	if !ok {
		return messaging.FireMessage{}, errors.New("detectionInfo is not an array")
	}

	// FireInfo 슬라이스 생성
	fireInfos := make([]messaging.FireInfo, 0, len(detectionArray))

	// 각 DetectionInfo를 FireInfo로 변환
	for _, detection := range detectionArray {
		if det, ok := detection.(map[string]interface{}); ok {
			fireInfo := messaging.FireInfo{
				ObjectID: int(det["objectID"].(float64)),
				Smoking:  int(det["classID"].(float64)) == 1, // classID가 1이면 연기(true), 0이면 불(false)
				BBox: messaging.BBox{
					X:      det["left"].(float64),
					Y:      det["top"].(float64),
					Width:  det["width"].(float64),
					Height: det["height"].(float64),
				},
				Confidence: det["confidence"].(float64),
			}
			fireInfos = append(fireInfos, fireInfo)
		}
	}

	return messaging.FireMessage{
		SensorID: validMsg.InferenceResult.ID,
		MessageMetadata: messaging.MessageMetadata{
			MessageID:        validMsg.MessageID,
			MessageTimestamp: validMsg.MessageTimestamp,
			SourceTimestamp:  validMsg.SourceTimestamp,
			SourceWidth:      validMsg.SourceWidth,
			SourceHeight:     validMsg.SourceHeight,
		},
		Fire: fireInfos,
	}, nil
}

func fireTransporter(ctx goka.Context, msg interface{}) {
	ValidMsg, ok := msg.(messaging.OriginMessage)
	if !ok {
		return
	}
	// fmt.Println("ValidMsg", ValidMsg)
	// ValidMsg: {6d869d24-9fea-4f9a-978b-c85cc93b9693 2025-06-11 19:55:23.028 +0000 UTC 2025-06-11 19:55:22.73788723 +0000 UTC 1920 1080 fire detection {AST_DS7.1_TEST [map[classID:0 confidence:0.99951171875 height:38 left:1009 objectID:0 top:216 width:28]]}}
	if shouldDrop(ctx) {
		return
	}
	fireMsg, err := convertToFireMessage(ValidMsg)
	if err != nil {
		fmt.Println("error converting to fire message:", err)
		return
	}
	// fmt.Printf("emit message to FireDetectionStream: \n%+v\n", fireMsg)
	// emit message to FireDetectionStream: {AST_DS7.1_TEST {b2231c8c-ae94-4c11-bf06-5d81015330a9 2025-06-11 19:58:19.028 +0000 UTC 2025-06-11 19:58:18.737523708 +0000 UTC 1920 1080} [{0 false {1009 217 28 37} 0.99853515625}]}
	ctx.Emit(messaging.FireDetectionStream, ValidMsg.InferenceResult.ID, fireMsg)
}

func PrepareFireTopics(brokers []string, config *sarama.Config) {
	topicinit.EnsureStreamExists(string(messaging.ValidInferenceStream), brokers, config)
	topicinit.EnsureStreamExists(string(messaging.FireDetectionStream), brokers, config)
}

func RunFireTransporter(ctx context.Context, brokers []string) func() error {
	return func() error {
		g := goka.DefineGroup(group,
			goka.Input(messaging.ValidInferenceStream, new(messaging.OriginMessageCodec), fireTransporter),
			goka.Output(messaging.FireDetectionStream, new(messaging.FireMessageCodec)),
		)
		p, err := goka.NewProcessor(brokers, g)
		if err != nil {
			return err
		}

		return p.Run(ctx)
	}
}
