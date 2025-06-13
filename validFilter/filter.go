package validFilter

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/lovoo/goka"
	messaging "github.com/oxymoron0/simple-goka-streaming-pipeline"
	"github.com/oxymoron0/simple-goka-streaming-pipeline/topicinit"
)

var (
	group goka.Group = "valid-inference-filter"
)

func shouldDrop(detectionInfo interface{}) bool {
	if detectionArray, ok := detectionInfo.([]interface{}); ok {
		return len(detectionArray) == 0
	}
	return false
}

func validFilter(ctx goka.Context, msg interface{}) {
	originMsg, ok := msg.(messaging.OriginMessage)
	if !ok {
		return
	}
	// fmt.Printf("originMsg: %+v\n", originMsg)
	// originMsg {6d869d24-9fea-4f9a-978b-c85cc93b9693 2025-06-11 19:55:23.028 +0000 UTC 2025-06-11 19:55:22.73788723 +0000 UTC 1920 1080 fire detection {AST_DS7.1_TEST [map[classID:0 confidence:0.99951171875 height:38 left:1009 objectID:0 top:216 width:28]]}}
	if shouldDrop(originMsg.InferenceResult.DetectionInfo) {
		return
	}
	ctx.Emit(messaging.ValidInferenceStream, originMsg.InferenceType, originMsg)
}

func PrepareTopics(brokers []string, config *sarama.Config) {
	topicinit.EnsureStreamExists(string(messaging.OriginTopic), brokers, config)
	topicinit.EnsureStreamExists(string(messaging.ValidInferenceStream), brokers, config)
}

func Run(ctx context.Context, brokers []string) func() error {
	return func() error {
		g := goka.DefineGroup(group,
			goka.Input(messaging.OriginTopic, new(messaging.OriginMessageCodec), validFilter),
			goka.Output(messaging.ValidInferenceStream, new(messaging.OriginMessageCodec)),
		)
		p, err := goka.NewProcessor(brokers, g)
		if err != nil {
			return err
		}

		return p.Run(ctx)
	}
}
