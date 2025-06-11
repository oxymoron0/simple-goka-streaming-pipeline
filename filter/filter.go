package filter

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

func filter(ctx goka.Context, msg interface{}) {
	originMsg, ok := msg.(messaging.OriginMessage)
	if !ok {
		return
	}

	if shouldDrop(originMsg.InferenceResult.DetectionInfo) {
		return
	}
	ctx.Emit(messaging.ValidInferenceStream, originMsg.InferenceResult.ID, originMsg)
}

func PrepareTopics(brokers []string, config *sarama.Config) {
	topicinit.EnsureStreamExists(string(messaging.OriginTopic), brokers, config)
	topicinit.EnsureStreamExists(string(messaging.ValidInferenceStream), brokers, config)
}

func Run(ctx context.Context, brokers []string) func() error {
	return func() error {
		g := goka.DefineGroup(group,
			goka.Input(messaging.OriginTopic, new(messaging.OriginMessageCodec), filter),
			goka.Output(messaging.ValidInferenceStream, new(messaging.OriginMessageCodec)),
		)
		p, err := goka.NewProcessor(brokers, g)
		if err != nil {
			return err
		}

		return p.Run(ctx)
	}
}
