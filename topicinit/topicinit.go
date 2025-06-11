package topicinit

import (
	"log"

	"github.com/IBM/sarama"
	"github.com/lovoo/goka"
)

// EnsureStreamExists is a convenience wrapper for TopicManager.EnsureStreamExists
func EnsureStreamExists(topic string, brokers []string, config *sarama.Config) {
	tm := createTopicManager(brokers, config)
	defer tm.Close()
	err := tm.EnsureStreamExists(topic, 8)
	if err != nil {
		log.Printf("Error creating kafka topic %s: %v", topic, err)
	}
}

// EnsureTableExists is a convenience wrapper for TopicManager.EnsureTableExists
func EnsureTableExists(topic string, brokers []string, config *sarama.Config) {
	tm := createTopicManager(brokers, config)
	defer tm.Close()
	err := tm.EnsureTableExists(topic, 8)
	if err != nil {
		log.Printf("Error creating kafka topic %s: %v", topic, err)
	}
}

func createTopicManager(brokers []string, config *sarama.Config) goka.TopicManager {
	tmc := goka.NewTopicManagerConfig()

	if config == nil {
		config = goka.DefaultConfig()
	}
	tm, err := goka.NewTopicManager(brokers, config, tmc)
	if err != nil {
		log.Fatalf("Error creating topic manager: %v", err)
	}
	return tm
}
