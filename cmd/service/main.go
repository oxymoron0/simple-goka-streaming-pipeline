package main

import (
	"log"

	"github.com/IBM/sarama"
	messaging "github.com/oxymoron0/simple-goka-streaming-pipeline"
	"github.com/oxymoron0/simple-goka-streaming-pipeline/config"
	"github.com/oxymoron0/simple-goka-streaming-pipeline/inference/fire"
)

var (
	cfg     *sarama.Config
	brokers []string
)

func main() {
	brokers, cfg, err := config.InitConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	fire.Run(brokers, messaging.FireDetectionStream, cfg)
}
