package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
	"github.com/oxymoron0/simple-goka-streaming-pipeline/config"
	"github.com/oxymoron0/simple-goka-streaming-pipeline/inference/fire"
	"github.com/oxymoron0/simple-goka-streaming-pipeline/validFilter"
	"golang.org/x/sync/errgroup"
)

var (
	cfg                *sarama.Config
	brokers            []string
	runFilter          = flag.Bool("filter", false, "run filter processor")
	runFireTransporter = flag.Bool("fire", false, "run fire transporter processor")
	runFireState       = flag.Bool("fireState", false, "run fire state processor")
)

func main() {
	flag.Parse()
	ctx, cancel := context.WithCancel(context.Background())
	grp, ctx := errgroup.WithContext(ctx)

	brokers, cfg, err := config.InitConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}
	if *runFilter {
		validFilter.PrepareTopics(brokers, cfg)
	}
	if *runFireTransporter {
		fire.PrepareFireTopics(brokers, cfg)
	}
	if *runFireState {
		fire.PrepareTopics(brokers, cfg)
	}
	if *runFilter {
		log.Println("starting filter")
		grp.Go(validFilter.Run(ctx, brokers))
	}
	if *runFireTransporter {
		log.Println("starting fire transporter")
		grp.Go(fire.RunTransporter(ctx, brokers))
	}
	if *runFireState {
		log.Println("starting fire state")
		grp.Go(fire.RunState(ctx, brokers))
	}
	// Wait for SIGINT/SIGTERM
	waiter := make(chan os.Signal, 1)
	signal.Notify(waiter, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-waiter:
	case <-ctx.Done():
	}
	cancel()
	if err := grp.Wait(); err != nil {
		log.Println(err)
	}
	log.Println("done")
}
