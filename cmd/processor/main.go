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
	"github.com/oxymoron0/simple-goka-streaming-pipeline/transporter"
	"github.com/oxymoron0/simple-goka-streaming-pipeline/validFilter"
	"golang.org/x/sync/errgroup"
)

var (
	cfg                *sarama.Config
	brokers            []string
	runFilter          = flag.Bool("filter", false, "run filter processor")
	runFireTransporter = flag.Bool("fire", false, "run fire transporter processor")
	runBlocker         = flag.Bool("blocker", false, "run blocker processor")
	runDetector        = flag.Bool("detector", false, "run detector processor")
	runTranslator      = flag.Bool("translator", false, "run translator processor")
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
		transporter.PrepareFireTopics(brokers, cfg)
	}

	if *runFilter {
		log.Println("starting filter")
		grp.Go(validFilter.Run(ctx, brokers))
	}
	if *runFireTransporter {
		log.Println("starting fire transporter")
		grp.Go(transporter.RunFireTransporter(ctx, brokers))
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
