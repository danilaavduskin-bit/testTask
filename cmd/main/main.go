package main

import (
	"context"
	"github.com/danilaavduskin-bit/testTask/config"
	vd "github.com/danilaavduskin-bit/testTask/internal/violations-detector"
	"github.com/kvolis/tesgode/cat"
	"github.com/kvolis/tesgode/dog"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg := config.EnvInit()
	catClient := cat.New()
	dogClient := dog.New()

	if err := catClient.Connect(""); err != nil {
		log.Fatal("Cat connect:", err)
	}
	defer catClient.Close()

	if err := dogClient.Connect(""); err != nil {
		log.Fatal("Dog connect:", err)
	}
	defer dogClient.Close()

	detector := vd.NewDetector(catClient, dogClient, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := detector.Run(ctx); err != nil {
			log.Println("Service error:", err)
		}
		cancel()
	}()

	select {
	case <-sigChan:
		log.Printf("Shutting down...")
	case <-ctx.Done():
	}

	time.Sleep(200 * time.Millisecond)
	log.Println("Detector stopped")
}
