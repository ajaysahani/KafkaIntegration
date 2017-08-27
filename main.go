package main

import (
	"context"
	"time"
)

// Runner runs through services operations
type Runner interface {
	Startup(context.Context)
	LoadPuller(context.Context)
	LoadPusher(context.Context)
	Run(context.Context) error
}

func main() {
	ctx := context.Background()

	config, err := LoadConfiguration()
	if err != nil {
		panic(err)
	}

	err = LoadLogger(config)
	if err != nil {
		panic(err)
	}

	runService(ctx, NewSimpleConsumer(config))
}

func runService(ctx context.Context, s Runner) {
	s.Startup(ctx)
	s.LoadPuller(ctx)
	s.LoadPusher(ctx)
	err := s.Run(ctx)
	if err != nil {
		time.Sleep(time.Second)
	}
}
