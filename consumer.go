package main

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/klauspost/shutdown2"
)

// SimpleConsumer is a message consumer type
type SimpleConsumer struct {
	Topic         string
	Group         string
	Partition     int32
	Offset        int64
	MaxProcessors int
	Handler       func(*sarama.ConsumerMessage)

	consumer sarama.PartitionConsumer
	listener *os.File

	messages chan message
	wg       sync.WaitGroup

	config Configuration
}

type message struct {
	// maybe consider adding some context implementation later on for logging purposes
	// ctx context.Context
	e *sarama.ConsumerMessage
}

// NewSimpleConsumer returns SimpleConsumer
func NewSimpleConsumer(config Configuration) *SimpleConsumer {
	consumer := SimpleConsumer{config: config}

	consumer.MaxProcessors = config.MaxProcessors
	consumer.Offset = config.Offset
	consumer.Group = config.GroupID
	consumer.messages = make(chan message)
	consumer.Handler = func(e *sarama.ConsumerMessage) {
		consumer.messages <- message{e: e}
	}
	return &consumer
}

// Startup sets specific types and handlers
func (c *SimpleConsumer) Startup(ctx context.Context) {
	if c.MaxProcessors == 0 {
		Log.L.Infof("%s was started with no processors. Assuming 1.", c)
		c.MaxProcessors = 1
	}

	Log.L.Infof("Starting %d %s processor(s)", c.MaxProcessors, c)
	c.wg.Add(c.MaxProcessors)
	for i := 0; i < c.MaxProcessors; i++ {
		go c.processor(ctx)
	}
}

// LoadPuller sets up the message puller from config
func (c *SimpleConsumer) LoadPuller(ctx context.Context) {
	Log.L.Info("Initializing Kafka consumer")
	// maybe consider using custom logger for sarama Logger ?
	//sarama.Logger = logger.Log.Logger
	client := sarama.NewConfig()
	client.ClientID = c.config.ClientID

	consumer, err := sarama.NewConsumer(c.config.ProducerURL, client)
	if err != nil {
		panic(err)
	}
	Log.L.Infof("Kafka client %q running", c.config.ClientID)

	partitionConsumer, err := consumer.ConsumePartition(c.config.KafkaTopics, 0, c.config.Offset)
	if err != nil {
		panic(err)
	}
	c.consumer = partitionConsumer
}

// LoadPusher sets up message pusher from config
func (c *SimpleConsumer) LoadPusher(ctx context.Context) {
	var err error
	var f *os.File

	if _, err = os.Stat(c.config.FileName); os.IsNotExist(err) {
		f, err = os.Create(c.config.FileName)
	} else {
		f, err = os.OpenFile(c.config.FileName, os.O_APPEND|os.O_WRONLY, 0600)
	}
	if err != nil {
		panic(err)
	}
	c.listener = f
}

// Run executes the consumer loop. Hands off events to Handle(event).
func (c *SimpleConsumer) Run(ctx context.Context) (err error) {

	if c.Handler == nil {
		return fmt.Errorf("Handler not configured for consumer %s", c)
	}

	// should pass context here - consider its usage when introduce context to service
	closeChan := shutdown.First(c)
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v\n%s", r, debug.Stack())
		}
		closeChan.Cancel()
	}()

	defer c.listener.Close()
	defer c.consumer.Close()

	if shutdown.Started() {
		return nil
	}

	defer Log.L.Info("Consumer stopped")

	//logfile if that gonna be stable endpoint (doubting), don't forget to implement big file Event and treatment

	for {
		// No need to process more if shutdown has started.
		if shutdown.Started() {
			close(<-closeChan)
			return nil
		}
		select {
		case msg := <-c.consumer.Messages():
			c.Handler(msg)
		case err := <-c.consumer.Errors():
			Log.L.Infof("Consumer error received: %s", err.Err)
		case v := <-closeChan:
			close(v)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// String returns the consumer group name.
func (c *SimpleConsumer) String() string {
	return c.Group
}

func (c *SimpleConsumer) processor(ctx context.Context) {
	defer Log.L.Infof("%s processor terminated", c)
	defer c.wg.Done()

	Log.L.Infof("%s Processor started", c)
	for e := range c.messages {
		func() {
			defer func() {
				if r := recover(); r != nil {
					Log.L.Infof("%s message processing paniced. Stack trace: %s", e.e.Value, string(debug.Stack()))
				}
			}()
			c.process(ctx, e.e)
		}()
	}
}

func (c *SimpleConsumer) process(ctx context.Context, e *sarama.ConsumerMessage) {
	text := string(e.Value) + "\n"
	_, err := c.listener.WriteString(text)
	if err != nil {
		Log.L.Panic(err)
		panic(err)
	}
}
