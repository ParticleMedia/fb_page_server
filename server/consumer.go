package server

import (
	"fmt"
	"golang.org/x/net/context"
	"log"
	"sync"
	"time"

	"github.com/ParticleMedia/nonlocal-indexer/common"
	"github.com/Shopify/sarama"
	"github.com/golang/glog"
	"github.com/rcrowley/go-metrics"
)

type KafkaConsumer struct {
	name         string
	conf         *common.KafkaConfig
	commitOffset bool
	worker       uint32
	client       sarama.ConsumerGroup
	swg          sync.WaitGroup
	cancel       context.CancelFunc
}

func NewKafkaConsumer(name string, conf *common.KafkaConfig) (*KafkaConsumer, error) {
	version, err := sarama.ParseKafkaVersion(conf.Version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	config := sarama.NewConfig()
	config.Version = version
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest //初始从最新的offset开始
	// var commitOffset bool = (conf.CommitInterval > 0)
	var commitOffset bool = false
	config.Consumer.Offsets.AutoCommit.Enable = commitOffset
	if commitOffset {
		// 提交commit
		config.Consumer.Offsets.AutoCommit.Interval = time.Duration(conf.CommitInterval) * time.Second
	} else {
		// 不提交commit
		glog.Infof("do not commit kafka")
	}

	c, err := sarama.NewConsumerGroup(conf.Addrs, conf.GroupId, config)
	if err != nil {
		glog.Errorf("Failed open consumer: %v", err)
		return nil, err
	}

	return &KafkaConsumer{
		name:         name,
		conf:         conf,
		commitOffset: commitOffset,
		worker:       conf.Consumer,
		client:       c,
		swg:          sync.WaitGroup{},
	}, nil
}

func (c *KafkaConsumer) Close() error {
	if c.cancel != nil {
		c.cancel()
	}
	if c.client != nil {
		err := c.client.Close()
		c.swg.Wait()
		c.client = nil
		glog.Infof("kafka consumer exited.")
		return err
	}
	return nil
}

type MessageHandler func([]byte) error

// consumer 消费者
func (c *KafkaConsumer) Consume(f MessageHandler) {
	handler := &KafkaHandler{
		name: c.name,
		commitOffset: c.commitOffset,
		process: f,
		worker: c.worker,
		ready: make(chan bool),
		swg: sync.WaitGroup{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.swg.Add(1)
	go func() {
		defer c.swg.Done()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := c.client.Consume(ctx, c.conf.Topics, handler); err != nil {
				glog.Fatalf("Consume Error: %+v", err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				glog.Infof("consumer stopped")
				return
			}
			handler.ready = make(chan bool)
		}
	}()

	// log client errors
	c.swg.Add(1)
	go func() {
		defer c.swg.Done()
		for err := range c.client.Errors() {
			glog.Warningf("Error from client: %+v", err)
		}
	} ()

	// wait until handler.Setup has been called
	<-handler.ready
}

func (c *KafkaConsumer) Join() {
	c.swg.Wait()
}


// KafkaHandler represents a Sarama consumer group handler
type KafkaHandler struct {
	name string
	commitOffset bool
	process MessageHandler
	worker uint32
	ready chan bool
	swg sync.WaitGroup
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *KafkaHandler) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(h.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (h *KafkaHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (h *KafkaHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29

	// workers
	h.swg.Add(int(h.worker))
	for i := uint32(0); i < h.worker; i++ {
		go func() {
			defer h.swg.Done()
			for msg := range claim.Messages() {
				h.handleMessage(msg)
				if h.commitOffset {
					session.MarkMessage(msg, "")
				}
			}
		}()
	}
	h.swg.Wait()
	return nil
}

func (h *KafkaHandler) handleMessage(msg *sarama.ConsumerMessage) {
	defer func(start time.Time) {
		metrics.GetOrRegisterTimer(fmt.Sprintf("consume.%s.latency", h.name), nil).Update(time.Since(start))
	}(time.Now())

	metrics.GetOrRegisterMeter(fmt.Sprintf("consume.%s.qps", h.name), nil).Mark(1)
	err := h.process(msg.Value)
	if err == nil {
		metrics.GetOrRegisterHistogram(fmt.Sprintf("consume.%s.error", h.name), nil, metrics.NewExpDecaySample(1024, 0.015)).Update(0)
	} else {
		glog.Warningf("handle message error: %+v", err)
		metrics.GetOrRegisterHistogram(fmt.Sprintf("consume.%s.error", h.name), nil, metrics.NewExpDecaySample(1024, 0.015)).Update(100)
	}
}
