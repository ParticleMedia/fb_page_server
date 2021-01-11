package server

import (
	"fmt"
	"golang.org/x/net/context"
	"sync"
	"time"

	"github.com/ParticleMedia/fb_page_tcat/common"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/golang/glog"
	"github.com/rcrowley/go-metrics"
)

type KafkaConsumer struct {
	name         string
	conf         *common.KafkaConfig
	commitOffset bool
	worker       uint32
	client       *cluster.Consumer
	swg          sync.WaitGroup
	cancel       context.CancelFunc
}

func NewKafkaConsumer(name string, conf *common.KafkaConfig) (*KafkaConsumer, error) {
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	var commitOffset bool = (conf.CommitInterval > 0)
	config.Consumer.Offsets.AutoCommit.Enable = commitOffset
	if commitOffset {
		config.Consumer.Offsets.AutoCommit.Interval = time.Duration(conf.CommitInterval) * time.Second
	} else {
		glog.Infof("do not commit kafka")
	}

	consumer, err := cluster.NewConsumer(conf.Addrs, conf.GroupId, conf.Topics, config)
	if err != nil {
		glog.Errorf("Failed open consumer: %v", err)
		return nil, err
	}

	return &KafkaConsumer{
		name:         name,
		conf:         conf,
		commitOffset: commitOffset,
		worker:       conf.Consumer,
		client:       consumer,
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
	c.swg.Add(1)
	go func() {
		defer c.swg.Done()
		for msg := range c.client.Messages() {
			f(msg.Value)
			c.client.MarkOffset(msg, "") // mark message as processed
		}
	}()

	c.swg.Add(1)
	go func() {
		defer c.swg.Done()
		for err := range c.client.Errors() {
			glog.Warningf("Error from client: %+v", err)
		}
	}()
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
	err := h.process(msg.Value)
	if err == nil {
		metrics.GetOrRegisterHistogram(fmt.Sprintf("consume.%s.error", h.name), nil, metrics.NewExpDecaySample(1024, 0.015)).Update(0)
	} else {
		glog.Warningf("handle message error: %+v", err)
		metrics.GetOrRegisterHistogram(fmt.Sprintf("consume.%s.error", h.name), nil, metrics.NewExpDecaySample(1024, 0.015)).Update(100)
	}
}
