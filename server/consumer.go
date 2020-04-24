package server

import (
	"fmt"
	"sync"
	"time"

	"github.com/ParticleMedia/nonlocal-indexer/common"
	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/golang/glog"
	"github.com/rcrowley/go-metrics"
)

type KafkaConsumer struct {
	name         string
	commitOffset bool
	worker       uint32
	consumer     *cluster.Consumer
	swg          sync.WaitGroup
}

func NewKafkaConsumer(name string, conf *common.KafkaConfig) (*KafkaConsumer, error) {
	config := cluster.NewConfig()
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest //初始从最新的offset开始
	var commitOffset bool = (conf.CommitInterval > 0)
	config.Consumer.Offsets.AutoCommit.Enable = commitOffset
	if commitOffset {
		// 提交commit
		config.Consumer.Offsets.AutoCommit.Interval = time.Duration(conf.CommitInterval) * time.Second
	} else {
		// 不提交commit
		glog.Infof("do not commit kafka")
	}

	c, err := cluster.NewConsumer(conf.Addrs, conf.GroupId, conf.Topics, config)
	if err != nil {
		glog.Errorf("Failed open consumer: %v", err)
		return nil, err
	}

	return &KafkaConsumer{
		name:         name,
		commitOffset: commitOffset,
		worker:       conf.Consumer,
		consumer:     c,
		swg:          sync.WaitGroup{},
	}, nil
}

func (c *KafkaConsumer) Close() error {
	if c.consumer != nil {
		err := c.consumer.Close()
		c.swg.Wait()
		c.consumer = nil
		glog.Infof("kafka consumer exited.")
		return err
	}
	return nil
}

type MessageHandler func([]byte) error

// consumer 消费者
func (c *KafkaConsumer) Consume(handler MessageHandler) {
	// workers
	for i := uint32(0); i < c.worker; i++ {
		c.swg.Add(1)
		go func() {
			defer c.swg.Done()
			for {
				select {
				case msg, ok := <-c.consumer.Messages():
					if ok {
						c.handleMessage(msg, handler)
						if c.commitOffset {
							c.consumer.MarkOffset(msg, "")
						}
					} else {
						return
					}
				case err := <-c.consumer.Errors():
					if err != nil {
						glog.Errorln(err)
					}
				case n := <-c.consumer.Notifications():
					if n != nil {
						glog.Infof("notification: %+v", n)
					}
				}
			}
		}()
	}
}

func (c *KafkaConsumer) Join() {
	c.swg.Wait()
}

func (c *KafkaConsumer) handleMessage(msg *sarama.ConsumerMessage, handler MessageHandler) {
	defer func(start time.Time) {
		metrics.GetOrRegisterTimer(fmt.Sprintf("consume.%s.latency", c.name), nil).Update(time.Since(start))
	}(time.Now())

	metrics.GetOrRegisterMeter(fmt.Sprintf("consume.%s.qps", c.name), nil).Mark(1)
	err := handler(msg.Value)
	if err == nil {
		metrics.GetOrRegisterHistogram(fmt.Sprintf("consume.%s.error", c.name), nil, metrics.NewExpDecaySample(1024, 0.015)).Update(0)
	} else {
		glog.Warningf("handle message error: %+v", err)
		metrics.GetOrRegisterHistogram(fmt.Sprintf("consume.%s.error", c.name), nil, metrics.NewExpDecaySample(1024, 0.015)).Update(100)
	}
}
