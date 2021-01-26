package server

import (
	"encoding/json"
	"github.com/ParticleMedia/fb_page_tcat/common"
	"github.com/ParticleMedia/fb_page_tcat/remote"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/golang/glog"
	"os"
	"os/signal"
	"sync"
	"time"
)

var clusterConf *cluster.Config

func InitClusterConfig(conf *common.KafkaConfig) {
	clusterConf = cluster.NewConfig()
	clusterConf.Consumer.Return.Errors = true
	clusterConf.Group.Return.Notifications = true
	clusterConf.Consumer.Offsets.Initial = conf.OffsetInitial
	commitOffset := (conf.CommitInterval > 0)
	clusterConf.Consumer.Offsets.AutoCommit.Enable = commitOffset
	if commitOffset {
		clusterConf.Consumer.Offsets.AutoCommit.Interval = time.Duration(conf.CommitInterval) * time.Second
		clusterConf.Consumer.Offsets.CommitInterval = clusterConf.Consumer.Offsets.AutoCommit.Interval
	} else {
		glog.Infof("do not commit kafka")
	}
}

func process(data *[]byte, conf *common.Config) error {
	var profile common.FBProfile
	parseErr := json.Unmarshal(*data, &profile)
	if parseErr != nil {
		glog.Warningf("parse FBProfile with error: %+v, data: %s", parseErr, *data)
		return parseErr
	}

	tcatErr := remote.ProcessTextCateGory(&profile, conf)
	if tcatErr != nil {
		glog.Warningf("process text_category with error: %+v, FBProfile: %+v", tcatErr, profile)
		return tcatErr
	}

	//chnErr := remote.ProcessChannel(&profile, conf)
	//if chnErr != nil {
	//	glog.Warningf("process channel with error: %+v, FBProfile: %+v", chnErr, profile)
	//	return chnErr
	//}

	return nil
}

func Consume()  {
	defer common.Wg.Done()

	// init consumer
	kafkaConf := common.FBConfig.KafkaConf
	consumer, createErr := cluster.NewConsumer(kafkaConf.Addr, kafkaConf.GroupId, kafkaConf.Topic, clusterConf)
	if createErr != nil {
		glog.Warningf("create kafka consumer with error: %+v", createErr)
		return
	}
	defer consumer.Close()

	// trap SIGINT to trigger a shutdown
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// consume errors
	go func() {
		for consumeErr := range consumer.Errors() {
			glog.Warningf("consumer with error: %+v", consumeErr)
		}
	}()

	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			glog.Infof("kafka rebalanced: %+v", ntf)
		}
	}()

	workerCnt := common.FBConfig.WorkerCnt
	var consumeWg = &sync.WaitGroup{}
	consumeWg.Add(workerCnt)
	chWorker := make(chan *[]byte, workerCnt)
	for i := 0; i < workerCnt; i++ {
		go func(input chan *[]byte, wg *sync.WaitGroup) {
			defer wg.Done()
			for data := range input {
				process(data, common.FBConfig)
			}
		} (chWorker, consumeWg)
	}

	// consume messages, watch signals
Loop:
	for {
		select {
		case msg, ok := <-consumer.Messages():
			if ok {
				chWorker <- &msg.Value
				// mark message as processed
				consumer.MarkOffset(msg, "")
			}
		case <-signals:
			break Loop
		}
	}

	close(chWorker)
	consumeWg.Wait()
}
