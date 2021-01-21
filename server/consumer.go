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
	var profile remote.FBProfile
	parseErr := json.Unmarshal(*data, &profile)
	if parseErr != nil {
		return parseErr
	}

	pageCnt := 0
	totalFirstCats := make(map[string]float64)
	totalSecondCats := make(map[string]float64)
	totalThirdCats := make(map[string]float64)
	for _, page := range profile.Pages {
		result, tcatErr := remote.DoTextCategory(&page, conf)
		if tcatErr != nil || result == nil {
			continue
		}
		if len(result.Tcats.FirstCats) == 0 && len(result.Tcats.SecondCats) == 0 && len(result.Tcats.ThirdCats) == 0 {
			continue
		}
		pageCnt += 1
		for cat, score := range result.Tcats.FirstCats {
			_, ok := totalFirstCats[cat]
			if ok {
				totalFirstCats[cat] += score
			} else {
				totalFirstCats[cat] = score
			}
		}
		for cat, score := range result.Tcats.SecondCats {
			_, ok := totalSecondCats[cat]
			if ok {
				totalSecondCats[cat] += score
			} else {
				totalSecondCats[cat] = score
			}
		}
		for cat, score := range result.Tcats.ThirdCats {
			_, ok := totalThirdCats[cat]
			if ok {
				totalThirdCats[cat] += score
			} else {
				totalThirdCats[cat] = score
			}
		}
	}

	if len(totalFirstCats) == 0 && len(totalSecondCats) == 0 && len(totalThirdCats) == 0 {
		return nil
	}

	for cat, score := range totalFirstCats {
		totalFirstCats[cat] = score / float64(pageCnt)
	}
	for cat, score := range totalSecondCats {
		totalSecondCats[cat] = score / float64(pageCnt)
	}
	for cat, score := range totalThirdCats {
		totalThirdCats[cat] = score / float64(pageCnt)
	}

	totalTextCategory := remote.TextCategory{
		FirstCats: totalFirstCats,
		SecondCats: totalSecondCats,
		ThirdCats: totalThirdCats,
	}

	totalTextCategoryBody := remote.TextCategoryBody{
		Tcats: totalTextCategory,
	}
	value, encodeErr := json.Marshal(totalTextCategoryBody)
	if encodeErr != nil {
		return encodeErr
	}

	glog.V(16).Infof("ready to write to ups, key: %d, value: %s", profile.Id, string(value))
	remote.WriteToUps(uint64(profile.Id), string(value), &conf.UpsConf)
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
				processErr := process(data, common.FBConfig)
				if processErr != nil {
					glog.Warningf("process with error: %+v, data: %s", processErr, *data)
				}
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
