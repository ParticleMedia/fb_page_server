package server

import (
	"encoding/json"
	"time"

	"github.com/ParticleMedia/fb_page_tcat/common"
	"github.com/golang/glog"
)

type Handler struct {
	newsConsumer *KafkaConsumer
}

func NewHandler(conf *common.Config) (*Handler, error) {
	var newsConsumer *KafkaConsumer = nil
	var err error = nil
	newsConsumer, err = NewKafkaConsumer("facebook", &conf.Kafka)
	if err != nil {
		return nil, err
	}

	return &Handler{
		newsConsumer: newsConsumer,
	}, nil
}

func (h *Handler) Start() {
	if h.newsConsumer != nil {
		h.newsConsumer.Consume(handlerWrapper("facebook", newsMessageHandler))
	}
}

func (h *Handler) Join() {
	if h.newsConsumer != nil {
		h.newsConsumer.Join()
	}
}

func (h *Handler) Stop() {
	if h.newsConsumer != nil {
		h.newsConsumer.Close()
	}
	h.Join()
}

func handlerWrapper(name string, handler func([]byte, *common.LogInfo) error) MessageHandler {
	return func(data []byte) error {
		start := time.Now()
		logInfo := common.NewLogInfo()
		err := handler(data, logInfo)
		var hasError int = 0
		var msg string = "OK"
		if err != nil {
			hasError = 1
			msg = err.Error()
		}
		glog.Infof("%s kafka=%s cost=%d error=%d msg=%s", logInfo.ToString(), name, time.Since(start).Nanoseconds()/1000, hasError, msg)
		return err
	}
}

func newsMessageHandler(data []byte, l *common.LogInfo) error {
	glog.Infof(string(data))

	var profile common.FBProfile
	err := json.Unmarshal(data, &profile)
	if err != nil {
		return err
	}

	//var page common.FBPage

	//// filter
	//if FilterNews(page, l) {
	//	return nil
	//}
	//glog.Infof("%+v", profile)
	//
	//// emit
	//err = emitor.EmitNews(doc, l)
	//if err != nil {
	//	return err
	//}
	return nil
}
