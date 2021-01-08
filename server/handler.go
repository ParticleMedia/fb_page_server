package server

import (
	"time"

	"github.com/ParticleMedia/nonlocal-indexer/common"
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
	//var cppDoc common.CppDocument
	//err := json.Unmarshal(data, &cppDoc)
	//if err != nil {
	//	return err
	//}
	//
	//if cppDoc.DisableIndex {
	//	metrics.GetOrRegisterMeter("news.disable.qps", nil).Mark(1)
	//	return nil
	//}
	//
	//doc := common.NewIndexerDocumentFromCpp(&cppDoc)
	//l.Set("docid", doc.DocId)
	//l.Set("is_old", doc.IsOldDoc)
	//l.Set("epoch", doc.Epoch)
	//l.Set("ctype", doc.ContentType)
	//l.Set("source", doc.Source)
	//l.Set("url", doc.Url)
	//
	//// filter
	//if FilterNews(doc, l) {
	//	metrics.GetOrRegisterMeter("news.filter.qps", nil).Mark(1)
	//	return nil
	//}
	//glog.V(16).Infof("%+v", doc)
	//
	//// emit
	//err = emitor.EmitNews(doc, l)
	//if err != nil {
	//	return err
	//}
	return nil
}
