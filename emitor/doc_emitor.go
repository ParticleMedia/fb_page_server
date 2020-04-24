package emitor

import (
	"github.com/ParticleMedia/nonlocal-indexer/common"
	"github.com/golang/glog"
	"sync"
)

type NewsEmitor func(*common.IndexerDocument, *common.LogInfo) error

var initOnce = &sync.Once{}
var newsEmitors map[string]NewsEmitor

func getNewsEmitors() map[string]NewsEmitor {
	initOnce.Do(func() {
		// init emitors
		newsEmitors = make(map[string]NewsEmitor)
		conf := common.ServiceConfig
		if conf.LocalEs.Enable {
			esIndexer, err := NewESIndexer(&conf.LocalEs)
			if err != nil {
				glog.Fatalf("create es client error %+v", err)
			}
			newsEmitors["es"] = esIndexer.indexNews
		} else {
			newsEmitors["mock_es"] = mockESEmitor(conf.LocalEs.Index)
		}

		if conf.TraceConfig.Enable {
			newsEmitors["trace"] = traceEmitor(conf.TraceConfig.Url, conf.TraceConfig.Event)
		}
	})
	return newsEmitors
}

func EmitNews(doc *common.IndexerDocument, l *common.LogInfo) error {
	dests := make([]string, 0)
	defer func() {
		l.Set("dests", dests)
	}()

	for name, emitor := range getNewsEmitors() {
		err := emitor(doc, l)
		dests = append(dests, name)
		if err != nil {
			return err
		}
	}
	return nil
}

