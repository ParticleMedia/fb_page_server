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
		if conf.NonLocalEs.Enable {
			esIndexer, err := NewESIndexer(&conf.NonLocalEs)
			if err != nil {
				glog.Fatalf("create es client error %+v", err)
			}
			newsEmitors["es"] = esIndexer.indexBaseNews
		} else {
			newsEmitors["mock_es"] = mockESEmitor(conf.NonLocalEs.Index)
		}

		if conf.NonLocalExpEs.Enable {
			esIndexer, err := NewESIndexer(&conf.NonLocalExpEs)
			if err != nil {
				glog.Fatalf("create exp es client error %+v", err)
			}
			newsEmitors["exp_es"] = esIndexer.indexExpNews
		}

		if conf.Redis.Enable {
			redisCli, err := NewRedisClient(&conf.Redis)
			if err != nil {
				glog.Fatalf("create redis client error %+v", err)
			}
			newsEmitors["newdoc"] = redisCli.indexNews
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

	var swg sync.WaitGroup
	for name, emitor := range getNewsEmitors() {
		swg.Add(1)
		go func(wg *sync.WaitGroup, name string, emitor NewsEmitor) {
			defer wg.Done()
			err := emitor(doc, l)
			dests = append(dests, name)
			if err != nil {
				glog.Warningf("emit doc %s to %s with error: %+v", doc.DocId, name, err)
			}
		} (&swg, name, emitor)
	}
	swg.Wait()
	return nil
}

