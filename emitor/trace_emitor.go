package emitor

import (
	"fmt"
	"github.com/ParticleMedia/nonlocal-indexer/common"
	"github.com/golang/glog"
	"github.com/rcrowley/go-metrics"
	"net/http"
	"time"
)

func traceEmitor(uri string, event string) NewsEmitor {
	return func(doc *common.IndexerDocument, l *common.LogInfo) error {
		start := time.Now()
		url := fmt.Sprintf("%s?docids=%s&event=%s&ts=%d", uri, doc.DocId, event, start.Unix())
		metrics.GetOrRegisterMeter("trace.qps", nil).Mark(1)
		resp, err := http.Get(url)
		//code := resp.StatusCode
		resp.Body.Close()
		if err == nil {
			metrics.GetOrRegisterHistogram("trace.error", nil, metrics.NewExpDecaySample(128, 0.015)).Update(0)
			metrics.GetOrRegisterTimer("trace.latency", nil).Update(time.Since(start))
		} else {
			metrics.GetOrRegisterHistogram("trace.error", nil, metrics.NewExpDecaySample(128, 0.015)).Update(100)
			glog.Warningf("send trace with error: %+v", err)
		}
		return err
	}
}
