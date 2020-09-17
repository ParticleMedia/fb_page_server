package emitor

import (
	"errors"
	"context"
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ParticleMedia/nonlocal-indexer/common"
	"github.com/golang/glog"
	"github.com/rcrowley/go-metrics"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

const (
	// 1 days per split
	ES_INDEX_SPLIT_WINDOW = 86400
)

type ESIndexer struct {
	client  *elasticsearch.Client
	timeout time.Duration
	conf    *common.ElasticSearchConfig
}

func NewESIndexer(conf *common.ElasticSearchConfig) (*ESIndexer, error) {
	cfg := elasticsearch.Config{
		Addresses: conf.Addr,
		Transport: &http.Transport{
			MaxIdleConnsPerHost:   10,
			ResponseHeaderTimeout: 5 * time.Second,
		},
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return &ESIndexer{
		client:  client,
		timeout: time.Duration(conf.Timeout) * time.Millisecond,
		conf:    conf,
	}, nil
}

func esPrepare(doc *common.IndexerDocument, isExp bool, indexPattern string) (*common.ESDocument, string) {
	indexSplit := doc.Epoch / ES_INDEX_SPLIT_WINDOW
	index := fmt.Sprintf("%s-%d", indexPattern, indexSplit)
	var esDoc *common.ESDocument = nil
	if isExp {
		esDoc = transformToExpEsDoc(doc)
	} else {
		esDoc = transformToBaseEsDoc(doc)
	}
	return esDoc, index
}

func (i *ESIndexer) doIndex(esDoc *common.ESDocument, index string, l *common.LogInfo) error {
	jsonDoc, err := json.Marshal(esDoc)
	if err != nil {
		glog.Warningf("encode es doc %+v to json error %+v", esDoc, err)
		return err
	}
	l.Set("es_doc", string(jsonDoc))
	indexReq := esapi.IndexRequest{
		Index:        index,
		DocumentID:   esDoc.DocId,
		Body:         bytes.NewReader(jsonDoc),
		Refresh:      "true",
	}

	ctx, cancel := context.WithTimeout(context.Background(), i.timeout)
	defer cancel()

	metrics.GetOrRegisterMeter("es.index.qps", nil).Mark(1)
	start := time.Now()
	res, esErr := indexReq.Do(ctx, i.client)
	if esErr != nil {
		glog.Warningf("index doc %+v to es error %+v", esDoc, esErr)
		return esErr
	}
	defer res.Body.Close()
	metrics.GetOrRegisterTimer("es.index.latency", nil).Update(time.Since(start))

	l.Set("es_status", res.StatusCode)
	if res.IsError() {
		body, _ := ioutil.ReadAll(res.Body)
		glog.Warningf("index doc %+v to es with error status %d body: %s", esDoc, res.StatusCode, string(body))
		return errors.New(fmt.Sprintf("es error status: %d", res.StatusCode))
	} else {
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			glog.Warningf("Error parsing the response body: %s", err)
			return err
		} else {
			// Print the response status and indexed document version.
			version := int(r["_version"].(float64))
			glog.V(16).Infof("es response: [%s] %s version=%d", res.Status(), r["result"], version)
			l.Set("es_version", version)
		}
	}
	return nil
}

func (i *ESIndexer) indexNews(doc *common.IndexerDocument, isExp bool, l *common.LogInfo) error {
	esDoc, index := esPrepare(doc, isExp, i.conf.Index)
	//glog.V(16).Infof("index %s doc %s", index, esDoc)
	l.Set("es_index", index)
	err := i.doIndex(esDoc, index, l)
	if err == nil {
		metrics.GetOrRegisterHistogram("es.index.error", nil, metrics.NewExpDecaySample(128, 0.015)).Update(0)
	} else {
		metrics.GetOrRegisterHistogram("es.index.error", nil, metrics.NewExpDecaySample(128, 0.01500)).Update(100)
		glog.Warningf("es index to %s error: %+v", index, err)
	}
	return err
}

func (i *ESIndexer) indexBaseNews(doc *common.IndexerDocument, l *common.LogInfo) error {
	return i.indexNews(doc, false, l)
}

func (i *ESIndexer) indexExpNews(doc *common.IndexerDocument, l *common.LogInfo) error {
	return i.indexNews(doc, true, l)
}

func mapKeyList(m map[string]float64, thr float64) []string {
	if len(m) == 0 {
		return nil
	}

	l := make([]string, 0, len(m))
	for k, v := range m {
		if v >= 0.3 {
			l = append(l, k)
		}
	}
	return l
}

func transformToBaseEsDoc(doc *common.IndexerDocument) *common.ESDocument {
	return &common.ESDocument{
		DocId:        doc.DocId,
		Date:         time.Unix(doc.Epoch, 0),
		Title:        doc.Title,
		Timestamp:    time.Now().UnixNano() / 1000000,
		ContentType:  doc.ContentType,
		Domain:       doc.Domain,
		Source:       common.ReplaceSpace(strings.ToLower(doc.Source)),
		Url:          doc.Url,
		Pois:         doc.Pois,
		Channels:     doc.Channels,
		NluTags:      doc.NluTags,
		Tpcs:         mapKeyList(doc.Tpcs, 0.3),
		FirstCats:    mapKeyList(doc.TextCategory.FirstCategory, 0.0),
		SecondCats:   mapKeyList(doc.TextCategory.SecondCategory, 0.0),
		ThirdCats:    mapKeyList(doc.TextCategory.ThirdCategory, 0.0),
	}
}

func transformToExpEsDoc(doc *common.IndexerDocument) *common.ESDocument {
	return &common.ESDocument{
		DocId:        doc.DocId,
		Date:         time.Unix(doc.Epoch, 0),
		Title:        doc.Title,
		Timestamp:    time.Now().UnixNano() / 1000000,
		ContentType:  doc.ContentType,
		Domain:       doc.Domain,
		Source:       common.ReplaceSpace(strings.ToLower(doc.Source)),
		Url:          doc.Url,
		Pois:         doc.Pois,
		Channels:     doc.ChannelsV2,
		NluTags:      doc.NluTags,
		Tpcs:         mapKeyList(doc.Tpcs, 0.3),
		FirstCats:    mapKeyList(doc.TextCategoryV2.FirstCategory, 0.0),
		SecondCats:   mapKeyList(doc.TextCategoryV2.SecondCategory, 0.0),
		ThirdCats:    mapKeyList(doc.TextCategoryV2.ThirdCategory, 0.0),
	}
}

func mockESEmitor(indexPattern string) NewsEmitor {
	return func(doc *common.IndexerDocument, l *common.LogInfo) error {
		esDoc, index := esPrepare(doc, false, indexPattern)
		jsonDoc, err := json.Marshal(esDoc)
		if err != nil {
			glog.Warningf("encode es doc %+v to json error %+v", esDoc, err)
			return err
		}
		l.Set("es_index", index)
		l.Set("es_doc", string(jsonDoc))
		glog.V(16).Infof("index %s doc %+v", index, esDoc)
		return nil
	}
}
