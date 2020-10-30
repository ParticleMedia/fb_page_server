package common

import (
	"context"
	"github.com/golang/glog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"sync"
	"time"
)

var reloadTickers []*time.Ticker

var sourceMap map[string]*SourceInfo

func GetSourceInfo(domain string) *SourceInfo {
	if len(domain) > 0 {
		return sourceMap[domain]
	}
	return nil
}

func loadSourceInfo() error {
	mongoAddr := "mongodb://172.31.23.34,172.31.27.53,172.24.22.248"
	timeout := 10000 * time.Millisecond
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoAddr).SetConnectTimeout(timeout))
	glog.V(16).Infof("connect to source mongodb %s", mongoAddr)
	if err != nil {
		return err
	}

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return err
	}
	defer client.Disconnect(context.Background())

	collection := client.Database("documentLabels").Collection("sourceInfo")
	filter := bson.M{}
	projection := bson.M{"_id": 1, "domain": 1, "sourceTier": 1, "quality": 1, "paywall_flag": 1, "compatibility": 1}

	cursor, err := collection.Find(ctx, filter, options.Find().SetProjection(projection))
	if err != nil {
		glog.Warningf("mongo find error: %+v", err)
		return err
	}
	if err = cursor.Err(); err != nil {
		glog.Warningf("mongo cursor error: %+v", err)
		return err
	}
	defer cursor.Close(context.Background())

	sourceInfoMap := make(map[string]*SourceInfo)
	for cursor.Next(context.Background()) {
		var sourceInfo SourceInfo
		err = bson.Unmarshal(cursor.Current, &sourceInfo)
		if err != nil {
			glog.Warningf("unmarshal bson %s error: %+v", cursor.Current.String(), err)
			continue
		}

		sourceInfoMap[sourceInfo.Id] = &sourceInfo
		sourceInfoMap[sourceInfo.Domain] = &sourceInfo
		//glog.V(16).Infof("city info: %+v", cityInfo)
	}
	sourceMap = sourceInfoMap
	glog.Infof("load source info from mongo size %d", len(sourceMap))
	return nil
}

func LoadAllDatas(conf *Config) error {
	start := time.Now()
	swg := sync.WaitGroup{}
	loadFuncs := make([]func() error, 0, 3)
	loadFuncs = append(loadFuncs, loadSourceInfo)

	swg.Add(len(loadFuncs))
	var err error = nil
	for _, f := range loadFuncs {
		go func(f func() error) {
			ferr := f()
			if err != nil {
				err = ferr
			}
			swg.Done()
		}(f)
	}
	swg.Wait()
	glog.Infof("all data load finished, cost: %d ms", time.Since(start).Nanoseconds() / 1000000)

	// register reloader
	reloadTickers = make([]*time.Ticker, 0)
	registerReloader(loadSourceInfo, "source_info", 10 * time.Minute)
	return err
}

func registerReloader(f func() error, name string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	go func(ticker *time.Ticker) {
		glog.Infof("Start ticker for reloader %s", name)
		for t := range ticker.C {
			glog.Infof("run reloader %s at %v", name, t)
			err := f()
			if err == nil {
				glog.Infof("run reloader %s success. cost: %d ms", name, time.Since(t).Nanoseconds() / 1000000)
			} else {
				glog.Warningf("run reloader %s with error: %+v", name, err)
			}
		}
		glog.Infof("reloader %s stopped", name)
	} (ticker)
	reloadTickers = append(reloadTickers, ticker)
}

func ReleaseAllDatas() {
	// stop reload tickers
	if len(reloadTickers) > 0 {
		for _, t := range reloadTickers {
			t.Stop()
		}
		reloadTickers = nil
	}
}
