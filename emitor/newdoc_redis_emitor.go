package emitor

import (
	"errors"
	"github.com/ParticleMedia/nonlocal-indexer/common"
	"github.com/golang/glog"
	"github.com/gomodule/redigo/redis"
	"github.com/rcrowley/go-metrics"
	"strings"
	"time"
)

var blockedCategories = []string{
	"CrimePublicsafety",
	"Obituary",
	"ClimateEnvironment",
}

type RedisClient struct {
	cli *redis.Pool
	conf *common.RedisConfig
	key string
	limit uint64
}

func NewRedisClient(config *common.RedisConfig) (*RedisClient, error) {
	// 建立连接池
	client := &RedisClient{
		conf: config,
		cli: nil,
		key: config.Key,
		limit: config.Limit,
	}

	client.cli = &redis.Pool{
		MaxIdle:     10,
		IdleTimeout: 10 * time.Second,
		Wait:        true,
		Dial:        client.Dial,
		MaxConnLifetime: 10 * time.Minute,
	}

	return client, nil
}

func (c *RedisClient) Dial() (redis.Conn, error) {
	if c.conf == nil || len(c.conf.Addr) == 0 {
		return nil, errors.New("missing server address")
	}
	con, err := redis.Dial(c.conf.Network, c.conf.Addr,
		redis.DialPassword(c.conf.Password),
		redis.DialDatabase(c.conf.Db),
		redis.DialConnectTimeout(time.Duration(c.conf.Dial.ConnTimeout) * time.Millisecond),
		redis.DialReadTimeout(time.Duration(c.conf.Dial.ReadTimeout) * time.Millisecond),
		redis.DialWriteTimeout(time.Duration(c.conf.Dial.WriteTimeout) * time.Millisecond))
	if err != nil {
		return nil, err
	}
	return con, nil
}

func filterForRedis(doc *common.IndexerDocument) bool {
	if doc.IsOldDoc {
		// new doc only
		return true
	}
	// short article
	if doc.TitleCCount <= 5 {
		return true
	}
	if doc.ImageCount == 0 {
		return true
	}
	if !doc.HasVideo && doc.ImageCount < 2 && doc.WordCount < 100 {
		return true
	}

	// select category
	if doc.TextCategory != nil {
		for _, cat := range blockedCategories {
			if doc.TextCategory.HasFirstCategory(cat) {
				return true
			}
		}
		if doc.TextCategory.HasThirdCategory("PoliticsGovernment_Federal_POTUS") {
			return true
		}
	}

	// low quality
	sourceInfo := common.GetSourceInfo(doc.Domain)
	if sourceInfo == nil || sourceInfo.Quality <= 3 || sourceInfo.SourceTier <= 1 || sourceInfo.Paywall {
		// ignore
		return true
	}
	if strings.HasPrefix(sourceInfo.Compatibility, "block_") {
		return true
	}

	return false
}

func (c *RedisClient) indexNews(doc *common.IndexerDocument, l *common.LogInfo) error {
	if filterForRedis(doc) {
		// ignore
		l.Set("force_explore", 0)
		return nil
	}

	defer func(start time.Time) {
		metrics.GetOrRegisterTimer("redis.index.latency", nil).UpdateSince(start)
	} (time.Now())
	l.Set("force_explore", 1)

	// 获取连接
	conn := c.cli.Get()
	defer conn.Close()
	connErr := conn.Err()
	if connErr != nil {
		common.GetOrRegisterHistogram("redis.index.error", nil, func() metrics.Sample { return metrics.NewExpDecaySample(1024, 0.015)}).Update(100)
		return connErr
	}

	metrics.GetOrRegisterMeter("redis.index.qps", nil).Mark(1)
	e1 := conn.Send("LPUSH", c.key, doc.DocId)
	e2 := conn.Send("LTRIM", c.key, 0, c.limit)
	err := common.SelectErrors(e1, e2)
	if err != nil {
		common.GetOrRegisterHistogram("redis.index.error", nil, func() metrics.Sample { return metrics.NewExpDecaySample(1024, 0.015)}).Update(100)
		glog.Warningf("index redis with error: %+v", err)
	}

	flushErr := conn.Flush()
	if flushErr != nil {
		common.GetOrRegisterHistogram("redis.index.error", nil, func() metrics.Sample { return metrics.NewExpDecaySample(1024, 0.015)}).Update(100)
		glog.Warningf("flush redis with error: %+v", flushErr)
		return flushErr
	}

	reply, recvErr := redis.ReceiveWithTimeout(conn, time.Duration(500) * time.Millisecond)
	if recvErr != nil {
		glog.Warningf("recv redis with error: %+v", recvErr)
		common.GetOrRegisterHistogram("redis.index.error", nil, func() metrics.Sample { return metrics.NewExpDecaySample(1024, 0.015)}).Update(100)
	} else {
		common.GetOrRegisterHistogram("redis.index.error", nil, func() metrics.Sample { return metrics.NewExpDecaySample(1024, 0.015)}).Update(0)
	}
	glog.V(16).Infof("redis index reply: %+v", reply)
	return nil
}
