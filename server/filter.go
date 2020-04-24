package server

import (
	"time"

	"github.com/ParticleMedia/nonlocal-indexer/common"
)

// return true if need filter, false if pass
type NewsFilter func(doc *common.IndexerDocument) bool

var newsFilters = map[string]NewsFilter{
	"basic":  basicFilter,
	"local": localFilter,
	"domain": domainFilter,
	"epoch":  epochFilter,
	"ctype":  cTypeFilter,
}

var domainBlackList = map[string]struct{}{}

func basicFilter(doc *common.IndexerDocument) bool {
	if len(doc.DocId) == 0 {
		return true
	} else if doc.Epoch <= 0 {
		return true
	}
	return false
}

func localFilter(doc *common.IndexerDocument) bool {
	return doc.IsLocalNews == "true"
}

func domainFilter(doc *common.IndexerDocument) bool {
	_, ok := domainBlackList[doc.Domain]
	return ok
}

func epochFilter(doc *common.IndexerDocument) bool {
	now := time.Now().Unix()
	return now-doc.Epoch > common.ServiceConfig.Expire
}

func cTypeFilter(doc *common.IndexerDocument) bool {
	return doc.ContentType != "news"
}

func FilterNews(doc *common.IndexerDocument, l *common.LogInfo) bool {
	for name, filter := range newsFilters {
		if filter(doc) {
			l.Set("filter", name)
			return true
		}
	}
	return false
}
