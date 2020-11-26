package server

import (
	"time"

	"github.com/ParticleMedia/nonlocal-indexer/common"
)

// return true if need filter, false if pass
type NewsFilter func(doc *common.IndexerDocument) bool

var newsFilters = map[string]NewsFilter{
	"basic":  basicFilter,
	"adult": adultFilter,
	"quality": sourceQualityFilter,
	"local": localFilter,
	"epoch":  epochFilter,
	"ctype":  cTypeFilter,
	"category": categoryFilter,
}

func basicFilter(doc *common.IndexerDocument) bool {
	if len(doc.DocId) == 0 {
		return true
	} else if doc.Epoch <= 0 {
		return true
	}
	return false
}

func localFilter(doc *common.IndexerDocument) bool {
	if doc.LocalScore < 0.5 {
		return false
	}
	return len(doc.GeoTags) > 0 && (doc.TextCategory == nil || !doc.TextCategory.IsSport())
}

func epochFilter(doc *common.IndexerDocument) bool {
	now := time.Now().Unix()
	return now - doc.Epoch > common.ServiceConfig.Expire
}

func cTypeFilter(doc *common.IndexerDocument) bool {
	return doc.ContentType != "news" && doc.ContentType != "3rd_video"
}

func categoryFilter(doc *common.IndexerDocument) bool {
	if doc.TextCategory == nil {
		return false
	}

	return doc.TextCategory.HasFirstCategory("Obituary")
}

func adultFilter(doc *common.IndexerDocument) bool {
	return doc.IsAdult
}

func sourceQualityFilter(doc *common.IndexerDocument) bool {
	sourceInfo := common.GetSourceInfo(doc.Domain)
	return sourceInfo == nil || sourceInfo.Quality < 2
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
