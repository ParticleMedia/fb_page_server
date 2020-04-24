package common

import (
	"time"
)

type GeoTag struct {
	Coord    string   `json:"coord"`
	Name     string   `json:"name"`
	Pid      string   `json:"pid"`
	Score    float64  `json:"score"`
	Type     string   `json:"type"`
	Zipcodes []string `json:"zipcodes,omitempty"`
}

type TextCategoryStruct struct {
	FirstCategory map[string]float64 `json:"first_cat,omitempty"`
	SecondCategory map[string]float64 `json:"second_cat,omitempty"`
	ThirdCategory map[string]float64 `json:"third_cat,omitempty"`
}

func (t *TextCategoryStruct) GetFirstCategory() string {
	if t == nil || t.FirstCategory == nil {
		return ""
	}
	// get first category with highest score
	var score float64 = -1.0
	var cat string = ""
	for c, v := range t.FirstCategory {
		if v >= score {
			cat = c
		}
	}
	return cat
}

// cpp kafka message
type CppDocument struct {
	DocId        string     `json:"_id"`
	Epoch        int64      `json:"epoch"`
	Title        string     `json:"seg_title"`
	ContentType  string     `json:"ctype"`
	Domain       string     `json:"domain"`
	Source       string     `json:"source"`
	Url          string     `json:"src_url"`
	IsLocalNews  string     `json:"is_local_news"`
	GeoTags      []GeoTag   `json:"geotag,omitempty"`
	Pois         []string   `json:"poi,omitempty"`
	Channels     []string   `json:"channels,omitempty"`
	Tpcs         map[string]float64   `json:"tpcs,omitempty"`
	TextCategory *TextCategoryStruct `json:"text_category,omitempty"`
}

type IndexerDocument struct {
	DocId        string     `json:"_id"`
	Epoch        int64      `json:"epoch"`
	Title        string     `json:"seg_title"`
	ContentType  string     `json:"ctype"`
	Domain       string     `json:"domain"`
	Source       string     `json:"source"`
	Url          string     `json:"url"`
	IsLocalNews  string     `json:"is_local_news"`
	GeoTags      []GeoTag   `json:"geotag,omitempty"`
	Pois         []string   `json:"poi,omitempty"`
	Channels     []string   `json:"channels,omitempty"`
	Tpcs         map[string]float64   `json:"tpcs,omitempty"`
	TextCategory *TextCategoryStruct `json:"text_category,omitempty"`
}

// 转换函数
func NewIndexerDocumentFromCpp(doc *CppDocument) *IndexerDocument {
	ctype := doc.ContentType
	if len(ctype) == 0 {
		ctype = "news"
	}

	cate := doc.TextCategory
	if cate == nil {
		cate = &TextCategoryStruct{}
	}
	return &IndexerDocument{
		DocId:        doc.DocId,
		Epoch:        doc.Epoch,
		Title:        doc.Title,
		ContentType:  ctype,
		Domain:       doc.Domain,
		Source:       doc.Source,
		GeoTags:      doc.GeoTags,
		IsLocalNews:  doc.IsLocalNews,
		Pois:         doc.Pois,
		Channels:     doc.Channels,
		Tpcs:         doc.Tpcs,
		TextCategory: cate,
	}
}

type ESDocument struct {
	DocId        string    `json:"docid"`
	Date         time.Time `json:"date"`
	Title        string    `json:"title"`
	Timestamp    int64     `json:"ts"`
	Domain       string    `json:"domain,omitempty"`
	Source       string    `json:"source,omitempty"`
	Url          string    `json:"url,omitempty"`
	Pois         []string  `json:"pois,omitempty"`
	Channels     []string  `json:"channels,omitempty"`
	Tpcs         []string  `json:"tpcs,omitempty"`
	FirstCats    []string  `json:"first_cat,omitempty"`
	SecondCats   []string  `json:"second_cat,omitempty"`
	ThirdCats    []string  `json:"third_cat,omitempty"`
}
