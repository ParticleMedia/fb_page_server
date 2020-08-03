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

func (t *TextCategoryStruct) IsSport() bool {
	if t == nil || t.FirstCategory == nil {
		return false
	}
	_, ok := t.FirstCategory["Sports"]
	return ok
}

// cpp kafka message
type CppDocument struct {
	DocId          string     `json:"_id"`
	DisableIndex   bool       `json:"disable_index"`
	Epoch          int64      `json:"epoch"`
	Title          string     `json:"seg_title"`
	ContentType    string     `json:"ctype"`
	Domain         string     `json:"domain"`
	Source         string     `json:"source"`
	Url            string     `json:"id"`
	IsLocalNews    string     `json:"is_local_news"`
	GeoTags        []GeoTag   `json:"geotag,omitempty"`
	Pois           []string   `json:"poi,omitempty"`
	Channels       []string   `json:"channels,omitempty"`
	ChannelsV2     []string   `json:"channels_v2,omitempty"`
	NluTags        []string   `json:"nlu_tags,omitempty"`
	Tpcs           map[string]float64   `json:"tpcs,omitempty"`
	TextCategory   *TextCategoryStruct  `json:"text_category,omitempty"`
	TextCategoryV2 *TextCategoryStruct  `json:"text_category_v2,omitempty"`
}

type IndexerDocument struct {
	DocId          string     `json:"_id"`
	Epoch          int64      `json:"epoch"`
	Title          string     `json:"seg_title"`
	ContentType    string     `json:"ctype"`
	Domain         string     `json:"domain"`
	Source         string     `json:"source"`
	Url            string     `json:"url"`
	IsLocalNews    string     `json:"is_local_news"`
	GeoTags        []GeoTag   `json:"geotag,omitempty"`
	Pois           []string   `json:"poi,omitempty"`
	Channels       []string   `json:"channels,omitempty"`
	ChannelsV2     []string   `json:"channels_v2,omitempty"`
	NluTags        []string   `json:"nlu_tags,omitempty"`
	Tpcs           map[string]float64   `json:"tpcs,omitempty"`
	TextCategory   *TextCategoryStruct  `json:"text_category,omitempty"`
	TextCategoryV2 *TextCategoryStruct  `json:"text_category_v2,omitempty"`
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

	cateV2 := doc.TextCategoryV2
	if cateV2 == nil {
		cateV2 = &TextCategoryStruct{}
	}

	tags := make([]string, 0, len(doc.NluTags))
	if doc.NluTags != nil {
		for _, tag := range doc.NluTags {
			tags = append(tags, ReplaceSpace(tag))
		}
	}
	return &IndexerDocument{
		DocId:          doc.DocId,
		Epoch:          doc.Epoch,
		Title:          doc.Title,
		ContentType:    ctype,
		Domain:         doc.Domain,
		Source:         doc.Source,
		Url:            doc.Url,
		GeoTags:        doc.GeoTags,
		IsLocalNews:    doc.IsLocalNews,
		Pois:           doc.Pois,
		Channels:       doc.Channels,
		ChannelsV2:     doc.ChannelsV2,
		NluTags:        tags,
		Tpcs:           doc.Tpcs,
		TextCategory:   cate,
		TextCategoryV2: cateV2,
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
	NluTags      []string  `json:"nlu_tags,omitempty"`
	Tpcs         []string  `json:"tpcs,omitempty"`
	FirstCats    []string  `json:"first_cat,omitempty"`
	SecondCats   []string  `json:"second_cat,omitempty"`
	ThirdCats    []string  `json:"third_cat,omitempty"`
}
