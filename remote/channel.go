package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/ParticleMedia/fb_page_tcat/common"
	"github.com/golang/glog"
	"go.mongodb.org/mongo-driver/bson"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

//type PageChn struct {
//	Id  string                       `bson:"_id"`
//	Chn map[string]map[string]float64 `bson:"text_category"`
//}
//
//type ChannelBody struct {
//	Tcats TextCategory `json:"text_category"`
//}
//
//type Channel struct {
//	FirstCats  map[string]float64 `json:"first_cat"`
//	SecondCats map[string]float64 `json:"second_cat"`
//	ThirdCats  map[string]float64 `json:"third_cat"`
//}

func ProcessChannel(profile *common.FBProfile, conf *common.Config) error {
	//pageCnt := 0
	//totalFirstCats := make(map[string]float64)
	//totalSecondCats := make(map[string]float64)
	//totalThirdCats := make(map[string]float64)
	//for _, page := range profile.Pages {
	//	result, tcatErr := getTextCategory(&page, conf)
	//	if tcatErr != nil || result == nil {
	//		continue
	//	}
	//	if len(result.Tcats.FirstCats) == 0 && len(result.Tcats.SecondCats) == 0 && len(result.Tcats.ThirdCats) == 0 {
	//		continue
	//	}
	//	pageCnt += 1
	//	for cat, score := range result.Tcats.FirstCats {
	//		_, ok := totalFirstCats[cat]
	//		if ok {
	//			totalFirstCats[cat] += score
	//		} else {
	//			totalFirstCats[cat] = score
	//		}
	//	}
	//	for cat, score := range result.Tcats.SecondCats {
	//		_, ok := totalSecondCats[cat]
	//		if ok {
	//			totalSecondCats[cat] += score
	//		} else {
	//			totalSecondCats[cat] = score
	//		}
	//	}
	//	for cat, score := range result.Tcats.ThirdCats {
	//		_, ok := totalThirdCats[cat]
	//		if ok {
	//			totalThirdCats[cat] += score
	//		} else {
	//			totalThirdCats[cat] = score
	//		}
	//	}
	//}
	//
	//if len(totalFirstCats) == 0 && len(totalSecondCats) == 0 && len(totalThirdCats) == 0 {
	//	return nil
	//}
	//
	//for cat, score := range totalFirstCats {
	//	totalFirstCats[cat] = score / float64(pageCnt)
	//}
	//for cat, score := range totalSecondCats {
	//	totalSecondCats[cat] = score / float64(pageCnt)
	//}
	//for cat, score := range totalThirdCats {
	//	totalThirdCats[cat] = score / float64(pageCnt)
	//}
	//
	//totalTextCategory := TextCategory{
	//	FirstCats: totalFirstCats,
	//	SecondCats: totalSecondCats,
	//	ThirdCats: totalThirdCats,
	//}
	//
	//totalTextCategoryBody := TextCategoryBody{
	//	Tcats: totalTextCategory,
	//}
	//value, encodeErr := json.Marshal(totalTextCategoryBody)
	//if encodeErr != nil {
	//	return encodeErr
	//}
	//
	//glog.Infof("ready to write to ups, key: %d, value: %s", profile.Id, string(value))
	//WriteToUps(uint64(profile.Id), string(value), conf.TcatConf.Profile, &conf.UpsConf)

	for _, page := range profile.Pages {
		result, chnErr := getChannel(&page, conf)
		if chnErr != nil || result == nil {
			glog.Infof("get chn with error: %+v", chnErr)
		}
	}
	return nil
}

func getChannel(page *common.FBPage, conf *common.Config) (*TextCategoryBody, error) {
	//pageTcat, getErr := getTextCategoryFromMongo(page.Id, conf)
	//if getErr == nil && pageTcat != nil && pageTcat.Tcat != nil {
	//	firstCats, ok := pageTcat.Tcat["first_cat"]
	//	if !ok {
	//		firstCats = make(map[string]float64)
	//	}
	//	secondCats, ok := pageTcat.Tcat["second_cat"]
	//	if !ok {
	//		secondCats = make(map[string]float64)
	//	}
	//	thirdCats, ok := pageTcat.Tcat["third_cat"]
	//	if !ok {
	//		thirdCats = make(map[string]float64)
	//	}
	//	if len(firstCats) != 0 || len(secondCats) != 0 || len(thirdCats) != 0 {
	//		tcats := TextCategory{
	//			FirstCats:  firstCats,
	//			SecondCats: secondCats,
	//			ThirdCats:  thirdCats,
	//		}
	//		tcat := TextCategoryBody{
	//			Tcats: tcats,
	//		}
	//		return &tcat, nil
	//	}
	//}

	bodyMap := map[string]string{
		"id": page.Id,
		"seg_title": page.Name,
		"seg_content": strings.ReplaceAll(page.About, "\n", ""),
	}
	body, encodeErr := json.Marshal(bodyMap)
	if encodeErr != nil {
		return nil, encodeErr
	}

	resp, respErr := http.Post(conf.ChnConf.Uri, conf.ChnConf.ContentType, bytes.NewBuffer(body))
	if respErr != nil {
		return nil, respErr
	}

	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		glog.Infof("channel resp code: %d", resp.StatusCode)
		return nil, nil
	}

	respBody, readErr := ioutil.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}

	glog.Infof("channel resp body: %s", respBody)
	return nil, nil

	//var tcat TextCategoryBody
	//parseErr := json.Unmarshal(respBody, &tcat)
	//if parseErr != nil {
	//	return nil, parseErr
	//}
	//
	//tcats := make(map[string]map[string]float64)
	//tcats["first_cat"] = tcat.Tcats.FirstCats
	//tcats["second_cat"] = tcat.Tcats.SecondCats
	//tcats["third_cat"] = tcat.Tcats.ThirdCats
	//pageTcat = &PageTcat{
	//	Id: page.Id,
	//	Tcat: tcats,
	//}
	//setErr := setTextCategoryToMongo(pageTcat.Id, pageTcat, conf)
	//if setErr != nil {
	//	glog.Warning("set to mongo with error: %v, value: %+v", setErr, *pageTcat)
	//}

	//return &tcat, nil
}

func setChannelToMongo(key string, value *PageTcat, conf *common.Config) (error) {
	timeout := time.Duration(conf.MongoConf.Timeout) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, insertErr := collectionMap[conf.TcatConf.Collection].InsertOne(ctx, *value)
	if insertErr != nil {
		// replace when exist
		replaceErr := replaceTextCategoryToMongo(key, value, conf)
		return replaceErr
	}
	return nil
}

func replaceChannelToMongo(key string, value *PageTcat, conf *common.Config) (error) {
	timeout := time.Duration(conf.MongoConf.Timeout) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	filter := bson.M{"_id": key}
	_, replaceErr := collectionMap[conf.TcatConf.Collection].ReplaceOne(ctx, filter, *value)
	return replaceErr
}

func getChannelFromMongo(key string, conf *common.Config) (*PageTcat, error) {
	timeout := time.Duration(conf.MongoConf.Timeout) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	filter := bson.M{"_id": key}
	result := collectionMap[conf.TcatConf.Collection].FindOne(ctx, filter)
	if result.Err() != nil {
		return nil, result.Err()
	}

	raw, decodeErr := result.DecodeBytes()
	if decodeErr != nil {
		return nil, decodeErr
	}

	var value PageTcat
	parseErr := bson.Unmarshal(raw, &value)
	if parseErr != nil {
		return nil, parseErr
	}
	return &value, nil
}
