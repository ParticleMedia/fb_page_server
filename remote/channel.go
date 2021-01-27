package remote

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ParticleMedia/fb_page_tcat/common"
	"github.com/golang/glog"
	"go.mongodb.org/mongo-driver/bson"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

var stopWordFilePath = "/mnt/models/fb-page-server/channel/stopWord.txt"
var channelVectorFilePath = "/mnt/models/fb-page-server/channel/channel.vector."
var blackListFilePath = "/mnt/models/fb-page-server/channel/blacklist.txt"
var stopWords map[string]bool
var channelVectorMap map[string][]float64
var channelFormalFormMap map[string]string
var channelIndexMap map[string]map[string]bool
var blackList map[string]bool


type PageChn struct {
	Id  string             `bson:"_id"`
	Chn map[string]float64 `bson:"channels"`
}

type ChannelBody struct {
	Channels []string  `json:"channels"`
	Scores   []float64 `json:"sc_channels"`
}

func LoadChannels() error {
	stopWordFile, stopWordErr := os.Open(stopWordFilePath)
	if stopWordErr != nil {
		return stopWordErr
	}
	stopWords = make(map[string]bool)
	scanner := bufio.NewScanner(stopWordFile)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}
		line = strings.ToLower(line)
		splits := strings.Split(line, "\t")
		word := strings.TrimSpace(splits[0])
		stopWords[word] = true
	}
	stopWordFile.Close()

	id := 0
	dimension := -1
	channelVectorMap = make(map[string][]float64)
	channelFormalFormMap = make(map[string]string)
	channelIndexMap = make(map[string]map[string]bool)
	for {
		channelVectorFile, channelVectorErr := os.Open(channelVectorFilePath + strconv.Itoa(id))
		if channelVectorErr != nil {
			break
		}
		scanner = bufio.NewScanner(channelVectorFile)
		for scanner.Scan() {
			line := scanner.Text()
			if len(line) == 0 {
				continue
			}
			splits := strings.Split(line, "\t")
			channel := splits[0]
			vector := make([]float64, 0, len(splits) - 1)
			for i := 1; i < len(splits); i++ {
				floatValue, floatErr := strconv.ParseFloat(splits[i],64)
				if floatErr != nil {
					return errors.New(fmt.Sprintf("parse to vector fail, line: %s", line))
				}
				vector = append(vector, floatValue)
			}
			if dimension < 0 {
				dimension = len(vector)
			} else if dimension != len(vector){
				return errors.New(fmt.Sprintf("dimention not same in channel vector files, line: %s", line))
			}
			channelVectorMap[channel] = vector
			channelFormalFormMap[strings.ToLower(channel)] = channel
			words := strings.Split(channel, " ")
			for _, word := range words {
				word = strings.ToLower(word)
				_, stop := stopWords[word]
				if !stop {
					channels, exist := channelIndexMap[word]
					if !exist {
						channels = make(map[string]bool)
					}
					channels[strings.ToLower(channel)] = true
					channelIndexMap[word] = channels
				}
			}
		}
		id += 1
		channelVectorFile.Close()
	}
	glog.Infof("load channel data success, stop word count: %d, channel count: %d, formal channel count: %d, uniq word count: %d", len(stopWords), len(channelVectorMap), len(channelFormalFormMap), len(channelIndexMap))
	return nil
}

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

func getChannel(page *common.FBPage, conf *common.Config) (map[string]float64, error) {
	//pageChn, getErr := getChannelFromMongo(page.Id, conf)
	//if getErr == nil && pageChn != nil && pageChn.Chn != nil && len(pageChn.Chn) != 0{
	//	return pageChn.Chn, nil
	//}

	seg_title := page.Name
	seg_content := strings.ReplaceAll(page.About, "\n", "")
	kws_candidate_next := strings.Split(seg_content, " ")

	bodyMap := map[string]interface{}{
		"url": "",
		"seg_title": seg_title,
		"seg_content": seg_content,
		"kws_candidate_next": kws_candidate_next,
	}
	body, encodeErr := json.Marshal(bodyMap)
	if encodeErr != nil {
		return nil, encodeErr
	}

	url := conf.ChnConf.Uri + "?q=" + url.QueryEscape(string(body))
	glog.Infof("channel req body: %s", string(body))
	glog.Infof("channel req url: %s", url)

	resp, respErr := http.Get(url)
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

func getChannelFromMongo(key string, conf *common.Config) (*PageChn, error) {
	timeout := time.Duration(conf.MongoConf.Timeout) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	filter := bson.M{"_id": key}
	result := collectionMap[conf.ChnConf.Collection].FindOne(ctx, filter)
	if result.Err() != nil {
		return nil, result.Err()
	}

	raw, decodeErr := result.DecodeBytes()
	if decodeErr != nil {
		return nil, decodeErr
	}

	var value PageChn
	parseErr := bson.Unmarshal(raw, &value)
	if parseErr != nil {
		return nil, parseErr
	}
	return &value, nil
}
