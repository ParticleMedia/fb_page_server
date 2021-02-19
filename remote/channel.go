package remote

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ParticleMedia/fb_page_server/common"
	"github.com/golang/glog"
	"go.mongodb.org/mongo-driver/bson"
	"io/ioutil"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"
)

var stopWordFilePath = "/mnt/models/fb-page-server/channel/stopWord.txt"
var blackListFilePath = "/mnt/models/fb-page-server/channel/blacklist.txt"
var channelVectorFilePath = "/mnt/models/fb-page-server/channel/channel.vector."
var stopWords map[string]bool
var blackList map[string]bool
var channelVectorMap map[string][]float64
var channelFormalFormMap map[string]string
var channelIndexMap map[string]map[string]bool
var entitySuffix = map[string]bool{"Inc.": true, "Corp.": true, "Corporation": true, "Award": true, "Awards": true}
var scoreThreshold = 0.2
var delimiters = []uint8{'?', ':', '!', '=', '(', ')', '[', ']', '{', '}', '\r', '\n', '\t', ' ', '"', '\'', '<', '>', ',', '.', '/', '\\', '+', '-', '*', '&', '|', '^', '%', ';'}

type PageChn struct {
	Id  string             `bson:"_id"`
	Chn map[string]float64 `bson:"channels"`
}

type ChannelBody struct {
	Channels []string  `json:"channels"`
	Scores   []float64 `json:"sc_channels"`
}

type PageContext struct {
	segTitle    string
	segContent  string
	contentKws  map[string]int64
	contentBws  map[string]int64
	contentSize int
	titleKws    map[string]int64
	titleBws    map[string]int64
	titleSize   int
	keywords    []string
	scores      []float64
	vector      []float64
	channels    []string
	scChannels  []float64
}

func NewPageContext(segTitle string, segContent string, keywords []string, scores []float64, vector []float64) *PageContext {
	return &PageContext {
		segTitle:    segTitle,
		segContent:  segContent,
		contentKws:  make(map[string]int64),
		contentBws:  make(map[string]int64),
		contentSize: 0,
		titleKws:    make(map[string]int64),
		titleBws:    make(map[string]int64),
		titleSize:   0,
		keywords:    keywords,
		scores:      scores,
		vector:      vector,
	}
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

	blackListFile, blackListErr := os.Open(blackListFilePath)
	if blackListErr != nil {
		return blackListErr
	}
	blackList = make(map[string]bool)
	scanner = bufio.NewScanner(blackListFile)
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}
		line = strings.ToLower(line)
		splits := strings.Split(line, "\t")
		//有空格，无大写
		channel := strings.TrimSpace(splits[0])
		blackList[channel] = true
	}
	blackListFile.Close()

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
			_, ok := channelVectorMap[channel]
			if !ok {
				//有空格，有大写
				channelVectorMap[channel] = vector
				//key有空格，无大写；value有空格，有大写
				channelFormalFormMap[strings.ToLower(channel)] = channel
			}
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
					//key单个词，无大写；value有空格，无大写
					channelIndexMap[word] = channels
				}
			}
		}
		id += 1
		channelVectorFile.Close()
	}
	return nil
}

func ProcessChannel(profile *common.FBProfile, conf *common.Config) error {
	pageCnt := 0
	totalChannelScores := make(map[string]float64)
	for _, page := range profile.Pages {
		result, chnErr := getChannel(&page, conf)
		if chnErr != nil || result == nil || len(result) == 0 {
			continue
		}
		pageCnt += 1
		for chn, score := range result {
			_, ok := totalChannelScores[chn]
			if ok {
				totalChannelScores[chn] += score
			} else {
				totalChannelScores[chn] = score
			}
		}
	}

	if len(totalChannelScores) == 0 {
		return nil
	}

	for chn, score := range totalChannelScores {
		totalChannelScores[chn] = score / float64(pageCnt)
	}

	value, encodeErr := json.Marshal(totalChannelScores)
	if encodeErr != nil {
		return encodeErr
	}

	glog.Infof("ready to write to ups, profile: %s, key: %d, value: %s", conf.ChnConf.Profile, profile.Id, string(value))
	WriteToUps(uint64(profile.Id), string(value), conf.ChnConf.Profile, &conf.UpsConf)
	return nil
}

func getChannel(page *common.FBPage, conf *common.Config) (map[string]float64, error) {
	channelScores := make(map[string]float64)
	pageChn, getErr := getChannelFromMongo(page.Id, conf)
	if getErr == nil && pageChn != nil && pageChn.Chn != nil && len(pageChn.Chn) != 0{
		return pageChn.Chn, nil
	}

	segTitle := page.Name
	segContent := strings.ReplaceAll(page.About, "\n", "")
	kwsCandidateNext := getKwsCandidate(segTitle, segContent)

	bodyMap := map[string]interface{}{
		"url":                "",
		"seg_title":          segTitle,
		"seg_content":        segContent,
		//有空格，有大写
		"kws_candidate_next": kwsCandidateNext,
	}
	body, encodeErr := json.Marshal(bodyMap)
	if encodeErr != nil {
		return nil, encodeErr
	}

	resp, respErr := http.Get(conf.ChnConf.Uri + "?q=" + url.QueryEscape(string(body)))
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

	respMap := make(map[string]interface{})
	parseErr := json.Unmarshal(respBody, &respMap)
	if parseErr != nil {
		return nil, parseErr
	}

	keywordScoreArray, keywordScoreOk := respMap["keyword"]
	vectorArray, vectorOk := respMap["vector"]
	if !keywordScoreOk || !vectorOk {
		return nil, errors.New(fmt.Sprintf("parse keyword response with error, has keyword score: %+v, has vector: %+v", keywordScoreOk, vectorOk))
	}
	keywords := make([]string, 0, len(keywordScoreArray.([]interface{})))
	scores := make([]float64, 0, len(keywordScoreArray.([]interface{})))
	for _, keywordScore := range keywordScoreArray.([]interface{}) {
		//有空格，有大写
		keyword, keywordOk := keywordScore.(map[string]interface{})["keyword"]
		score, scoreOk := keywordScore.(map[string]interface{})["score"]
		if keywordOk && scoreOk && len(keyword.(string)) > 0 && !strings.Contains(keyword.(string), "http") && score.(float64) > 0 {
			//无空格，有大写
			keywords = append(keywords, strings.ReplaceAll(keyword.(string), " ", "^^"))
			scores = append(scores, score.(float64))
		}
	}
	//无空格，有大写
	keywords, scores = dedupKeywords(keywords, scores)
	vector := make([]float64, 0, len(vectorArray.([]interface{})))
	for _, value := range vectorArray.([]interface{}) {
		vector = append(vector, value.(float64))
	}
	pageContext := NewPageContext(segTitle, segContent, keywords, scores, vector)
	pageContext.channels, pageContext.scChannels = rankChannels(pageContext)
	pageContext.channels, pageContext.scChannels = filterChannels(pageContext)
	for i := 0; i < len(pageContext.channels); i++ {
		channelScores[pageContext.channels[i]] = pageContext.scChannels[i]
	}
	pageChn = &PageChn{
		Id: page.Id,
		Chn: channelScores,
	}
	setErr := setChannelToMongo(pageChn.Id, pageChn, conf)
	if setErr != nil {
		glog.Warning("set chn to mongo with error: %v, value: %+v", setErr, *pageChn)
	}

	return channelScores, nil
}

func getKwsCandidate(title string, content string) []string {
	keywords := make(map[string]bool)
	if len(title) > 0 {
		words := strings.Split(strings.ReplaceAll(title, "\\s+", " "), " ")
		for keyword, _ := range countKws(words) {
			keywords[keyword] = true
		}
		for keyword, _ := range countBws(words) {
			keywords[keyword] = true
		}
		for keyword, _ := range countTws(words) {
			keywords[keyword] = true
		}
		for keyword, _ := range countQws(words) {
			keywords[keyword] = true
		}
	}
	if len(content) > 0 {
		words := strings.Split(strings.ReplaceAll(content, "\\s+", " "), " ")
		for keyword, _ := range countKws(words) {
			keywords[keyword] = true
		}
		for keyword, _ := range countBws(words) {
			keywords[keyword] = true
		}
		for keyword, _ := range countTws(words) {
			keywords[keyword] = true
		}
		for keyword, _ := range countQws(words) {
			keywords[keyword] = true
		}
	}
	result := make([]string, 0, len(keywords))
	for keyword, _ := range keywords {
		result = append(result, keyword)
	}
	return result
}

func dedupKeywords(keywords []string, scores []float64) ([]string, []float64) {
	keywords_new := make([]string, 0, len(keywords))
	scores_new := make([]float64, 0, len(scores))
	existKeywords := make(map[string]bool)
	for index, keyword := range keywords {
		score := scores[index]
		_, exist := existKeywords[strings.ToLower(keyword)]
		if !exist {
			keywords_new = append(keywords_new, keyword)
			scores_new = append(scores_new, score)
			existKeywords[strings.ToLower(keyword)] = true
		}
	}
	return keywords_new, scores_new
}

func rankChannels(pageContext *PageContext) ([]string, []float64) {
	channels := make(map[string]bool)
	channelScores := make(map[string]float64)

	text := pageContext.segTitle
	if len(text) > 0 {
		words := strings.Split(strings.ReplaceAll(text, "\\s+", " "), " ")
		pageContext.titleSize = len(words)
		// 单个词，有大写
		pageContext.titleKws = countKws(words)
		// 无空格，有大写
		pageContext.titleBws = countBws(words)
	}

	text = pageContext.segContent
	if len(text) > 0 {
		words := strings.Split(strings.ReplaceAll(text, "\\s+", " "), " ")
		pageContext.contentSize = len(words)
		// 单个词，有大写
		pageContext.contentKws = countKws(words)
		// 无空格，有大写
		pageContext.contentBws = countBws(words)
	}

	//无空格，有大写
	for _, keyword := range pageContext.keywords {
		// case 1: keyword是channel
		//有空格，无大写
		keyword = strings.ToLower(strings.ReplaceAll(keyword, "^^", " "))
		_, ok := channelFormalFormMap[keyword]
		if ok {
			//有空格，有大写
			keyword = channelFormalFormMap[keyword]
			_, exist := channels[keyword]
			if !exist {
				channelVector := channelVectorMap[keyword]
				sim := simVector(channelVector, pageContext.vector)
				channelScores[keyword] = sim
				channels[keyword] = true
			}
		}

		// case 2: keyword分词后有数字则不继续
		words := strings.Split(keyword, " ")
		containsNumber := false
		for _, word := range words {
			if isNumber(word) {
				containsNumber = true
				break
			}
		}
		if (containsNumber) {
			continue
		}

		// case 3: keyword与channel共有某分词，且该分词是该channel的后缀，且page信息里包含该channel的除entity外的所有分词
		for offset := 0; offset < len(words); offset++ {
			//单个词，无大写
			word := strings.ToLower(words[offset])
			// 有空格，无大写
			chns, ok := channelIndexMap[word]
			if ok {
				for chn := range chns {
					// 有空格，有大写
					chn = channelFormalFormMap[chn]
					// 有空格，有大写
					chnEntityRemoved := removeEntity(chn)
					if strings.HasSuffix(strings.ToLower(chnEntityRemoved), word) {
						if !areAllWordsAppearInPage(chnEntityRemoved, pageContext) {
							continue
						}
						_, exist := channels[chn]
						if !exist {
							channelVector := channelVectorMap[chn]
							sim := simVector(channelVector, pageContext.vector)
							channelScores[chn] = sim
							channels[chn] = true
						}
					}
				}
			}
		}
	}

	return sortMap(channelScores)
}

func filterChannels(pageContext *PageContext) ([]string, []float64) {
	channels := make(map[string]bool)
	channelSorted := make([]string, 0, len(pageContext.channels))
	scoresSorted := make([]float64, 0, len(pageContext.scChannels))
	for i := 0; i < len(pageContext.channels); i++ {
		//case1: 过滤掉末尾score低于阈值的channel
		//有空格，有大写
		chn := pageContext.channels[i]
		score := pageContext.scChannels[i]
		if score <= scoreThreshold {
			break
		}

		//case2: 过滤掉重复channel
		_, exist := channels[chn]
		if exist {
			continue
		}
		channels[chn] = true

		//case3: 过滤掉黑名单中的channel
		//有空格，无大写
		chnLowerCase := strings.ToLower(chn)
		_, ok := blackList[chnLowerCase]
		if ok {
			continue
		}

		//case4: 过滤掉带entity但page信息里不包含除entity外的所有分词的channel
		//有空格，有大写
		chnEntityRemoved := removeEntity(chn)
		if chnEntityRemoved != chn && !areAllWordsAppearInPageContent(chnEntityRemoved, pageContext) {
			continue
		}

		//case5: 过滤掉互相有包含关系的channel中排序靠后的channel
		//无空格，有大写
		chn = strings.ReplaceAll(chn, " ", "^^")
		overlap := false
		for _, existChannel := range channelSorted {
			if containWholeString(existChannel, chn) || containWholeString(chn, existChannel) {
				overlap = true
				break
			}
		}
		if overlap {
			continue
		}

		channelSorted = append(channelSorted, chn)
		scoresSorted = append(scoresSorted, score)
	}
	return channelSorted, scoresSorted
}

func countKws(words []string) map[string]int64 {
	kws := make(map[string]int64)
	for _, word := range words {
		if len(word) >= 2 {
			count, exist := kws[word]
			if !exist {
				count = 0
			}
			kws[word] = count + 1
		}
	}
	return kws
}

func countBws(words []string) map[string]int64 {
	bws := make(map[string]int64)
	for i := 0; i < len(words) - 1; i++ {
		if len(words[i]) >= 2 && len(words[i + 1]) >= 2 {
			phrase := words[i] + "^^" + words[i + 1]
			count, exist := bws[phrase]
			if !exist {
				count = 0
			}
			bws[phrase] = count + 1
		}
	}
	return bws
}

func countTws(words []string) map[string]int64 {
	tws := make(map[string]int64)
	for i := 0; i < len(words) - 2; i++ {
		if len(words[i]) >= 2 && len(words[i + 1]) >= 2 && len(words[i + 2]) >= 2 {
			phrase := words[i] + "^^" + words[i + 1] + "^^" + words[i + 2]
			count, exist := tws[phrase]
			if !exist {
				count = 0
			}
			tws[phrase] = count + 1
		}
	}
	return tws
}

func countQws(words []string) map[string]int64 {
	qws := make(map[string]int64)
	for i := 0; i < len(words) - 3; i++ {
		if len(words[i]) >= 2 && len(words[i + 1]) >= 2 && len(words[i + 2]) >= 2 && len(words[i + 3]) >= 2 {
			phrase := words[i] + "^^" + words[i + 1] + "^^" + words[i + 2] + "^^" + words[i + 3]
			count, exist := qws[phrase]
			if !exist {
				count = 0
			}
			qws[phrase] = count + 1
		}
	}
	return qws
}

func simVector(x []float64, y []float64) float64 {
	score := 0.0
	if len(x) > 0 && len(y) > 0 {
		d_0 := 0.0
		d_1 := 0.0
		dimension := len(x)
		for i := 0; i < dimension; i++ {
			score += float64(x[i]) * float64(y[i])
			d_0 += float64(x[i])  * float64(x[i])
			d_1 += float64(y[i])  * float64(y[i])
		}
		d_0 = math.Sqrt(d_0)
		d_1 = math.Sqrt(d_1)
		if d_0 != 0.0 && d_1 != 0.0 {
			score /= d_0
			score /= d_1
			return score
		} else {
			return 0.0
		}
	} else {
		return 0.0
	}
}

func isNumber(str string) bool {
	if len(str) == 0 {
		return false
	} else {
		dotNumber := 0
		for _, r := range str {
			if r == '.' {
				dotNumber++
				if dotNumber >= 2 {
					return false
				}
			} else if !unicode.IsNumber(r) {
				return false;
			}
		}
		return true;
	}
}

func removeEntity(input string) string {
	words := strings.Split(input, " ")
	result := ""
	for _, word := range words {
		_, ok := entitySuffix[word]
		if !ok {
			if len(result) > 0 {
				result += " "
			}
			result += word
		}
	}
	return result
}

func areAllWordsAppearInPage(input string, pageContext *PageContext) bool {
	if len(input) == 0 || pageContext == nil {
		return false
	}
	words := strings.Split(input, " ")
	for _, word := range words {
		if getTitleTermFrequency(word, pageContext) == 0 && getContentTermFrequency(word, pageContext) == 0 && getTitleTermFrequency(strings.ToLower(word), pageContext) == 0 && getContentTermFrequency(strings.ToLower(word), pageContext) == 0 {
			return false
		}
	}
	return true
}

func areAllWordsAppearInPageContent(input string, pageContext *PageContext) bool {
	if len(input) == 0 || pageContext == nil {
		return false
	}
	words := strings.Split(input, " ")
	for _, word := range words {
		if getContentTermFrequency(word, pageContext) == 0 {
			return false
		}
	}
	return true
}

func getTitleTermFrequency(term string, pageContext *PageContext) int64 {
	frequencyInKws, ok := pageContext.titleKws[term]
	if !ok {
		frequencyInKws = 0
	}
	frequencyInBws, ok := pageContext.titleBws[term]
	if !ok {
		frequencyInBws = 0
	}
	return frequencyInKws + frequencyInBws
}

func getContentTermFrequency(term string, pageContext *PageContext) int64 {
	frequencyInKws, ok := pageContext.contentKws[term]
	if !ok {
		frequencyInKws = 0
	}
	frequencyInBws, ok := pageContext.contentBws[term]
	if !ok {
		frequencyInBws = 0
	}
	return frequencyInKws + frequencyInBws
}

// 按value降序
func sortMap(paramMap map[string]float64) ([]string, []float64) {
	keys := make([]string, 0, len(paramMap))
	values := make([]float64, 0, len(paramMap))
	for key, _ := range paramMap {
		keys = append(keys, key)
	}
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if paramMap[keys[i]] < paramMap[keys[j]] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	for _, key := range keys {
		values = append(values, paramMap[key])
	}
	return keys, values
}

func containWholeString(str0 string, str1 string) bool {
	if len(str0) == 0 {
		return false
	}
	if len(str1) == 0 {
		return true
	}
	result := containWholeLowerString(strings.ToLower(str0), strings.ToLower(str1))
	return result
}

func containWholeLowerString(str0 string, str1 string) bool {
	if len(str0) == 0 {
		return false
	} else if len(str1) == 0 {
		return true
	} else {
		offset := 0
		for {
			newOffset := strings.Index(str0[offset :], str1)
			if newOffset < 0 {
				break
			}
			offset += newOffset
			leftIsBoundary := offset == 0
			if !leftIsBoundary {
				c := str0[offset - 1]
				leftIsBoundary = isDelimiter(c)
				if !leftIsBoundary {
					c = str1[0]
					leftIsBoundary = isDelimiter(c)
				}
			}
			if !leftIsBoundary {
				offset++
			} else {
				rightOffset := offset + len(str1) - 1
				rightIsBoundary := rightOffset + 1 == len(str0)
				if !rightIsBoundary {
					c := str0[rightOffset + 1]
					rightIsBoundary = isDelimiter(c)
					if !rightIsBoundary {
						c = str1[len(str1) - 1]
						rightIsBoundary = isDelimiter(c)
					}
				}
				if rightIsBoundary {
					return true
				}
				offset++
			}
			if offset >= len(str0) {
				break
			}
		}
		return false
	}
}

func isDelimiter(c uint8) bool {
	if isSpaceChar(c) {
		return true
	} else if c == '^' {
		return true
	} else {
		for _, d := range delimiters {
			if c == d {
				return true
			}
		}
		return false
	}
}

func isSpaceChar(c uint8) bool {
	return c == 12 || c == 13 || c == 14
}

func setChannelToMongo(key string, value *PageChn, conf *common.Config) error {
	timeout := time.Duration(conf.MongoConf.Timeout) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, insertErr := collectionMap[conf.ChnConf.Collection].InsertOne(ctx, *value)
	if insertErr != nil {
		// replace when exist
		replaceErr := replaceChannelToMongo(key, value, conf)
		return replaceErr
	}
	return nil
}

func replaceChannelToMongo(key string, value *PageChn, conf *common.Config) error {
	timeout := time.Duration(conf.MongoConf.Timeout) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	filter := bson.M{"_id": key}
	_, replaceErr := collectionMap[conf.ChnConf.Collection].ReplaceOne(ctx, filter, *value)
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
