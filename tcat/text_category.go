package tcat

import (
	"bytes"
	"encoding/json"
	"github.com/ParticleMedia/fb_page_tcat/common"
	"github.com/ParticleMedia/fb_page_tcat/storage"
	"github.com/golang/glog"
	"io/ioutil"
	"net/http"
	"strings"
)

func DoTextCategory(page *common.FBPage, conf *common.Config) (*common.TextCategoryBody, error) {
	pageTcat, getErr := storage.GetFromMongo(page.Id, &conf.MongoConf)
	if getErr == nil && pageTcat != nil && pageTcat.Tcat != nil {
		firstCats, ok := pageTcat.Tcat["first_cat"]
		if !ok {
			firstCats = make(map[string]float64)
		}
		secondCats, ok := pageTcat.Tcat["second_cat"]
		if !ok {
			secondCats = make(map[string]float64)
		}
		thirdCats, ok := pageTcat.Tcat["third_cat"]
		if !ok {
			thirdCats = make(map[string]float64)
		}
		if len(firstCats) != 0 || len(secondCats) != 0 || len(thirdCats) != 0 {
			tcats := common.TextCategory{
				FirstCats:  firstCats,
				SecondCats: secondCats,
				ThirdCats:  thirdCats,
			}
			tcat := common.TextCategoryBody{
				Tcats: tcats,
			}
			return &tcat, nil
		}
	}

	bodyMap := map[string]string{
		"id": page.Id,
		"seg_title": page.Name,
		"seg_content": strings.ReplaceAll(page.About, "\n", ""),
	}
	body, encodeErr := json.Marshal(bodyMap)
	if encodeErr != nil {
		return nil, encodeErr
	}

	resp, respErr := http.Post(conf.TcatConf.Uri, conf.TcatConf.ContentType, bytes.NewBuffer(body))
	if respErr != nil {
		return nil, respErr
	}

	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, nil
	}

	respBody, readErr := ioutil.ReadAll(resp.Body)
	if readErr != nil {
		return nil, readErr
	}

	var tcat common.TextCategoryBody
	parseErr := json.Unmarshal(respBody, &tcat)
	if parseErr != nil {
		return nil, parseErr
	}

	tcats := make(map[string]map[string]float64)
	tcats["first_cat"] = tcat.Tcats.FirstCats
	tcats["second_cat"] = tcat.Tcats.SecondCats
	tcats["third_cat"] = tcat.Tcats.ThirdCats
	pageTcat = &common.PageTcat{
		Id: page.Id,
		Tcat: tcats,
	}
	setErr := storage.SetToMongo(pageTcat.Id, pageTcat, &conf.MongoConf)
	if setErr != nil {
		glog.Warning("set to mongo with error: %v, value: %+v", setErr, *pageTcat)
	}

	return &tcat, nil
}