package remote

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ParticleMedia/fb_page_tcat/common"
	user_profile_pb "github.com/ParticleMedia/fb_page_tcat/proto"
	"github.com/golang/glog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/grpc"
	"io/ioutil"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var mongoClient *mongo.Client
var collection *mongo.Collection
var valueType user_profile_pb.ProfileValueType

type FBProfile struct {
	Id    int32    `json:"_id"`
	Pages []FBPage `json:"likes"`
}

type FBPage struct {
	Id       string `json:"_id"`
	Name     string `json:"name"`
	About    string `json:"about"`
	Category string `json:"category"`
}

type PageTcat struct {
	Id   string                        `bson:"_id"`
	Tcat map[string]map[string]float64 `bson:"text_category"`
}

type TextCategoryBody struct {
	Tcats TextCategory `json:"text_category"`
}

type TextCategory struct {
	FirstCats  map[string]float64 `json:"first_cat"`
	SecondCats map[string]float64 `json:"second_cat"`
	ThirdCats  map[string]float64 `json:"third_cat"`
}

func DoTextCategory(page *FBPage, conf *common.Config) (*TextCategoryBody, error) {
	pageTcat, getErr := getFromMongo(page.Id, &conf.MongoConf)
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
			tcats := TextCategory{
				FirstCats:  firstCats,
				SecondCats: secondCats,
				ThirdCats:  thirdCats,
			}
			tcat := TextCategoryBody{
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

	var tcat TextCategoryBody
	parseErr := json.Unmarshal(respBody, &tcat)
	if parseErr != nil {
		return nil, parseErr
	}

	tcats := make(map[string]map[string]float64)
	tcats["first_cat"] = tcat.Tcats.FirstCats
	tcats["second_cat"] = tcat.Tcats.SecondCats
	tcats["third_cat"] = tcat.Tcats.ThirdCats
	pageTcat = &PageTcat{
		Id: page.Id,
		Tcat: tcats,
	}
	setErr := setToMongo(pageTcat.Id, pageTcat, &conf.MongoConf)
	if setErr != nil {
		glog.Warning("set to mongo with error: %v, value: %+v", setErr, *pageTcat)
	}

	return &tcat, nil
}

func BuildMongoClient(conf *common.MongoConfig) error {
	timeout := time.Duration(conf.Timeout) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	uri := fmt.Sprintf("mongodb://%s/?replicaSet=%s&authSource=admin&authMechanism=SCRAM-SHA-1&readPreference=secondaryPreferred", conf.Addr, conf.ReplicaSet)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetConnectTimeout(timeout))
	if err != nil {
		return err
	}

	mongoClient = client
	collection = mongoClient.Database(conf.Database).Collection(conf.Collection)
	return err
}

func setToMongo(key string, value *PageTcat, conf *common.MongoConfig) (error) {
	timeout := time.Duration(conf.Timeout) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, insertErr := collection.InsertOne(ctx, *value)
	if insertErr != nil {
		// replace when exist
		replaceErr := replaceToMongo(key, value, conf)
		return replaceErr
	}
	return nil
}

func replaceToMongo(key string, value *PageTcat, conf *common.MongoConfig) (error) {
	timeout := time.Duration(conf.Timeout) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	filter := bson.M{"_id": key}
	_, replaceErr := collection.ReplaceOne(ctx, filter, *value)
	return replaceErr
}

func getFromMongo(key string, conf *common.MongoConfig) (*PageTcat, error) {
	timeout := time.Duration(conf.Timeout) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	filter := bson.M{"_id": key}
	result := collection.FindOne(ctx, filter)
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

func MongoDisconnect() error {
	disConnErr := mongoClient.Disconnect(context.Background())
	return disConnErr
}

func fromString(data string, valueType user_profile_pb.ProfileValueType) (*user_profile_pb.ProfileValue, error) {
	if len(data) == 0 {
		return nil, nil
	}
	value := &user_profile_pb.ProfileValue{
		Type: valueType,
	}
	switch valueType {
	case user_profile_pb.ProfileValueType_STRING:
		{
			value.Value = &user_profile_pb.ProfileValue_StrValue{
				StrValue: data,
			}
		}
	case user_profile_pb.ProfileValueType_RAW_BYTES:
		{
			byteValue, err := base64.StdEncoding.DecodeString(data)
			if err != nil {
				return nil, err
			}
			value.Value = &user_profile_pb.ProfileValue_BytesValue{
				BytesValue: byteValue,
			}
		}
	case user_profile_pb.ProfileValueType_INT:
		{
			intValue, err := strconv.ParseInt(data, 0, 64)
			if err != nil {
				return nil, err
			}
			value.Value = &user_profile_pb.ProfileValue_IntValue{
				IntValue: intValue,
			}
		}
	case user_profile_pb.ProfileValueType_FLOAT:
		{
			floatValue, err := strconv.ParseFloat(data, 64)
			if err != nil {
				return nil, err
			}
			value.Value = &user_profile_pb.ProfileValue_FloatValue{
				FloatValue: floatValue,
			}
		}
	case user_profile_pb.ProfileValueType_LIST:
		{
			var stringList []string
			err := json.Unmarshal([]byte(data), &stringList)
			if err != nil {
				return nil, err
			}
			value.Value  = &user_profile_pb.ProfileValue_ListValue{
				ListValue: &user_profile_pb.StringList{
					List: stringList,
				},
			}
		}
	default:
		{
			return nil, errors.New("unsupported value type")
		}
	}
	return value, nil
}

func SetValueType(conf *common.UserProfileConfig) {
	intType, _ := user_profile_pb.ProfileValueType_value[strings.ToUpper(conf.Format)]
	valueType = user_profile_pb.ProfileValueType(intType)
}

func WriteToUps(key uint64, value string, conf *common.UserProfileConfig) {
	conn, dialErr := grpc.Dial(conf.Addr, grpc.WithInsecure())
	if dialErr != nil {
		glog.Warningf("ups connect to %s with error: %+v", conf.Addr, dialErr)
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(conf.Timeout) * time.Millisecond)
	defer cancel()

	logid := rand.Int63()
	c := user_profile_pb.NewUserProfileServiceClient(conn)
	pbValue, valErr := fromString(value, valueType)
	if valErr != nil {
		glog.Warning("ups format with error: %+v, key: %d", valErr, key)
		return
	}
	resp, setErr := c.Set(ctx, &user_profile_pb.SetRequest{
		LogId: logid,
		From: conf.ReqFrom,
		Profile: &user_profile_pb.UserProfile{
			Uid: key,
			ProfileList: []*user_profile_pb.ProfileItem{
				{
					Id: &user_profile_pb.ProfileIdentity{
						Name: conf.Profile,
						Version: uint32(conf.Version),
					},
					Value: pbValue,
				},
			},
		},
		DisableCache: conf.DisableCache,
	})

	if setErr != nil || resp.Status != 0 {
		glog.Warningf("ups set with error: %v, key: %d", setErr, key)
	}
}
