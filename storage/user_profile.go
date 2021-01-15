package storage

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"github.com/ParticleMedia/fb_page_tcat/common"
	user_profile_pb "github.com/ParticleMedia/fb_page_tcat/proto"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

var valueType user_profile_pb.ProfileValueType

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
