package storage

import "C"
import (
	"context"
	"fmt"
	"github.com/ParticleMedia/fb_page_tcat/common"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

var mongoClient *mongo.Client
var Collection *mongo.Collection

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
	Collection = mongoClient.Database(conf.Database).Collection(conf.Collection)
	return err
}

func SetToMongo(key string, value *common.PageTcat, conf *common.MongoConfig) (error) {
	timeout := time.Duration(conf.Timeout) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	_, insertErr := Collection.InsertOne(ctx, *value)
	if insertErr != nil {
		// replace when exist
		replaceErr := replaceToMongo(key, value, conf)
		return replaceErr
	}
	return nil
}

func replaceToMongo(key string, value *common.PageTcat, conf *common.MongoConfig) (error) {
	timeout := time.Duration(conf.Timeout) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	filter := bson.M{"_id": key}
	_, replaceErr := Collection.ReplaceOne(ctx, filter, *value)
	return replaceErr
}

func GetFromMongo(key string, conf *common.MongoConfig) (*common.PageTcat, error) {
	timeout := time.Duration(conf.Timeout) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	filter := bson.M{"_id": key}
	result := Collection.FindOne(ctx, filter)
	if result.Err() != nil {
		return nil, result.Err()
	}

	raw, decodeErr := result.DecodeBytes()
	if decodeErr != nil {
		return nil, decodeErr
	}

	var value common.PageTcat
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