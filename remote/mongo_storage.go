package remote

import (
	"context"
	"fmt"
	"github.com/ParticleMedia/fb_page_tcat/common"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

var mongoClient *mongo.Client
var collectionMap map[string]*mongo.Collection

func BuildMongoCollections(conf *common.MongoConfig) error {
	timeout := time.Duration(conf.Timeout) * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	uri := fmt.Sprintf("mongodb://%s/?replicaSet=%s&authSource=admin&authMechanism=SCRAM-SHA-1&readPreference=secondaryPreferred", conf.Addr, conf.ReplicaSet)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetConnectTimeout(timeout))
	if err != nil {
		return err
	}

	mongoClient = client
	collectionMap = make(map[string]*mongo.Collection)
	for _, name := range conf.Collection {
		collectionMap[name] = mongoClient.Database(conf.Database).Collection(name)
	}
	return err
}

func MongoDisconnect() error {
	disConnErr := mongoClient.Disconnect(context.Background())
	return disConnErr
}
