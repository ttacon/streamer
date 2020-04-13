package streamer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/ttacon/pretty"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Source interface {
	Start(sender chan<- interface{}) error
}

type Sink interface {
	Send(i interface{}) error
}

type MongoSource struct {
	Streams []*mongo.ChangeStream

	db *mongo.Database
}

func NewMongoSource(db *mongo.Database) *MongoSource {
	return &MongoSource{
		db: db,
	}
}

func (m *MongoSource) WatchCollection(collectionName string, pipeline interface{}, opts ...*options.ChangeStreamOptions) error {
	coll := m.db.Collection(collectionName)

	changeStream, err := coll.Watch(context.Background(), pipeline, opts...)
	if err != nil {
		return err
	}

	m.Streams = append(m.Streams, changeStream)

	return nil
}

func (m *MongoSource) Start(sender chan<- interface{}) error {
	for _, stream := range m.Streams {
		go func(stream *mongo.ChangeStream) {
			for stream.Next(context.TODO()) {
				var m = make(map[string]interface{})
				if err := bson.Unmarshal(stream.Current, &m); err != nil {
					// TODO(ttacon): Pass errors back
					return
				}
				sender <- m
			}
		}(stream)
	}
	return nil
}

// Desired sinks:
//    - Elasticsearch
//    - Webhook
//    - Kafka
//    - SNS
//    - SQS

func NewWebhookProducer(method string, path string) Sink {
	return &WebhookProducer{
		Method: method,
		Path:   path,
	}
}

type WebhookProducer struct {
	Method string
	Path   string
}

func (w *WebhookProducer) Send(i interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	buf := bytes.NewBuffer(nil)
	if err := json.NewEncoder(buf).Encode(i); err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, w.Method, w.Path, buf)
	if err != nil {
		return err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if resp.Body != nil {
		return resp.Body.Close()
	}

	return nil
}

func Connect(source Source, sink Sink) error {
	intermediate := make(chan interface{}, 5)
	if err := source.Start(intermediate); err != nil {
		return err
	}

	wp := workerpool.New(5)
	go func() {
		for val := range intermediate {
			wp.Submit(func() {
				if err := sink.Send(val); err != nil {
					fmt.Println(err)
				}
			})
		}
	}()
	return nil
}

type stdOutSink struct{}

func NewStdOutSink() Sink {
	return stdOutSink{}
}

func (s stdOutSink) Send(i interface{}) error {
	pretty.Println(i)
	return nil
}

var (
	MongoInsertsOnly = mongo.Pipeline{bson.D{{"$match", bson.D{{"operationType", "insert"}}}}}
)
