package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/ttacon/streamer"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoSource struct {
	Name       string
	URI        string
	Database   string
	Mode       string
	Collection string
}

type StdOutSink struct {
	Pretty bool
	Name   string
}

var (
	configFileLoc = flag.String("config", "", "config file location")
)

type Config struct {
	MongoSources []MongoSource
	StdOutSinks  []StdOutSink

	Connections []Connection
}

type Connection struct {
	Name   string
	Source string
	Sink   string
}

func main() {
	flag.Parse()

	var config Config
	data, err := ioutil.ReadFile(*configFileLoc)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	if err := json.Unmarshal(data, &config); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	var sources = make(map[string]streamer.Source)
	var sinks = make(map[string]streamer.Sink)

	if err := initEngine(sources, sinks, &config); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	for _, connection := range config.Connections {
		fmt.Println("starting connection: ", connection.Name)
		source, foundSource := sources[connection.Source]
		if !foundSource {
			fmt.Printf("no such source %q\n", connection.Source)
			os.Exit(1)
		}
		sink, foundSink := sinks[connection.Sink]
		if !foundSink {
			fmt.Printf("no such sink %q\n", connection.Sink)
			os.Exit(1)
		}
		if err := streamer.Connect(source, sink); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	wait := make(chan bool)
	<-wait
}

func initEngine(sources map[string]streamer.Source, sinks map[string]streamer.Sink, config *Config) error {
	for _, mongoSource := range config.MongoSources {
		fmt.Println("starting source: ", mongoSource.Name)
		client, err := mongo.NewClient(
			options.Client().
				ApplyURI(mongoSource.URI),
		)
		if err != nil {
			return err
		}

		ctx, _ := context.WithTimeout(context.Background(), 20*time.Second)
		if err := client.Connect(ctx); err != nil {
			return err
		}

		db := client.Database(mongoSource.Database)
		source := streamer.NewMongoSource(db)
		pipeline := MongoSourceModes[mongoSource.Mode]
		if pipeline == nil {
			pipeline = streamer.MongoInsertsOnly
		}
		if err := source.WatchCollection(mongoSource.Collection, pipeline); err != nil {
			return err
		}
		sources[mongoSource.Name] = source
	}

	for _, stdOutSink := range config.StdOutSinks {
		fmt.Println("starting sink: ", stdOutSink.Name)
		sinks[stdOutSink.Name] = streamer.NewStdOutSink()
	}

	return nil
}

var (
	MongoSourceModes = map[string]interface{}{
		"insert-only": streamer.MongoInsertsOnly,
	}
)
