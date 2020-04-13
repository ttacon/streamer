streamer
========

This is a very WIP project. Basic usage:

## Installation
```sh
# Assuming $GOBIN or $GOPATH/bin is on your $PATH...
go get github.com/ttacon/streamer/engine

engine -config=config.json
```

## Example

See an example config here:

```json
{
  "MongoSources": [{
    "URI": "mongodb://localhost:27012",
    "Database": "my_db",
    "Collection": "my_collection",
    "Name": "mongo-test"
  }],
  "StdOutSinks": [{
    "Name": "yolo"
  }],
  "Connections": [{
    "Name": "test-connection",
    "Source": "mongo-test",
    "Sink": "yolo"
  }]
}
```


## Notes

 - Currently, the Mongo DB must be a replicaset.
